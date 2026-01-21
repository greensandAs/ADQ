USE DATABASE {{ snowflake_database }};

USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "EXECUTE_DQ_RULES_MASTER"("P_DATASET_ID" NUMBER(38,0), "P_PARALLEL_JOBS" NUMBER(38,0) DEFAULT 2)
RETURNS NUMBER(38,0)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python==*','joblib')
HANDLER = 'main'
EXECUTE AS CALLER
AS '
import snowflake.snowpark as snowpark
import datetime
import joblib
import json
import re
from json.decoder import JSONDecodeError

def main(session: snowpark.Session, P_DATASET_ID, P_PARALLEL_JOBS):

    # --- Helper to quote SQL identifiers ---
    def q_ident(name: str) -> str:
        """Quotes an identifier for safe SQL use."""
        if name is None: return "NULL"
        name = name.strip(''"'') 
        return ''"'' + name.replace(''"'', ''""'') + ''"''

    # --- Fetch dynamic location ---
    FRAMEWORK_DB = q_ident(session.get_current_database())
    FRAMEWORK_SCHEMA = q_ident(session.get_current_schema())
    FRAMEWORK_PATH = f"{FRAMEWORK_DB}.{FRAMEWORK_SCHEMA}"

    # --- Define Stage (Ensure @ is present for file operations) ---
    DQ_STAGE_NAME = f"@{FRAMEWORK_PATH}.DQ_FAILURES_STAGE" 

    def parallel_worker(worker_session, rule_config, config):
        """Execute a single SQL DQ rule and return its execution status and audit data."""
        start_time = now()
        
        try:
            sp_name = rule_config.get("SP_NAME")
            if not sp_name:
                end_time = now()
                return {
                    "status": "ERROR",
                    "code": config.get("EXECUTION_ERROR", 400),
                    "audit_log": {
                        "rule_config_id": rule_config.get("RULE_CONFIG_ID"),
                        "status": "ERROR",
                        "start_time": start_time,
                        "end_time": end_time,
                        "duration": (end_time - start_time).total_seconds(),
                        "error_message": f"Missing SP_NAME for rule {rule_config.get(''RULE_CONFIG_ID'')}"
                    }
                }
                
            sp_path = f"{FRAMEWORK_PATH}.{q_ident(sp_name)}"
            result = worker_session.call(sp_path, rule_config)
            
            end_time = now()
            result_status = "SUCCESS" if str(result) == str(config["SUCCESS_CODE"]) else "FAILURE" if str(result) == str(config["FAILED_CODE"]) else "ERROR"
            
            return {
                "status": result_status,
                "code": result,
                "audit_log": {
                    "rule_config_id": rule_config.get("RULE_CONFIG_ID")
                }
            }
        except Exception as e:
            end_time = now()
            return {
                "status": "ERROR",
                "code": config.get("EXECUTION_ERROR", 400),
                "audit_log": {
                    "rule_config_id": rule_config.get("RULE_CONFIG_ID"),
                    "status": "ERROR",
                    "start_time": start_time,
                    "end_time": end_time,
                    "duration": (end_time - start_time).total_seconds(),
                    "error_message": f"Error during call to {sp_name}: {str(e)}"
                }
            }

    def format_for_sql(val, is_string=False):
        if val is None:
            return "NULL"
        if isinstance(val, datetime.datetime):
            val = val.strftime("%Y-%m-%d %H:%M:%S.%f")
            is_string = True
        if is_string:
            escaped_val = str(val).replace("''", "''''")
            return f"''{escaped_val}''"
        return str(val)

    def log_and_return(session, config, master_audit_logs, return_code):
        audit_log_path = f"{config[''DQ_DB_NAME'']}.{config[''DQ_SCHEMA_NAME'']}.DQ_RULE_AUDIT_LOG"
        if master_audit_logs:
            values_list = []
            for log in master_audit_logs:
                run_id_val = format_for_sql(log[0])
                proc_name = format_for_sql(log[1], is_string=True)
                step_name = format_for_sql(log[2], is_string=True)
                log_msg = format_for_sql(log[3], is_string=True)
                start_ts = format_for_sql(log[4], is_string=True)
                end_ts = format_for_sql(log[5], is_string=True)
                status = format_for_sql(log[6], is_string=True)
                error_message = format_for_sql(log[7], is_string=True)
                values_list.append(f"({run_id_val}, {proc_name}, {step_name}, {log_msg}, {start_ts}, {end_ts}, {status}, {error_message})")
            
            insert_sql = f"""
                INSERT INTO {audit_log_path}
                (DATASET_RUN_ID, PROCEDURE_NAME, STEP_NAME, LOG_MESSAGE, START_TIMESTAMP, END_TIMESTAMP, STATUS, ERROR_MESSAGE)
                VALUES {", ".join(values_list)}
            """
            session.sql(insert_sql).collect()
        return return_code

    master_start_time = now()
    master_audit_logs = []
    config = {}
    
    run_id_seq_path = f"{FRAMEWORK_PATH}.RUN_ID_SEQ.NEXTVAL"
    run_id = session.sql(f"SELECT {run_id_seq_path}").collect()[0][0]

    try:
        step_start_time = now()
        config_table_path = f"{FRAMEWORK_PATH}.DQ_JOB_EXEC_CONFIG"
        
        config_sql = f"""
            SELECT SUCCESS_CODE, FAILED_CODE, EXECUTION_ERROR, DQ_DB_NAME, DQ_SCHEMA_NAME
            FROM {config_table_path} LIMIT 1
        """
        config_result = session.sql(config_sql).collect()
        step_end_time = now()
        
        if not config_result:
            config = {
                "DQ_DB_NAME": FRAMEWORK_DB, 
                "DQ_SCHEMA_NAME": FRAMEWORK_SCHEMA, 
                "EXECUTION_ERROR": 404
            }
            master_audit_logs.append((0, "EXECUTE_DQ_RULES_MASTER", "CONFIG_LOADING", "Configuration not found in DQ_JOB_EXEC_CONFIG", step_start_time, step_end_time, "FAILED", "Configuration not found in DQ_JOB_EXEC_CONFIG"))
            return log_and_return(session, config, master_audit_logs, 404)
            
        config = config_result[0].as_dict()
        master_audit_logs.append((run_id, "EXECUTE_DQ_RULES_MASTER", "CONFIG_LOADING","Captured the configuartion details" ,step_start_time, step_end_time, "COMPLETED", None))
    except Exception as e:
        config = {
            "DQ_DB_NAME": FRAMEWORK_DB, 
            "DQ_SCHEMA_NAME": FRAMEWORK_SCHEMA, 
            "EXECUTION_ERROR": 404
        }
        master_audit_logs.append((0, "EXECUTE_DQ_RULES_MASTER", "CONFIG_LOADING", f"Failed to retrieve config: {str(e)}", step_start_time, now(), "FAILED", f"Failed to retrieve config: {str(e)}"))
        return log_and_return(session, config, master_audit_logs, 404)

    parallel_jobs = max(1, P_PARALLEL_JOBS or 2)

    step_start_time = now()
    try:
        dataset_table_path = f"{FRAMEWORK_PATH}.DQ_DATASET"
        data_asset_df = session.sql(f"SELECT DATASET_ID, DATASET_NAME, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, DATASET_TYPE, CUSTOM_SQL FROM {dataset_table_path} WHERE DATASET_ID = {P_DATASET_ID}").collect()
        step_end_time = now()
        if not data_asset_df:
            master_audit_logs.append((run_id, "EXECUTE_DQ_RULES_MASTER", "VALIDATE DATASET", f"Dataset ID {P_DATASET_ID} not found.", step_start_time, step_end_time, "FAILED", f"Dataset ID {P_DATASET_ID} not found."))
            return log_and_return(session, config, master_audit_logs, int(config["EXECUTION_ERROR"]))
        data_asset = data_asset_df[0].as_dict()
        db_name, schema_name, table_name, dataset_type, custom_sql = data_asset["DATABASE_NAME"], data_asset["SCHEMA_NAME"], data_asset["TABLE_NAME"], data_asset["DATASET_TYPE"], data_asset["CUSTOM_SQL"]
        master_audit_logs.append((run_id, "EXECUTE_DQ_RULES_MASTER", "VALIDATE DATASET","Captured the dataset details", step_start_time, step_end_time, "COMPLETED", None))
    except Exception as e:
        master_audit_logs.append((run_id, "EXECUTE_DQ_RULES_MASTER", "VALIDATE DATASET", f"Error validating dataset: {str(e)}", step_start_time, now(), "FAILED", f"Error validating dataset: {str(e)}"))
        return log_and_return(session, config, master_audit_logs, int(config["EXECUTION_ERROR"]))
    
    step_start_time = now()
    try:
        rules_config_path = f"{FRAMEWORK_PATH}.DQ_RULE_CONFIG"
        exp_master_path = f"{FRAMEWORK_PATH}.DQ_EXPECTATION_MASTER"
        handler_map_path = f"{FRAMEWORK_PATH}.DQ_EXPECTATION_HANDLER_MAPPING"
        
        # Updated SQL to remove GE specific checks, assuming SQL engine is the default or filtering explicitly for SQL
        rules_sql = f"""
            SELECT cc.RULE_CONFIG_ID, cc.CHECK_TYPE, COALESCE(cc.COLUMN_NAME, '''') AS COLUMN_NAME, COALESCE(cc.EXPECTATION_ID, ''0'') AS EXPECTATION_ID, cc.EXPECTATION_NAME, cc.KWARGS, cc.DQ_ENGINE, cc.DIMENSION, ehm.SP_NAME 
            FROM {rules_config_path} cc 
            LEFT JOIN {exp_master_path} em ON cc.expectation_id = em.expectation_id 
            LEFT JOIN {handler_map_path} ehm ON UPPER(em.VALIDATION_NAME) = UPPER(ehm.EXPECTATION_TYPE) AND ehm.IS_ACTIVE = TRUE 
            WHERE cc.DATASET_ID = {data_asset[''DATASET_ID'']} AND SP_NAME IS NOT NULL AND cc.IS_ACTIVE = TRUE AND (cc.DQ_ENGINE IS NULL OR UPPER(cc.DQ_ENGINE) = ''SQL'')
        """
        rules_df = session.sql(rules_sql).to_pandas()
        step_end_time = now()
    except Exception as e:
        master_audit_logs.append((run_id, "EXECUTE_DQ_RULES_MASTER", "RULES_FETCHING", f"Error fetching rules: {str(e)}", step_start_time, now(), "FAILED", f"Error fetching rules: {str(e)}"))
        return log_and_return(session, config, master_audit_logs, int(config["EXECUTION_ERROR"]))
    
    if rules_df.empty:
        master_audit_logs.append((run_id, "EXECUTE_DQ_RULES_MASTER", "RULES_FETCHING", f"No SQL rules found for dataset_id {P_DATASET_ID}", step_start_time, step_end_time, "FAILED", f"No SQL rules found for dataset_id {P_DATASET_ID}"))
        return log_and_return(session, config, master_audit_logs, int(config["EXECUTION_ERROR"]))
    
    master_audit_logs.append((run_id, "EXECUTE_DQ_RULES_MASTER", "RULES_FETCHING", f"Captured {len(rules_df)} SQL rules for dataset_id {P_DATASET_ID}", step_start_time, step_end_time, "COMPLETED", None))

    success_count, failure_count, error_count, total_rules = 0, 0, 0, 0
    error_rule_ids = []
    
    sql_rules_to_run = rules_df
    
    if not sql_rules_to_run.empty:
        step_start_time = now()
        rule_configs = []
        for _, rule in sql_rules_to_run.iterrows():
            kwargs_str, kwargs_obj = rule["KWARGS"], {}
            if kwargs_str:
                try: kwargs_obj = json.loads(kwargs_str)
                except JSONDecodeError as e:
                    master_audit_logs.append((run_id, "EXECUTE_DQ_RULES_MASTER", "KWARGS_PARSING", f"JSON parsing failed for rule {rule[''RULE_CONFIG_ID'']}: {str(e)}", now(), now(), "FAILED", f"JSON parsing failed for rule {rule[''RULE_CONFIG_ID'']}: {str(e)}"))
                    error_count += 1
                    continue
            rule_configs.append({
                "DATASET_RUN_ID": run_id, "DATASET_ID": data_asset["DATASET_ID"], "RULE_CONFIG_ID": int(rule["RULE_CONFIG_ID"]), "EXPECTATION_ID": rule["EXPECTATION_ID"], "DATABASE_NAME": db_name, "SCHEMA_NAME": schema_name, "TABLE_NAME": table_name, "COLUMN_NAME": rule["COLUMN_NAME"], "EXPECTATION_NAME": rule["EXPECTATION_NAME"], "RUN_NAME": f"DQ_RUN_{run_id}", "DATASET_NAME": data_asset["DATASET_NAME"], "KWARGS": kwargs_obj, "SP_NAME": rule["SP_NAME"], "DATASET_TYPE": dataset_type, "CUSTOM_SQL": custom_sql, "DIMENSION":rule["DIMENSION"]
            })

        if rule_configs:
            if P_PARALLEL_JOBS is None or P_PARALLEL_JOBS <= 0:
                parallel_jobs = len(rule_configs)
            else:
                parallel_jobs = P_PARALLEL_JOBS
                
            parallel_jobs = min(parallel_jobs, 30)
            
            results_from_parallel_execution = joblib.Parallel(n_jobs=parallel_jobs)(joblib.delayed(parallel_worker)(session, rc, config) for rc in rule_configs)

            failed_rule_ids = []
            
            for result in results_from_parallel_execution:
                total_rules += 1
                if result["status"] == "SUCCESS":
                    success_count += 1
                elif result["status"] == "FAILURE":
                    failure_count += 1
                    if "audit_log" in result and "rule_config_id" in result["audit_log"]:
                        failed_rule_ids.append(result["audit_log"]["rule_config_id"])
                else: 
                    error_count += 1
                    if "audit_log" in result and "rule_config_id" in result["audit_log"]:
                        error_rule_ids.append(result["audit_log"]["rule_config_id"])
        
        step_end_time = now()
        
        if error_count > 0:
            error_ids_str = ", ".join(map(str, error_rule_ids))
            log_message = f"Rule have Execution error, Please check step of invidulal Rules"
            log_status = "ERROR"
            error_message = f"Execution completed with Execution errors for dataset_id {P_DATASET_ID}. {error_count} rule(s) failed with an error. Errored Rules: {error_ids_str}."
        elif failure_count > 0 and success_count > 0:
            failed_ids_str = ", ".join(map(str, failed_rule_ids))
            log_message = f"Execution completed with partial success for dataset_id {P_DATASET_ID}. {success_count} rule(s) succeeded and {failure_count} rule(s) failed. Failed Rules: {failed_ids_str}."
            log_status = "PARTIAL_SUCCESS"
            error_message = None
        elif success_count > 0 and failure_count == 0 and error_count == 0:
            log_message = f"All {success_count} rules for dataset_id {P_DATASET_ID} executed successfully."
            log_status = "SUCCESS"
            error_message = None
        else: 
            failed_ids_str = ", ".join(map(str, failed_rule_ids))
            log_message = f"All {failure_count} rules for dataset_id {P_DATASET_ID} failed. Failed Rules: {failed_ids_str}."
            log_status = "FAILED"
            error_message = None
        
        master_audit_logs.append((run_id, "EXECUTE_DQ_RULES_MASTER", "SQL_EXECUTION", log_message, step_start_time, step_end_time, log_status, error_message))

    # =========================================================================
    # DATASET-LEVEL CONSOLIDATED FILE GENERATION
    # =========================================================================
    generated_file_url = "NULL"
    file_generation_error = False  # Flag to track file generation issues
    
    if failure_count > 0:
        step_start_time = now()
        try:
            # 1. Construct the standard failure table name based on Dataset Name
            clean_dataset_name = re.sub(r''[^a-zA-Z0-9]'', ''_'', data_asset["DATASET_NAME"])
            failure_table_name = f"{q_ident(config[''DQ_DB_NAME''])}.{q_ident(config[''DQ_SCHEMA_NAME''])}.{clean_dataset_name}_DQ_FAILURE"
            
            # 2. Define File Name and Stage Path
            export_file_name = f"FAILED_ROWS_{clean_dataset_name}_{run_id}.csv.gz"
            export_file_path = f"dataset_failures/{export_file_name}"
            
            # 3. Aggregated COPY INTO (NO LIMIT)
            # CAUTION: Removing Limit on SINGLE=TRUE file generation.
            
            copy_sql = f"""
                COPY INTO {DQ_STAGE_NAME}/{export_file_path}
                FROM (
                    SELECT * FROM {failure_table_name} 
                    WHERE DATASET_RUN_ID = {run_id}
                )
                FILE_FORMAT = (TYPE = CSV, compression=''gzip'', FIELD_OPTIONALLY_ENCLOSED_BY = ''"'')
                HEADER = TRUE
                SINGLE = TRUE
                OVERWRITE = TRUE
                MAX_FILE_SIZE = 5368709120
            """
            session.sql(copy_sql).collect()
            
            # 4. Generate Presigned URL (7 days validity)
            url_sql = f"SELECT GET_PRESIGNED_URL(''{DQ_STAGE_NAME}'', ''{export_file_path}'', 604800)"
            url_result = session.sql(url_sql).collect()
            
            if url_result:
                raw_url = url_result[0][0]
                generated_file_url = f"''{raw_url}''" # Quote for SQL insert
                
            master_audit_logs.append((run_id, "EXECUTE_DQ_RULES_MASTER", "GENERATE_FAILED_FILE", f"Generated consolidated file: {export_file_name}", step_start_time, now(), "COMPLETED", None))
            
        except Exception as e:
            file_generation_error = True # Mark as critical error
            master_audit_logs.append((run_id, "EXECUTE_DQ_RULES_MASTER", "GENERATE_FAILED_FILE", f"CRITICAL: Failed to generate consolidated file: {str(e)}", step_start_time, now(), "FAILED", str(e)))

    # =========================================================================
    
    end_time = now()
    run_time_seconds = (end_time - master_start_time).total_seconds()
    
    # Determine Final Status (Prioritize Execution Error if File Gen Fails)
    if error_count > 0 or file_generation_error:
        final_status = "ERROR"
    elif failure_count > 0:
        final_status = "FAILURE"
    else:
        final_status = "SUCCESS"
        
    success_percent = (success_count * 100.0) / total_rules if total_rules > 0 else 0
    
    log_and_return(session, config, master_audit_logs, 0)
    
    run_log_table_path = f"{config[''DQ_DB_NAME'']}.{config[''DQ_SCHEMA_NAME'']}.DQ_DATASET_RUN_LOG"
    
    final_insert_sql = f"""
        INSERT INTO {run_log_table_path} 
        (DATASET_RUN_ID, DATASET_ID, RUN_TIME, EVALUATED_EXPECTATIONS, SUCCESSFULL_EXPECTATIONS, UNSUCCESSFULL_EXPECTATIONS, RUN_STATUS, CREATED_BY, SUCCESS_PERCENT, CREATED_TIMESTAMP, FAILED_FILE_URL) 
        VALUES ({run_id}, {P_DATASET_ID}, {run_time_seconds}, {total_rules}, {success_count}, {failure_count}, ''{final_status}'', ''{session.get_current_user()}'', {success_percent}, ''{master_start_time}'', {generated_file_url})
    """
    session.sql(final_insert_sql).collect()

    # Final Return Code Logic
    if error_count > 0 or file_generation_error: 
        return int(config["EXECUTION_ERROR"])
    elif failure_count > 0: 
        return int(config["FAILED_CODE"])
    else: 
        return int(config["SUCCESS_CODE"])
';