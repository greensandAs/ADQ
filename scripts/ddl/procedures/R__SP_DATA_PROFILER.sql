USE DATABASE {{ snowflake_database }};

USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "SP_DATA_PROFILER"("RUN_ID" VARCHAR, "DATASET_ID" VARCHAR, "DBNAME" VARCHAR, "SCHEMANAME" VARCHAR, "TABLENAME" VARCHAR, "CUSTOMSQL" VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python','ydata-profiling','pandas','numpy')
HANDLER = 'run_profiler_sp'
EXECUTE AS CALLER
AS '
import warnings
import json
import re
import traceback
from datetime import datetime
import pandas as pd
import numpy as np
from ydata_profiling import ProfileReport
from functions import udf, col, cast, call_udf
from types import StringType, FloatType

# --- Helper Function for SQL Identifier Quoting ---
def q_ident(name: str) -> str:
    """Quotes an identifier for safe SQL use."""
    if name is None: return ''NULL''
    name = name.strip(''"'') 
    return ''"'' + name.replace(''"'', ''""'') + ''"''

def q_str(val: str) -> str:
    """Escapes single quotes for SQL string literals."""
    if val is None: return ''NULL''
    return "''" + str(val).replace("''", "''''") + "''"

# ==========================================
#  AUDIT LOGGING CONTEXT MANAGER
# ==========================================
class AuditLogManager:
    def __init__(self, session, db, schema, audit_table, run_id, dataset_id, ds_name, table_name, custom_sql):
        self.session = session
        self.full_table_path = f"{q_ident(db)}.{q_ident(schema)}.{q_ident(audit_table)}"
        self.run_id = run_id
        self.dataset_id = dataset_id
        self.ds_name = ds_name
        self.db_name = db
        self.schema_name = schema
        self.table_name = table_name
        self.custom_sql = custom_sql

    def step(self, step_name):
        return AuditStep(self, step_name)

class AuditStep:
    def __init__(self, manager, step_name):
        self.mgr = manager
        self.step_name = step_name

    def __enter__(self):
        """Insert the log entry with STATUS = STARTED"""
        try:
            sql = f"""
                INSERT INTO {full_table_path} 
                (RUN_ID, DATASET_ID, DATASET_NAME, DATABASE_NAME, SCHEMA_NAME, 
                 TABLE_NAME, CUSTOM_SQL_QUERY, STEP, STATUS, MESSAGE, LOG_TIMESTAMP)
                VALUES (
                    {q_str(run_id)}, {q_str(dataset_id)}, {q_str(ds_name)},
                    {q_str(db_name)}, {q_str(schema_name)}, {q_str(table_name)},
                    {q_str(custom_sql)}, {q_str(self.step_name)}, ''STARTED'', NULL, CURRENT_TIMESTAMP()
                )
            """
            session.sql(sql).collect()
        except Exception as e:
            # If logging fails, print but do not crash (unless critical)
            print(f"AUDIT INIT FAILED: {str(e)}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Update the log entry to SUCCESS or FAILURE"""
        status = "SUCCESS"
        message = ""
        
        if exc_type:
            status = "FAILURE"
            # format the exception to be readable
            message = "".join(traceback.format_exception(exc_type, exc_val, exc_tb))
            message = message[:16777216] # Limit Snowflake VARCHAR max

        try:
            # We match on RUN_ID and STEP. 
            # Note: This assumes step names are unique within a run.
            sql = f"""
                UPDATE {full_table_path}
                SET STATUS = {q_str(status)},
                    MESSAGE = {q_str(message)},
                    LOG_TIMESTAMP = CURRENT_TIMESTAMP()
                WHERE RUN_ID = {q_str(run_id)} 
                  AND STEP = {q_str(self.step_name)}
            """
            session.sql(sql).collect()
        except Exception as e:
            print(f"AUDIT UPDATE FAILED: {str(e)}")

        # If an exception occurred in the block, we return False to let it propagate (Raise)
        if exc_type:
            return False 
        return True

# ==========================================
#  LOGIC HELPERS
# ==========================================

def _identify_kanji_numeral_columns(session, snowpark_df, sample_size: int = 1000) -> list:
    """Identifies columns that likely contain Kanji numerals."""
    original_string_cols = [f.name for f in fields if isinstance(f.datatype, StringType)]
    if not original_string_cols: return []
    try:
        sample_df = snowpark_df.select(original_string_cols).sample(n=sample_size).to_pandas()
    except Exception: return []
    
    kanji_pattern = re.compile(r''[^一二三四五六七八九十百千万億兆0-9.,s・点〇零]'')
    col_name_map = {c.strip(''"''): c for c in original_string_cols}
    kanji_cols = []
    for col_pd in sample_df.columns:
        if col_pd in col_name_map:
            col_data = sample_df[col_pd].dropna().astype(str)
            if not col_data.empty:
                non_compliant_count = col_data.apply(lambda x: bool(kanji_pattern.search(x))).sum()
                compliant_percentage = ((len(col_data) - non_compliant_count) / len(col_data)) * 100
                if compliant_percentage >= 100.0:
                    kanji_cols.append(col_name_map[col_pd])
    return kanji_cols
    
def convert_num_to_kanji_str(session, num, framework_db: str, framework_schema: str):
    """Dynamically calls the NUMERIC_TO_KANJI UDF using q_ident."""
    if num is None: return None
    try:
        db_ident = q_ident(framework_db)
        schema_ident = q_ident(framework_schema)
        sql_query = f"SELECT {db_ident}.{schema_ident}.NUMERIC_TO_KANJI({num}) AS KANJI_VAL"
        res = session.sql(sql_query).collect()
        if res and ''KANJI_VAL'' in res[0]:
            return res[0][''KANJI_VAL'']
    except Exception:
        pass
    return str(num)     

def _parse_and_restructure_report(session, audit_mgr, profile_data: dict, ds_name: str, dataset_id: str, run_id: str,
                                 dbname: str, schemaname: str, tablename: str, customsql: str,
                                 framework_db: str, framework_schema: str) -> (pd.DataFrame, pd.DataFrame):
    
    # We use the audit manager passed down to log sub-steps
    with audit_mgr.step("Parse Profiling JSON"):
        column_data = []
        for idx, (col_name, stats) in enumerate(profile_data.get("variables", {}).items(), 1):
            col_level = {"column_name": col_name, **stats}
            alert_msg = None
            for msg in profile_data.get("alerts", []):
                if col_name in msg: 
                    alert_msg = msg
                    break

            col_level.update({
                "dataset_name": ds_name, "dataset_id": dataset_id, "run_id": run_id,
                "database_name": dbname, "schema_name": schemaname,   
                "table_name": tablename, "custom_sql": customsql,       
                "column_id": idx,
                "profile_start_timestamp": profile_data.get("analysis", {}).get("date_start"),
                "profile_end_timestamp": profile_data.get("analysis", {}).get("date_end"),
                "profiled_at": profile_data.get("analysis", {}).get("date_end"),
                "column_alert": alert_msg,    
            })
            column_data.append(col_level)

        column_mapping = {
            "type": "data_type", "n_distinct": "column_distinct_count", "p_distinct": "column_distinct_percentage",
            "n_unique": "column_unique_count", "p_unique": "column_unique_percent", "n_missing": "column_missing_count",
            "p_missing": "column_missing_percentage", "n_zeros": "column_zero_count", "p_zeros": "column_zero_percentage",
            "mean": "column_mean_value", "std": "column_std_dev", "min": "column_min_value", "max": "column_max_value",
            "sum": "column_sum_value", "memory_size": "column_memory_size_bytes", "column_alert": "column_alert"  
        }
        
        table = profile_data.get("table", {})
        correlations = profile_data.get("correlations", {})
        
        table_mapping = {
            "run_id": run_id, "dataset_id": dataset_id,
            "database_name": dbname, "schema_name": schemaname, "table_name": tablename,  
            "custom_sql": customsql, "dataset_name": ds_name,
            "total_rows": table.get("n"), "total_columns": table.get("n_var"),
            "null_count": table.get("n_cells_missing"), "null_percent": table.get("p_cells_missing"),
            "duplicate_count": table.get("n_duplicates"),
            "correlation_matrix": json.dumps(correlations, ensure_ascii=False) if correlations else None,
            "missing_values_count": table.get("n_cells_missing"),
            "profile_start_timestamp": profile_data.get("analysis", {}).get("date_start"),
            "profile_end_timestamp": profile_data.get("analysis", {}).get("date_end"),
            "profiled_at": profile_data.get("analysis", {}).get("date_end"),
            # Kanji conversions
            "total_rows_ja": convert_num_to_kanji_str(session,table.get("n"), framework_db, framework_schema),
            "total_columns_ja": convert_num_to_kanji_str(session,table.get("n_var"), framework_db, framework_schema),
            "missing_values_count_ja": convert_num_to_kanji_str(session, table.get("n_cells_missing"), framework_db, framework_schema),
            "null_count_ja": convert_num_to_kanji_str(session, table.get("n_cells_missing"), framework_db, framework_schema),
            "duplicate_count_ja": convert_num_to_kanji_str(session,table.get("n_duplicates"), framework_db, framework_schema),
            "null_percent_ja": convert_num_to_kanji_str(session,table.get("p_cells_missing"), framework_db, framework_schema),
        }

        # Convert complex objects
        def convert_complex(df):
            for c in df.columns:
                if df[c].apply(lambda x: isinstance(x, (list, dict))).any():
                    df[c] = df[c].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else x)
            return df

        columns_df = pd.DataFrame(column_data).rename(columns=column_mapping)
        table_df = pd.DataFrame([table_mapping])
        
        columns_df = convert_complex(columns_df)
        table_df = convert_complex(table_df)

    return columns_df, table_df

# --- Main Stored Procedure Handler ---
def run_profiler_sp(session, run_id: str, dataset_id: str, dbname: str, schemaname: str, tablename: str, customsql: str) -> str:
    
    # --- Internal Configuration ---
    sample_row_count = 50000
    column_stats_table = "PROFILER_ATTRIBUTE_STATS"
    table_summary_table = "PROFILER_DATASET_STATS"
    audit_log_table = "PROFILER_AUDIT_LOGS" 
    translate_kanji_numerals = True
    
    framework_db = session.get_current_database().strip(''"'') 
    framework_schema = session.get_current_schema().strip(''"'')
    dataset_name_for_report = tablename if tablename else "custom_query"

    # Initialize Audit Manager
    audit = AuditLogManager(session, framework_db, framework_schema, audit_log_table, 
                            run_id, dataset_id, dataset_name_for_report, tablename, customsql)

    # 1. Ensure Audit Table Exists (Critical for immediate insertion)
    try:
        create_audit_sql = f"""
        CREATE TABLE IF NOT EXISTS {audit.full_table_path} (
            RUN_ID VARCHAR, DATASET_ID VARCHAR, DATASET_NAME VARCHAR, DATABASE_NAME VARCHAR,
            SCHEMA_NAME VARCHAR, TABLE_NAME VARCHAR, CUSTOM_SQL_QUERY VARCHAR,
            STEP VARCHAR, STATUS VARCHAR, MESSAGE VARCHAR, LOG_TIMESTAMP TIMESTAMP_NTZ
        )
        """
        session.sql(create_audit_sql).collect()
    except Exception as e:
        return f"CRITICAL FAILURE: Could not access audit table. {str(e)}"

    # ============================================
    # EXECUTION BLOCKS WITH TRANSACTIONAL AUDITING
    # ============================================
    
    # NOTE: Exceptions inside ''with audit.step()'' are caught, logged as FAILURE, and then Re-Raised.
    
    with audit.step("Process Start"):
        if (not tablename and not customsql) or (tablename and customsql):
            raise ValueError("Provide either ''TABLENAME'' or ''CUSTOMSQL'', but not both.")

    with audit.step("Create Snowpark DataFrame"):
        if tablename:
            table_name_parts = [dbname.strip(''"''), schemaname.strip(''"''), tablename.strip(''"'')]
            snowpark_df = session.table(table_name_parts)
        else: 
            snowpark_df = session.sql(customsql)

    if translate_kanji_numerals:
        with audit.step("Kanji Numeral Translation"):
            kanji_cols_to_translate = _identify_kanji_numeral_columns(session, snowpark_df)
            if kanji_cols_to_translate:
                udf_path = f"{q_ident(framework_db)}.{q_ident(framework_schema)}.KANJI_TO_NUMERIC"
                for col_name in kanji_cols_to_translate:
                    snowpark_df = snowpark_df.withColumn(col_name, cast(call_udf(udf_path, col(col_name)), FloatType()))

    with audit.step("Sample Data"):
        if sample_row_count > 0:
            snowpark_df = snowpark_df.sample(n=sample_row_count)

    with audit.step("Convert to Pandas"):
        pandas_df = snowpark_df.to_pandas()

    with audit.step("Generate ydata-profiling Report"):
        if pandas_df.empty:
            raise ValueError("Dataset is empty after sampling.")
        profile = ProfileReport(pandas_df, title=f"Profile for {dataset_name_for_report}", minimal=True)
        report_json_data = json.loads(profile.to_json())

    # This function has internal audit logging
    column_stats_df, table_summary_df = _parse_and_restructure_report(
        session, audit, report_json_data, ds_name=dataset_name_for_report, 
        dataset_id=dataset_id, run_id=run_id, dbname=dbname, schemaname=schemaname, 
        tablename=tablename, customsql=customsql, framework_db=framework_db, framework_schema=framework_schema
    )

    with audit.step(f"Upload Column Stats to {column_stats_table}"):
        table_path = f"{q_ident(framework_db)}.{q_ident(framework_schema)}.{q_ident(column_stats_table)}"
        
        # Get existing columns to align schema
        existing_cols = [c.upper() for c in session.table(table_path).columns]     
        upload_cols_df = column_stats_df[[c for c in column_stats_df.columns if c.upper() in existing_cols]].copy()
        
        # Add inferred types and uppercase headers
        inferred_types = {c: str(pandas_df[c].dtype) for c in pandas_df.columns}
        upload_cols_df["column_inferred_data_type"] = upload_cols_df["column_name"].map(inferred_types)
        upload_cols_df.columns = [c.upper() for c in upload_cols_df.columns]

        # Fix numeric types
        numeric_stat_cols = ["COLUMN_DISTINCT_COUNT", "COLUMN_UNIQUE_COUNT", "COLUMN_MISSING_COUNT", 
                             "COLUMN_ZERO_COUNT", "COLUMN_MEAN_VALUE", "COLUMN_STD_DEV", 
                             "COLUMN_MIN_VALUE", "COLUMN_MAX_VALUE"]
        for stat_col in numeric_stat_cols:
            if stat_col in upload_cols_df.columns:
                upload_cols_df[stat_col] = pd.to_numeric(upload_cols_df[stat_col], errors=''coerce'')

        session.write_pandas(upload_cols_df, table_name=column_stats_table, 
                             database=framework_db, schema=framework_schema, 
                             auto_create_table=False, overwrite=False)

    with audit.step(f"Upload Table Summary to {table_summary_table}"):
        upload_summary_df = table_summary_df.copy()
        upload_summary_df.columns = [c.upper() for c in upload_summary_df.columns]
        
        summary_numeric_cols = ["TOTAL_ROWS", "TOTAL_COLUMNS", "NULL_COUNT", "NULL_PERCENT", 
                                "DUPLICATE_COUNT", "MISSING_VALUES_COUNT"]
        for summary_col in summary_numeric_cols:
            if summary_col in upload_summary_df.columns:
                upload_summary_df[summary_col] = pd.to_numeric(upload_summary_df[summary_col], errors=''coerce'')

        session.write_pandas(upload_summary_df, table_name=table_summary_table, 
                             database=framework_db, schema=framework_schema, 
                             overwrite=False, auto_create_table=False)
                             
    return "SUCCESS: Profiling complete."
';