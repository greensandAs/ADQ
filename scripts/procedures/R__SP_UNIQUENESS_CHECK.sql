USE DATABASE {{ snowflake_database }};
USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "SP_UNIQUENESS_CHECK"("RULE" VARIANT)
RETURNS NUMBER(38, 0)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    -- Standard Variables
    v_sql TEXT;
    v_result RESULTSET;
    v_total INT DEFAULT 0;
    v_unexpected INT DEFAULT 0;
    v_percent FLOAT DEFAULT 0;
    v_status_code NUMBER;
    v_allowed_deviation FLOAT DEFAULT 0;
    v_error_message STRING;
    v_step STRING DEFAULT ''INITIALIZATION'';
    v_run_id NUMBER DEFAULT -1;
    v_column_nm STRING;
    v_database_name STRING;
    v_schema_name STRING;
    v_table_name STRING;
    v_data_asset_id NUMBER;
    v_check_config_id NUMBER;
    v_expectation_id NUMBER;
    v_run_name STRING;
    v_data_asset_name STRING;
    v_expectation_name STRING;
    v_dq_db_name STRING;
    v_dq_schema_name STRING;
    v_success_code NUMBER;
    v_failed_code NUMBER;
    v_execution_error NUMBER;
    v_failed_records_table STRING;
    v_failed_rows_cnt_limit NUMBER;
    v_kwargs_variant VARIANT;
    v_batch_id NUMBER DEFAULT -1;
    v_procedure_name STRING;
    v_stage_path STRING;
    v_input_rule_str STRING;
    v_log_message STRING;
    v_rows_inserted NUMBER DEFAULT 0;
    v_failed_rows_threshold INT DEFAULT 10000000;
    v_dimension STRING;

    -- Generic Check Variables
    v_dataset_type STRING;
    v_sql_query STRING;
    v_from_clause STRING;
    v_column_expr STRING;

    -- Metrics
    v_unique_count INT DEFAULT 0;
    v_missing_count INT DEFAULT 0;
    v_unexpected_percent_total FLOAT DEFAULT 0;
    v_unexpected_percent_nonmissing FLOAT DEFAULT 0;
    v_missing_percent FLOAT DEFAULT 0;

    -- Key Capture
    v_key_column_names STRING;
    v_pk_column_names STRING;
    v_ck_column_names STRING;
    v_key_construct_expr STRING;
    v_key_parts_list STRING;
    v_partial_unexpected_list VARIANT;

    -- Dynamic Table Variables
    v_clean_dataset_name STRING;
    v_full_target_table_name STRING;

BEGIN
    v_input_rule_str := TO_VARCHAR(RULE);

    -- 1. Load Config
    v_step := ''CONFIG_LOADING'';
    v_procedure_name := COALESCE(RULE:PROCEDURE_NAME::STRING, ''SP_UNIQUENESS_CHECK2'');
    v_run_id := COALESCE(RULE:DATASET_RUN_ID::NUMBER, -1);
    v_check_config_id := COALESCE(RULE:RULE_CONFIG_ID::NUMBER, -1);

    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Loading configuration'');

    BEGIN
        v_sql := ''SELECT DQ_DB_NAME, DQ_SCHEMA_NAME, SUCCESS_CODE, FAILED_CODE, EXECUTION_ERROR FROM DQ_JOB_EXEC_CONFIG LIMIT 1'';
        v_result := (EXECUTE IMMEDIATE v_sql);
        LET v_config_cursor CURSOR FOR v_result;
        FOR config_record IN v_config_cursor DO
            v_dq_db_name := config_record.DQ_DB_NAME;
            v_dq_schema_name := config_record.DQ_SCHEMA_NAME;
            v_success_code := config_record.SUCCESS_CODE;
            v_failed_code := config_record.FAILED_CODE;
            v_execution_error := config_record.EXECUTION_ERROR;
            BREAK;
        END FOR;

        IF (v_dq_db_name IS NULL OR v_dq_schema_name IS NULL) THEN
            v_error_message := ''Required Configuration parameter is missing in DQ_JOB_EXEC_CONFIG'';
            v_status_code := 400;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        END IF;
        v_status_code := v_execution_error; 
    EXCEPTION
        WHEN OTHER THEN
            v_error_message := ''Error loading config: '' || SQLERRM;
            v_status_code := 400;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
    END;
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Config Loaded'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    -- 2. Parse Rules
    v_step := ''RULE_PARSING'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', :v_input_rule_str);

    BEGIN
        v_column_nm := RULE:COLUMN_NAME::STRING;
        v_database_name := RULE:DATABASE_NAME::STRING;
        v_schema_name := RULE:SCHEMA_NAME::STRING;
        v_table_name := RULE:TABLE_NAME::STRING;
        v_dataset_type := RULE:DATASET_TYPE::STRING;
        v_sql_query := RULE:CUSTOM_SQL::STRING;
        v_data_asset_name := RULE:DATASET_NAME::STRING;
        v_kwargs_variant := PARSE_JSON(RULE:KWARGS);
        v_allowed_deviation := COALESCE(v_kwargs_variant:mostly::FLOAT, 1.0);
        v_failed_rows_cnt_limit := v_kwargs_variant:failed_row_count::NUMBER;
        v_dimension := RULE:DIMENSION;
        v_data_asset_id := COALESCE(RULE:DATASET_ID::NUMBER, -1);
        v_run_name := RULE:RUN_NAME::STRING;
        v_expectation_name := RULE:EXPECTATION_NAME::STRING;
        v_key_column_names := RULE:KEY_COLUMN_NAMES::STRING;

        IF (v_column_nm IS NULL) THEN
            v_error_message := ''COLUMN_NAME is required.'';
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        END IF;

        IF (UPPER(v_dataset_type) = ''QUERY'') THEN
             IF (v_sql_query IS NULL) THEN
                 v_error_message := ''CUSTOM_SQL is required for QUERY type.'';
                 v_status_code := v_execution_error;
                 RETURN v_status_code;
             END IF;
             v_from_clause := ''('' || v_sql_query || '') AS custom_query_source'';
             v_column_expr := ''"'' || v_column_nm || ''"'';
        ELSE
             IF (v_database_name IS NULL OR v_table_name IS NULL) THEN
                 v_error_message := ''DATABASE_NAME and TABLE_NAME are required for TABLE type.'';
                 v_status_code := v_execution_error;
                 RETURN v_status_code;
             END IF;
             v_from_clause := ''"'' || v_database_name || ''"."'' || v_schema_name || ''"."'' || v_table_name || ''"'';
             v_column_expr := ''"'' || v_column_nm || ''"'';
        END IF;
    EXCEPTION
        WHEN OTHER THEN
            v_error_message := ''Parsing Error: '' || SQLERRM;
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
    END;
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Rule Parsed'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    -- 3. Main Query
    v_step := ''MAIN_QUERY'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Validation'');

    IF (v_error_message IS NULL) THEN
        BEGIN
            v_sql := ''SELECT
                          COUNT(*) AS total_count,
                          COUNT(DISTINCT '' || v_column_expr || '') AS unique_count,
                          COUNT_IF('' || v_column_expr || '' IS NULL) AS missing_count,
                          COALESCE(SUM(CASE WHEN val_count > 1 THEN 1 ELSE 0 END), 0) AS unexpected_count
                        FROM (
                          SELECT 
                            '' || v_column_expr || '',
                            COUNT('' || v_column_expr || '') OVER (PARTITION BY '' || v_column_expr || '') as val_count
                          FROM '' || v_from_clause || ''
                        ) T '';

            v_result := (EXECUTE IMMEDIATE v_sql);
            LET v_cursor CURSOR FOR v_result;
            FOR record IN v_cursor DO
                v_total := COALESCE(record.total_count, 0);
                v_unique_count := COALESCE(record.unique_count, 0);
                v_missing_count := COALESCE(record.missing_count, 0);
                v_unexpected := COALESCE(record.unexpected_count, 0);
                BREAK;
            END FOR;
            
            v_missing_percent := CASE WHEN v_total = 0 THEN 0 ELSE (v_missing_count::FLOAT / v_total) END;
            v_unexpected_percent_total := CASE WHEN v_total = 0 THEN 0 ELSE (v_unexpected::FLOAT) / v_total END;
            v_percent := v_unexpected_percent_total;

            v_status_code := CASE WHEN (1.0 - v_percent) >= v_allowed_deviation THEN v_success_code ELSE v_failed_code END;
            v_log_message := ''Uniqueness adherence: '' || ((1.0 - v_percent) * 100)::STRING || ''%. Required: '' || (v_allowed_deviation * 100)::STRING || ''%.'';
        EXCEPTION
            WHEN OTHER THEN
                v_error_message := ''Validation Error: '' || SQLERRM;
                v_status_code := v_execution_error;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
                RETURN v_status_code;
        END;
    END IF;
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    -- 4. Failed Keys (CORRECTED LOGIC)
    v_step := ''CAPTURE_FAILED_KEYS'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Key Capture'');

    IF (v_unexpected > 0 AND UPPER(v_dataset_type) = ''TABLE'') THEN
        BEGIN
            v_sql := ''SELECT PRIMARY_KEY_COLUMNS FROM DQ_DATASET WHERE DATASET_ID = '' || v_data_asset_id;
            v_result := (EXECUTE IMMEDIATE v_sql);
            LET v_pk_cursor CURSOR FOR v_result;
            FOR pk_record IN v_pk_cursor DO
                v_pk_column_names := ARRAY_TO_STRING(PARSE_JSON(pk_record.PRIMARY_KEY_COLUMNS):primary_key, '','');
                BREAK;
            END FOR;
            v_key_column_names := COALESCE(v_pk_column_names, v_key_column_names);

            IF (v_key_column_names IS NOT NULL) THEN
                SELECT LISTAGG('''''''' || TRIM(value) || '''''''' || '', '' || TRIM(value), '', '') WITHIN GROUP (ORDER BY seq)
                INTO v_key_parts_list FROM TABLE(SPLIT_TO_TABLE(:v_key_column_names, '',''));
                v_key_construct_expr := ''OBJECT_CONSTRUCT('' || v_key_parts_list || '')'';

                v_sql := ''INSERT INTO DQ_FAILED_ROW_KEYS (DATASET_RUN_ID, RULE_CONFIG_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, FAILED_KEY) SELECT '' ||
                         v_run_id || '', '' || v_check_config_id || '', '''''' || COALESCE(v_database_name, ''N/A'') || '''''', '''''' || COALESCE(v_schema_name, ''N/A'') || '''''', '''''' || COALESCE(v_table_name, ''N/A'') || '''''', '' || v_key_construct_expr ||
                         '' FROM '' || v_from_clause || 
                         '' QUALIFY COUNT('' || v_column_expr || '') OVER (PARTITION BY '' || v_column_expr || '') > 1 '' ||
                         CASE WHEN v_failed_rows_cnt_limit > 0 THEN '' LIMIT '' || v_failed_rows_cnt_limit ELSE '''' END;
                EXECUTE IMMEDIATE v_sql;
                v_rows_inserted := SQLROWCOUNT;
                v_log_message := v_rows_inserted || '' keys captured.'';
            ELSE
                 v_log_message := ''Skipped key capture - No Primary Key found.'';
            END IF;
        EXCEPTION
             WHEN OTHER THEN 
                v_log_message := ''Key Capture Failed: '' || SQLERRM;
        END;
    ELSE
         v_log_message := ''Skipped key capture - No unexpected rows or Query type.'';
    END IF;
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    -- 5. Failed Records
    v_step := ''INSERT_FAILED_RECORDS'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Failed Records'');

    IF (v_unexpected > 0) THEN
        BEGIN
            v_clean_dataset_name := REGEXP_REPLACE(v_data_asset_name, ''[^a-zA-Z0-9]'', ''_'');
            v_failed_records_table := v_clean_dataset_name || ''_DQ_FAILURE'';
            v_full_target_table_name := ''"'' || v_dq_db_name || ''"."'' || v_dq_schema_name || ''"."'' || v_failed_records_table || ''"'';
            
            v_sql := ''CREATE TABLE IF NOT EXISTS '' || v_full_target_table_name || '' AS SELECT '' ||
                     v_run_id || ''::NUMBER(38,0) AS DATASET_RUN_ID, '' ||
                     v_data_asset_id || ''::NUMBER(38,0) AS DATASET_ID, '' ||
                     v_check_config_id || ''::NUMBER(38,0) AS RULE_CONFIG_ID, '' ||
                     ''CURRENT_TIMESTAMP()::TIMESTAMP_LTZ AS DQ_LOAD_TIMESTAMP, * FROM '' || v_from_clause || '' WHERE 1=0'';
            EXECUTE IMMEDIATE v_sql;

            v_sql := ''INSERT INTO '' || v_full_target_table_name || '' SELECT '' || v_run_id || '', '' || v_data_asset_id || '', '' || v_check_config_id || '', CURRENT_TIMESTAMP(), * FROM '' || v_from_clause || 
                     '' QUALIFY COUNT('' || v_column_expr || '') OVER (PARTITION BY '' || v_column_expr || '') > 1 '' ||
                     CASE WHEN v_failed_rows_cnt_limit > 0 THEN '' LIMIT '' || v_failed_rows_cnt_limit ELSE '''' END;
            EXECUTE IMMEDIATE v_sql;
            v_rows_inserted := SQLROWCOUNT;
            v_log_message := v_rows_inserted || '' records stored in '' || v_failed_records_table;
        EXCEPTION
             WHEN OTHER THEN 
                v_log_message := ''Failed Record Capture Error: '' || SQLERRM;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
                RETURN 400;
        END;
    ELSE
        v_log_message := ''No failed records to capture.'';
    END IF;
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    -- 6. Insert Results
    v_step := ''INSERT_DQ_RESULTS_TABLE'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Results'');

    IF (v_error_message IS NULL) THEN
        BEGIN
            LET v_details_obj VARIANT := OBJECT_CONSTRUCT(''column'', v_column_nm, ''mostly'', v_allowed_deviation);
            LET v_results_obj VARIANT := OBJECT_CONSTRUCT(''element_count'', v_total, ''unexpected_count'', v_unexpected, ''unexpected_percent'', v_percent * 100);

            LET v_safe_details_str STRING := REPLACE(REPLACE(TO_JSON(v_details_obj), ''\\\\'', ''\\\\\\\\''), ''\\'''', ''\\''\\'''');
            LET v_safe_results_str STRING := REPLACE(REPLACE(TO_JSON(v_results_obj), ''\\\\'', ''\\\\\\\\''), ''\\'''', ''\\''\\'''');
            LET v_safe_rule_str STRING := REPLACE(REPLACE(COALESCE(RULE::STRING, ''null''), ''\\\\'', ''\\\\\\\\''), ''\\'''', ''\\''\\'''');

            v_sql := ''INSERT INTO "'' || v_dq_db_name || ''"."'' || v_dq_schema_name || ''".DQ_RULE_RESULTS (
                BATCH_ID, DATASET_RUN_ID, DATASET_ID, RULE_CONFIG_ID, EXPECTATION_ID, RUN_NAME, RUN_TIMESTAMP, DATASET_NAME,
                EXPECATION_CONFIG, IS_SUCCESS, RESULTS, EXPECTATION_NAME, DETAILS, ELEMENT_COUNT, UNEXPECTED_COUNT, 
                UNEXPECTED_PERCENT, DIMENSION
                ) SELECT '' || 
                COALESCE(v_batch_id::STRING, ''null'') || '', '' || COALESCE(v_run_id::STRING, ''null'') || '', '' || COALESCE(v_data_asset_id::STRING, ''null'') || '', '' ||
                COALESCE(v_check_config_id::STRING, ''null'') || '', '' || COALESCE(v_expectation_id::STRING, ''null'') || '', \\'''' || REPLACE(COALESCE(v_run_name, ''null''), ''\\'''', ''\\''\\'''') || ''\\'', CURRENT_TIMESTAMP(), \\'''' || REPLACE(COALESCE(v_data_asset_name, ''null''), ''\\'''', ''\\''\\'''') || ''\\'', 
                PARSE_JSON(\\'''' || v_safe_rule_str || ''\\''), '' || 
                CASE WHEN v_status_code = v_success_code THEN ''TRUE'' ELSE ''FALSE'' END || '', 
                PARSE_JSON(\\'''' || v_safe_results_str || ''\\''), \\'''' || REPLACE(COALESCE(v_expectation_name, ''null''), ''\\'''', ''\\''\\'''') || ''\\'', 
                PARSE_JSON(\\'''' || v_safe_details_str || ''\\''), '' ||
                COALESCE(v_total::STRING, ''null'') || '', '' || COALESCE(v_unexpected::STRING, ''null'') || '', '' || COALESCE(v_percent*100::STRING, ''null'') || 
                '', \\'''' || COALESCE(v_dimension, ''null'') || ''\\'''';
            
            EXECUTE IMMEDIATE v_sql;
        EXCEPTION
            WHEN OTHER THEN
                v_error_message := ''Result Insert Failed: '' || SQLERRM;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
                RETURN 400;
        END;
    END IF;

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Results stored'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
    RETURN v_status_code;

EXCEPTION
    WHEN OTHER THEN
        INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, END_TIMESTAMP, STATUS, ERROR_MESSAGE)
        VALUES (COALESCE(:v_run_id, -1), COALESCE(:v_check_config_id, -1), COALESCE(:v_procedure_name, ''SP_UNIQUENESS_CHECK2''), ''UNKNOWN'', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''FAILED'', SQLERRM);
        RETURN 400;
END;
';;
