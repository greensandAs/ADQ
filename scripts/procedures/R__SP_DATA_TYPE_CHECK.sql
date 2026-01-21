USE DATABASE {{ snowflake_database }};
USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "SP_DATA_TYPE_CHECK"("RULE" VARIANT)
RETURNS NUMBER(38, 0)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    v_sql TEXT;
    v_result RESULTSET;
    v_total INT DEFAULT 0;
    v_unexpected INT DEFAULT 0;
    v_percent FLOAT DEFAULT 0;
    v_status_code NUMBER;
    v_allowed_deviation FLOAT DEFAULT 1.0; -- Default for type check is 1.0 (100% compliance)
    v_error_message STRING;
    v_step STRING DEFAULT ''INITIALIZATION'';
    v_run_id NUMBER DEFAULT -1;
    v_column_nm STRING;
    v_type_expected STRING;
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
    v_batch_id NUMBER DEFAULT -1;
    v_procedure_name STRING DEFAULT ''SP_DATA_TYPE_CHECK'';
    v_input_rule_str STRING;
    v_log_message STRING;
    
    -- Variables specific to this check
    v_kwargs_variant VARIANT;
    v_dataset_type STRING;
    v_sql_query STRING;
    v_dimension STRING;
    v_failed_records_table STRING; 
    v_from_clause STRING;
    v_where_clause_condition STRING;
    v_failed_rows_cnt_limit NUMBER;
    v_failed_rows_threshold INT DEFAULT 10000000;
    v_rows_inserted NUMBER DEFAULT 0;
    v_stage_path STRING;
    
    -- Variables for failed row keys enhancement
    v_key_column_names STRING;
    v_pk_column_names STRING;
    v_ck_column_names STRING;
    v_key_construct_expr STRING;
    v_key_parts_list STRING;
    v_observed_value VARIANT;

    -- DYNAMIC TABLE VARIABLES (NEW)
    v_clean_dataset_name STRING;
    v_full_target_table_name STRING;

BEGIN
    v_input_rule_str := TO_VARCHAR(RULE);

    ----------------------------------------------------------------------------------------------------
    -- 1. Load Configuration
    ----------------------------------------------------------------------------------------------------
    
    v_step := ''CONFIG_LOADING'';
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
        IF (v_dq_db_name IS NULL OR v_dq_schema_name IS NULL OR v_success_code IS NULL OR v_failed_code IS NULL OR v_execution_error IS NULL ) THEN
            v_error_message := ''Required Configurtion parameter is missing or NULL. Please check DQ_JOB_EXEC_CONFIG'';
            v_status_code := 400;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = ''Configuration Loading - failed'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        END IF;
        v_status_code := v_execution_error;
    EXCEPTION
        WHEN OTHER THEN
            v_error_message := ''Error loading configuration: '' || SQLERRM;
            v_status_code := 400;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
    END;
    
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Config loaded'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ---
    -- 2. Parse and Validate the Rule Parameter

    v_step := ''RULE_PARSING'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', :v_input_rule_str);

    IF (RULE IS NULL) THEN
        v_error_message := ''Rule parameter is NULL'';
        v_status_code := v_execution_error;
        UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_input_rule_str WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
        RETURN v_status_code;
    END IF;

    BEGIN
        -- Standard Parameter Loading
        v_batch_id := COALESCE(RULE:BATCH_ID::NUMBER, -1);
        v_run_id := COALESCE(RULE:DATASET_RUN_ID::NUMBER, -1);
        v_check_config_id := COALESCE(RULE:RULE_CONFIG_ID::NUMBER, -1);
        v_data_asset_id := COALESCE(RULE:DATASET_ID::NUMBER, -1);
        v_expectation_id := COALESCE(RULE:EXPECTATION_ID::NUMBER, -1);
        v_run_name := RULE:RUN_NAME::STRING;
        v_data_asset_name := RULE:DATASET_NAME::STRING;
        v_expectation_name := RULE:EXPECTATION_NAME::STRING;
        v_database_name := RULE:DATABASE_NAME::STRING;
        v_schema_name := RULE:SCHEMA_NAME::STRING;
        v_table_name := RULE:TABLE_NAME::STRING;
        v_dataset_type := UPPER(RULE:DATASET_TYPE::STRING);
        v_sql_query := RULE:CUSTOM_SQL::STRING;
        v_dimension := RULE:DIMENSION;
        v_key_column_names := RULE:KEY_COLUMN_NAMES::STRING;
        
        -- KWARGS Parsing and Extraction
        v_kwargs_variant := PARSE_JSON(RULE:KWARGS);
        v_column_nm := v_kwargs_variant:column::STRING;
        v_type_expected := UPPER(v_kwargs_variant:type_::STRING); 
        v_allowed_deviation := COALESCE(v_kwargs_variant:mostly::FLOAT, 1.0);
        v_failed_rows_cnt_limit := v_kwargs_variant:failed_row_count::NUMBER;
        
        -- Validation Checks
        IF (v_column_nm IS NULL OR v_type_expected IS NULL) THEN
            v_error_message := ''Required rule parameters COLUMN (or KWARGS:column) or TYPE_ (or KWARGS:type_) are missing or NULL.'';
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_input_rule_str WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        ELSEIF (v_dataset_type = ''TABLE'' AND (v_database_name IS NULL OR v_schema_name IS NULL OR v_table_name IS NULL)) THEN
            v_error_message := ''For DATASET_TYPE ''''TABLE'''', DATABASE_NAME, SCHEMA_NAME, and TABLE_NAME are required.'';
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_input_rule_str WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        ELSEIF (v_dataset_type = ''QUERY'' AND v_sql_query IS NULL) THEN
            v_error_message := ''For DATASET_TYPE ''''QUERY'''', a CUSTOM_SQL is required.'';
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_input_rule_str WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        END IF;

    EXCEPTION
        WHEN OTHER THEN
            v_error_message := ''Error parsing rule parameter: '' || SQLERRM;
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_input_rule_str WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
    END;

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Input rule - parsing completed'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 3. Execute the Main Data Quality Check Query
    ----------------------------------------------------------------------------------------------------

    v_step := ''MAIN_QUERY'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Starting data type validation query'');

    -- Dynamically build the FROM clause based on dataset_type
    IF (v_dataset_type = ''QUERY'') THEN
        v_from_clause := ''('' || v_sql_query || '') AS custom_query_source'';
    ELSE
        v_from_clause := ''"'' || v_database_name || ''"."'' || v_schema_name || ''"."'' || v_table_name || ''"'';
    END IF;

    -- Define the failure condition: Value is NOT NULL AND TRY_CAST fails.
    v_where_clause_condition := v_column_nm || '' IS NOT NULL AND TRY_CAST('' || v_column_nm || '' AS '' || v_type_expected || '') IS NULL'';

    v_sql := ''SELECT 
                 COUNT(*) AS total_count,
                 COUNT_IF('' || v_where_clause_condition || '') AS unexpected_count
               FROM '' || v_from_clause;

    BEGIN
        v_result := (EXECUTE IMMEDIATE v_sql);
        LET v_cursor CURSOR FOR v_result;
        
        FOR record IN v_cursor DO
            v_total := COALESCE(record.total_count, 0);
            v_unexpected := COALESCE(record.unexpected_count, 0);
            BREAK;
        END FOR;

        v_percent := CASE WHEN v_total = 0 THEN 0 ELSE (v_unexpected::FLOAT / v_total) END;
        
        -- Check against the allowed deviation (mostly)
        v_status_code := CASE WHEN v_percent <= (1 - v_allowed_deviation) THEN v_success_code ELSE v_failed_code END;
        
    EXCEPTION
        WHEN OTHER THEN
            v_error_message := ''Error in main query execution: '' || SQLERRM;
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
    END;

    v_log_message := ''Column '' || v_column_nm || '' expected '' || v_type_expected || '' (mostly >= '' || v_allowed_deviation*100 || ''%). Found '' || v_unexpected || '' unexpected out of '' || v_total || '' rows ('' || ROUND(v_percent*100, 2) || ''%).'';

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 4. Capture Failed Row Keys
    ----------------------------------------------------------------------------------------------------

    v_step := ''CAPTURE_FAILED_KEYS'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Processing failed row keys'');
    
    IF (v_unexpected > 0) THEN
        BEGIN
            -- Key lookup logic from DQ_DATASET (only if KEY_COLUMN_NAMES wasn''t provided in the RULE variant)
            IF (v_key_column_names IS NULL) THEN
                v_sql := ''SELECT PRIMARY_KEY_COLUMNS, CANDIDATE_KEY_COLUMNS FROM DQ_DATASET WHERE DATASET_ID = '' || v_data_asset_id;
                v_result := (EXECUTE IMMEDIATE v_sql);
                LET v_pk_cursor CURSOR FOR v_result;
                FOR pk_record IN v_pk_cursor DO
                    v_pk_column_names := ARRAY_TO_STRING(PARSE_JSON(pk_record.PRIMARY_KEY_COLUMNS):primary_key, '', '');
                    v_ck_column_names := pk_record.CANDIDATE_KEY_COLUMNS;
                    BREAK;
                END FOR;
            
                IF (v_pk_column_names IS NOT NULL AND TRIM(v_pk_column_names) != '''') THEN
                    v_key_column_names := v_pk_column_names;
                    v_log_message := ''Using Primary Key (''||v_key_column_names||'') for failed row key capture.'';
                ELSEIF (v_ck_column_names IS NOT NULL AND TRIM(v_ck_column_names) != '''') THEN
                    v_key_column_names := v_ck_column_names;
                    v_log_message := ''Primary key not found, using Candidate Key (''||v_key_column_names||'') for failed row key capture.'';
                ELSE
                    v_key_column_names := NULL;
                    v_log_message := ''No Primary or Candidate Key found for the dataset. Skipping failed key capture.'';
                END IF;
            END IF;
            
            IF (v_key_column_names IS NOT NULL) THEN
                -- Construct the OBJECT_CONSTRUCT string for the key columns
                SELECT LISTAGG('''''''' || TRIM(value) || '''''''' || '', '' || TRIM(value), '', '') WITHIN GROUP (ORDER BY seq)
                INTO v_key_parts_list
                FROM TABLE(SPLIT_TO_TABLE(:v_key_column_names, '',''));
                v_key_construct_expr := ''OBJECT_CONSTRUCT('' || v_key_parts_list || '')'';
                
                v_sql := ''INSERT INTO DQ_FAILED_ROW_KEYS (DATASET_RUN_ID, RULE_CONFIG_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, FAILED_KEY) SELECT '' ||
                         v_run_id || '', '' || v_check_config_id || '', '' ||
                         '''''''' || COALESCE(v_database_name, ''N/A'') || '''''', '' || 
                         '''''''' || COALESCE(v_schema_name, ''N/A'') || '''''', '' ||   
                         '''''''' || COALESCE(v_table_name, ''N/A'') || '''''', '' ||   
                         v_key_construct_expr ||
                         '' FROM '' || v_from_clause || '' WHERE '' || v_where_clause_condition;
                
                EXECUTE IMMEDIATE v_sql;
                v_rows_inserted := SQLROWCOUNT;
                v_log_message := v_rows_inserted || '' keys of failed rows captured. '' || v_log_message;
            END IF;
        EXCEPTION
            WHEN OTHER THEN
                v_error_message := ''Error capturing failed row keys: '' || SQLERRM;
                v_status_code := v_execution_error;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
                RETURN v_status_code;
        END;
    ELSE
        v_log_message := ''No failed rows found, skipping failed key capture.'';
    END IF;
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 5. Handle and Log Failed Records (UPDATED: Structured Failure Table)
    ----------------------------------------------------------------------------------------------------

    v_step := ''INSERT_FAILED_RECORDS'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Processing failed records into structured table'');

    IF (v_unexpected > 0) THEN
        BEGIN
            -- 1. Construct standard failure table name: <DATASET_NAME>_DQ_FAILURE
            -- Remove special characters from dataset name to ensure valid table identifier
            v_clean_dataset_name := REGEXP_REPLACE(v_data_asset_name, ''[^a-zA-Z0-9]'', ''_'');
            v_failed_records_table := v_clean_dataset_name || ''_DQ_FAILURE'';
            v_full_target_table_name := ''"'' || v_dq_db_name || ''"."'' || v_dq_schema_name || ''"."'' || v_failed_records_table || ''"'';

            -- 2. Create Failure Table IF NOT EXISTS
            -- We replicate the source schema (from v_from_clause) and prepend audit columns
            v_sql := ''CREATE TABLE IF NOT EXISTS '' || v_full_target_table_name || '' AS '' ||
                     ''SELECT '' ||
                     v_run_id || ''::NUMBER(38,0) AS DATASET_RUN_ID, '' ||
                     v_data_asset_id || ''::NUMBER(38,0) AS DATASET_ID, '' ||
                     v_check_config_id || ''::NUMBER(38,0) AS RULE_CONFIG_ID, '' ||
                     ''CURRENT_TIMESTAMP()::TIMESTAMP_LTZ AS DQ_LOAD_TIMESTAMP, '' ||
                     '' * FROM '' || v_from_clause || '' WHERE 1=0'';
            
            EXECUTE IMMEDIATE v_sql;

            -- 3. Insert Failed Records
            v_sql := ''INSERT INTO '' || v_full_target_table_name || '' '' ||
                     ''SELECT '' ||
                     v_run_id || '', '' ||
                     v_data_asset_id || '', '' ||
                     v_check_config_id || '', '' ||
                     ''CURRENT_TIMESTAMP(), '' ||
                     '' * FROM '' || v_from_clause || 
                     '' WHERE '' || v_where_clause_condition ||
                     CASE WHEN v_failed_rows_cnt_limit > 0 THEN '' LIMIT '' || v_failed_rows_cnt_limit ELSE '''' END;

            EXECUTE IMMEDIATE v_sql;
            v_rows_inserted := SQLROWCOUNT;
            
            v_log_message := v_rows_inserted || '' rows inserted into structured failure table: '' || v_failed_records_table;

        EXCEPTION
            WHEN OTHER THEN
                v_error_message := ''Failed to process structured failed records: '' || SQLERRM;
                v_status_code := v_execution_error;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
                RETURN v_status_code;
        END;
    ELSE
        v_failed_records_table := ''No Failed Records'';
        v_log_message := ''No failed records found.'';
    END IF;

    v_observed_value := NULL;

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 6. Insert Results into the DQ_RULE_RESULTS Table
    ----------------------------------------------------------------------------------------------------

    v_step := ''INSERT_DQ_RESULTS_TABLE'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Loading results'');

    IF (v_error_message IS NULL) THEN
        BEGIN
        -- Prepare JSON strings
        LET details_json_str STRING := ''{"failed_records_table": "'' || COALESCE(v_failed_records_table, ''null'') || ''", "expected_type": "'' || COALESCE(v_type_expected, ''null'') || ''", "mostly": '' || COALESCE(v_allowed_deviation::STRING, ''null'') || '', "pk_columns_used": "'' || COALESCE(v_key_column_names, ''N/A'') || ''"}'';
        LET results_json_str STRING := ''{'' ||
            ''"element_count": '' || COALESCE(v_total::STRING, ''null'') || '','' ||
            ''"unexpected_count": '' || COALESCE(v_unexpected::STRING, ''null'') || '','' ||
            ''"unexpected_percent": '' || COALESCE(v_percent*100::STRING, ''null'') || '','' ||
            ''"failed_records_table": "'' || COALESCE(v_failed_records_table, ''null'') || ''",'' ||
            ''"observed_value": "'' || COALESCE(v_unexpected::STRING, ''null'') || '' out of '' || COALESCE(v_total::STRING, ''null'') || '' values were not castable to '' || v_type_expected || ''"'' ||
        ''}'';

        -- Use intermediate escaped strings to construct the final SQL reliably
        LET v_rule_str_for_sql STRING := REPLACE(COALESCE(v_input_rule_str, ''null''), '''''''', '''''''''''');
        LET v_results_json_for_sql STRING := REPLACE(results_json_str, '''''''', '''''''''''');
        LET v_details_json_for_sql STRING := REPLACE(details_json_str, '''''''', '''''''''''');
        
        v_sql := ''INSERT INTO "'' || v_dq_db_name || ''"."'' || v_dq_schema_name || ''".DQ_RULE_RESULTS (
            BATCH_ID, DATASET_RUN_ID, DATASET_ID, RULE_CONFIG_ID, EXPECTATION_ID, RUN_NAME, RUN_TIMESTAMP, DATASET_NAME,
            EXPECATION_CONFIG, IS_SUCCESS, RESULTS, EXPECTATION_NAME, DETAILS, ELEMENT_COUNT,
            UNEXPECTED_COUNT, UNEXPECTED_PERCENT, UNEXPECTED_PERCENT_NONMISSING, UNEXPECTED_PERCENT_TOTAL,
             FAILED_ROWS, DIMENSION
            )
            SELECT
            '' || COALESCE(v_batch_id::STRING, ''null'') || '', '' ||
            COALESCE(v_run_id::STRING, ''null'') || '', '' ||
            COALESCE(v_data_asset_id::STRING, ''null'') || '', '' ||
            COALESCE(v_check_config_id::STRING, ''null'') || '', '' ||
            COALESCE(v_expectation_id::STRING, ''null'') || '', '''''' || REPLACE(COALESCE(v_run_name, ''null''), '''''''', '''''''''''') || '''''', CURRENT_TIMESTAMP(), '''''' || REPLACE(COALESCE(v_data_asset_name, ''null''), '''''''', '''''''''''') || '''''', 
            PARSE_JSON('''''' || v_rule_str_for_sql || ''''''), '' || CASE WHEN v_status_code = v_success_code THEN ''TRUE'' ELSE ''FALSE'' END || '', 
            PARSE_JSON('''''' || v_results_json_for_sql || ''''''), '''''' || REPLACE(COALESCE(v_expectation_name, ''null''), '''''''', '''''''''''') || '''''', 
            PARSE_JSON('''''' || v_details_json_for_sql || ''''''), '' ||
            COALESCE(v_total::STRING, ''null'') || '', '' ||
            COALESCE(v_unexpected::STRING, ''null'') || '', '' ||
            COALESCE(v_percent*100::STRING, ''null'') || '', NULL::FLOAT, '' || COALESCE(v_percent*100::STRING, ''null'') || '', '' ||
            ''NULL::VARIANT, '''''' || COALESCE(RULE:DIMENSION, ''null'') || '''''''';

            EXECUTE IMMEDIATE v_sql;
        EXCEPTION
            WHEN OTHER THEN
                v_error_message := ''Failed to insert into DQ_RULE_RESULTS: '' || SQLERRM;
                v_status_code := v_execution_error;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
                RETURN v_status_code;
        END;
    END IF;

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Results is loaded'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    RETURN v_status_code;

EXCEPTION
    WHEN OTHER THEN
        BEGIN
            v_error_message := ''Global exception in step '' || COALESCE(:v_step, ''UNKNOWN'') || '': '' || SQLERRM;

            INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, END_TIMESTAMP, STATUS, ERROR_MESSAGE)
            VALUES (COALESCE(:v_run_id, -1), COALESCE(:v_check_config_id, -1), COALESCE(:v_procedure_name, ''SP_DATA_TYPE_CHECK''), COALESCE(:v_step, ''UNKNOWN''), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''FAILED'', :v_error_message);

        EXCEPTION WHEN OTHER THEN NULL;
        END;
        RETURN COALESCE(v_execution_error, 400);
END;
';;
