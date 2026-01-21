USE DATABASE {{ snowflake_database }};
USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "SP_UNIQUE_VALUES_WITHIN_RECORD_CHECK"("RULE" VARIANT)
RETURNS NUMBER(38, 0)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    -- Standard Framework Variables
    v_sql TEXT;
    v_result RESULTSET;
    v_total INT DEFAULT 0;
    v_missing_count INT DEFAULT 0;
    v_unexpected INT DEFAULT 0;
    v_percent FLOAT DEFAULT 0;
    v_status_code NUMBER;
    v_allowed_deviation FLOAT DEFAULT 0;
    v_error_message STRING;
    v_step STRING DEFAULT ''INITIALIZATION'';
    v_run_id NUMBER DEFAULT -1;
    v_column_list_array ARRAY;
    v_column_list_str STRING;
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
    v_procedure_name STRING DEFAULT ''SP_UNIQUE_VALUES_WITHIN_RECORD_CHECK'';
    v_stage_path STRING;
    v_input_rule_str STRING;
    v_log_message STRING;
    v_rows_inserted NUMBER DEFAULT 0;
    v_failed_rows_threshold INT DEFAULT 10000000;
    
    -- Expectation specific variables
    v_ignore_row_if STRING DEFAULT ''NEVER'';
    v_ignore_condition STRING;
    v_unexpected_row_condition STRING;
    v_where_clause_condition STRING;
    v_non_ignored_count_expr STRING;
    v_unexpected_percent_total FLOAT DEFAULT 0;
    v_unexpected_percent_nonmissing FLOAT DEFAULT 0;
    v_missing_percent FLOAT DEFAULT 0;
    v_partial_unexpected_list VARIANT;
    
    -- Variables for dynamic source
    v_dataset_type STRING;
    v_sql_query STRING;
    v_from_clause STRING;
    
    -- Variables for failed row keys enhancement
    v_key_column_names STRING;
    v_pk_column_names STRING;
    v_ck_column_names STRING;
    v_key_construct_expr STRING;
    v_key_parts_list STRING;
    v_observed_value VARIANT;

    -- INCREMENTAL LOAD VARIABLES
    v_is_incremental BOOLEAN;
    v_incr_date_col_1 STRING;
    v_incr_date_col_2 STRING;
    v_last_validated_ts TIMESTAMP_NTZ;
    v_incremental_filter STRING DEFAULT '''';

    -- DYNAMIC TABLE VARIABLES
    v_clean_dataset_name STRING;
    v_full_target_table_name STRING;

BEGIN
    v_input_rule_str := TO_VARCHAR(RULE);

    ----------------------------------------------------------------------------------------------------
    -- 1. Load configuration
    v_step := ''CONFIG_LOADING'';
    v_procedure_name := COALESCE(RULE:PROCEDURE_NAME::STRING, v_procedure_name);
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
    
    ----------------------------------------------------------------------------------------------------
    -- 2. Parse and validate the rule parameter
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
        v_batch_id := COALESCE(RULE:BATCH_ID::NUMBER, -1);
        v_run_id := COALESCE(RULE:DATASET_RUN_ID::NUMBER, -1);
        v_data_asset_id := COALESCE(RULE:DATASET_ID::NUMBER, -1);
        v_check_config_id := COALESCE(RULE:RULE_CONFIG_ID::NUMBER, -1);
        v_expectation_id := COALESCE(RULE:EXPECTATION_ID::NUMBER, -1);
        v_database_name := RULE:DATABASE_NAME::STRING;
        v_schema_name := RULE:SCHEMA_NAME::STRING;
        v_table_name := RULE:TABLE_NAME::STRING;
        v_run_name := RULE:RUN_NAME::STRING;
        v_data_asset_name := RULE:DATASET_NAME::STRING;
        v_expectation_name := RULE:EXPECTATION_NAME::STRING;
        v_dataset_type := RULE:DATASET_TYPE::STRING;
        v_sql_query := RULE:CUSTOM_SQL::STRING;
        v_key_column_names := RULE:KEY_COLUMN_NAMES::STRING;

        v_kwargs_variant := PARSE_JSON(RULE:KWARGS);
        LET raw_column_list_variant VARIANT := v_kwargs_variant:column_list;
        
        v_allowed_deviation := COALESCE(v_kwargs_variant:mostly::FLOAT, 1.0);
        v_failed_rows_cnt_limit := COALESCE(v_kwargs_variant:failed_row_count::NUMBER, 20); 
        v_ignore_row_if := UPPER(COALESCE(v_kwargs_variant:ignore_row_if::STRING, ''NEVER''));
        v_procedure_name := COALESCE(RULE:PROCEDURE_NAME::STRING, v_procedure_name);

        v_is_incremental := COALESCE(RULE:IS_INCREMENTAL::BOOLEAN, FALSE);
        v_incr_date_col_1 := RULE:INCR_DATE_COLUMN_1::STRING;
        v_incr_date_col_2 := RULE:INCR_DATE_COLUMN_2::STRING;
        v_last_validated_ts := RULE:LAST_VALIDATED_TIMESTAMP::TIMESTAMP_NTZ;

        -- Robustly determine v_column_list_array
        IF (TYPEOF(raw_column_list_variant) = ''STRING'') THEN
            v_sql := ''SELECT ARRAY_AGG(TRIM(T.VALUE)) FROM TABLE(SPLIT_TO_TABLE('''''' || raw_column_list_variant::STRING || '''''', '''','''')) T'';
            v_result := (EXECUTE IMMEDIATE v_sql);
            v_column_list_array := (SELECT "$1" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
        ELSE
            v_column_list_array := raw_column_list_variant::ARRAY;
        END IF;

        -- Validation
        IF (v_column_list_array IS NULL OR ARRAY_SIZE(v_column_list_array) < 2) THEN
            v_error_message := ''Required rule parameter column_list is missing or contains less than two columns.'';
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_input_rule_str WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        ELSEIF (UPPER(v_dataset_type) = ''TABLE'' AND (v_database_name IS NULL OR v_schema_name IS NULL OR v_table_name IS NULL)) THEN
            v_error_message := ''For DATASET_TYPE ''''TABLE'''', DATABASE_NAME, SCHEMA_NAME, and TABLE_NAME are required.'';
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_input_rule_str WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        ELSEIF (UPPER(v_dataset_type) = ''QUERY'' AND v_sql_query IS NULL) THEN
            v_error_message := ''For DATASET_TYPE ''''QUERY'''', a CUSTOM_SQL is required.'';
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_input_rule_str WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        END IF;
        
        -- 1. Build the comma-separated list of quoted columns
        v_column_list_str := '''';
        LET null_check_parts ARRAY := ARRAY_CONSTRUCT();

        FOR i IN 0 TO ARRAY_SIZE(v_column_list_array) - 1 DO
            LET col_name STRING := v_column_list_array[i]::STRING;
            LET quoted_col_name STRING := ''"'' || col_name || ''"'';
            
            IF (i > 0) THEN
                v_column_list_str := v_column_list_str || '', '';
            END IF;
            v_column_list_str := v_column_list_str || quoted_col_name;
            
            null_check_parts := ARRAY_APPEND(null_check_parts, quoted_col_name || '' IS NULL'');
        END FOR;
        
        -- 2. Build the final SQL ignore condition
        v_ignore_condition := CASE 
            WHEN v_ignore_row_if = ''NEVER'' THEN ''FALSE''
            WHEN v_ignore_row_if = ''ANY_VALUE_IS_MISSING'' THEN ARRAY_TO_STRING(null_check_parts, '' OR '')
            WHEN v_ignore_row_if = ''ALL_VALUES_ARE_MISSING'' THEN ARRAY_TO_STRING(null_check_parts, '' AND '')
            ELSE ''FALSE''
        END;

        -- 3. Build the horizontal uniqueness check (Core logic)
        LET total_columns_count INT := ARRAY_SIZE(v_column_list_array);
        LET v_column_array_construct STRING := ''ARRAY_CONSTRUCT('' || v_column_list_str || '')'';

        -- Failure Logic: If distinct count of values < total count of values, duplicates exist in row
        v_unexpected_row_condition := ''
            ARRAY_SIZE(ARRAY_DISTINCT('' || v_column_array_construct || '')) < '' || total_columns_count;
        
        v_where_clause_condition := ''NOT ('' || v_ignore_condition || '') AND ('' || v_unexpected_row_condition || '')'';
        v_non_ignored_count_expr := ''COUNT_IF(NOT ('' || v_ignore_condition || ''))'';

    EXCEPTION
        WHEN OTHER THEN
            v_error_message := ''Error parsing rule parameter: '' || SQLERRM;
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_input_rule_str WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
    END;

    -- Dynamically build the FROM clause
    IF (UPPER(v_dataset_type) = ''QUERY'') THEN
        v_from_clause := ''('' || v_sql_query || '') AS custom_query_source'';
    ELSE
        v_from_clause := ''"'' || v_database_name || ''"."'' || v_schema_name || ''"."'' || v_table_name || ''"'';
    END IF;

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Input rule parsing completed.'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 3. Construct Incremental Filter
    v_step := ''CONSTRUCT_INCREMENTAL_FILTER'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Checking for incremental load logic'');

    IF (v_is_incremental = TRUE AND v_last_validated_ts IS NOT NULL) THEN
        LET v_incr_col STRING := COALESCE(v_incr_date_col_1, v_incr_date_col_2);
        IF (v_incr_col IS NOT NULL) THEN
            v_incremental_filter := '' AND "'' || v_incr_col || ''" > '''''' || v_last_validated_ts::STRING || '''''''';
            v_log_message := ''Incremental filter applied on '' || v_incr_col || '' > '' || v_last_validated_ts::STRING;
        ELSE
            v_log_message := ''Incremental enabled, but no INCR_DATE_COLUMN found. Executing full load for DQ.'';
        END IF;
    ELSE
        v_log_message := ''Not an incremental run. Executing full load for DQ.'';
    END IF;
    
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 4. Execute the main data quality check query
    v_step := ''MAIN_QUERY'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Starting horizontal uniqueness check query'');

    v_sql := ''SELECT
                '' || v_non_ignored_count_expr || '' AS total_count,
                COUNT_IF('' || v_ignore_condition || '') AS missing_count,
                COUNT_IF('' || v_where_clause_condition || '') AS unexpected_count
              FROM '' || v_from_clause || '' 
              WHERE 1=1 '' || v_incremental_filter;
              
    IF (v_error_message IS NULL) THEN
        BEGIN
            v_result := (EXECUTE IMMEDIATE v_sql);
            LET v_cursor CURSOR FOR v_result;
            
            FOR record IN v_cursor DO
                v_total := COALESCE(record.total_count, 0);
                v_missing_count := COALESCE(record.missing_count, 0);
                v_unexpected := COALESCE(record.unexpected_count, 0);
                BREAK;
            END FOR;
            
            -- Calculations
            LET total_population INT := v_total + v_missing_count;
            v_missing_percent := CASE WHEN total_population = 0 THEN 0.0 ELSE (v_missing_count::FLOAT / total_population) END;
            LET non_missing_total INT := v_total; 
            v_unexpected_percent_nonmissing := CASE WHEN non_missing_total = 0 THEN 0.0 ELSE (v_unexpected::FLOAT) / non_missing_total END;
            v_unexpected_percent_total := CASE WHEN total_population = 0 THEN 0.0 ELSE (v_unexpected::FLOAT) / total_population END;
            
            v_percent := v_unexpected_percent_total; 
            v_status_code := CASE WHEN v_percent <= (1 - v_allowed_deviation) THEN v_success_code ELSE v_failed_code END;
            
            -- 5. Get Partial Unexpected List (Limit 20)
            LET v_object_construct_str STRING := ''OBJECT_CONSTRUCT('' || 
                (SELECT LISTAGG('''''''''''''''' || TRIM(value) || '''''''''''''''' || '', '' || TRIM(value), '', '') WITHIN GROUP (ORDER BY SEQ) FROM TABLE(FLATTEN(input => :v_column_list_array))) || 
            '')'';

            v_sql := ''SELECT ARRAY_AGG('' || v_object_construct_str || '') AS PARTIAL_LIST FROM '' || v_from_clause || '' WHERE '' || v_where_clause_condition || COALESCE(v_incremental_filter, '''') || '' LIMIT 20'';
            
            LET v_partial_result RESULTSET := (EXECUTE IMMEDIATE v_sql);
            LET v_partial_cursor CURSOR FOR v_partial_result;
            FOR partial_record IN v_partial_cursor DO
                v_partial_unexpected_list := partial_record.PARTIAL_LIST;
                BREAK;
            END FOR;
            
        EXCEPTION
            WHEN OTHER THEN
                v_error_message := ''Error in main query execution: '' || SQLERRM || '' SQL: '' || v_sql;
                v_status_code := v_execution_error;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
                RETURN v_status_code;
        END;
    END IF;

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Validation done. Violations found: '' || :v_unexpected WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 5. Capture Failed Row Keys (Capture rows where horizontal uniqueness failed)
    v_step := ''CAPTURE_FAILED_KEYS'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Processing failed row keys'');
    
    IF (v_unexpected > 0 AND UPPER(v_dataset_type) = ''TABLE'') THEN
        BEGIN
            -- 1. Try to get PK from Metadata
            v_sql := ''SELECT PRIMARY_KEY_COLUMNS, CANDIDATE_KEY_COLUMNS FROM DQ_DATASET WHERE DATASET_ID = '' || v_data_asset_id;
            v_result := (EXECUTE IMMEDIATE v_sql);
            LET v_pk_cursor CURSOR FOR v_result;
            FOR pk_record IN v_pk_cursor DO
                v_pk_column_names := ARRAY_TO_STRING(PARSE_JSON(pk_record.PRIMARY_KEY_COLUMNS):primary_key::ARRAY, '','');
                BREAK;
            END FOR;

            -- 2. Determine Key Columns
            IF (v_pk_column_names IS NOT NULL AND TRIM(v_pk_column_names) != '''') THEN
                v_key_column_names := v_pk_column_names;
                v_log_message := ''Using Primary Key (''||v_key_column_names||'') for failed row key capture.'';
            ELSE
                v_key_column_names := REPLACE(v_column_list_str, ''"'', ''''); 
                v_log_message := ''No Primary Key found. Using checked columns (''||v_key_column_names||'') as the key.'';
            END IF;

            -- 3. Build Key Construct
            SELECT LISTAGG('''''''' || TRIM(value) || '''''''' || '', "'' || TRIM(value) || ''"'', '', '') WITHIN GROUP (ORDER BY seq)
            INTO v_key_parts_list
            FROM TABLE(SPLIT_TO_TABLE(:v_key_column_names, '',''));
            
            v_key_construct_expr := ''OBJECT_CONSTRUCT('' || v_key_parts_list || '')'';
            
            -- 4. Execute Insert (Select only failing rows)
            v_sql := ''INSERT INTO DQ_FAILED_ROW_KEYS (DATASET_RUN_ID, RULE_CONFIG_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, FAILED_KEY)
                      SELECT '' || v_run_id || '', '' || v_check_config_id || '', '''''' || v_database_name || '''''', '''''' || v_schema_name || '''''', '''''' || v_table_name || '''''', 
                             '' || v_key_construct_expr || ''
                      FROM '' || v_from_clause || ''
                      WHERE '' || v_where_clause_condition || COALESCE(v_incremental_filter, '''') ||
                      CASE WHEN v_failed_rows_cnt_limit > 0 THEN '' LIMIT '' || v_failed_rows_cnt_limit ELSE '''' END;
            
            EXECUTE IMMEDIATE v_sql;
            v_rows_inserted := SQLROWCOUNT;
            v_log_message := v_rows_inserted || '' keys of failed rows captured. '' || v_log_message;

        EXCEPTION
            WHEN OTHER THEN
                v_error_message := ''Error capturing failed row keys: '' || SQLERRM;
                v_status_code := v_execution_error;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
                RETURN v_status_code;
        END;
    ELSE
        v_log_message := ''No failed rows or source is QUERY type. Skipping failed key capture.'';
    END IF;
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 6. Handle and log failed records (Dynamic Table)
    v_step := ''INSERT_FAILED_RECORDS'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Processing failed records'');

    IF (v_unexpected > 0) THEN
        BEGIN
            -- 1. Construct standard failure table name
            LET v_clean_dataset_name STRING := REGEXP_REPLACE(v_data_asset_name, ''[^a-zA-Z0-9]'', ''_'');
            v_failed_records_table := v_clean_dataset_name || ''_DQ_FAILURE'';
            LET v_full_target_table_name STRING := ''"'' || v_dq_db_name || ''"."'' || v_dq_schema_name || ''"."'' || v_failed_records_table || ''"'';

            -- 2. Create Failure Table IF NOT EXISTS
            v_sql := ''CREATE TABLE IF NOT EXISTS '' || v_full_target_table_name || '' AS '' ||
                     ''SELECT '' ||
                     v_run_id || ''::NUMBER(38,0) AS DATASET_RUN_ID, '' ||
                     v_data_asset_id || ''::NUMBER(38,0) AS DATASET_ID, '' ||
                     v_check_config_id || ''::NUMBER(38,0) AS RULE_CONFIG_ID, '' ||
                     ''CURRENT_TIMESTAMP()::TIMESTAMP_LTZ AS DQ_LOAD_TIMESTAMP, '' ||
                     '' * FROM '' || v_from_clause || '' WHERE 1=0'';
            EXECUTE IMMEDIATE v_sql;

            -- 3. Insert Failed Records
            IF (v_unexpected < v_failed_rows_threshold OR (v_failed_rows_cnt_limit IS NOT NULL AND v_failed_rows_cnt_limit > 0)) THEN
                v_sql := ''INSERT INTO '' || v_full_target_table_name || '' '' ||
                          ''SELECT '' ||
                          v_run_id || '', '' ||
                          v_data_asset_id || '', '' ||
                          v_check_config_id || '', '' ||
                          ''CURRENT_TIMESTAMP(), '' ||
                          '' * FROM '' || v_from_clause || '' '' ||
                          ''WHERE '' || v_where_clause_condition || COALESCE(v_incremental_filter, '''') || 
                          CASE WHEN v_failed_rows_cnt_limit > 0 THEN '' LIMIT '' || v_failed_rows_cnt_limit ELSE '''' END;

                EXECUTE IMMEDIATE v_sql;
                v_rows_inserted := SQLROWCOUNT;
                v_log_message := v_rows_inserted || '' rows inserted directly into failed records table: '' || v_failed_records_table;
            ELSE
                v_log_message := ''Skipping full failed record capture due to large volume.'';
            END IF;
        EXCEPTION
            WHEN OTHER THEN
                v_error_message := ''Failed to insert data into failure table: '' || SQLERRM;
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
    -- 7. Insert results into the DQ_RULE_RESULTS table
    v_step := ''INSERT_DQ_RESULTS_TABLE'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Loading results'');

    IF (v_error_message IS NULL) THEN
        BEGIN
            LET v_details_obj VARIANT := OBJECT_CONSTRUCT(
                ''column_list'', v_column_list_array,
                ''ignore_row_if'', v_ignore_row_if
            );
            LET v_results_obj VARIANT := OBJECT_CONSTRUCT(
                ''element_count'', v_total,
                ''unexpected_count'', v_unexpected,
                ''unexpected_percent'', v_percent * 100,
                ''missing_count'', v_missing_count,
                ''missing_percent'', v_missing_percent * 100,
                ''failed_records_table'', v_failed_records_table
            );

            LET v_safe_details_str STRING := REPLACE(REPLACE(TO_JSON(v_details_obj), ''\\\\'', ''\\\\\\\\''), ''\\'''', ''\\''\\'''');
            LET v_safe_results_str STRING := REPLACE(REPLACE(TO_JSON(v_results_obj), ''\\\\'', ''\\\\\\\\''), ''\\'''', ''\\''\\'''');
            LET v_safe_rule_str STRING := REPLACE(REPLACE(COALESCE(RULE::STRING, ''null''), ''\\\\'', ''\\\\\\\\''), ''\\'''', ''\\''\\'''');
            LET v_safe_run_name STRING := REPLACE(COALESCE(v_run_name, ''null''), ''\\'''', ''\\''\\'''');
            LET v_safe_dataset_name STRING := REPLACE(COALESCE(v_data_asset_name, ''null''), ''\\'''', ''\\''\\'''');
            LET v_safe_expectation_name STRING := REPLACE(COALESCE(v_expectation_name, ''null''), ''\\'''', ''\\''\\'''');

            LET v_observed_value_sql_inject STRING := ''NULL::VARIANT'';

            v_sql := ''INSERT INTO "'' || v_dq_db_name || ''"."'' || v_dq_schema_name || ''".DQ_RULE_RESULTS (
                BATCH_ID, DATASET_RUN_ID, DATASET_ID, RULE_CONFIG_ID, EXPECTATION_ID, RUN_NAME, RUN_TIMESTAMP, DATASET_NAME,
                EXPECATION_CONFIG, IS_SUCCESS, RESULTS, EXPECTATION_NAME, DETAILS, ELEMENT_COUNT, MISSING_COUNT, MISSING_PERCENT,
                OBSERVED_VALUE, UNEXPECTED_COUNT, UNEXPECTED_PERCENT, UNEXPECTED_PERCENT_TOTAL, FAILED_ROWS
                )
                SELECT
                '' || COALESCE(v_batch_id::STRING, ''null'') || '', '' ||
                COALESCE(v_run_id::STRING, ''null'') || '', '' ||
                COALESCE(v_data_asset_id::STRING, ''null'') || '', '' ||
                COALESCE(v_check_config_id::STRING, ''null'') || '', '' ||
                COALESCE(v_expectation_id::STRING, ''null'') || '', \\'''' || v_safe_run_name || ''\\'', CURRENT_TIMESTAMP(), \\'''' || v_safe_dataset_name || ''\\'', 
                PARSE_JSON(\\'''' || v_safe_rule_str || ''\\''), '' || 
                CASE WHEN v_status_code = v_success_code THEN ''TRUE'' ELSE ''FALSE'' END || '', 
                PARSE_JSON(\\'''' || v_safe_results_str || ''\\''), 
                \\'''' || v_safe_expectation_name || ''\\'', 
                PARSE_JSON(\\'''' || v_safe_details_str || ''\\''), '' ||
                COALESCE(v_total::STRING, ''null'') || '', '' ||
                COALESCE(v_missing_count::STRING, ''null'') || '', '' ||
                COALESCE(v_missing_percent * 100::STRING, ''null'') || '', '' ||
                v_observed_value_sql_inject || '', '' ||
                COALESCE(v_unexpected::STRING, ''null'') || '', '' ||
                COALESCE(v_percent * 100::STRING, ''null'') || '', '' ||
                COALESCE(v_unexpected_percent_total * 100::STRING, ''null'') || '', NULL::VARIANT'';

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
            v_error_message := ''Global exception in step '' || COALESCE(v_step, ''UNKNOWN'') || '': '' || SQLERRM;

            INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, END_TIMESTAMP, STATUS, ERROR_MESSAGE)
            VALUES (COALESCE(v_run_id, -1), COALESCE(v_check_config_id, -1), COALESCE(v_procedure_name, ''SP_UNIQUE_VALUES_WITHIN_RECORD_CHECK''), COALESCE(v_step, ''UNKNOWN''), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''FAILED'', v_error_message);

        EXCEPTION WHEN OTHER THEN NULL;
        END;
        RETURN COALESCE(v_execution_error, 400);
END;
';;
