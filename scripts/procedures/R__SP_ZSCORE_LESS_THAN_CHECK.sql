USE DATABASE {{ snowflake_database }};
USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "SP_ZSCORE_LESS_THAN_CHECK"("RULE" VARIANT)
RETURNS NUMBER(38,0)
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
    v_procedure_name STRING DEFAULT ''SP_ZSCORE_LESS_THAN_CHECK'';
    v_input_rule_str STRING;
    v_log_message STRING;
    v_rows_inserted NUMBER DEFAULT 0;
    v_failed_rows_threshold INT DEFAULT 10000000;
    
    -- Expectation specific variables
    v_threshold FLOAT;
    v_double_sided BOOLEAN DEFAULT FALSE;
    v_column_mean FLOAT;
    v_column_stddev FLOAT;
    v_zscore_condition_pass STRING; 
    v_where_clause_condition STRING; 
    v_zscore_expression STRING;
    v_partial_unexpected_list VARIANT;
    v_unexpected_percent_total FLOAT DEFAULT 0;
    v_unexpected_percent_nonmissing FLOAT DEFAULT 0;
    v_missing_percent FLOAT DEFAULT 0;

    -- Dynamic Source Variables
    v_dataset_type STRING;
    v_sql_query STRING;
    v_from_clause STRING;
    
    -- Variables for failed row keys enhancement
    v_key_column_names STRING;
    v_pk_column_names STRING;
    v_ck_column_names STRING;
    v_key_construct_expr STRING;
    v_key_parts_list STRING;

    -- INCREMENTAL LOAD VARIABLES
    v_is_incremental BOOLEAN;
    v_incr_date_col_1 STRING;
    v_incr_date_col_2 STRING;
    v_last_validated_ts TIMESTAMP_NTZ;
    v_incremental_filter STRING DEFAULT ''''; 
    v_observed_value VARIANT;

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
        v_dataset_type := COALESCE(RULE:DATASET_TYPE::STRING, ''TABLE'');
        v_sql_query := RULE:CUSTOM_SQL::STRING;
        v_key_column_names := RULE:KEY_COLUMN_NAMES::STRING;

        v_kwargs_variant := PARSE_JSON(RULE:KWARGS);
        v_column_nm := v_kwargs_variant:column::STRING;
        v_threshold := v_kwargs_variant:threshold::FLOAT;
        v_double_sided := COALESCE(v_kwargs_variant:double_sided::BOOLEAN, FALSE);
        v_allowed_deviation := COALESCE(v_kwargs_variant:mostly::FLOAT, 1.0);
        v_failed_rows_cnt_limit := COALESCE(v_kwargs_variant:failed_row_count::NUMBER, 20); 

        -- Validation
        IF (v_column_nm IS NULL OR v_threshold IS NULL) THEN
            v_error_message := ''Required parameters (column, threshold) are missing.'';
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_input_rule_str WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        ELSEIF (UPPER(v_dataset_type) = ''TABLE'' AND (v_database_name IS NULL OR v_schema_name IS NULL OR v_table_name IS NULL)) THEN
            v_error_message := ''For DATASET_TYPE ''''TABLE'''', DATABASE_NAME, SCHEMA_NAME, and TABLE_NAME are required.'';
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_input_rule_str WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        ELSEIF (UPPER(v_dataset_type) = ''QUERY'' AND v_sql_query IS NULL) THEN
            v_error_message := ''For DATASET_TYPE ''''QUERY'''', CUSTOM_SQL is required.'';
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_input_rule_str WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        END IF;

        -- Define the source for the query (v_from_clause)
        IF (UPPER(v_dataset_type) = ''QUERY'') THEN
            v_from_clause := ''('' || v_sql_query || '') AS custom_query_source'';
        ELSE
            v_from_clause := ''"'' || v_database_name || ''"."'' || v_schema_name || ''"."'' || v_table_name || ''"'';
        END IF;
        
        -- Define the Z-score SQL expression template (will be filled with mean/stddev later)
        v_zscore_expression := '' (TRY_CAST("'' || v_column_nm || ''" AS FLOAT) - :v_column_mean) / NULLIF(:v_column_stddev, 0)'';

    EXCEPTION
        WHEN OTHER THEN
            v_error_message := ''Error parsing rule parameter: '' || SQLERRM;
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
    END;
    
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Input rule parsing completed.'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 3. Pass 1: Calculate Mean and Standard Deviation (used for Z-score calculation)
    v_step := ''CALCULATE_AGGREGATES'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Calculating Mean and StdDev'');

    v_sql := ''
        SELECT 
            AVG("'' || v_column_nm || ''") AS col_mean,
            STDDEV_SAMP("'' || v_column_nm || ''") AS col_stddev
        FROM '' || v_from_clause;

    BEGIN
        v_result := (EXECUTE IMMEDIATE v_sql);
        LET v_agg_cursor CURSOR FOR v_result;

        FOR agg_record IN v_agg_cursor DO
            v_column_mean := agg_record.COL_MEAN;
            v_column_stddev := agg_record.COL_STDDEV;
            BREAK;
        END FOR;
        
        IF (v_column_stddev IS NULL OR v_column_stddev = 0) THEN
            v_log_message := ''Skipping Z-score check: Column is constant or null.'';
            v_status_code := v_success_code;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        END IF;
        
    EXCEPTION
        WHEN OTHER THEN
            v_error_message := ''Error calculating mean/stddev: '' || SQLERRM;
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
    END;
    
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Mean: '' || :v_column_mean || '' StdDev: '' || :v_column_stddev WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 4. Pass 2: Calculate Z-scores and Count Violations
    v_step := ''MAIN_QUERY'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Counting Z-score violations'');
    
    -- 4a. Construct Incremental Filter
    IF (v_is_incremental = TRUE AND v_last_validated_ts IS NOT NULL) THEN
        LET v_incr_col STRING := COALESCE(v_incr_date_col_1, v_incr_date_col_2);
        IF (v_incr_col IS NOT NULL) THEN
            v_incremental_filter := '' AND "'' || v_incr_col || ''" > '''''' || v_last_validated_ts::STRING || '''''''';
        END IF;
    END IF;
    
    -- 4b. Define the success condition SQL based on double_sided flag
    IF (v_double_sided = TRUE) THEN
        -- Success if Z is in (-threshold, +threshold). Failure if ABS(Z) >= threshold
        v_zscore_condition_pass := '' ABS(zscore) < '' || v_threshold;
    ELSE
        -- Success if Z is in (-infinity, +threshold). Failure if Z >= threshold
        v_zscore_condition_pass := '' zscore < '' || v_threshold;
    END IF;
    
    v_where_clause_condition := '' NOT ('' || v_zscore_condition_pass || '')'';

    v_sql := ''WITH ZScores AS (
                SELECT
                    *,
                    '' || REPLACE(REPLACE(v_zscore_expression, '':v_column_mean'', v_column_mean), '':v_column_stddev'', v_column_stddev) || '' AS zscore
                FROM '' || v_from_clause || ''
                WHERE "'' || v_column_nm || ''" IS NOT NULL -- Exclude NULLs
                '' || v_incremental_filter || ''
            )
            SELECT
                COUNT(*) AS total_count, -- Evaluatable rows in the incremental set
                COUNT_IF(zscore IS NULL) AS missing_count, -- Rows that failed TRY_CAST
                COUNT_IF('' || v_where_clause_condition || '') AS unexpected_count
            FROM ZScores'';

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
            LET element_count INT := v_total; 
            v_unexpected_percent_total := CASE WHEN element_count = 0 THEN 0.0 ELSE (v_unexpected::FLOAT) / element_count END;
            v_percent := v_unexpected_percent_total; 

            v_status_code := CASE WHEN v_percent <= (1 - v_allowed_deviation) THEN v_success_code ELSE v_failed_code END;

            v_observed_value := OBJECT_CONSTRUCT(''mean'', v_column_mean, ''stddev'', v_column_stddev);
            
        EXCEPTION
            WHEN OTHER THEN
                v_error_message := ''Error in main query execution: '' || SQLERRM || '' SQL: '' || v_sql;
                v_status_code := v_execution_error;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
                RETURN v_status_code;
        END;
    END IF;

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Validation done. Outliers found: '' || :v_unexpected WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 5. Capture Failed Row Keys
    v_step := ''CAPTURE_FAILED_KEYS'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Processing failed row keys'');
    
    IF (v_unexpected > 0 AND UPPER(v_dataset_type) = ''TABLE'') THEN
        BEGIN
            v_sql := ''SELECT PRIMARY_KEY_COLUMNS, CANDIDATE_KEY_COLUMNS FROM DQ_DATASET WHERE DATASET_ID = '' || v_data_asset_id;
            v_result := (EXECUTE IMMEDIATE v_sql);
            LET v_pk_cursor CURSOR FOR v_result;
            FOR pk_record IN v_pk_cursor DO
                v_pk_column_names := ARRAY_TO_STRING(PARSE_JSON(pk_record.PRIMARY_KEY_COLUMNS):primary_key, '','');
                v_ck_column_names := pk_record.CANDIDATE_KEY_COLUMNS;
                BREAK;
            END FOR;

            IF (v_pk_column_names IS NOT NULL AND TRIM(v_pk_column_names) != '''') THEN
                v_key_column_names := v_pk_column_names;
            ELSEIF (v_ck_column_names IS NOT NULL AND TRIM(v_ck_column_names) != '''') THEN
                v_key_column_names := v_ck_column_names;
            ELSE
                v_key_column_names := NULL;
            END IF;

            IF (v_key_column_names IS NOT NULL) THEN
                SELECT LISTAGG('''''''' || TRIM(value) || '''''''' || '', '' || TRIM(value), '', '') WITHIN GROUP (ORDER BY seq)
                INTO v_key_parts_list
                FROM TABLE(SPLIT_TO_TABLE(:v_key_column_names, '',''));
                v_key_construct_expr := ''OBJECT_CONSTRUCT('' || v_key_parts_list || '')'';
                
                v_sql := ''WITH ZScores AS (
                            SELECT 
                                '' || v_key_construct_expr || '' AS failed_key,
                                '' || REPLACE(REPLACE(v_zscore_expression, '':v_column_mean'', v_column_mean), '':v_column_stddev'', v_column_stddev) || '' AS zscore
                            FROM '' || v_from_clause || ''
                            WHERE "'' || v_column_nm || ''" IS NOT NULL 
                            '' || v_incremental_filter || ''
                          )
                          INSERT INTO DQ_FAILED_ROW_KEYS (DATASET_RUN_ID, RULE_CONFIG_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, FAILED_KEY)
                          SELECT '' || v_run_id || '', '' || v_check_config_id || '', '''''' || v_database_name || '''''', '''''' || v_schema_name || '''''', '''''' || v_table_name || '''''', failed_key
                          FROM ZScores
                          WHERE '' || v_where_clause_condition || '' LIMIT '' || v_failed_rows_cnt_limit;
                
                EXECUTE IMMEDIATE v_sql;
                v_rows_inserted := SQLROWCOUNT;
                v_log_message := v_rows_inserted || '' keys of failed rows captured.'';
            ELSE
                v_log_message := ''No Key found. Skipping failed key capture.'';
            END IF;
        EXCEPTION
            WHEN OTHER THEN
                v_error_message := ''Error capturing failed row keys: '' || SQLERRM;
                v_status_code := v_execution_error;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
                RETURN v_status_code;
        END;
    ELSE
        v_log_message := ''Skipping failed key capture (QUERY source or no outliers).'';
    END IF;
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 6. Handle and log failed records (UPDATED: STRUCTURED TABLE CAPTURE)
    v_step := ''INSERT_FAILED_RECORDS'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Processing failed records'');

    IF (v_unexpected > 0) THEN
        BEGIN
            -- 1. Construct standard failure table name: <DATASET_NAME>_DQ_FAILURE
            v_clean_dataset_name := REGEXP_REPLACE(v_data_asset_name, ''[^a-zA-Z0-9]'', ''_'');
            v_failed_records_table := v_clean_dataset_name || ''_DQ_FAILURE'';
            v_full_target_table_name := ''"'' || v_dq_db_name || ''"."'' || v_dq_schema_name || ''"."'' || v_failed_records_table || ''"'';

            -- 2. Create Failure Table IF NOT EXISTS (Append Audit Columns Only)
            v_sql := ''CREATE TABLE IF NOT EXISTS '' || v_full_target_table_name || '' AS '' ||
                     ''SELECT '' ||
                     v_run_id || ''::NUMBER(38,0) AS DATASET_RUN_ID, '' ||
                     v_data_asset_id || ''::NUMBER(38,0) AS DATASET_ID, '' ||
                     v_check_config_id || ''::NUMBER(38,0) AS RULE_CONFIG_ID, '' ||
                     ''CURRENT_TIMESTAMP()::TIMESTAMP_LTZ AS DQ_LOAD_TIMESTAMP, '' ||
                     '' * '' || -- All source columns first
                     '' FROM '' || v_from_clause || '' WHERE 1=0'';
            
            EXECUTE IMMEDIATE v_sql;

            -- 3. Insert Failed Records
            -- ALIGNMENT: 
            -- * from ZScores selects Source Cols + zscore_calc
            -- We EXCLUDE zscore_calc to match the table definition
            v_sql := ''INSERT INTO '' || v_full_target_table_name || '' '' ||
                     ''WITH ZScores AS (
                        SELECT 
                            *,
                            '' || REPLACE(REPLACE(v_zscore_expression, '':v_column_mean'', v_column_mean), '':v_column_stddev'', v_column_stddev) || '' AS zscore
                        FROM '' || v_from_clause || ''
                        WHERE "'' || v_column_nm || ''" IS NOT NULL 
                        '' || v_incremental_filter || ''
                     )
                     SELECT '' ||
                     v_run_id || '', '' ||
                     v_data_asset_id || '', '' ||
                     v_check_config_id || '', '' ||
                     ''CURRENT_TIMESTAMP() '' || '', '' ||
                     '' * EXCLUDE(zscore) '' || -- Select source columns (excluding the calc)
                     '' FROM ZScores'' || 
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

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 7. Insert Results into the DQ_RULE_RESULTS Table
    v_step := ''INSERT_DQ_RESULTS_TABLE'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Loading results'');

    IF (v_error_message IS NULL) THEN
        LET details_json_str STRING := OBJECT_CONSTRUCT(
            ''threshold'', v_threshold,
            ''double_sided'', v_double_sided,
            ''column_mean'', v_column_mean,
            ''column_stddev'', v_column_stddev,
            ''failed_records_table'', v_failed_records_table
        )::STRING;
        
        LET results_json_str STRING := OBJECT_CONSTRUCT(
            ''element_count'', v_total,
            ''unexpected_count'', v_unexpected,
            ''unexpected_percent'', v_percent * 100,
            ''missing_count'', v_missing_count,
            ''missing_percent'', v_missing_count::FLOAT / NULLIF(v_total, 0) * 100,
            ''observed_value'', v_observed_value,
            ''partial_unexpected_list'', v_partial_unexpected_list,
            ''failed_records_table'', v_failed_records_table
        )::STRING;
        BEGIN
            v_sql := ''INSERT INTO "'' || v_dq_db_name || ''"."'' || v_dq_schema_name || ''".DQ_RULE_RESULTS (
                BATCH_ID, DATASET_RUN_ID, DATASET_ID, RULE_CONFIG_ID, EXPECTATION_ID, RUN_NAME, RUN_TIMESTAMP, DATASET_NAME,
                EXPECATION_CONFIG, IS_SUCCESS, RESULTS, EXPECTATION_NAME, DETAILS, ELEMENT_COUNT, MISSING_COUNT, UNEXPECTED_COUNT, OBSERVED_VALUE
                )
                SELECT
                '' || COALESCE(v_batch_id::STRING, ''null'') || '', '' || COALESCE(v_run_id::STRING, ''null'') || '', '' || COALESCE(v_data_asset_id::STRING, ''null'') || '', '' ||
                COALESCE(v_check_config_id::STRING, ''null'') || '', '' || COALESCE(v_expectation_id::STRING, ''null'') || '', '''''' || REPLACE(COALESCE(v_run_name, ''null''), '''''''', '''''''''''') || '''''', CURRENT_TIMESTAMP(), '''''' || REPLACE(COALESCE(v_data_asset_name, ''null''), '''''''', '''''''''''') || '''''', PARSE_JSON('''''' || REPLACE(COALESCE(RULE::STRING, ''null''), '''''''', '''''''''''') || ''''''), '' || CASE WHEN v_status_code = v_success_code THEN ''TRUE'' ELSE ''FALSE'' END || '', PARSE_JSON('''''' || REPLACE(results_json_str, '''''''', '''''''''''') || ''''''), '''''' || REPLACE(COALESCE(v_expectation_name, ''null''), '''''''', '''''''''''') || '''''', PARSE_JSON('''''' || REPLACE(details_json_str, '''''''', '''''''''''') || ''''''), '' ||
                COALESCE(v_total::STRING, ''null'') || '', '' || COALESCE(v_missing_count::STRING, ''null'') || '', '' || COALESCE(v_unexpected::STRING, ''null'') || 
                '', PARSE_JSON('''''' || REPLACE(v_observed_value::STRING,'''''''', '''''''''''') ||'''''') '';
            
            EXECUTE IMMEDIATE v_sql;
    
        EXCEPTION
            WHEN OTHER THEN
                v_error_message := ''Failed to insert into DQ_RULE_RESULTS: '' || SQLERRM;
                v_status_code := v_execution_error;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
                RETURN v_status_code;
        END;
    END IF;
    
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Results loaded'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    RETURN v_status_code;

EXCEPTION
    WHEN OTHER THEN
        BEGIN
            v_error_message := ''Global exception in step '' || COALESCE(v_step, ''UNKNOWN'') || '': '' || SQLERRM;
            INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, END_TIMESTAMP, STATUS, ERROR_MESSAGE)
            VALUES (COALESCE(v_run_id, -1), COALESCE(v_check_config_id, -1), COALESCE(v_procedure_name, ''SP_ZSCORE_LESS_THAN_CHECK''), COALESCE(v_step, ''UNKNOWN''), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''FAILED'', v_error_message);
        END;
        RETURN COALESCE(v_execution_error, 400);
END;
';;
