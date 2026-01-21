USE DATABASE {{ snowflake_database }};

USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "SP_TABLE_COLUMNS_MATCH_SET_CHECK"("RULE" VARIANT)
RETURNS NUMBER(38,0)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    -- Standard Framework Variables
    v_sql TEXT;
    v_result RESULTSET;
    v_total INT DEFAULT 0; -- Total expected columns
    v_unexpected INT DEFAULT 0; -- Number of columns that did not match (total mismatches)
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
    v_input_rule_str STRING;
    v_log_message STRING;
    v_rows_inserted NUMBER DEFAULT 0;
    v_failed_rows_threshold INT DEFAULT 10000000;
    
    -- Check specific variables
    v_exact_match BOOLEAN;
    v_dataset_type STRING;
    v_sql_query STRING;
    
    -- Metadata/Result variables
    v_actual_columns ARRAY;
    v_expected_columns ARRAY; -- Final processed array of expected columns
    v_unexpected_columns ARRAY DEFAULT ARRAY_CONSTRUCT();
    v_missing_columns ARRAY DEFAULT ARRAY_CONSTRUCT();
    v_comparison_set_expected ARRAY;
    v_comparison_set_actual ARRAY;
    v_actual_total INT DEFAULT 0;
    v_set_mismatches INT DEFAULT 0;
    v_success_flag BOOLEAN DEFAULT FALSE;
    
    -- Variables for DQ_RULE_RESULTS table compatibility (mostly NULL for this check)
    v_observed_value VARIANT;
    v_missing_count INT DEFAULT 0;
    v_missing_percent FLOAT DEFAULT 0;
    v_unexpected_percent_nonmissing FLOAT DEFAULT 0;
    v_unexpected_percent_total FLOAT DEFAULT 0;
    v_partial_unexpected_list VARIANT;
    v_unexpected_rows VARIANT;
    
    -- Variables to fetch results from the single comparison query (Used in Step 3 SELECT INTO)
    v_calculated_unexpected_cols ARRAY;
    v_calculated_missing_cols ARRAY;
    v_calculated_mismatches INT;
    v_calculated_success_flag BOOLEAN;
    v_calculated_percent FLOAT;

BEGIN
    v_input_rule_str := TO_VARCHAR(RULE);

    ----------------------------------------------------------------------------------------------------
    -- 1. Load Configuration
    v_step := ''CONFIG_LOADING'';
    v_procedure_name := COALESCE(RULE:PROCEDURE_NAME::STRING, ''SP_TABLE_COLUMNS_MATCH_SET_CHECK'');
    v_run_id := COALESCE(RULE:DATASET_RUN_ID::NUMBER, -1);
    v_check_config_id := COALESCE(RULE:RULE_CONFIG_ID::NUMBER, -1);

    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Loading configuration'');
    BEGIN
        v_sql := ''SELECT DQ_DB_NAME, DQ_SCHEMA_NAME, SUCCESS_CODE, FAILED_CODE, EXECUTION_ERROR FROM DQ_JOB_EXEC_CONFIG WHERE DQ_DB_NAME = ''''DQ_FRAMEWORK'''' AND DQ_SCHEMA_NAME = ''''METADATA'''''';
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

        v_kwargs_variant := PARSE_JSON(RULE:KWARGS);
        
        -- Directly extract the column_set ARRAY from the variant.
        v_expected_columns := v_kwargs_variant:column_set::ARRAY;
        
        v_exact_match := COALESCE(v_kwargs_variant:exact_match::BOOLEAN, TRUE);
        v_allowed_deviation := COALESCE(v_kwargs_variant:mostly::FLOAT, 1.0);
        v_procedure_name := COALESCE(RULE:PROCEDURE_NAME::STRING, ''SP_TABLE_COLUMNS_MATCH_SET_CHECK'');
        
        -- Validation
        IF (v_expected_columns IS NULL OR TYPEOF(v_expected_columns) != ''ARRAY'') THEN
            v_error_message := ''Required rule parameter column_set is missing or is not a valid array in KWARGS.'';
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
        ELSEIF (UPPER(v_dataset_type) != ''TABLE'' AND UPPER(v_dataset_type) != ''QUERY'') THEN
            v_error_message := ''Invalid value for DATASET_TYPE. Must be ''''TABLE'''' or ''''QUERY''''.'';
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
    -- 3. Execute the Main Data Quality Check (Metadata Check)
    v_step := ''MAIN_METADATA_QUERY'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Retrieving actual column metadata'');

    IF (v_error_message IS NULL) THEN
        BEGIN
            -- Retrieve actual columns dynamically
            IF (UPPER(v_dataset_type) = ''TABLE'') THEN
                v_sql := ''SELECT ARRAY_AGG(COLUMN_NAME) WITHIN GROUP (ORDER BY ORDINAL_POSITION) AS ACTUAL_COLUMNS FROM '' ||
                        :v_database_name || ''.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '''''' || :v_schema_name || '''''' AND TABLE_NAME = '''''' || :v_table_name || '''''''';
            ELSE
                -- Execute a zero-row query to populate the result cache metadata
                v_sql_query := ''SELECT * FROM ('' || v_sql_query || '') LIMIT 0'';
                EXECUTE IMMEDIATE v_sql_query;
                -- Then query the result scan for column names and positions
                v_sql := ''SELECT ARRAY_AGG("name") WITHIN GROUP (ORDER BY "position") AS ACTUAL_COLUMNS FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))'';
            END IF;

            v_result := (EXECUTE IMMEDIATE v_sql);
            
            -- Use SELECT ... INTO with RESULT_SCAN to fetch the single column ARRAY result
            SELECT "ACTUAL_COLUMNS" INTO v_actual_columns FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
            
            v_actual_total := ARRAY_SIZE(v_actual_columns);

            IF (v_actual_columns IS NULL OR v_actual_total = 0) THEN
                v_error_message := ''The source asset (table or query) returned no columns. Check permissions or SQL syntax.'';
                v_status_code := v_execution_error;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
                RETURN v_status_code;
            END IF;
            
            -- Expected Set Processing (Case Normalization: UPPER for unquoted, preserve case for quoted)
            SELECT ARRAY_AGG(
                TRIM(value::STRING) 
            )
            INTO v_comparison_set_expected
            FROM TABLE(FLATTEN(input => :v_expected_columns));
            
            v_total := ARRAY_SIZE(v_comparison_set_expected); -- Set total based on processed array
            
            IF (v_total = 0) THEN
                v_error_message := ''The provided column_set is empty or contains only NULL/empty values.'';
                v_status_code := v_execution_error;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_input_rule_str WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
                RETURN v_status_code;
            END IF;

            -- Actual Set Processing (Case Normalization: UPPER for unquoted, preserve case for quoted)
            SELECT ARRAY_AGG(
                TRIM(value::STRING)
            )
            INTO v_comparison_set_actual
            FROM TABLE(FLATTEN(input => :v_actual_columns));
            
            -- Prepare quoted array strings for safe dynamic injection (logic embedded)
            LET expected_array_quoted STRING := '''''''' || REPLACE(TO_VARCHAR(v_comparison_set_expected), '''''''''', '''''''''''''') || '''''''' ;
            LET actual_array_quoted STRING := '''''''' || REPLACE(TO_VARCHAR(v_comparison_set_actual), '''''''''', '''''''''''''') || '''''''' ;

            -- Combined calculation and comparison logic into a single dynamic SELECT
            -- FIX: Explicitly use PARSE_JSON to convert the string literal back to an ARRAY type
            v_sql := ''
SELECT
    ARRAY_EXCEPT(PARSE_JSON('' || expected_array_quoted || '')::ARRAY, PARSE_JSON('' || actual_array_quoted || '')::ARRAY) AS MISSING_COLS,
    ARRAY_EXCEPT(PARSE_JSON('' || actual_array_quoted || '')::ARRAY, PARSE_JSON('' || expected_array_quoted || '')::ARRAY) AS UNEXPECTED_COLS,
    (
        ARRAY_SIZE(ARRAY_EXCEPT(PARSE_JSON('' || expected_array_quoted || '')::ARRAY, PARSE_JSON('' || actual_array_quoted || '')::ARRAY)) +
        ARRAY_SIZE(ARRAY_EXCEPT(PARSE_JSON('' || actual_array_quoted || '')::ARRAY, PARSE_JSON('' || expected_array_quoted || '')::ARRAY))
    ) AS TOTAL_MISMATCHES,
    CASE
        -- Check 1: Missing columns fail if the count exceeds allowed deviation
        WHEN ARRAY_SIZE(ARRAY_EXCEPT(PARSE_JSON('' || expected_array_quoted || '')::ARRAY, PARSE_JSON('' || actual_array_quoted || '')::ARRAY)) > (1 - '' || v_allowed_deviation || '') * '' || v_total || '' THEN FALSE
        -- Check 2: Unexpected columns fail if exact_match is true AND count exceeds deviation
        WHEN '' || v_exact_match || '' AND ARRAY_SIZE(ARRAY_EXCEPT(PARSE_JSON('' || actual_array_quoted || '')::ARRAY, PARSE_JSON('' || expected_array_quoted || '')::ARRAY)) > (1 - '' || v_allowed_deviation || '') * '' || v_total || '' THEN FALSE
        ELSE TRUE
    END AS SUCCESS_FLAG,
    (
        ARRAY_SIZE(ARRAY_EXCEPT(PARSE_JSON('' || expected_array_quoted || '')::ARRAY, PARSE_JSON('' || actual_array_quoted || '')::ARRAY)) +
        ARRAY_SIZE(ARRAY_EXCEPT(PARSE_JSON('' || actual_array_quoted || '')::ARRAY, PARSE_JSON('' || expected_array_quoted || '')::ARRAY))
    )::FLOAT / NULLIF('' || v_total || ''::FLOAT, 0) AS PERCENT_MISMATCH
'';
            
            v_result := (EXECUTE IMMEDIATE v_sql);

            -- Fetch all calculated results into variables
            SELECT
                MISSING_COLS, UNEXPECTED_COLS, TOTAL_MISMATCHES, SUCCESS_FLAG, PERCENT_MISMATCH
            INTO
                v_missing_columns, v_unexpected_columns, v_unexpected, v_success_flag, v_percent
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

            v_log_message := CASE WHEN v_success_flag THEN ''Column set check passed.'' ELSE ''Column set check failed due to mismatch.'' END;
            
            -- Final metrics update
            v_set_mismatches := ARRAY_SIZE(v_missing_columns) + ARRAY_SIZE(v_unexpected_columns);
            v_status_code := CASE WHEN v_success_flag THEN v_success_code ELSE v_failed_code END;
            
            -- Set v_observed_value (the actual list of columns)
            v_observed_value := v_actual_columns;
            
        EXCEPTION
            WHEN OTHER THEN
                v_error_message := ''Error in main metadata query execution: '' || SQLERRM;
                v_status_code := v_execution_error;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
                RETURN v_status_code;
        END;
    END IF;

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 4 & 5. Failed record/key capture (Skipped - Not applicable for metadata checks)
    v_step := ''SKIPPING_FAILED_ROW_CAPTURE'';
    v_log_message := ''Not applicable. Failed row/key capture is not performed for metadata checks.'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, END_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''COMPLETED'', :v_log_message);
    v_failed_records_table := ''No Failed Records'';

    ----------------------------------------------------------------------------------------------------
    -- 6. Insert Results into the DQ_RULE_RESULTS Table 
    v_step := ''INSERT_DQ_RESULTS_TABLE'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Loading results'');

    IF (v_error_message IS NULL) THEN
        BEGIN
            -- Using TO_JSON to ensure array elements are properly quoted inside the JSON strings
            LET expected_cols_json_str STRING := COALESCE(TO_JSON(v_expected_columns), ''null'');
            LET missing_cols_json_str STRING := COALESCE(TO_JSON(v_missing_columns), ''[]'');
            LET unexpected_cols_json_str STRING := COALESCE(TO_JSON(v_unexpected_columns), ''[]'');
            
            LET details_json_str STRING := ''{'' ||
                ''"expected_column_set": '' || expected_cols_json_str || '','' ||
                ''"exact_match": '' || COALESCE(v_exact_match::STRING, ''false'') ||
            ''}'' ;
            
            LET results_json_str STRING := ''{'' ||
                ''"expected_count": '' || COALESCE(v_total::STRING, ''null'') || '','' ||
                ''"actual_count": '' || COALESCE(v_actual_total::STRING, ''null'') || '','' ||
                ''"set_mismatch_count": '' || COALESCE(v_unexpected::STRING, ''0'') || '','' ||
                ''"missing_column_count": '' || COALESCE(ARRAY_SIZE(v_missing_columns)::STRING, ''0'') || '','' ||
                ''"unexpected_column_count": '' || COALESCE(ARRAY_SIZE(v_unexpected_columns)::STRING, ''0'') || '','' ||
                ''"missing_columns": '' || missing_cols_json_str || '','' ||
                ''"unexpected_columns": '' || unexpected_cols_json_str ||
                ''"failed_records_table": Schema level Check - No Failed Records'' ||
            ''}'';

            -- Prepare variables for safe dynamic injection, replacing single quotes with double single quotes
            LET escaped_results_json_str STRING := REPLACE(results_json_str, '''''''', '''''''''''');
            LET escaped_details_json_str STRING := REPLACE(details_json_str, '''''''', '''''''''''');
            LET escaped_rule_str STRING := REPLACE(COALESCE(RULE::STRING, ''null''), '''''''', '''''''''''');
            
            -- FIX for OBSERVED_VALUE: Wrap the TO_VARCHAR output of the array in single quotes and escape internal quotes
            LET escaped_observed_value STRING := '''''''' || COALESCE(REPLACE(TO_VARCHAR(v_observed_value), '''''''', ''''''''''''), ''NULL'') || ''''''''; 

            v_sql := ''INSERT INTO "'' || v_dq_db_name || ''"."'' || v_dq_schema_name || ''".DQ_RULE_RESULTS (
                BATCH_ID, DATASET_RUN_ID, DATASET_ID, RULE_CONFIG_ID, EXPECTATION_ID, RUN_NAME, RUN_TIMESTAMP, DATASET_NAME,
                EXPECATION_CONFIG, IS_SUCCESS, RESULTS, EXPECTATION_NAME, DETAILS, ELEMENT_COUNT,
                UNEXPECTED_COUNT, UNEXPECTED_PERCENT, UNEXPECTED_PERCENT_NONMISSING, UNEXPECTED_PERCENT_TOTAL,
                OBSERVED_VALUE, FAILED_ROWS
                )
                SELECT
                '' || COALESCE(v_batch_id::STRING, ''null'') || '', '' ||
                COALESCE(v_run_id::STRING, ''null'') || '', '' ||
                COALESCE(v_data_asset_id::STRING, ''null'') || '', '' ||
                COALESCE(v_check_config_id::STRING, ''null'') || '', '' ||
                COALESCE(v_expectation_id::STRING, ''null'') || '', '''''' || REPLACE(COALESCE(v_run_name, ''null''), '''''''', '''''''''''') || '''''', CURRENT_TIMESTAMP(), '''''' || REPLACE(COALESCE(v_data_asset_name, ''null''), '''''''', '''''''''''') || '''''', PARSE_JSON('''''' || escaped_rule_str || ''''''), '' || CASE WHEN v_status_code = v_success_code THEN ''TRUE'' ELSE ''FALSE'' END || '', PARSE_JSON('''''' || escaped_results_json_str || ''''''), '''''' || REPLACE(COALESCE(v_expectation_name, ''null''), '''''''', '''''''''''') || '''''', PARSE_JSON('''''' || escaped_details_json_str || ''''''), '' ||
                COALESCE(''null'', ''null'') || '', '' ||
                COALESCE(''null'', ''null'') || '', '' ||
                COALESCE(''null'', ''null'') || '', NULL::FLOAT, NULL::FLOAT, PARSE_JSON('''''' ||
                v_observed_value || '''''')::VARIANT, NULL::VARIANT'';

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
            VALUES (COALESCE(:v_run_id, -1), COALESCE(:v_check_config_id, -1), COALESCE(:v_procedure_name, ''SP_TABLE_COLUMNS_MATCH_SET_CHECK''), COALESCE(:v_step, ''UNKNOWN''), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''FAILED'', :v_error_message);

        EXCEPTION WHEN OTHER THEN NULL;
        END;
        RETURN COALESCE(v_execution_error, 400);
END;
';