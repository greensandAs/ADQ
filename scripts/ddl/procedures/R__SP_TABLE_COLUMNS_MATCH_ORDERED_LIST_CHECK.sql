USE DATABASE {{ snowflake_database }};

USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "SP_TABLE_COLUMNS_MATCH_ORDERED_LIST_CHECK"("RULE" VARIANT)
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
    v_allowed_deviation FLOAT DEFAULT 1.0; -- Remains 1.0 (100% match)
    v_error_message STRING;
    v_step STRING DEFAULT ''INITIALIZATION'';
    v_run_id NUMBER DEFAULT -1;
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
    v_kwargs_variant VARIANT;
    v_batch_id NUMBER DEFAULT -1;
    v_procedure_name STRING;
    v_input_rule_str STRING;
    v_log_message STRING;
    
    -- Check specific variables
    v_dataset_type STRING;
    v_sql_query STRING;
    
    -- Metadata/Result variables
    v_actual_columns ARRAY; -- Actual columns in correct order
    v_expected_columns ARRAY; -- Expected column list from KWARGS (must be kept in order)
    v_success_flag BOOLEAN DEFAULT FALSE;
    v_observed_value VARIANT;
    
    -- Variables for Detailed Mismatch Reporting (NEW)
    v_actual_size INT DEFAULT 0;
    v_expected_size INT DEFAULT 0;
    v_max_size INT DEFAULT 0;
    v_mismatch_details ARRAY DEFAULT ARRAY_CONSTRUCT(); -- Stores the detailed mismatch objects
    v_mismatch_count INT DEFAULT 0;
    v_expected_col_name STRING;
    v_actual_col_name STRING;
    i INT;

BEGIN
    v_input_rule_str := TO_VARCHAR(RULE);

    ----------------------------------------------------------------------------------------------------
    -- 1. Load Configuration
    v_step := ''CONFIG_LOADING'';
    v_procedure_name := COALESCE(RULE:PROCEDURE_NAME::STRING, ''SP_TABLE_COLUMNS_MATCH_ORDERED_LIST_CHECK'');
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
        
        -- Extract the column_list ARRAY from the variant.
        v_expected_columns := v_kwargs_variant:column_list::ARRAY;
        
        -- Validation
        IF (v_expected_columns IS NULL OR TYPEOF(v_expected_columns) != ''ARRAY'') THEN
            v_error_message := ''Required rule parameter column_list is missing or is not a valid array in KWARGS.'';
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
        
        v_expected_size := ARRAY_SIZE(v_expected_columns);
        v_total := v_expected_size;
        
        IF (v_total = 0) THEN
            v_error_message := ''The provided column_list is empty.'';
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
            -- 3a. Retrieve actual columns dynamically, preserving order
            IF (UPPER(v_dataset_type) = ''TABLE'') THEN
                v_sql := ''SELECT ARRAY_AGG(COLUMN_NAME) WITHIN GROUP (ORDER BY ORDINAL_POSITION) AS ACTUAL_COLUMNS FROM '' ||
                         :v_database_name || ''.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '''''' || :v_schema_name || '''''' AND TABLE_NAME = '''''' || :v_table_name || '''''''';
            ELSE
                v_sql_query := ''SELECT * FROM ('' || v_sql_query || '') LIMIT 0'';
                EXECUTE IMMEDIATE v_sql_query;
                v_sql := ''SELECT ARRAY_AGG("name") WITHIN GROUP (ORDER BY "position") AS ACTUAL_COLUMNS FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))'';
            END IF;

            v_result := (EXECUTE IMMEDIATE v_sql);
            
            SELECT "ACTUAL_COLUMNS" INTO v_actual_columns FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
            
            -- Set v_observed_value and sizes
            v_actual_columns := COALESCE(v_actual_columns, ARRAY_CONSTRUCT());
            v_observed_value := v_actual_columns;
            v_actual_size := ARRAY_SIZE(v_actual_columns);
            v_max_size := GREATEST(v_expected_size, v_actual_size);
            
            -- 3b. Comparison Logic: Loop to find positional and content mismatches (NEW LOGIC)
            i := 0;
            WHILE (i < v_max_size) DO
                -- Get expected column (or NULL if index out of bounds)
                v_expected_col_name := (
                    CASE 
                        WHEN i < v_expected_size THEN v_expected_columns[i]::STRING 
                        ELSE NULL 
                    END
                );
                
                -- Get actual column (or NULL if index out of bounds)
                v_actual_col_name := (
                    CASE 
                        WHEN i < v_actual_size THEN v_actual_columns[i]::STRING 
                        ELSE NULL 
                    END
                );

                -- Check if names (or NULL status) mismatch
                IF (v_expected_col_name != v_actual_col_name) THEN
                    v_mismatch_count := v_mismatch_count + 1;
                    
                    -- Construct the detailed mismatch object
                    v_mismatch_details := ARRAY_APPEND(v_mismatch_details, 
                        PARSE_JSON(
                            ''{
                                "Expected Column Position": '' || (i + 1) || '', 
                                "Expected": '' || COALESCE('''''''' || v_expected_col_name || '''''''', ''null'') || '',
                                "Found": '' || COALESCE('''''''' || v_actual_col_name || '''''''', ''null'') || ''
                            }''
                        )
                    );
                END IF;
                
                i := i + 1;
            END WHILE;
            
            -- 3c. Final Result Assignment
            v_success_flag := (v_mismatch_count = 0);
            v_unexpected := v_mismatch_count;
            
            v_log_message := CASE WHEN v_success_flag THEN ''Column ordered list check passed: exact match found.'' ELSE ''Column ordered list check failed: '' || v_mismatch_count || '' positional mismatch(es) found.'' END;
            
            v_status_code := CASE WHEN v_success_flag THEN v_success_code ELSE v_failed_code END;
            
            -- Percentage is 0 if success, or total mismatches / expected size (can be > 100% or just 1.0 for simplicity)
            v_percent := CASE WHEN v_success_flag THEN 0.0 ELSE v_mismatch_count::FLOAT / NULLIF(v_expected_size::FLOAT, 0) END;
            
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
    -- 4 & 5. Skipping Failed Row Capture
    v_step := ''SKIPPING_FAILED_ROW_CAPTURE'';
    v_log_message := ''Not applicable. Failed row/key capture is not performed for metadata checks.'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, END_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''COMPLETED'', :v_log_message);

    ----------------------------------------------------------------------------------------------------
    -- 6. Insert Results into the DQ_RULE_RESULTS Table 
    v_step := ''INSERT_DQ_RESULTS_TABLE'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Loading results'');

    IF (v_error_message IS NULL) THEN
        BEGIN
            -- Prepare JSON strings
            LET expected_cols_json_str STRING := COALESCE(TO_JSON(v_expected_columns), ''null'');
            LET mismatched_details_json_str STRING := COALESCE(TO_JSON(v_mismatch_details), ''[]''); -- NEW
            
            LET details_json_str STRING := ''{'' ||
                ''"expected_column_list": '' || expected_cols_json_str || '','' ||
                ''"mismatched_details": '' || mismatched_details_json_str || -- NEW
            ''}'' ;
            
            LET results_json_str STRING := ''{'' ||
                ''"expected_column_count": '' || COALESCE(v_expected_size::STRING, ''null'') || '','' ||
                ''"actual_column_count": '' || COALESCE(v_actual_size::STRING, ''null'') || '','' ||
                ''"mismatched_column_count": '' || COALESCE(v_mismatch_count::STRING, ''0'') ||
                ''"failed_records_table": Schema level Check - No Failed Records'' ||
            ''}'' ;

            -- Prepare variables for safe dynamic injection
            LET escaped_results_json_str STRING := REPLACE(results_json_str, '''''''', '''''''''''');
            LET escaped_details_json_str STRING := REPLACE(details_json_str, '''''''', '''''''''''');
            LET escaped_rule_str STRING := REPLACE(COALESCE(RULE::STRING, ''null''), '''''''', '''''''''''');
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
                COALESCE(0::STRING, ''null'') || '', '' ||
                COALESCE(0::STRING, ''null'') || '', '' ||
                COALESCE(0::STRING, ''null'') || '', NULL::FLOAT, NULL::FLOAT, PARSE_JSON('''''' ||
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
            VALUES (COALESCE(:v_run_id, -1), COALESCE(:v_check_config_id, -1), COALESCE(:v_procedure_name, ''SP_TABLE_COLUMNS_MATCH_ORDERED_LIST_CHECK''), COALESCE(:v_step, ''UNKNOWN''), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''FAILED'', :v_error_message);

        EXCEPTION WHEN OTHER THEN NULL;
        END;
        RETURN COALESCE(v_execution_error, 400);
END;
';