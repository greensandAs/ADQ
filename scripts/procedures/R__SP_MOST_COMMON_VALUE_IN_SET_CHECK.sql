USE DATABASE {{ snowflake_database }};
USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "SP_MOST_COMMON_VALUE_IN_SET_CHECK"("RULE" VARIANT)
RETURNS NUMBER(38,0)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    -- Standard Framework Variables
    v_sql TEXT;
    v_result RESULTSET;
    v_total INT DEFAULT 0;
    v_status_code NUMBER;
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
    v_kwargs_variant VARIANT;
    v_batch_id NUMBER DEFAULT -1;
    v_procedure_name STRING DEFAULT ''SP_MOST_COMMON_VALUE_IN_SET_CHECK'';
    v_input_rule_str STRING;
    v_log_message STRING;
    
    -- Expectation specific variables
    v_value_set ARRAY;
    v_ties_okay BOOLEAN DEFAULT FALSE;
    v_most_common_values ARRAY DEFAULT []; -- The observed mode(s)
    v_max_count INT DEFAULT 0;
    v_all_modes_in_set BOOLEAN;
    v_any_mode_in_set BOOLEAN;
    
    -- Variables for dynamic source
    v_dataset_type STRING;
    v_sql_query STRING;
    v_from_clause STRING;
    
    -- INCREMENTAL LOAD VARIABLES
    v_is_incremental BOOLEAN;
    v_incr_date_col_1 STRING;
    v_incr_date_col_2 STRING;
    v_last_validated_ts TIMESTAMP_NTZ;
    v_incremental_filter STRING DEFAULT '''';
    
    -- Result variables (Aggregate check uses fewer of these)
    v_is_success BOOLEAN;
    v_observed_value VARIANT;

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

---
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

        v_kwargs_variant := PARSE_JSON(RULE:KWARGS);
        v_column_nm := v_kwargs_variant:column::STRING;
        v_value_set := v_kwargs_variant:value_set::ARRAY;
        v_ties_okay := COALESCE(v_kwargs_variant:ties_okay::BOOLEAN, FALSE);
        v_procedure_name := COALESCE(RULE:PROCEDURE_NAME::STRING, v_procedure_name);

        -- INCREMENTAL LOAD PARAMETERS
        v_is_incremental := COALESCE(RULE:IS_INCREMENTAL::BOOLEAN, FALSE);
        v_incr_date_col_1 := RULE:INCR_DATE_COLUMN_1::STRING;
        v_incr_date_col_2 := RULE:INCR_DATE_COLUMN_2::STRING;
        v_last_validated_ts := RULE:LAST_VALIDATED_TIMESTAMP::TIMESTAMP_NTZ;


        -- Validation
        IF (v_column_nm IS NULL OR v_value_set IS NULL) THEN
            v_error_message := ''Required rule parameter is missing or NULL. Please check COLUMN or VALUE_SET.'';
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

---
    ----------------------------------------------------------------------------------------------------
    -- 3. Construct Incremental Filter
    v_step := ''CONSTRUCT_INCREMENTAL_FILTER'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Checking for incremental load logic'');

    IF (v_is_incremental = TRUE AND v_last_validated_ts IS NOT NULL) THEN
        LET v_incr_col STRING := COALESCE(v_incr_date_col_1, v_incr_date_col_2);

        IF (v_incr_col IS NOT NULL) THEN
            v_incremental_filter := '' WHERE 1=1 AND "'' || v_incr_col || ''" > '''''' || v_last_validated_ts::STRING || '''''''';
            v_log_message := ''Incremental filter applied on '' || v_incr_col || '' > '' || v_last_validated_ts::STRING;
        ELSE
            v_incremental_filter := '' WHERE 1=1'';
            v_log_message := ''Incremental enabled, but no INCR_DATE_COLUMN found. Executing full load for DQ.'';
        END IF;
    ELSE
        v_incremental_filter := '' WHERE 1=1'';
        v_log_message := ''Not an incremental run. Executing full load for DQ.'';
    END IF;
    
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

---
    ----------------------------------------------------------------------------------------------------
    -- 4. Execute the main aggregate query (Find Mode and Max Count)
    v_step := ''MAIN_AGGREGATE_QUERY'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Starting mode calculation query'');
    
    -- STEP 1: Find the count and maximum count
    v_sql := ''WITH Counts AS (
                SELECT 
                    "'' || v_column_nm || ''" AS mode_value,
                    COUNT(*) AS mode_count
                FROM '' || v_from_clause || v_incremental_filter || ''
                GROUP BY 1
                HAVING mode_value IS NOT NULL -- Ignore NULLs for mode calculation
            ),
            MaxCount AS (
                SELECT MAX(mode_count) AS max_count
                FROM Counts
            )
            SELECT 
                (SELECT max_count FROM MaxCount) AS max_count_val,
                ARRAY_AGG(C.mode_value) WITHIN GROUP (ORDER BY C.mode_value) AS most_common_values_arr,
                (SELECT COUNT(*) FROM '' || v_from_clause || v_incremental_filter || '') AS total_rows
            FROM Counts C, MaxCount MC
            WHERE C.mode_count = MC.max_count'';

    IF (v_error_message IS NULL) THEN
        BEGIN
            v_result := (EXECUTE IMMEDIATE v_sql);
            LET v_cursor CURSOR FOR v_result;
            
            -- Fetching results
            FOR record IN v_cursor DO
                v_max_count := COALESCE(record.max_count_val, 0);
                v_most_common_values := COALESCE(record.most_common_values_arr, ARRAY_CONSTRUCT());
                v_total := COALESCE(record.total_rows, 0);
                BREAK;
            END FOR;
            
            v_observed_value := v_most_common_values; -- Set observed value for result logging

            -- STEP 2: Determine SUCCESS based on mode(s) and ties_okay
            IF (ARRAY_SIZE(v_most_common_values) = 0) THEN
                -- No non-NULL data found, expectation should fail as mode cannot be determined
                v_is_success := FALSE; 
            ELSE
                v_all_modes_in_set := TRUE;
                v_any_mode_in_set := FALSE;

                FOR i IN 0 TO ARRAY_SIZE(v_most_common_values) - 1 DO
                    LET mode_val VARIANT := v_most_common_values[i];
                    -- Check if the current mode value is NOT in the designated set
                    IF (NOT ARRAY_CONTAINS(:mode_val, :v_value_set)) THEN
                        v_all_modes_in_set := FALSE;
                    ELSE
                        v_any_mode_in_set := TRUE;
                    END IF;
                END FOR;

                -- Logic:
                -- If ties_okay is FALSE: ALL most common values MUST be in the set.
                -- If ties_okay is TRUE: AT LEAST ONE most common value MUST be in the set.
                
                v_is_success := CASE
                    WHEN v_ties_okay = FALSE THEN v_all_modes_in_set
                    WHEN v_ties_okay = TRUE THEN v_any_mode_in_set
                    ELSE FALSE
                END;
            END IF;

            v_status_code := CASE WHEN v_is_success THEN v_success_code ELSE v_failed_code END;
            
        EXCEPTION
            WHEN OTHER THEN
                v_error_message := ''Error in main aggregate query execution: '' || SQLERRM || '' SQL: '' || v_sql;
                v_status_code := v_execution_error;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
                RETURN v_status_code;
        END;
    END IF;

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Validation done. Max Count: '' || :v_max_count WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

---
    ----------------------------------------------------------------------------------------------------
    -- 5. SKIPPED (No Key Capture, No Row Capture)
    
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, END_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, ''CAPTURE_FAILED_ROWS'', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''COMPLETED'', ''Skipped: Failed Rows capture not required for Column Aggregate Check'');
    
    -- 6. Insert results into the DQ_RULE_RESULTS table (FIXED for OBSERVED_VALUE injection)
    v_step := ''INSERT_DQ_RESULTS_TABLE'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Loading results'');

    IF (v_error_message IS NULL) THEN
        LET details_json_str STRING := ''{'' ||
            ''"column": "'' || COALESCE(v_column_nm, ''null'') || ''",'' ||
            ''"value_set": '' || COALESCE(v_value_set::STRING, ''null'') || '','' ||
            ''"ties_okay": '' || COALESCE(v_ties_okay::STRING, ''false'') ||
        ''}'';
        LET results_json_str STRING := ''{'' ||
            ''"element_count": '' || COALESCE(v_total::STRING, ''null'') || '','' ||
            ''"observed_value": '' || COALESCE(v_observed_value::STRING, ''null'') ||
            ''"failed_records_table": "N/A (Column Aggregate Check)"'' ||
        ''}'';

        -- OBSERVED_VALUE needs to be carefully handled to avoid compilation error.
        -- We will inject the variable as a string literal and cast it inside the SELECT.
        LET v_observed_value_str STRING := COALESCE(v_observed_value::STRING, ''NULL'');
        LET v_observed_value_sql_inject STRING := 
            CASE 
                WHEN v_observed_value_str = ''NULL'' 
                THEN ''NULL::VARIANT''
                -- Inject the array string literal and cast to VARIANT. Must escape inner quotes.
                ELSE ''PARSE_JSON('''''' || REPLACE(v_observed_value_str, '''''''', '''''''''''') || '''''')''
            END;

        v_sql := ''INSERT INTO "'' || v_dq_db_name || ''"."'' || v_dq_schema_name || ''".DQ_RULE_RESULTS (
            BATCH_ID, DATASET_RUN_ID, DATASET_ID, RULE_CONFIG_ID, EXPECTATION_ID, RUN_NAME, RUN_TIMESTAMP, DATASET_NAME,
            EXPECATION_CONFIG, IS_SUCCESS, RESULTS, EXPECTATION_NAME, DETAILS, ELEMENT_COUNT, 
            OBSERVED_VALUE
            )
            SELECT
            '' || COALESCE(v_batch_id::STRING, ''null'') || '', '' ||
            COALESCE(v_run_id::STRING, ''null'') || '', '' ||
            COALESCE(v_data_asset_id::STRING, ''null'') || '', '' ||
            COALESCE(v_check_config_id::STRING, ''null'') || '', '' ||
            COALESCE(v_expectation_id::STRING, ''null'') || '', '''''' || REPLACE(COALESCE(v_run_name, ''null''), '''''''', '''''''''''') || '''''', CURRENT_TIMESTAMP(), '''''' || REPLACE(COALESCE(v_data_asset_name, ''null''), '''''''', '''''''''''') || '''''', PARSE_JSON('''''' || REPLACE(COALESCE(RULE::STRING, ''null''), '''''''', '''''''''''') || ''''''), '' || CASE WHEN v_is_success THEN ''TRUE'' ELSE ''FALSE'' END || '', PARSE_JSON('''''' || REPLACE(results_json_str, '''''''', '''''''''''') || ''''''), '''''' || REPLACE(COALESCE(v_expectation_name, ''null''), '''''''', '''''''''''') || '''''', PARSE_JSON('''''' || REPLACE(details_json_str, '''''''', '''''''''''') || ''''''), '' ||
            COALESCE(v_total::STRING, ''null'') || '', '' ||
            v_observed_value_sql_inject;
            
        BEGIN
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
            VALUES (COALESCE(v_run_id, -1), COALESCE(v_check_config_id, -1), COALESCE(v_procedure_name, ''SP_MOST_COMMON_VALUE_IN_SET_CHECK''), COALESCE(v_step, ''UNKNOWN''), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''FAILED'', v_error_message);

        EXCEPTION WHEN OTHER THEN NULL;
        END;
        RETURN COALESCE(v_execution_error, 400);
END;
';;
