USE DATABASE {{ snowflake_database }};
USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "SP_DISTINCT_VALUES_IN_SET_CHECK"("RULE" VARIANT)
RETURNS NUMBER(38, 0)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    -- Standard Framework Variables
    v_sql TEXT;
    v_result RESULTSET;
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
    v_procedure_name STRING DEFAULT ''SP_DISTINCT_VALUES_IN_SET_CHECK'';
    v_input_rule_str STRING;
    v_log_message STRING;
    
    -- Expectation specific variables
    v_value_set ARRAY;
    v_observed_distinct_arr ARRAY;
    v_value_counts_arr ARRAY;
    v_missing_arr ARRAY; 
    v_extra_arr ARRAY;   
    v_is_success BOOLEAN;
    
    -- Variables for result logging
    v_observed_value VARIANT;
    v_details_json VARIANT;
    v_total INT DEFAULT 1; 

    -- Dynamic Source Variables
    v_dataset_type STRING;
    v_sql_query STRING;
    v_from_clause STRING;
    v_incremental_filter STRING DEFAULT ''''; 

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
        v_dataset_type := COALESCE(RULE:DATASET_TYPE::STRING, ''TABLE'');
        v_sql_query := RULE:CUSTOM_SQL::STRING;

        v_kwargs_variant := PARSE_JSON(RULE:KWARGS);
        v_column_nm := v_kwargs_variant:column::STRING;
        v_value_set := v_kwargs_variant:value_set::ARRAY;
        
        -- Validation
        IF (v_column_nm IS NULL OR v_value_set IS NULL) THEN
            v_error_message := ''Required parameters (column, value_set) are missing.'';
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        ELSEIF (UPPER(v_dataset_type) = ''TABLE'' AND (v_database_name IS NULL OR v_schema_name IS NULL OR v_table_name IS NULL)) THEN
            v_error_message := ''For DATASET_TYPE ''''TABLE'''', DATABASE_NAME, SCHEMA_NAME, and TABLE_NAME are required.'';
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        ELSEIF (UPPER(v_dataset_type) = ''QUERY'' AND v_sql_query IS NULL) THEN
            v_error_message := ''For DATASET_TYPE ''''QUERY'''', CUSTOM_SQL is required.'';
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        END IF;

        -- Define the source for the query (v_from_clause)
        IF (UPPER(v_dataset_type) = ''QUERY'') THEN
            v_from_clause := ''('' || v_sql_query || '') AS custom_query_source'';
        ELSE
            v_from_clause := ''"'' || v_database_name || ''"."'' || v_schema_name || ''"."'' || v_table_name || ''"'';
        END IF;


    EXCEPTION
        WHEN OTHER THEN
            v_error_message := ''Error parsing rule parameter: '' || SQLERRM;
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
    END;
    
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Input rule parsing completed.'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

---
    ----------------------------------------------------------------------------------------------------
    -- 3. Execute the main aggregate query (Get Observed Distinct Set and Value Counts)
    v_step := ''MAIN_AGGREGATE_QUERY'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Calculating distinct set and value counts'');

    -- This query generates the observed distinct values and their counts (excluding NULLs)
    v_sql := ''
        SELECT 
            ARRAY_AGG(OBJECT_CONSTRUCT(''''value'''', T."'' || v_column_nm || ''", ''''count'''', T.val_count)) AS value_counts_arr,
            ARRAY_AGG(T."'' || v_column_nm || ''") WITHIN GROUP (ORDER BY T."'' || v_column_nm || ''") AS observed_distinct_arr,
            SUM(T.val_count) AS non_null_rows_count
        FROM 
            (
                SELECT 
                    "'' || v_column_nm || ''", 
                    COUNT(*) AS val_count 
                FROM '' || v_from_clause || ''
                WHERE "'' || v_column_nm || ''" IS NOT NULL
                GROUP BY 1
            ) T
    '';
        
    BEGIN
        v_result := (EXECUTE IMMEDIATE v_sql);
        LET v_cursor CURSOR FOR v_result;
        
        FOR record IN v_cursor DO
            v_value_counts_arr := COALESCE(record.value_counts_arr, ARRAY_CONSTRUCT());
            v_observed_distinct_arr := COALESCE(record.observed_distinct_arr, ARRAY_CONSTRUCT());
            v_total := COALESCE(record.non_null_rows_count, 0); -- ELEMENT_COUNT is non-null rows
            BREAK;
        END FOR;
        
        v_observed_value := v_observed_distinct_arr; 

    EXCEPTION
        WHEN OTHER THEN
            v_error_message := ''Error in main aggregate query execution: '' || SQLERRM || '' SQL: '' || v_sql;
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
    END;

---
    ----------------------------------------------------------------------------------------------------
    -- 4. Determine Match and Final Status
    -- v_step := ''MATCH_CALCULATION'';
    
    -- 4a. Find Missing Values (Expected - Observed)
    -- This check is NOT required for "ExpectColumnDistinctValuesToBeInSet"
    -- However, we calculate it to populate the ''missing'' section of the details JSON.
     v_missing_arr := (
        SELECT ARRAY_EXCEPT(:v_value_set, :v_observed_distinct_arr)
    );
    v_missing_arr := COALESCE(v_missing_arr, ARRAY_CONSTRUCT());

    -- 4b. Find Extra Values (Observed - Expected)
    v_extra_arr := (
        SELECT ARRAY_EXCEPT(:v_observed_distinct_arr, :v_value_set)
    );
    v_extra_arr := COALESCE(v_extra_arr, ARRAY_CONSTRUCT());
    
    -- Success only if the extra array is empty (Observed set is contained by Expected set)
    v_is_success := (ARRAY_SIZE(v_extra_arr) = 0); 

    v_status_code := CASE WHEN v_is_success THEN v_success_code ELSE v_failed_code END;
    
    -- Prepare detailed results JSON
    v_details_json := OBJECT_CONSTRUCT(
        ''value_counts'', v_value_counts_arr,
        ''mismatched'', OBJECT_CONSTRUCT(
            ''missing'', v_missing_arr,
            ''extra'', v_extra_arr
        )
    );

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Match result calculated. Success: '' || :v_is_success WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

---
    ----------------------------------------------------------------------------------------------------
    -- 4. SKIPPED (No Key Capture, No Row Capture)
    
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, END_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, ''CAPTURE_FAILED_ROWS'', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''COMPLETED'', ''Skipped: Failed Rows capture not required for Column Aggregate Check'');

    -- 5. Insert Results into the DQ_RULE_RESULTS Table
    v_step := ''INSERT_DQ_RESULTS_TABLE'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Loading results'');

    IF (v_error_message IS NULL) THEN
        LET details_json_str STRING := TO_VARCHAR(v_details_json);
        
        LET results_json_str STRING := ''{'' ||
            ''"observed_value": '' || COALESCE(v_observed_value::STRING, ''null'') || '','' ||
            ''"details": '' || COALESCE(details_json_str, ''null'') ||
            ''"failed_records_table": "N/A (Column Aggregate Check)"'' ||
        ''}'';
        
        BEGIN
        v_sql := ''INSERT INTO "'' || v_dq_db_name || ''"."'' || v_dq_schema_name || ''".DQ_RULE_RESULTS (
            BATCH_ID, DATASET_RUN_ID, DATASET_ID, RULE_CONFIG_ID, EXPECTATION_ID, RUN_NAME, RUN_TIMESTAMP, DATASET_NAME,
            EXPECATION_CONFIG, IS_SUCCESS, RESULTS, EXPECTATION_NAME, DETAILS, ELEMENT_COUNT, OBSERVED_VALUE
            )
            SELECT
            '' || COALESCE(v_batch_id::STRING, ''null'') || '', '' ||
            COALESCE(v_run_id::STRING, ''null'') || '', '' ||
            COALESCE(v_data_asset_id::STRING, ''null'') || '', '' ||
            COALESCE(v_check_config_id::STRING, ''null'') || '', '' ||
            COALESCE(v_expectation_id::STRING, ''null'') || '', '''''' || REPLACE(COALESCE(v_run_name, ''null''), '''''''', '''''''''''') || '''''', CURRENT_TIMESTAMP(), '''''' || REPLACE(COALESCE(v_data_asset_name, ''null''), '''''''', '''''''''''') || '''''', PARSE_JSON('''''' || REPLACE(COALESCE(RULE::STRING, ''null''), '''''''', '''''''''''') || ''''''), '' || CASE WHEN v_is_success THEN ''TRUE'' ELSE ''FALSE'' END || '', PARSE_JSON('''''' || REPLACE(results_json_str, '''''''', '''''''''''') || ''''''), '''''' || REPLACE(COALESCE(v_expectation_name, ''null''), '''''''', '''''''''''') || '''''', PARSE_JSON('''''' || REPLACE(details_json_str, '''''''', '''''''''''') || ''''''), '' ||
            COALESCE(v_total::STRING, ''null'') || 
            '', PARSE_JSON('''''' || REPLACE(v_observed_value::STRING, '''''''', '''''''''''') || '''''') '';
        
        EXECUTE IMMEDIATE v_sql;
        
        EXCEPTION
        WHEN OTHER THEN
            v_error_message := ''Error in loading the Result: '' || SQLERRM || '' SQL: '' || v_sql;
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
            VALUES (COALESCE(v_run_id, -1), COALESCE(v_check_config_id, -1), COALESCE(v_procedure_name, ''SP_DISTINCT_VALUES_IN_SET_CHECK''), COALESCE(v_step, ''UNKNOWN''), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''FAILED'', v_error_message);
        END;
        RETURN COALESCE(v_execution_error, 400);
END;
';;
