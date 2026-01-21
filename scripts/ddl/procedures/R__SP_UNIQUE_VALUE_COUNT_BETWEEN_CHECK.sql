USE DATABASE {{ snowflake_database }};

USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "SP_UNIQUE_VALUE_COUNT_BETWEEN_CHECK"("RULE" VARIANT)
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
    v_procedure_name STRING DEFAULT ''SP_UNIQUE_VALUE_COUNT_BETWEEN_CHECK'';
    v_input_rule_str STRING;
    v_log_message STRING;
    
    -- Expectation specific variables
    v_min_value INT;
    v_max_value INT;
    v_strict_min BOOLEAN DEFAULT FALSE;
    v_strict_max BOOLEAN DEFAULT FALSE;
    v_observed_unique_count INT DEFAULT 0;
    
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
    v_failed_records_table STRING;
    
    -- Result variables
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
        v_min_value := v_kwargs_variant:min_value::INT; -- Note: Cast to INT
        v_max_value := v_kwargs_variant:max_value::INT; -- Note: Cast to INT
        v_strict_min := COALESCE(v_kwargs_variant:strict_min::BOOLEAN, FALSE);
        v_strict_max := COALESCE(v_kwargs_variant:strict_max::BOOLEAN, FALSE);
        v_procedure_name := COALESCE(RULE:PROCEDURE_NAME::STRING, v_procedure_name);

        -- INCREMENTAL LOAD PARAMETERS
        v_is_incremental := COALESCE(RULE:IS_INCREMENTAL::BOOLEAN, FALSE);
        v_incr_date_col_1 := RULE:INCR_DATE_COLUMN_1::STRING;
        v_incr_date_col_2 := RULE:INCR_DATE_COLUMN_2::STRING;
        v_last_validated_ts := RULE:LAST_VALIDATED_TIMESTAMP::TIMESTAMP_NTZ;


        -- Validation
        IF (v_column_nm IS NULL OR (v_min_value IS NULL AND v_max_value IS NULL)) THEN
            v_error_message := ''Required rule parameter is missing or NULL. Check COLUMN, MIN_VALUE, or MAX_VALUE.'';
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
    -- 4. Execute the main aggregate query (Calculate Unique Count)
    v_step := ''MAIN_AGGREGATE_QUERY'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Starting unique count calculation query'');
    
    -- Calculate: COUNT(DISTINCT column) and COUNT(*)
    v_sql := ''SELECT 
                COUNT(DISTINCT "'' || v_column_nm || ''") AS unique_count,
                COUNT(*) AS total_rows
            FROM '' || v_from_clause || v_incremental_filter;

    IF (v_error_message IS NULL) THEN
        BEGIN
            v_result := (EXECUTE IMMEDIATE v_sql);
            LET v_cursor CURSOR FOR v_result;
            
            FOR record IN v_cursor DO
                v_observed_unique_count := COALESCE(record.unique_count, 0);
                v_total := COALESCE(record.total_rows, 0);
                BREAK;
            END FOR;
            
            v_observed_value := v_observed_unique_count; 

            -- STEP 2: Determine SUCCESS based on min/max and strictness
            v_is_success := TRUE;

            -- Check MIN condition
            IF (v_min_value IS NOT NULL) THEN
                IF (v_strict_min = TRUE AND v_observed_unique_count <= v_min_value) THEN
                    v_is_success := FALSE;
                ELSEIF (v_strict_min = FALSE AND v_observed_unique_count < v_min_value) THEN
                    v_is_success := FALSE;
                END IF;
            END IF;

            -- Check MAX condition (only check if MIN passed)
            IF (v_is_success = TRUE AND v_max_value IS NOT NULL) THEN
                IF (v_strict_max = TRUE AND v_observed_unique_count >= v_max_value) THEN
                    v_is_success := FALSE;
                ELSEIF (v_strict_max = FALSE AND v_observed_unique_count > v_max_value) THEN
                    v_is_success := FALSE;
                END IF;
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

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Validation done. Unique Count: '' || :v_observed_unique_count WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

--- -- 4. & 5. Skip Failed Row/Key Capture (Aggregate Check)
    v_step := ''SKIPPING_FAILED_ROW_CAPTURE'';
    v_log_message := ''Not applicable. Failed row/key capture is not performed for aggregate checks .'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, END_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''COMPLETED'', :v_log_message);
    v_failed_records_table := ''Column level check - No Failed Records''; -- Set consistently for results table
    ----------------------------------------------------------------------------------------------------
    -- 7. Insert results into the DQ_RULE_RESULTS table
    v_step := ''INSERT_DQ_RESULTS_TABLE'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Loading results'');

    IF (v_error_message IS NULL) THEN
        LET details_json_str STRING := ''{'' ||
            ''"column": "'' || COALESCE(v_column_nm, ''null'') || ''",'' ||
            ''"min_value": '' || COALESCE(v_min_value::STRING, ''null'') || '','' ||
            ''"max_value": '' || COALESCE(v_max_value::STRING, ''null'') || '','' ||
            ''"strict_min": '' || COALESCE(v_strict_min::STRING, ''false'') || '','' ||
            ''"strict_max": '' || COALESCE(v_strict_max::STRING, ''false'') ||
        ''}'';
        LET results_json_str STRING := ''{'' ||
            ''"element_count": '' || COALESCE(v_total::STRING, ''null'') || '','' ||
            ''"observed_value": '' || COALESCE(v_observed_value::STRING, ''null'') || '','' ||
            ''"unique_count": '' || COALESCE(v_observed_unique_count::STRING, ''null'') || '','' ||
            ''"failed_records_table": "'' || COALESCE(v_failed_records_table, ''null'') || ''"'' ||
        ''}'';

        -- OBSERVED_VALUE (which is an INT here) injection
        LET v_observed_value_str STRING := COALESCE(v_observed_value::STRING, ''NULL'');
        LET v_observed_value_sql_inject STRING := 
            CASE 
                WHEN v_observed_value_str = ''NULL'' 
                THEN ''NULL::VARIANT''
                -- Inject the integer string literal and cast to VARIANT
                ELSE '''''''' || v_observed_value_str || ''''''::INT::VARIANT''
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
            VALUES (COALESCE(v_run_id, -1), COALESCE(v_check_config_id, -1), COALESCE(v_procedure_name, ''SP_PROPORTION_UNIQUE_BETWEEN_CHECK''), COALESCE(v_step, ''UNKNOWN''), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''FAILED'', v_error_message);

        EXCEPTION WHEN OTHER THEN NULL;
        END;
        RETURN COALESCE(v_execution_error, 400);
END;
';