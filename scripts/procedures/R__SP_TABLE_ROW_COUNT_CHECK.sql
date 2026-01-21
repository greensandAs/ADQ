USE DATABASE {{ snowflake_database }};
USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "SP_TABLE_ROW_COUNT_CHECK"("RULE" VARIANT)
RETURNS NUMBER(38, 0)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    -- Standard framework variables
    v_sql TEXT;
    v_result RESULTSET;
    v_total INT DEFAULT 0;
    v_status_code_flag INT DEFAULT 0;
    v_percent FLOAT DEFAULT 0;
    v_status_code NUMBER;
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
    v_failed_records_table STRING;
    v_kwargs_variant VARIANT;
    v_batch_id NUMBER DEFAULT -1;
    v_procedure_name STRING;
    v_input_rule_str STRING;
    v_log_message STRING;

    -- Row count check specific variables
    v_min_value FLOAT;
    v_max_value FLOAT;
    v_strict_min BOOLEAN;
    v_strict_max BOOLEAN;
    v_observed_row_count NUMBER;
    
    -- Variables for dynamic source (NEW)
    v_dataset_type STRING;
    v_sql_query STRING;
    v_from_clause STRING;

    -- Variables for Great Expectations style results
    v_observed_value VARIANT;
    v_missing_count INT DEFAULT 0;
    v_missing_percent FLOAT DEFAULT 0;
    v_unexpected_percent_nonmissing FLOAT DEFAULT 0;
    v_unexpected_percent_total FLOAT DEFAULT 0;
    v_partial_unexpected_list VARIANT;
    v_unexpected_rows VARIANT;

BEGIN
    v_input_rule_str := TO_VARCHAR(RULE);

    ----------------------------------------------------------------------------------------------------
    -- 1. Load configuration
    v_step := ''CONFIG_LOADING'';
    v_procedure_name := COALESCE(RULE:PROCEDURE_NAME::STRING, ''SP_TABLE_ROW_COUNT_CHECK'');
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
    -- 2. Parse and validate the rule parameter (FIXED for Dataset Type and SQL Query)
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
        
        -- New parameters for source flexibility
        v_dataset_type := RULE:DATASET_TYPE::STRING;
        v_sql_query := RULE:CUSTOM_SQL::STRING;
        
        v_kwargs_variant := PARSE_JSON(RULE:KWARGS);
        v_min_value := v_kwargs_variant:min_value::FLOAT;
        v_max_value := v_kwargs_variant:max_value::FLOAT;
        v_strict_min := COALESCE(v_kwargs_variant:strict_min::BOOLEAN, FALSE);
        v_strict_max := COALESCE(v_kwargs_variant:strict_max::BOOLEAN, FALSE);

        -- Validation
        IF (UPPER(v_dataset_type) = ''TABLE'' AND (v_database_name IS NULL OR v_schema_name IS NULL OR v_table_name IS NULL)) THEN
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
    
    -- Dynamically build the FROM clause
    IF (UPPER(v_dataset_type) = ''QUERY'') THEN
        v_from_clause := ''('' || v_sql_query || '') AS custom_query_source'';
    ELSE
        v_from_clause := ''"'' || v_database_name || ''"."'' || v_schema_name || ''"."'' || v_table_name || ''"'';
    END IF;

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Input rule - parsing completed'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 3. Execute the main data quality check query (FIXED for dynamic FROM clause)
    v_step := ''MAIN_QUERY'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Starting validation query'');

    IF (v_error_message IS NULL) THEN
        v_sql := ''SELECT COUNT(*) AS observed_row_count FROM '' || v_from_clause; -- Use dynamic FROM clause

        BEGIN
            v_result := (EXECUTE IMMEDIATE v_sql);
            LET v_cursor CURSOR FOR v_result;
            FOR record IN v_cursor DO
                v_observed_row_count := COALESCE(record.observed_row_count, 0);
                BREAK;
            END FOR;

            v_observed_value := v_observed_row_count;
            v_total := v_observed_row_count;

            v_status_code_flag := 0;

            -- Check min value constraint
            IF (v_min_value IS NOT NULL) THEN
                IF (v_strict_min AND v_observed_row_count <= v_min_value) THEN
                    v_status_code_flag := 1;
                    v_error_message := ''Row count failed strict minimum check: Observed ('' || v_observed_row_count || '') is not strictly greater than minimum ('' || v_min_value || '').'';
                ELSEIF (NOT v_strict_min AND v_observed_row_count < v_min_value) THEN
                    v_status_code_flag := 1;
                    v_error_message := ''Row count failed minimum check: Observed ('' || v_observed_row_count || '') is less than minimum ('' || v_min_value || '').'';
                END IF;
            END IF;

            -- Check max value constraint (only if min check passed)
            IF (v_status_code_flag = 0 AND v_max_value IS NOT NULL) THEN
                IF (v_strict_max AND v_observed_row_count >= v_max_value) THEN
                    v_status_code_flag := 1;
                    v_error_message := ''Row count failed strict maximum check: Observed ('' || v_observed_row_count || '') is not strictly less than maximum ('' || v_max_value || '').'';
                ELSEIF (NOT v_strict_max AND v_observed_row_count > v_max_value) THEN
                    v_status_code_flag := 1;
                    v_error_message := ''Row count failed maximum check: Observed ('' || v_observed_row_count || '') is greater than maximum ('' || v_max_value || '').'';
                END IF;
            END IF;
            
            -- Set final status code
            v_status_code := CASE WHEN v_status_code_flag = 0 THEN v_success_code ELSE v_failed_code END;
            
            -- Update log message if check failed, otherwise confirm success
            IF (v_status_code_flag != 0) THEN
                v_log_message := v_error_message;
            ELSE
                v_log_message := ''Row count validation passed. Observed count: '' || v_observed_row_count;
            END IF;

        EXCEPTION
            WHEN OTHER THEN
                v_error_message := ''Error in main query execution: '' || SQLERRM;
                v_status_code := v_execution_error;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
                RETURN v_status_code;
        END;
    END IF;
    
    -- Log main query result
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 4 & 5. Failed record/key capture is not applicable for aggregate checks
    v_step := ''SKIPPING_FAILED_ROW_CAPTURE'';
    v_log_message := ''Not applicable. Failed row/key capture is not performed for aggregate checks like row count.'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, END_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''COMPLETED'', :v_log_message);
    v_failed_records_table := ''No Failed Records'';

    ----------------------------------------------------------------------------------------------------
    -- 6. Insert results into the DQ_RULE_RESULTS table
    v_step := ''INSERT_DQ_RESULTS_TABLE'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Loading results'');
    IF (v_error_message IS NULL) THEN
        LET details_json_str STRING := ''{'' ||
             ''"min_value": '' || COALESCE(v_min_value::STRING, ''null'') || '','' ||
             ''"max_value": '' || COALESCE(v_max_value::STRING, ''null'') || '','' ||
             ''"strict_min": '' || COALESCE(v_strict_min::STRING, ''false'') || '','' ||
             ''"strict_max": '' || COALESCE(v_strict_max::STRING, ''false'') ||
        ''}'' ;

        LET results_json_str STRING := ''{'' ||
            ''"observed_value": '' || COALESCE(v_observed_value::STRING, ''null'') || '','' ||
            ''"element_count": '' || COALESCE(v_total::STRING, ''null'') || '','' ||
            ''"success": '' || CASE WHEN v_status_code = v_success_code THEN ''true'' ELSE ''false'' END || ''
        }'';

        v_sql := ''INSERT INTO "'' || v_dq_db_name || ''"."'' || v_dq_schema_name || ''".DQ_RULE_RESULTS (
                BATCH_ID, DATASET_RUN_ID, DATASET_ID, RULE_CONFIG_ID, EXPECTATION_ID, RUN_NAME, RUN_TIMESTAMP, DATASET_NAME,
                EXPECATION_CONFIG, IS_SUCCESS, RESULTS, EXPECTATION_NAME, DETAILS, ELEMENT_COUNT, MISSING_COUNT, MISSING_PERCENT,
                OBSERVED_VALUE, PARTIAL_UNEXPECTED_COUNTS, PARTIAL_UNEXPECTED_INDEX_LIST, PARTIAL_UNEXPECTED_LIST, UNEXPECTED_COUNT,
                UNEXPECTED_PERCENT, UNEXPECTED_PERCENT_NONMISSING, UNEXPECTED_PERCENT_TOTAL, UNEXPECTED_ROWS, DATA_ROWS,
                 FAILED_ROWS
                )
                SELECT
                '' || COALESCE(v_batch_id::STRING, ''null'') || '', '' ||
                COALESCE(v_run_id::STRING, ''null'') || '', '' ||
                COALESCE(v_data_asset_id::STRING, ''null'') || '', '' ||
                COALESCE(v_check_config_id::STRING, ''null'') || '', '' ||
                COALESCE(v_expectation_id::STRING, ''null'') || '', '''''' || REPLACE(COALESCE(v_run_name, ''null''), '''''''', '''''''''''') || '''''', CURRENT_TIMESTAMP(), '''''' || REPLACE(COALESCE(v_data_asset_name, ''null''), '''''''', '''''''''''') || '''''', PARSE_JSON('''''' || REPLACE(COALESCE(RULE::STRING, ''null''), '''''''', '''''''''''') || ''''''), '' || CASE WHEN v_status_code = v_success_code THEN ''TRUE'' ELSE ''FALSE'' END || '', PARSE_JSON('''''' || REPLACE(results_json_str, '''''''', '''''''''''') || ''''''), '''''' || REPLACE(COALESCE(v_expectation_name, ''null''), '''''''', '''''''''''') || '''''', PARSE_JSON('''''' || REPLACE(details_json_str, '''''''', '''''''''''') || ''''''), '' ||
                COALESCE(v_total::STRING, ''null'') || '', '' ||
                0 || '', '' ||
                0 || '', '' ||
                COALESCE(v_observed_value::STRING, ''null'') || '', NULL::VARIANT, NULL::VARIANT, NULL::VARIANT, '' ||
                0 || '', '' ||
                0 || '', '' ||
                0 || '', '' ||
                0 || '', NULL::VARIANT, NULL::VARIANT, NULL::VARIANT '';
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
            VALUES (COALESCE(:v_run_id, -1), COALESCE(:v_check_config_id, -1), COALESCE(:v_procedure_name, ''SP_TABLE_ROW_COUNT_CHECK''), COALESCE(v_step, ''UNKNOWN''), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''FAILED'', :v_error_message);
        EXCEPTION WHEN OTHER THEN NULL;
        END;
        RETURN COALESCE(v_execution_error, 400);
END;
';;
