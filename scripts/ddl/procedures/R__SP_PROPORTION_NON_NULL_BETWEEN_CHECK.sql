USE DATABASE {{ snowflake_database }};

USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "SP_PROPORTION_NON_NULL_BETWEEN_CHECK"("RULE" VARIANT)
RETURNS NUMBER(38, 0)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    v_sql TEXT;
    v_result RESULTSET;
    v_total INT DEFAULT 0;
    v_unexpected INT DEFAULT 0;
    v_proportion FLOAT DEFAULT 0; 
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
    -- v_failed_records_table STRING; -- REMOVED: No longer capturing failed rows
    v_kwargs_variant VARIANT;
    v_batch_id NUMBER DEFAULT -1;
    v_procedure_name STRING;
    v_input_rule_str STRING;
    v_log_message STRING;
    
    -- Check specific variables
    v_min_value FLOAT; 
    v_max_value FLOAT; 
    v_strict_min BOOLEAN DEFAULT FALSE;
    v_strict_max BOOLEAN DEFAULT FALSE;
    v_where_clause_condition STRING; 
    v_observed_value VARIANT;
    
    v_dataset_type STRING;
    v_sql_query STRING;
    v_from_clause STRING;
BEGIN
    v_input_rule_str := TO_VARCHAR(RULE);

    ----------------------------------------------------------------------------------------------------
    -- 1. Load Configuration (Kept Standard)
    v_step := ''CONFIG_LOADING'';
    v_procedure_name := COALESCE(RULE:PROCEDURE_NAME::STRING, ''SP_PROPORTION_NON_NULL_BETWEEN_CHECK'');
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
           v_status_code := 400;
           RETURN v_status_code;
        END IF;
        v_status_code := v_execution_error;
    EXCEPTION
        WHEN OTHER THEN
            v_status_code := 400;
            RETURN v_status_code;
    END;
    
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 2. Parse Rule (Kept Standard)
    v_step := ''RULE_PARSING'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', :v_input_rule_str);

    BEGIN
        v_batch_id := COALESCE(RULE:BATCH_ID::NUMBER, -1);
        v_data_asset_id := COALESCE(RULE:DATASET_ID::NUMBER, -1);
        v_expectation_id := COALESCE(RULE:EXPECTATION_ID::NUMBER, -1);
        v_column_nm := RULE:COLUMN_NAME::STRING;
        v_run_name := RULE:RUN_NAME::STRING;
        v_data_asset_name := RULE:DATASET_NAME::STRING;
        v_expectation_name := RULE:EXPECTATION_NAME::STRING;
        v_kwargs_variant := PARSE_JSON(RULE:KWARGS);
        
        -- Aggregate Parameters
        v_min_value := v_kwargs_variant:min_value::FLOAT;
        v_max_value := v_kwargs_variant:max_value::FLOAT;
        v_strict_min := COALESCE(v_kwargs_variant:strict_min::BOOLEAN, FALSE);
        v_strict_max := COALESCE(v_kwargs_variant:strict_max::BOOLEAN, FALSE);
        
        v_dataset_type := RULE:DATASET_TYPE::STRING;
        v_sql_query := RULE:CUSTOM_SQL::STRING;
        v_database_name := RULE:DATABASE_NAME::STRING;
        v_schema_name := RULE:SCHEMA_NAME::STRING;
        v_table_name := RULE:TABLE_NAME::STRING;
    EXCEPTION
        WHEN OTHER THEN
            v_status_code := v_execution_error;
            RETURN v_status_code;
    END;

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 3. Execute Main Check (Calculates the Observed Value)
    v_step := ''MAIN_QUERY'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Starting validation query'');

    IF (UPPER(v_dataset_type) = ''QUERY'') THEN
        v_from_clause := ''('' || v_sql_query || '') AS custom_query_source'';
    ELSE
        v_from_clause := ''"'' || v_database_name || ''"."'' || v_schema_name || ''"."'' || v_table_name || ''"'';
    END IF;

    -- We check for IS NOT NULL
    v_where_clause_condition := ''"'' || v_column_nm || ''" IS NOT NULL''; 

    v_sql := ''SELECT
                COUNT(*) AS total_count,
                COUNT_IF('' || v_where_clause_condition || '') AS expected_count
              FROM '' || v_from_clause;
              
    BEGIN
        v_result := (EXECUTE IMMEDIATE v_sql);
        LET v_cursor CURSOR FOR v_result;
        FOR record IN v_cursor DO
            v_total := COALESCE(record.total_count, 0);
            v_unexpected := v_total - COALESCE(record.expected_count, 0); -- Nulls are "unexpected" here
            BREAK;
        END FOR;
        
        -- THE KEY METRIC: Proportion of Non-Nulls
        v_proportion := CASE WHEN v_total = 0 THEN 0 ELSE ( (v_total - v_unexpected)::FLOAT / v_total) END; 
        
        -- Check Logic
        LET v_is_success BOOLEAN := TRUE;
        
        IF (v_min_value IS NOT NULL) THEN
            IF (v_strict_min AND v_proportion <= v_min_value) THEN v_is_success := FALSE;
            ELSEIF (NOT v_strict_min AND v_proportion < v_min_value) THEN v_is_success := FALSE;
            END IF;
        END IF;
        
        IF (v_max_value IS NOT NULL AND v_is_success) THEN 
            IF (v_strict_max AND v_proportion >= v_max_value) THEN v_is_success := FALSE;
            ELSEIF (NOT v_strict_max AND v_proportion > v_max_value) THEN v_is_success := FALSE;
            END IF;
        END IF;

        v_status_code := CASE WHEN v_is_success THEN v_success_code ELSE v_failed_code END;
        v_observed_value := v_proportion; -- STORE THE OBSERVED VALUE
        
    EXCEPTION
        WHEN OTHER THEN
            v_status_code := v_execution_error;
            RETURN v_status_code;
    END;

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 4. & 5. SKIPPED (No Key Capture, No Row Capture)
    
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, END_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, ''CAPTURE_FAILED_ROWS'', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''COMPLETED'', ''Skipped: Failed Rows capture not implemented'');

    ----------------------------------------------------------------------------------------------------
    -- 6. Insert Results (Focus on Observed Value)
    v_step := ''INSERT_DQ_RESULTS_TABLE'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Loading results'');

    BEGIN
        -- Minimal Details
        LET details_json_str STRING := ''{'' ||
            ''"min_value": '' || COALESCE(v_min_value::STRING, ''null'') || '','' ||
            ''"max_value": '' || COALESCE(v_max_value::STRING, ''null'') ||
        ''}'';
        
        -- Results focused on Observed Value
        LET results_json_str STRING := ''{'' ||
            ''"observed_value": '' || COALESCE(v_observed_value::STRING, ''null'') ||
            ''"failed_records_table": Column Level Check - No Failed Records'' ||
            ''}'';

        -- Use BIND VARIABLES for safer insertion or REPLACE for string safety
        v_sql := ''INSERT INTO "'' || v_dq_db_name || ''"."'' || v_dq_schema_name || ''".DQ_RULE_RESULTS (
            BATCH_ID, DATASET_RUN_ID, DATASET_ID, RULE_CONFIG_ID, EXPECTATION_ID, RUN_NAME, RUN_TIMESTAMP, DATASET_NAME,
            EXPECATION_CONFIG, IS_SUCCESS, RESULTS, EXPECTATION_NAME, DETAILS, ELEMENT_COUNT,
            UNEXPECTED_COUNT, UNEXPECTED_PERCENT, UNEXPECTED_PERCENT_NONMISSING, UNEXPECTED_PERCENT_TOTAL,
            FAILED_ROWS
            )
            SELECT
            '' || COALESCE(v_batch_id::STRING, ''null'') || '', '' ||
            COALESCE(v_run_id::STRING, ''null'') || '', '' ||
            COALESCE(v_data_asset_id::STRING, ''null'') || '', '' ||
            COALESCE(v_check_config_id::STRING, ''null'') || '', '' ||
            COALESCE(v_expectation_id::STRING, ''null'') || '', '''''' || REPLACE(COALESCE(v_run_name, ''null''), '''''''', '''''''''''') || '''''', CURRENT_TIMESTAMP(), '''''' || REPLACE(COALESCE(v_data_asset_name, ''null''), '''''''', '''''''''''') || '''''', 
            PARSE_JSON('''''' || REPLACE(COALESCE(RULE::STRING, ''null''), '''''''', '''''''''''') || ''''''), '' || CASE WHEN v_status_code = v_success_code THEN ''TRUE'' ELSE ''FALSE'' END || '', 
            PARSE_JSON('''''' || REPLACE(results_json_str, '''''''', '''''''''''') || ''''''), '''''' || REPLACE(COALESCE(v_expectation_name, ''null''), '''''''', '''''''''''') || '''''', 
            PARSE_JSON('''''' || REPLACE(details_json_str, '''''''', '''''''''''') || ''''''), '' ||
            COALESCE(v_total::STRING, ''null'') || '', '' ||
            COALESCE(0::STRING, ''null'') || '', '' ||
            COALESCE((0::FLOAT / NULLIF(v_total, 0))*100, 0)::STRING || '', '' ||
            ''NULL, NULL, '' || -- Skipping detailed percent breakdowns often not used in aggregate checks
            ''NULL::VARIANT''; -- FAILED_ROWS is explicitly NULL

            EXECUTE IMMEDIATE v_sql;
    EXCEPTION
        WHEN OTHER THEN
            v_status_code := v_execution_error;
            -- (Log failure update here)
            RETURN v_status_code;
    END;

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Results loaded (Observed Value only)'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    RETURN v_status_code;
EXCEPTION
    WHEN OTHER THEN
        RETURN COALESCE(v_execution_error, 400);
END;
';
