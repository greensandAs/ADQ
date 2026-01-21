USE DATABASE {{ snowflake_database }};

USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "SP_SUM_RANGE_CHECK"("RULE" VARIANT)
RETURNS NUMBER(38,0)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    -- Standard Framework Variables
    v_sql TEXT;
    v_result RESULTSET;
    v_total INT DEFAULT 0;
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
    v_procedure_name STRING;
    v_audit_sql TEXT;
    v_stage_path STRING;
    v_input_rule_str STRING;
    v_log_message STRING;
    v_rows_inserted NUMBER DEFAULT 0;
    v_failed_rows_threshold INT DEFAULT 10000000;
    v_dimension STRING;

    -- Generic Check Variables
    v_where_clause_condition STRING;
    v_key_column_names STRING;
    v_pk_column_names STRING;
    v_ck_column_names STRING;
    v_key_construct_expr STRING;
    v_key_parts_list STRING;
    v_dataset_type STRING;
    v_sql_query STRING;
    v_from_clause STRING;

    -- EXPECTATION-SPECIFIC VARIABLES (Sum Range Check)
    v_column_type STRING;
    v_column_expr STRING;
    v_min_value FLOAT;
    v_max_value FLOAT;
    v_strict_min BOOLEAN;
    v_strict_max BOOLEAN;
    v_is_successful BOOLEAN;
    v_observed_value VARIANT; -- Holds the calculated SUM
    v_missing_count INT DEFAULT 0;


BEGIN
    -- Capture the RULE variant as a string at the very beginning
    v_input_rule_str := TO_VARCHAR(RULE);

    ----------------------------------------------------------------------------------------------------
    -- 1. Load Configuration

    v_step := ''CONFIG_LOADING'';
    v_procedure_name := COALESCE(RULE:PROCEDURE_NAME::STRING, ''SP_SUM_RANGE_CHECK'');
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
        -- Common Parameter Parsing
        v_batch_id := COALESCE(RULE:BATCH_ID::NUMBER, -1);
        v_run_id := COALESCE(RULE:DATASET_RUN_ID::NUMBER, -1);
        v_data_asset_id := COALESCE(RULE:DATASET_ID::NUMBER, -1);
        v_check_config_id := COALESCE(RULE:RULE_CONFIG_ID::NUMBER, -1);
        v_expectation_id := COALESCE(RULE:EXPECTATION_ID::NUMBER, -1);
        v_column_nm := RULE:COLUMN_NAME::STRING;
        v_run_name := RULE:RUN_NAME::STRING;
        v_data_asset_name := RULE:DATASET_NAME::STRING;
        v_expectation_name := RULE:EXPECTATION_NAME::STRING;
        v_kwargs_variant := PARSE_JSON(RULE:KWARGS);
        v_failed_rows_cnt_limit := v_kwargs_variant:failed_row_count::NUMBER; -- Will be ignored for aggregate check
        v_procedure_name := COALESCE(RULE:PROCEDURE_NAME::STRING, ''SP_SUM_RANGE_CHECK'');
        v_dataset_type := RULE:DATASET_TYPE::STRING;
        v_sql_query := RULE:CUSTOM_SQL::STRING;
        v_database_name := RULE:DATABASE_NAME::STRING;
        v_schema_name := RULE:SCHEMA_NAME::STRING;
        v_table_name := RULE:TABLE_NAME::STRING;
        v_dimension := RULE:DIMENSION;

        -- EXPECTATION-SPECIFIC PARAMETER PARSING
        v_min_value := v_kwargs_variant:min_value::FLOAT;
        v_max_value := v_kwargs_variant:max_value::FLOAT;
        v_strict_min := COALESCE(v_kwargs_variant:strict_min::BOOLEAN, FALSE);
        v_strict_max := COALESCE(v_kwargs_variant:strict_max::BOOLEAN, FALSE);

        -- Validation
        IF (v_column_nm IS NULL) THEN
            v_error_message := ''COLUMN_NM is required for this expectation.'';
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_input_rule_str WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        ELSEIF (v_min_value IS NULL AND v_max_value IS NULL) THEN
            v_error_message := ''KWARGS must contain at least one of "min_value" or "max_value".'';
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_input_rule_str WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        ELSEIF (UPPER(v_dataset_type) = ''TABLE'' AND (v_database_name IS NULL OR v_schema_name IS NULL OR v_table_name IS NULL)) THEN
            v_error_message := ''For DATASET_TYPE ''''TABLE'''', DATABASE_NAME, SCHEMA_NAME, and TABLE_NAME are required.'';
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_input_rule_str WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        ELSEIF (UPPER(v_dataset_type) = ''QUERY'' AND v_sql_query IS NULL) THEN
            v_error_message := ''For DATASET_TYPE ''''QUERY'''', a SQL_QUERY is required.'';
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
    -- 3. Execute the Main Data Quality Check Query (Aggregate Logic)

    v_step := ''MAIN_QUERY'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Starting validation query'');

    -- Dynamically build the FROM clause
    IF (UPPER(v_dataset_type) = ''QUERY'') THEN
        v_from_clause := ''('' || v_sql_query || '') AS custom_query_source'';
        -- For custom query, assume the column is directly usable as a float/number
        v_column_expr := ''"'' || v_column_nm || ''"'';
    ELSE
        v_from_clause := ''"'' || v_database_name || ''"."'' || v_schema_name || ''"."'' || v_table_name || ''"'';

        -- Get Column Metadata to potentially handle non-numeric types (e.g., KANJI_TO_NUMERIC)
        BEGIN
            v_sql := ''SELECT DATA_TYPE FROM "'' || v_database_name || ''COLUMNS WHERE TABLE_SCHEMA = ''''"'' || v_schema_name || ''"'''' AND TABLE_NAME = ''''"'' || v_table_name || ''"'''' AND COLUMN_NAME = ''''"'' || v_column_nm || ''"'''''';
            v_result := (EXECUTE IMMEDIATE v_sql);
            LET v_col_type_cursor CURSOR FOR v_result;
            FOR col_record IN v_col_type_cursor DO
                v_column_type := col_record.DATA_TYPE;
                BREAK;
            END FOR;
        EXCEPTION
            WHEN OTHER THEN
                -- Non-critical failure for metadata fetching (default to direct column name)
                v_column_type := NULL;
        END;

        -- Determine the column expression (removed TRY_TO_VARIANT)
        v_column_expr := CASE
                             WHEN UPPER(v_column_type) IN (''VARCHAR'', ''STRING'',''TEXT'') THEN ''KANJI_TO_NUMERIC("'' || v_column_nm || ''")''
                             ELSE ''"'' || v_column_nm || ''"''
                         END;
    END IF;

    -- Main aggregation query - Summing the column expression cast explicitly to FLOAT
    v_sql := ''SELECT COUNT(*) AS total_count, COUNT_IF("'' || v_column_nm || ''" IS NULL) as missing_count, SUM('' || v_column_expr || ''::FLOAT) AS observed_sum FROM '' || v_from_clause;

    IF (v_error_message IS NULL) THEN
        BEGIN
            v_result := (EXECUTE IMMEDIATE v_sql);
            LET v_cursor CURSOR FOR v_result;

            -- Fetch Aggregate Results
            FOR record IN v_cursor DO
                v_total := COALESCE(record.total_count, 0);
                v_missing_count := COALESCE(record.missing_count, 0);
                v_observed_value := record.observed_sum; -- VARIANT to hold NULL or FLOAT
                BREAK;
            END FOR;

            v_is_successful := TRUE;
            LET observed_float FLOAT := v_observed_value::FLOAT;
            v_unexpected := 0;

            IF (observed_float IS NULL) THEN
                v_is_successful := FALSE;
                v_log_message := ''Check failed: Observed sum is NULL (or non-numeric after conversion).'';
            ELSE
                -- Check Min Value
                IF ((v_min_value IS NOT NULL AND observed_float < v_min_value) OR (v_strict_min AND observed_float = v_min_value)) THEN
                    v_is_successful := FALSE;
                    v_log_message := ''Check failed: Observed sum ('' || observed_float || '') is below the minimum allowed value ('' || v_min_value || '').'';
                END IF;
                -- Check Max Value (only if min check hasn''t failed)
                IF (v_is_successful AND ((v_max_value IS NOT NULL AND observed_float > v_max_value) OR (v_strict_max AND observed_float = v_max_value))) THEN
                    v_is_successful := FALSE;
                    v_log_message := ''Check failed: Observed sum ('' || observed_float || '') is above the maximum allowed value ('' || v_max_value || '').'';
                END IF;

                IF (v_is_successful) THEN
                    v_log_message := ''Check passed. Observed sum ('' || observed_float || '') is within the expected range.'';
                END IF;
            END IF;

            -- Set final metrics
            IF (v_is_successful) THEN
                v_status_code := v_success_code;
            ELSE
                v_status_code := v_failed_code;
                v_unexpected := 1; -- 1 fail for aggregate check
            END IF;

            v_percent := CASE WHEN v_is_successful THEN 0.0 ELSE 100.0 END; -- 100% unexpected if the aggregate check fails

        EXCEPTION
            WHEN OTHER THEN
                v_error_message := ''Error in main query execution: '' || SQLERRM;
                v_status_code := v_execution_error;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
                RETURN v_status_code;
        END;
    END IF;

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 4. Skip Capture Failed Row Keys (Aggregate Check)

    v_step := ''SKIPPING_FAILED_ROW_CAPTURE'';
    v_log_message := ''Not applicable. Failed row/key capture is not performed for aggregate checks.'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, END_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''COMPLETED'', :v_log_message);
    v_failed_records_table := ''Column Level check - No Failed Records''; -- Set consistently for results table


    ----------------------------------------------------------------------------------------------------
    -- 5. Skip Handle and Log Failed Records (Aggregate Check)

    v_step := ''SKIPPING_FAILED_RECORDS_INSERT'';
    v_log_message := ''Not applicable. Failed row record insert is not performed for aggregate checks.'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, END_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''COMPLETED'', :v_log_message);

    ----------------------------------------------------------------------------------------------------
    -- 6. Insert Results into the DQ_RULE_RESULTS Table

    v_step := ''INSERT_DQ_RESULTS_TABLE'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Loading results'');

    IF (v_error_message IS NULL) THEN
        BEGIN

        LET details_json_str STRING := ''{'' ||
            ''"column": "'' || COALESCE(v_column_nm, ''null'') || ''",'' ||
            ''"min_value": '' || COALESCE(v_min_value::STRING, ''null'') || '','' ||
            ''"max_value": '' || COALESCE(v_max_value::STRING, ''null'') || '','' ||
            ''"strict_min": '' || v_strict_min::STRING || '','' ||
            ''"strict_max": '' || v_strict_max::STRING ||
        ''}'';

        LET results_json_str STRING := ''{'' ||
            ''"element_count": '' || COALESCE(v_total::STRING, ''null'') || '','' ||
            ''"missing_count": '' || COALESCE(v_missing_count::STRING, ''null'') || '','' ||
            ''"observed_value": '' || COALESCE(v_observed_value::STRING, ''null'') || '','' ||
            ''"unexpected_count": '' || COALESCE(v_unexpected::STRING, ''null'') || '','' ||
            ''"unexpected_percent": '' || COALESCE(v_percent::STRING, ''null'') || '','' ||
            ''"failed_records_table": "'' || COALESCE(v_failed_records_table, ''null'') || ''"'' ||
        ''}'';

        v_sql := ''INSERT INTO "'' || v_dq_db_name || ''"."'' || v_dq_schema_name || ''".DQ_RULE_RESULTS (
            BATCH_ID, DATASET_RUN_ID, DATASET_ID, RULE_CONFIG_ID, EXPECTATION_ID, RUN_NAME, RUN_TIMESTAMP, DATASET_NAME,
            EXPECATION_CONFIG, IS_SUCCESS, RESULTS, EXPECTATION_NAME, DETAILS, ELEMENT_COUNT, MISSING_COUNT,
            OBSERVED_VALUE, UNEXPECTED_COUNT, UNEXPECTED_PERCENT, UNEXPECTED_PERCENT_NONMISSING, UNEXPECTED_PERCENT_TOTAL,
            FAILED_ROWS, DIMENSION
            )
            SELECT
            '' || COALESCE(v_batch_id::STRING, ''null'') || '', '' ||
            COALESCE(v_run_id::STRING, ''null'') || '', '' ||
            COALESCE(v_data_asset_id::STRING, ''null'') || '', '' ||
            COALESCE(v_check_config_id::STRING, ''null'') || '', '' ||
            COALESCE(v_expectation_id::STRING, ''null'') || '', '''''' || REPLACE(COALESCE(v_run_name, ''null''), '''''''', '''''''''''') || '''''', CURRENT_TIMESTAMP(), '''''' || REPLACE(COALESCE(v_data_asset_name, ''null''), '''''''', '''''''''''') || '''''', PARSE_JSON('''''' || REPLACE(COALESCE(RULE::STRING, ''null''), '''''''', '''''''''''') || ''''''), '' || CASE WHEN v_status_code = v_success_code THEN ''TRUE'' ELSE ''FALSE'' END || '', PARSE_JSON('''''' || REPLACE(results_json_str, '''''''', '''''''''''') || ''''''), '''''' || REPLACE(COALESCE(v_expectation_name, ''null''), '''''''', '''''''''''') || '''''', PARSE_JSON('''''' || REPLACE(details_json_str, '''''''', '''''''''''') || ''''''), '' ||
            COALESCE(v_total::STRING, ''null'') || '', '' ||
            COALESCE(v_missing_count::STRING, ''null'') || '', '' ||
            COALESCE(v_observed_value::STRING, ''null'') || ''::VARIANT, '' ||
            COALESCE(0::STRING, ''null'') || '', '' ||
            COALESCE(0::STRING, ''null'') || '', NULL::FLOAT, NULL::FLOAT, '' ||
            ''NULL::VARIANT, '''''' || COALESCE(v_dimension::STRING, ''null'') || '''''''';


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
            VALUES (COALESCE(:v_run_id, -1), COALESCE(:v_check_config_id, -1), COALESCE(:v_procedure_name, ''SP_SUM_RANGE_CHECK''), COALESCE(:v_step, ''UNKNOWN''), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''FAILED'', :v_error_message);

        EXCEPTION WHEN OTHER THEN NULL;
        END;
        RETURN COALESCE(v_execution_error, 400);
END;
';