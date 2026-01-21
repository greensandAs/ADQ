USE DATABASE {{ snowflake_database }};

USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "SP_REGEX_NOT_MATCH_LIST_CHECK"("RULE" VARIANT)
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
    v_procedure_name STRING;
    v_input_rule_str STRING;
    v_log_message STRING;
    v_rows_inserted NUMBER DEFAULT 0;
    
    -- Regex Not Match List specific variables
    v_regex_list ARRAY;
    v_match_on STRING;
    v_regex_condition_parts ARRAY DEFAULT [];
    v_match_operator STRING;
    v_fail_condition STRING;
    v_where_clause_condition STRING;
    v_unexpected_percent_total FLOAT DEFAULT 0;
    v_missing_percent FLOAT DEFAULT 0;
    
    -- Dynamic Framework Variables
    v_dataset_type STRING;
    v_sql_query STRING;
    v_from_clause STRING;
    v_clean_dataset_name STRING;
    v_full_target_table_name STRING;
    
    -- Incremental Load Variables
    v_is_incremental BOOLEAN;
    v_incr_date_col_1 STRING;
    v_incr_date_col_2 STRING;
    v_last_validated_ts TIMESTAMP_NTZ;
    v_incremental_filter STRING DEFAULT ''''; 

    -- Failed Row Keys
    v_key_column_names STRING;
    v_pk_column_names STRING;
    v_key_construct_expr STRING;
    v_key_parts_list STRING;

BEGIN
    v_input_rule_str := TO_VARCHAR(RULE);

    ----------------------------------------------------------------------------------------------------
    -- 1. Load Configuration
    ----------------------------------------------------------------------------------------------------
    v_step := ''CONFIG_LOADING'';
    v_procedure_name := COALESCE(RULE:PROCEDURE_NAME::STRING, ''SP_REGEX_NOT_MATCH_LIST_CHECK'');
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
            v_error_message := ''Required Config parameter missing. Check DQ_JOB_EXEC_CONFIG'';
            v_status_code := 400;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = ''Configuration Loading - failed'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        END IF;
        v_status_code := v_execution_error;
    EXCEPTION
        WHEN OTHER THEN
            v_error_message := ''Error loading config: '' || SQLERRM;
            v_status_code := 400;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
    END;

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Config loaded'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 2. Parse and Validate Rule Parameter
    ----------------------------------------------------------------------------------------------------
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
        v_column_nm := COALESCE(v_kwargs_variant:column::STRING, RULE:COLUMN_NAME::STRING);
        v_regex_list := v_kwargs_variant:regex_list::ARRAY;
        v_match_on := UPPER(COALESCE(v_kwargs_variant:match_on::STRING, ''ANY''));
        v_allowed_deviation := COALESCE(v_kwargs_variant:mostly::FLOAT, 1.0);
        v_failed_rows_cnt_limit := v_kwargs_variant:failed_row_count::NUMBER;

        -- Incremental Load watermarks
        v_is_incremental := COALESCE(RULE:IS_INCREMENTAL::BOOLEAN, FALSE);
        v_incr_date_col_1 := RULE:INCR_DATE_COLUMN_1::STRING;
        v_incr_date_col_2 := RULE:INCR_DATE_COLUMN_2::STRING;
        v_last_validated_ts := RULE:LAST_VALIDATED_TIMESTAMP::TIMESTAMP_NTZ;

        -- Validation logic with logging
        IF (v_column_nm IS NULL OR v_regex_list IS NULL OR ARRAY_SIZE(v_regex_list) = 0) THEN
            v_error_message := ''Missing required parameter: COLUMN_NAME or REGEX_LIST.'';
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_input_rule_str WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        ELSEIF (v_match_on != ''ANY'' AND v_match_on != ''ALL'') THEN
            v_error_message := ''match_on must be ''''any'''' or ''''all''''.'';
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_input_rule_str WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        ELSEIF (UPPER(v_dataset_type) = ''TABLE'' AND (v_database_name IS NULL OR v_schema_name IS NULL OR v_table_name IS NULL)) THEN
            v_error_message := ''TABLE source requires DATABASE, SCHEMA, and TABLE names.'';
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_input_rule_str WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        ELSEIF (UPPER(v_dataset_type) = ''QUERY'' AND v_sql_query IS NULL) THEN
            v_error_message := ''QUERY source requires CUSTOM_SQL.'';
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_input_rule_str WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        END IF;
        
        v_match_operator := CASE WHEN v_match_on = ''ANY'' THEN '' OR '' ELSE '' AND '' END;
        FOR i IN 0 TO ARRAY_SIZE(v_regex_list) - 1 DO
            -- FIX: Use CHR(92) to safely represent backslash without confusing the SQL parser
            LET regex_escaped STRING := REPLACE(REPLACE(v_regex_list[i]::STRING, CHR(92), CHR(92)||CHR(92)), '''''''', '''''''''''');
            v_regex_condition_parts := ARRAY_APPEND(v_regex_condition_parts, ''REGEXP_LIKE(CAST("'' || v_column_nm || ''" AS VARCHAR), '''''' || regex_escaped || '''''')'');
        END FOR;
        
        v_fail_condition := ARRAY_TO_STRING(v_regex_condition_parts, v_match_operator);
        v_where_clause_condition := ''"'' || v_column_nm || ''" IS NOT NULL AND ('' || v_fail_condition || '')'';

    EXCEPTION
        WHEN OTHER THEN
            v_error_message := ''Error parsing rule: '' || SQLERRM;
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_input_rule_str WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
    END;

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Input parsing completed'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 3. Construct Incremental Filter
    ----------------------------------------------------------------------------------------------------
    v_step := ''CONSTRUCT_INCREMENTAL_FILTER'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Constructing watermark filter'');

    IF (v_is_incremental = TRUE AND v_last_validated_ts IS NOT NULL) THEN
        LET v_incr_col STRING := COALESCE(v_incr_date_col_1, v_incr_date_col_2);
        IF (v_incr_col IS NOT NULL) THEN
            v_incremental_filter := '' AND "'' || v_incr_col || ''" > '''''' || v_last_validated_ts::STRING || '''''''';
            v_log_message := ''Filter applied on '' || v_incr_col;
        ELSE
            v_log_message := ''Incremental enabled but no date column found. Full load.'';
        END IF;
    ELSE
        v_log_message := ''Full load execution.'';
    END IF;
    
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 4. Execute Main DQ Query
    ----------------------------------------------------------------------------------------------------
    v_step := ''MAIN_QUERY'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Starting main validation query'');

    IF (UPPER(v_dataset_type) = ''QUERY'') THEN
        v_from_clause := ''('' || v_sql_query || '') AS custom_query_source'';
    ELSE
        v_from_clause := ''"'' || v_database_name || ''"."'' || v_schema_name || ''"."'' || v_table_name || ''"'';
    END IF;

    v_sql := ''SELECT
                  COUNT(*) AS total_count,
                  COUNT_IF("'' || v_column_nm || ''" IS NULL) AS missing_count,
                  COUNT_IF('' || v_where_clause_condition || '') AS unexpected_count
                FROM '' || v_from_clause || '' WHERE 1=1 '' || v_incremental_filter;

    BEGIN
        v_result := (EXECUTE IMMEDIATE v_sql);
        LET v_cursor CURSOR FOR v_result;
        FOR record IN v_cursor DO
            v_total := COALESCE(record.total_count, 0);
            v_missing_count := COALESCE(record.missing_count, 0);
            v_unexpected := COALESCE(record.unexpected_count, 0);
            BREAK;
        END FOR;
        
        v_missing_percent := CASE WHEN v_total = 0 THEN 0 ELSE (v_missing_count::FLOAT / v_total) END;
        v_unexpected_percent_total := CASE WHEN v_total = 0 THEN 0 ELSE (v_unexpected::FLOAT / v_total) END;
        v_percent := v_unexpected_percent_total;
        v_status_code := CASE WHEN v_percent <= (1 - v_allowed_deviation) THEN v_success_code ELSE v_failed_code END;
    EXCEPTION
        WHEN OTHER THEN
            v_error_message := ''Main query error: '' || SQLERRM;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_execution_error;
    END;

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Main query finished'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 5. Capture Failed Row Keys
    ----------------------------------------------------------------------------------------------------
    v_step := ''CAPTURE_FAILED_KEYS'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Processing failed row keys'');

    IF (v_unexpected > 0) THEN
        BEGIN
            v_sql := ''SELECT PRIMARY_KEY_COLUMNS FROM DQ_DATASET WHERE DATASET_ID = '' || v_data_asset_id;
            v_result := (EXECUTE IMMEDIATE v_sql);
            LET v_pk_cursor CURSOR FOR v_result;
            FOR pk_record IN v_pk_cursor DO
                v_pk_column_names := ARRAY_TO_STRING(PARSE_JSON(pk_record.PRIMARY_KEY_COLUMNS):primary_key::ARRAY, '','');
                BREAK;
            END FOR;

            IF (v_pk_column_names IS NOT NULL AND TRIM(v_pk_column_names) != '''') THEN
                SELECT LISTAGG('''''''' || TRIM(value) || '''''''' || '', '' || TRIM(value), '', '') WITHIN GROUP (ORDER BY seq)
                INTO v_key_parts_list FROM TABLE(SPLIT_TO_TABLE(:v_pk_column_names, '',''));
                v_key_construct_expr := ''OBJECT_CONSTRUCT('' || v_key_parts_list || '')'';
                
                v_sql := ''INSERT INTO DQ_FAILED_ROW_KEYS (DATASET_RUN_ID, RULE_CONFIG_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, FAILED_KEY) 
                          SELECT '' || v_run_id || '', '' || v_check_config_id || '', '''''' || COALESCE(v_database_name, ''N/A'') || '''''', '''''' || COALESCE(v_schema_name, ''N/A'') || '''''', '''''' || COALESCE(v_table_name, ''N/A'') || '''''', '' || v_key_construct_expr ||
                          '' FROM '' || v_from_clause || '' WHERE '' || v_where_clause_condition || v_incremental_filter;
                EXECUTE IMMEDIATE v_sql;
            END IF;
        EXCEPTION
            WHEN OTHER THEN
                v_error_message := ''Key capture error: '' || SQLERRM;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
                RETURN v_execution_error;
        END;
    END IF;
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Keys captured'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 6. Structured Failure Table Capture
    ----------------------------------------------------------------------------------------------------
    v_step := ''INSERT_FAILED_RECORDS'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Processing failed records'');

    IF (v_unexpected > 0) THEN
        BEGIN
            v_clean_dataset_name := REGEXP_REPLACE(v_data_asset_name, ''[^a-zA-Z0-9]'', ''_'');
            v_failed_records_table := v_clean_dataset_name || ''_DQ_FAILURE'';
            v_full_target_table_name := ''"'' || v_dq_db_name || ''"."'' || v_dq_schema_name || ''"."'' || v_failed_records_table || ''"'';

            v_sql := ''CREATE TABLE IF NOT EXISTS '' || v_full_target_table_name || '' AS 
                     SELECT '' || v_run_id || ''::NUMBER AS DATASET_RUN_ID, '' || v_data_asset_id || ''::NUMBER AS DATASET_ID, '' || v_check_config_id || ''::NUMBER AS RULE_CONFIG_ID, 
                     CURRENT_TIMESTAMP()::TIMESTAMP_LTZ AS DQ_LOAD_TIMESTAMP, * FROM '' || v_from_clause || '' WHERE 1=0'';
            EXECUTE IMMEDIATE v_sql;

            v_sql := ''INSERT INTO '' || v_full_target_table_name || 
                     '' SELECT '' || v_run_id || '', '' || v_data_asset_id || '', '' || v_check_config_id || '', CURRENT_TIMESTAMP(), * FROM '' || v_from_clause || 
                     '' WHERE '' || v_where_clause_condition || v_incremental_filter ||
                     CASE WHEN v_failed_rows_cnt_limit > 0 THEN '' LIMIT '' || v_failed_rows_cnt_limit ELSE '''' END;
            EXECUTE IMMEDIATE v_sql;
        EXCEPTION
            WHEN OTHER THEN
                v_error_message := ''Failure table capture error: '' || SQLERRM;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
                RETURN v_execution_error;
        END;
    ELSE
        v_failed_records_table := ''No Failed Records'';
    END IF;
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Failure capture complete'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 7. Insert Results (SELECT Pattern)
    ----------------------------------------------------------------------------------------------------
    v_step := ''INSERT_DQ_RESULTS_TABLE'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Loading final results'');

    BEGIN
        LET details_json_str STRING := ''{"regex_list": '' || COALESCE(v_regex_list::STRING, ''[]'') || '', "match_on": "'' || v_match_on || ''"}'';
        LET results_json_str STRING := ''{"element_count": '' || v_total || '', "unexpected_count": '' || v_unexpected || '', "failed_records_table": "'' || COALESCE(v_failed_records_table, ''null'') || ''"}'';
        
        v_sql := ''INSERT INTO "'' || v_dq_db_name || ''"."'' || v_dq_schema_name || ''".DQ_RULE_RESULTS (
                BATCH_ID, DATASET_RUN_ID, DATASET_ID, RULE_CONFIG_ID, EXPECTATION_ID, RUN_NAME, RUN_TIMESTAMP, DATASET_NAME,
                EXPECATION_CONFIG, IS_SUCCESS, RESULTS, EXPECTATION_NAME, DETAILS, ELEMENT_COUNT,
                UNEXPECTED_COUNT, UNEXPECTED_PERCENT, MISSING_COUNT, MISSING_PERCENT, UNEXPECTED_PERCENT_TOTAL
                ) SELECT '' ||
                COALESCE(v_batch_id::STRING, ''null'') || '', '' || v_run_id || '', '' || v_data_asset_id || '', '' || v_check_config_id || '', '' || v_expectation_id || '', '''''' || 
                REPLACE(COALESCE(v_run_name, ''null''), '''''''', '''''''''''') || '''''', CURRENT_TIMESTAMP(), '''''' || REPLACE(COALESCE(v_data_asset_name, ''null''), '''''''', '''''''''''') || '''''', 
                PARSE_JSON('''''' || REPLACE(v_input_rule_str, '''''''', '''''''''''') || ''''''), '' || 
                CASE WHEN v_status_code = v_success_code THEN ''TRUE'' ELSE ''FALSE'' END || '', 
                PARSE_JSON('''''' || REPLACE(results_json_str, '''''''', '''''''''''') || ''''''), '''''' || REPLACE(COALESCE(v_expectation_name, ''null''), '''''''', '''''''''''') || '''''', 
                PARSE_JSON('''''' || REPLACE(details_json_str, '''''''', '''''''''''') || ''''''), '' ||
                v_total || '', '' || v_unexpected || '', '' || (v_percent*100) || '', '' || v_missing_count || '', '' || (v_missing_percent*100) || '', '' || (v_unexpected_percent_total*100);

        EXECUTE IMMEDIATE v_sql;
    EXCEPTION
        WHEN OTHER THEN
            v_error_message := ''Results insertion error: '' || SQLERRM;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_execution_error;
    END;

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Process complete'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    RETURN v_status_code;

EXCEPTION
    WHEN OTHER THEN
        v_error_message := ''Global procedure exception: '' || SQLERRM;
        INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, END_TIMESTAMP, STATUS, ERROR_MESSAGE)
        VALUES (COALESCE(:v_run_id, -1), COALESCE(:v_check_config_id, -1), COALESCE(:v_procedure_name, ''SP_REGEX_NOT_MATCH_LIST_CHECK''), COALESCE(:v_step, ''UNKNOWN''), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''FAILED'', :v_error_message);
        RETURN COALESCE(v_execution_error, 400);
END;
';