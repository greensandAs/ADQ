USE DATABASE {{ snowflake_database }};
USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "SP_MULTICOLUMN_SUM_EQUAL_CHECK"("RULE" VARIANT)
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
    v_dataset_type STRING; 
    v_sql_query STRING; 
    v_from_clause STRING; 

    -- Expectation-specific variables
    v_column_list_variant VARIANT;
    v_sum_total FLOAT;
    v_ignore_row_if STRING; -- NEW VARIABLE
    v_sum_expression STRING;
    v_missing_condition STRING; -- Condition for "either" missing
    v_all_missing_condition STRING; -- Condition for "both/all" missing
    v_where_clause_condition STRING;
    v_ignore_clause STRING DEFAULT ''''; -- Dynamic ignore logic
    v_is_successful BOOLEAN;

    -- Variables for failed row keys
    v_key_column_names STRING;
    v_pk_column_names STRING;
    v_ck_column_names STRING;
    v_key_construct_expr STRING;
    v_key_parts_list STRING;
    v_kwargs_pk_columns STRING; 
    v_kwargs_ck_columns STRING; 

    -- Variables for results
    v_observed_value VARIANT;
    v_partial_unexpected_list VARIANT;
    v_missing_count INT DEFAULT 0;
    v_missing_percent FLOAT DEFAULT 0;
    v_unexpected_percent_nonmissing FLOAT DEFAULT 0;
    v_unexpected_percent_total FLOAT DEFAULT 0;

    -- DYNAMIC TABLE VARIABLES
    v_clean_dataset_name STRING;
    v_full_target_table_name STRING;

BEGIN
    v_input_rule_str := TO_VARCHAR(RULE);

    ----------------------------------------------------------------------------------------------------
    -- 1. Load Configuration
    v_step := ''CONFIG_LOADING'';
    v_procedure_name := COALESCE(RULE:PROCEDURE_NAME::STRING, ''SP_MULTICOLUMN_SUM_EQUAL_CHECK'');
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

        IF (v_dq_db_name IS NULL) THEN
            v_error_message := ''Required Configuration parameter is missing or NULL.'';
            v_status_code := 400;
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

    BEGIN
        -- Standard Params
        v_batch_id := COALESCE(RULE:BATCH_ID::NUMBER, -1);
        v_run_id := COALESCE(RULE:DATASET_RUN_ID::NUMBER, -1);
        v_data_asset_id := COALESCE(RULE:DATASET_ID::NUMBER, -1);
        v_check_config_id := COALESCE(RULE:RULE_CONFIG_ID::NUMBER, -1);
        v_expectation_id := COALESCE(RULE:EXPECTATION_ID::NUMBER, -1);
        v_run_name := RULE:RUN_NAME::STRING;
        v_data_asset_name := RULE:DATASET_NAME::STRING;
        v_expectation_name := RULE:EXPECTATION_NAME::STRING;
        v_kwargs_variant := PARSE_JSON(RULE:KWARGS);
        v_procedure_name := COALESCE(RULE:PROCEDURE_NAME::STRING, ''SP_MULTICOLUMN_SUM_EQUAL_CHECK'');
        v_database_name := RULE:DATABASE_NAME::STRING;
        v_schema_name := RULE:SCHEMA_NAME::STRING;
        v_table_name := RULE:TABLE_NAME::STRING;
        v_dimension := RULE:DIMENSION;
        
        v_dataset_type := COALESCE(RULE:DATASET_TYPE::STRING, ''TABLE''); 
        v_sql_query := RULE:CUSTOM_SQL::STRING; 

        -- EXPECTATION SPECIFIC
        v_column_list_variant := v_kwargs_variant:column_list;
        v_sum_total := v_kwargs_variant:sum_total::FLOAT;
        v_allowed_deviation := COALESCE(v_kwargs_variant:mostly::FLOAT, 1.0); 
        v_failed_rows_cnt_limit := v_kwargs_variant:failed_row_count::NUMBER;
        
        -- NEW: ignore_row_if parameter (Default to ''neither'')
        v_ignore_row_if := COALESCE(LOWER(v_kwargs_variant:ignore_row_if::STRING), ''neither'');

        -- Key Columns
        v_kwargs_pk_columns := v_kwargs_variant:primary_key_columns::STRING;
        v_kwargs_ck_columns := v_kwargs_variant:candidate_key_columns::STRING;

        -- Validation
        IF (v_column_list_variant IS NULL OR ARRAY_SIZE(v_column_list_variant) < 1) THEN
            v_error_message := ''KWARGS must contain a non-empty "column_list" parameter.'';
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        ELSEIF (v_sum_total IS NULL) THEN
            v_error_message := ''KWARGS must contain the "sum_total" parameter.'';
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        END IF;

        IF (UPPER(v_dataset_type) = ''QUERY'') THEN
            v_from_clause := ''('' || v_sql_query || '') AS custom_query_source'';
            v_database_name := COALESCE(v_database_name, ''QUERY_SOURCE'');
            v_schema_name := COALESCE(v_schema_name, ''QUERY_SOURCE'');
            v_table_name := COALESCE(v_table_name, ''QUERY_SOURCE'');
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

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Input rule parsed. Ignore mode: '' || :v_ignore_row_if WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;


    ----------------------------------------------------------------------------------------------------
    -- 3. Execute the Main Data Quality Check Query
    v_step := ''MAIN_QUERY'';
    
    BEGIN
        -- 3a. Build Dynamic SQL components
        IF (UPPER(v_dataset_type) = ''QUERY'') THEN
            v_sql := ''
                WITH column_data AS (
                    SELECT value::STRING AS column_name, index
                    FROM TABLE(FLATTEN(INPUT => PARSE_JSON('''''' || REPLACE(TO_VARCHAR(TO_JSON(:v_column_list_variant)), '''''''', '''''''''''') || '''''')))
                )
                SELECT 
                    LISTAGG(''''COALESCE(TRY_CAST("'''' || column_name || ''''" AS FLOAT), 0)'''', '''' + '''') WITHIN GROUP (ORDER BY index) AS sum_expr, 
                    LISTAGG(''''("'''' || column_name || ''''" IS NULL)'''', '''' OR '''') WITHIN GROUP (ORDER BY index) AS any_missing_expr,
                    LISTAGG(''''("'''' || column_name || ''''" IS NULL)'''', '''' AND '''') WITHIN GROUP (ORDER BY index) AS all_missing_expr
                FROM column_data
            '';
        ELSE
            v_sql := ''
                WITH input_columns AS (
                    SELECT value::STRING AS column_name, index
                    FROM TABLE(FLATTEN(INPUT => PARSE_JSON('''''' || REPLACE(TO_VARCHAR(TO_JSON(:v_column_list_variant)), '''''''', '''''''''''') || '''''')))
                )
                SELECT
                    LISTAGG(
                        CASE
                            WHEN T2.DATA_TYPE IN (''''VARCHAR'''', ''''STRING'''', ''''TEXT'''')
                            THEN ''''COALESCE(KANJI_TO_NUMERIC(TRY_TO_VARIANT("'''' || T1.column_name || ''''"))::FLOAT, 0)''''
                            ELSE ''''COALESCE(TRY_CAST("'''' || T1.column_name || ''''" AS FLOAT), 0)'''' 
                        END, '''' + ''''
                    ) WITHIN GROUP (ORDER BY T1.index) AS sum_expr,
                    LISTAGG(''''("'''' || T1.column_name || ''''" IS NULL)'''', '''' OR '''') WITHIN GROUP (ORDER BY T1.index) AS any_missing_expr,
                    LISTAGG(''''("'''' || T1.column_name || ''''" IS NULL)'''', '''' AND '''') WITHIN GROUP (ORDER BY T1.index) AS all_missing_expr
                FROM input_columns T1
                LEFT JOIN "'' || v_database_name || ''".INFORMATION_SCHEMA.COLUMNS T2
                    ON T1.column_name = T2.COLUMN_NAME
                    AND T2.TABLE_SCHEMA = '''''' || v_schema_name || ''''''
                    AND T2.TABLE_NAME = '''''' || v_table_name || ''''''
            '';
        END IF;

        v_result := (EXECUTE IMMEDIATE v_sql);
        LET v_expr_cursor CURSOR FOR v_result;
        FOR record IN v_expr_cursor DO
            v_sum_expression := record.sum_expr;
            v_missing_condition := record.any_missing_expr; -- "either_value_is_missing"
            v_all_missing_condition := record.all_missing_expr; -- "both_values_are_missing"
            BREAK;
        END FOR;

        -- 3b. Determine Ignore Logic
        IF (v_ignore_row_if = ''any_value_is_missing'' OR v_ignore_row_if = ''either_value_is_missing'') THEN
            v_ignore_clause := '' AND NOT ('' || v_missing_condition || '') '';
        ELSEIF (v_ignore_row_if = ''all_values_are_missing'' OR v_ignore_row_if = ''both_values_are_missing'') THEN
            v_ignore_clause := '' AND NOT ('' || v_all_missing_condition || '') '';
        ELSE 
            -- ''neither'' or unknown value: Do not ignore any rows
            v_ignore_clause := '''';
        END IF;

        -- 3c. Build Queries
        -- The failure condition (ABS > 0.0001) is checked ONLY on rows that are NOT ignored.
        v_where_clause_condition := '' ABS(('' || v_sum_expression || '') - '' || v_sum_total || '') > 0.0001 '';
        
        -- Combined WHERE clause for unexpected count: (Condition Fails) AND (Row is NOT Ignored)
        v_where_clause_condition := v_where_clause_condition || v_ignore_clause;

        v_sql := ''SELECT
                    COUNT(*) AS total_count, -- Total rows in table (or filtered by ignore if strictly interpreting "element_count")
                    -- Note: GE typically counts "element_count" as rows AFTER the ignore filter.
                    SUM(CASE WHEN 1=1 '' || v_ignore_clause || '' THEN 1 ELSE 0 END) AS effective_element_count,
                    COUNT_IF('' || v_missing_condition || '') AS missing_count,
                    COUNT_IF('' || v_where_clause_condition || '') AS unexpected_count
                    FROM '' || v_from_clause;

        v_result := (EXECUTE IMMEDIATE v_sql);
        LET v_cursor CURSOR FOR v_result;
        FOR record IN v_cursor DO
            -- For GE compliance: "element_count" usually refers to the rows evaluated. 
            -- If we ignore rows, the denominator changes.
            v_total := COALESCE(record.effective_element_count, 0); 
            v_missing_count := COALESCE(record.missing_count, 0);
            v_unexpected := COALESCE(record.unexpected_count, 0);
            BREAK;
        END FOR;

        -- Calculate Metrics
        v_percent := CASE WHEN v_total = 0 THEN 0 ELSE (v_unexpected::FLOAT / v_total) END;
        -- Missing percent is purely informational in GE, usually based on the full raw count, but here we keep it simple.
        v_missing_percent := CASE WHEN v_total = 0 THEN 0 ELSE (v_missing_count::FLOAT / v_total) END;
        v_unexpected_percent_total := v_percent;
        v_unexpected_percent_nonmissing := v_percent; -- Simplified for this specific rule type

        v_is_successful := (1.0 - v_percent) >= v_allowed_deviation;
        v_status_code := CASE WHEN v_is_successful THEN v_success_code ELSE v_failed_code END;
        v_log_message := ''Evaluated '' || v_total || '' rows (after ignore logic). Unexpected: '' || v_unexpected;

    EXCEPTION
        WHEN OTHER THEN
            v_error_message := ''Error in main query execution: '' || SQLERRM;
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
    END;

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;


    ----------------------------------------------------------------------------------------------------
    -- 4. Capture Failed Row Keys
    v_step := ''CAPTURE_FAILED_KEYS'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Processing failed row keys'');

    IF (v_unexpected > 0) THEN
        BEGIN
            v_key_column_names := NULL;
            v_pk_column_names := NULL;
            v_ck_column_names := NULL;

            IF (UPPER(v_dataset_type) = ''TABLE'') THEN
                v_sql := ''SELECT PRIMARY_KEY_COLUMNS, CANDIDATE_KEY_COLUMNS FROM DQ_DATASET WHERE DATASET_ID = '' || :v_data_asset_id;
                v_result := (EXECUTE IMMEDIATE v_sql);
                LET v_pk_cursor CURSOR FOR v_result;
                FOR pk_record IN v_pk_cursor DO
                    v_pk_column_names := ARRAY_TO_STRING(PARSE_JSON(pk_record.PRIMARY_KEY_COLUMNS):primary_key, '','');
                    v_ck_column_names := pk_record.CANDIDATE_KEY_COLUMNS;
                    BREAK;
                END FOR;
            END IF;
            
            v_pk_column_names := COALESCE(v_kwargs_pk_columns, v_pk_column_names);
            v_ck_column_names := COALESCE(v_kwargs_ck_columns, v_ck_column_names);

            IF (v_pk_column_names IS NOT NULL AND TRIM(v_pk_column_names) != '''') THEN
                v_key_column_names := v_pk_column_names;
            ELSEIF (v_ck_column_names IS NOT NULL AND TRIM(v_ck_column_names) != '''') THEN
                v_key_column_names := v_ck_column_names;
            END IF;
            
            IF (v_key_column_names IS NOT NULL) THEN
                SELECT LISTAGG('''''''' || TRIM(value) || '''''''' || '', '' || TRIM(value), '', '') WITHIN GROUP (ORDER BY seq)
                INTO v_key_parts_list
                FROM TABLE(SPLIT_TO_TABLE(:v_key_column_names, '',''));
                v_key_construct_expr := ''OBJECT_CONSTRUCT('' || v_key_parts_list || '')'';
                v_sql := ''INSERT INTO DQ_FAILED_ROW_KEYS (DATASET_RUN_ID, RULE_CONFIG_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, FAILED_KEY) SELECT '' || :v_run_id || '', '' || :v_check_config_id || '', '''''' || v_database_name || '''''', '''''' || v_schema_name || '''''', '''''' || v_table_name || '''''', '' || v_key_construct_expr || '' FROM '' || v_from_clause || '' WHERE '' || v_where_clause_condition || CASE WHEN v_failed_rows_cnt_limit > 0 THEN '' LIMIT '' || v_failed_rows_cnt_limit ELSE '''' END;
                EXECUTE IMMEDIATE v_sql;
                v_rows_inserted := SQLROWCOUNT;
                v_log_message := v_rows_inserted || '' keys captured.'';
            ELSE 
                v_log_message := ''No explicit PK/CK usable. Capturing entire failed row.'';
                v_sql := ''INSERT INTO DQ_FAILED_ROW_KEYS (DATASET_RUN_ID, RULE_CONFIG_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, FAILED_KEY) SELECT '' || :v_run_id || '', '' || :v_check_config_id || '', '''''' || v_database_name || '''''', '''''' || v_schema_name || '''''', '''''' || v_table_name || '''''', OBJECT_CONSTRUCT(*) FROM '' || v_from_clause || '' WHERE '' || v_where_clause_condition || CASE WHEN v_failed_rows_cnt_limit > 0 THEN '' LIMIT '' || v_failed_rows_cnt_limit ELSE '''' END;
                EXECUTE IMMEDIATE v_sql;
                v_rows_inserted := SQLROWCOUNT;
                v_log_message := v_rows_inserted || '' rows captured as keys.'';
            END IF;

        EXCEPTION
            WHEN OTHER THEN
                v_error_message := ''Error capturing failed row keys: '' || SQLERRM;
                v_status_code := v_execution_error;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
                RETURN v_status_code;
        END;
    ELSE
        v_log_message := ''No failed rows found.'';
    END IF;
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;


    ----------------------------------------------------------------------------------------------------
    -- 5. Handle and Log Failed Records (Structured Table)
    v_step := ''INSERT_FAILED_RECORDS'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Processing failed records'');

    IF (v_unexpected > 0) THEN
        BEGIN
            LET v_partial_list_expr STRING;
            SELECT ''OBJECT_CONSTRUCT('' || LISTAGG('''''''' || TRIM(value::STRING) || '''''''' || '', '' || TRIM(value::STRING), '', '') WITHIN GROUP (ORDER BY index) || '')''
            INTO v_partial_list_expr
            FROM TABLE(FLATTEN(INPUT => :v_column_list_variant));

            v_sql := ''SELECT ARRAY_AGG('' || v_partial_list_expr || '') AS partial_list FROM (SELECT * FROM '' || v_from_clause || '' WHERE '' || v_where_clause_condition || '' LIMIT 20)'';
            v_result := (EXECUTE IMMEDIATE v_sql);
            LET v_cursor CURSOR FOR v_result;
            FOR record IN v_cursor DO
                v_partial_unexpected_list := record.partial_list;
            END FOR;
        EXCEPTION
            WHEN OTHER THEN
                v_partial_unexpected_list := PARSE_JSON(''[]'');
        END;

        BEGIN
            v_clean_dataset_name := REGEXP_REPLACE(v_data_asset_name, ''[^a-zA-Z0-9]'', ''_'');
            v_failed_records_table := v_clean_dataset_name || ''_DQ_FAILURE'';
            v_full_target_table_name := ''"'' || v_dq_db_name || ''"."'' || v_dq_schema_name || ''"."'' || v_failed_records_table || ''"'';

            v_sql := ''CREATE TABLE IF NOT EXISTS '' || v_full_target_table_name || '' AS '' ||
                     ''SELECT '' ||
                     v_run_id || ''::NUMBER(38,0) AS DATASET_RUN_ID, '' ||
                     v_data_asset_id || ''::NUMBER(38,0) AS DATASET_ID, '' ||
                     v_check_config_id || ''::NUMBER(38,0) AS RULE_CONFIG_ID, '' ||
                     ''CURRENT_TIMESTAMP()::TIMESTAMP_LTZ AS DQ_LOAD_TIMESTAMP, '' ||
                     '' * FROM '' || v_from_clause || '' WHERE 1=0'';
            EXECUTE IMMEDIATE v_sql;

            v_sql := ''INSERT INTO '' || v_full_target_table_name || '' '' ||
                     ''SELECT '' ||
                     v_run_id || '', '' ||
                     v_data_asset_id || '', '' ||
                     v_check_config_id || '', '' ||
                     ''CURRENT_TIMESTAMP(), '' ||
                     '' * FROM '' || v_from_clause || 
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
        v_partial_unexpected_list := PARSE_JSON(''[]'');
    END IF;
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;


    ----------------------------------------------------------------------------------------------------
    -- 6. Insert Results
    v_step := ''INSERT_DQ_RESULTS_TABLE'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Loading results'');

    IF (v_error_message IS NULL) THEN
        BEGIN
        LET details_json_str STRING := ''{'' ||
            ''"column_list": '' || COALESCE(TO_VARCHAR(v_column_list_variant), ''null'') || '','' ||
            ''"sum_total": '' || COALESCE(v_sum_total::STRING, ''null'') || '','' ||
            ''"mostly": '' || COALESCE(v_allowed_deviation::STRING, ''null'') || '','' ||
            ''"ignore_row_if": "'' || COALESCE(v_ignore_row_if, ''null'') || ''"'' ||
        ''}'' ;

        LET results_json_str STRING := ''{'' ||
            ''"element_count": '' || COALESCE(v_total::STRING, ''null'') || '','' ||
            ''"unexpected_count": '' || COALESCE(v_unexpected::STRING, ''null'') || '','' ||
            ''"unexpected_percent": '' || COALESCE(v_percent*100::STRING, ''null'') || '','' ||
            ''"missing_count": '' || COALESCE(v_missing_count::STRING, ''null'') || '','' ||
            ''"missing_percent": '' || COALESCE(v_missing_percent*100::STRING, ''null'') || '','' ||
            ''"unexpected_percent_total": '' || COALESCE(v_unexpected_percent_total*100::STRING, ''null'') || '','' ||
            ''"unexpected_percent_nonmissing": '' || COALESCE(v_unexpected_percent_nonmissing*100::STRING, ''null'') || '','' ||
            ''"partial_unexpected_list": '' || IFF(v_partial_unexpected_list IS NULL, ''[]'', TO_JSON(v_partial_unexpected_list)) || '','' ||
            ''"failed_records_table": "'' || COALESCE(v_failed_records_table, ''null'') || ''"'' ||
        ''}'' ;

        v_sql := ''INSERT INTO "'' || v_dq_db_name || ''"."'' || v_dq_schema_name || ''".DQ_RULE_RESULTS (
            BATCH_ID, DATASET_RUN_ID, DATASET_ID, RULE_CONFIG_ID, EXPECTATION_ID, RUN_NAME, RUN_TIMESTAMP, DATASET_NAME,
            EXPECATION_CONFIG, IS_SUCCESS, RESULTS, EXPECTATION_NAME, DETAILS, ELEMENT_COUNT, MISSING_COUNT, MISSING_PERCENT,
            OBSERVED_VALUE, PARTIAL_UNEXPECTED_COUNTS, PARTIAL_UNEXPECTED_INDEX_LIST, PARTIAL_UNEXPECTED_LIST, UNEXPECTED_COUNT,
            UNEXPECTED_PERCENT, UNEXPECTED_PERCENT_NONMISSING, UNEXPECTED_PERCENT_TOTAL, UNEXPECTED_ROWS, DATA_ROWS, FAILED_ROWS, DIMENSION
            )
            SELECT
            '' || COALESCE(v_batch_id::STRING, ''null'') || '', '' ||
            COALESCE(v_run_id::STRING, ''null'') || '', '' ||
            COALESCE(v_data_asset_id::STRING, ''null'') || '', '' ||
            COALESCE(v_check_config_id::STRING, ''null'') || '', '' ||
            COALESCE(v_expectation_id::STRING, ''null'') || '', \\'''' || REPLACE(COALESCE(v_run_name, ''null''), ''\\'''', '''''''''''') || ''\\'', CURRENT_TIMESTAMP(), \\'''' || REPLACE(COALESCE(v_data_asset_name, ''null''), ''\\'''', '''''''''''') || ''\\'', PARSE_JSON(\\'''' || REPLACE(COALESCE(RULE::STRING, ''null''), ''\\'''', '''''''''''') || ''\\''), '' || CASE WHEN v_status_code = v_success_code THEN ''TRUE'' ELSE ''FALSE'' END || '', PARSE_JSON(\\'''' || REPLACE(results_json_str, ''\\'''', '''''''''''') || ''\\''), \\'''' || REPLACE(COALESCE(v_expectation_name, ''null''), ''\\'''', '''''''''''') || ''\\'', PARSE_JSON(\\'''' || REPLACE(details_json_str, ''\\'''', '''''''''''') || ''\\''), '' ||
            COALESCE(v_total::STRING, ''null'') || '', '' ||
            COALESCE(v_missing_count::STRING, ''null'') || '', '' ||
            COALESCE(v_missing_percent*100::STRING, ''null'') || '', '' ||
            ''NULL::VARIANT, NULL::VARIANT, NULL::VARIANT, PARSE_JSON(\\'''' || IFF(v_partial_unexpected_list IS NULL, ''[]'', REPLACE(TO_VARCHAR(v_partial_unexpected_list), ''\\'''', '''''''''''')) || ''\\''), '' ||
            COALESCE(v_unexpected::STRING, ''null'') || '', '' ||
            COALESCE(v_percent*100::STRING, ''null'') || '', '' ||
            COALESCE(v_unexpected_percent_nonmissing*100::STRING, ''null'') || '', '' ||
            COALESCE(v_unexpected_percent_total*100::STRING, ''null'') || '', NULL::VARIANT , NULL::VARIANT,  NULL::VARIANT, \\'''' || COALESCE(v_dimension::STRING, ''null'') || ''\\'''';
        
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
            VALUES (COALESCE(:v_run_id, -1), COALESCE(:v_check_config_id, -1), COALESCE(:v_procedure_name, ''SP_MULTICOLUMN_SUM_EQUAL_CHECK''), COALESCE(:v_step, ''UNKNOWN''), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''FAILED'', :v_error_message);
        EXCEPTION WHEN OTHER THEN NULL;
        END;
        RETURN COALESCE(v_execution_error, 400);
END;
';;
