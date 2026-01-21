USE DATABASE {{ snowflake_database }};
USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "SP_COLUMN_PAIR_A_GREATER_THAN_B2"("RULE" VARIANT)
RETURNS NUMBER(38,0)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    -- Standard framework variables
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
    v_where_clause_condition STRING;
    v_stage_path STRING;

    -- NEW: Variables for dynamic source handling
    v_dataset_type STRING;
    v_sql_query STRING;
    v_from_clause STRING;

    -- Column Pair specific variables
    v_column_a STRING;
    v_column_b STRING;
    v_or_equal BOOLEAN;
    v_column_a_type STRING;
    v_column_b_type STRING;
    v_column_a_expr STRING;
    v_column_b_expr STRING;
    v_comparison_operator STRING;

    -- Variables for failed row keys enhancement
    v_key_column_names STRING;
    v_pk_column_names STRING;
    v_ck_column_names STRING;
    v_key_construct_expr STRING;
    v_key_parts_list STRING;

    -- Variables for Great Expectations style results
    v_observed_value VARIANT;
    v_missing_percent FLOAT DEFAULT 0;
    v_unexpected_percent_nonmissing FLOAT DEFAULT 0;
    v_unexpected_percent_total FLOAT DEFAULT 0;
    v_unexpected_percent FLOAT DEFAULT 0;

BEGIN
    v_input_rule_str := TO_VARCHAR(RULE);

    -- 1. Load configuration
    v_step := ''CONFIG_LOADING'';
    v_procedure_name := COALESCE(RULE:PROCEDURE_NAME::STRING, ''SP_COLUMN_PAIR_A_GREATER_THAN_B'');
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
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
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

    -- 2. Parse and validate the rule parameter
    v_step := ''RULE_PARSING'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', :v_input_rule_str);
    BEGIN
        v_batch_id := COALESCE(RULE:BATCH_ID::NUMBER, -1);
        v_data_asset_id := COALESCE(RULE:DATASET_ID::NUMBER, -1);
        v_expectation_id := COALESCE(RULE:EXPECTATION_ID::NUMBER, -1);
        
        -- Source parameters
        v_dataset_type := COALESCE(RULE:DATASET_TYPE::STRING, ''TABLE''); -- Default to TABLE
        v_sql_query := RULE:CUSTOM_SQL::STRING;
        v_database_name := RULE:DATABASE_NAME::STRING;
        v_schema_name := RULE:SCHEMA_NAME::STRING;
        v_table_name := RULE:TABLE_NAME::STRING;

        v_run_name := RULE:RUN_NAME::STRING;
        v_data_asset_name := RULE:DATASET_NAME::STRING;
        v_expectation_name := RULE:EXPECTATION_NAME::STRING;
        v_kwargs_variant := PARSE_JSON(RULE:KWARGS);
        v_column_a := v_kwargs_variant:column_A::STRING;
        v_column_b := v_kwargs_variant:column_B::STRING;
        v_or_equal := COALESCE(v_kwargs_variant:or_equal::BOOLEAN, FALSE);
        v_allowed_deviation := COALESCE(v_kwargs_variant:mostly::FLOAT, 1.0);
        v_failed_rows_cnt_limit := v_kwargs_variant:failed_row_count::NUMBER;

        IF (v_column_a IS NULL OR v_column_b IS NULL) THEN
            v_error_message := ''Required rule parameter is missing or NULL. Please check column_A or column_B.'';
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        END IF;

        -- Validation for source type
        IF (UPPER(v_dataset_type) = ''TABLE'' AND (v_database_name IS NULL OR v_schema_name IS NULL OR v_table_name IS NULL)) THEN
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

        -- Dynamic FROM clause construction
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
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Input rule - parsing completed'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    -- 3. Execute the main data quality check query
    v_step := ''MAIN_QUERY'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Starting validation query'');
    
    BEGIN
        -- Get column data types for TABLE source only. 
        IF (UPPER(v_dataset_type) = ''TABLE'') THEN
            v_sql := ''SELECT 
                         MAX(CASE WHEN COLUMN_NAME = '''''' || v_column_a || '''''' THEN DATA_TYPE ELSE NULL END) as type_a,
                         MAX(CASE WHEN COLUMN_NAME = '''''' || v_column_b || '''''' THEN DATA_TYPE ELSE NULL END) as type_b
                       FROM "'' || v_database_name || ''".INFORMATION_SCHEMA.COLUMNS 
                       WHERE TABLE_SCHEMA = '''''' || v_schema_name || '''''' AND TABLE_NAME = '''''' || v_table_name || '''''' AND COLUMN_NAME IN ('''''' || v_column_a || '''''', '''''' || v_column_b || '''''')'';
            v_result := (EXECUTE IMMEDIATE v_sql);
            LET v_col_type_cursor CURSOR FOR v_result;
            FOR col_record IN v_col_type_cursor DO
                v_column_a_type := col_record.type_a;
                v_column_b_type := col_record.type_b;
                BREAK;
            END FOR;
        ELSE
            -- Assume standard types for QUERY
            v_column_a_type := ''UNKNOWN'';
            v_column_b_type := ''UNKNOWN'';
        END IF;
        
        -- KANJI_TO_NUMERIC conversion is applied only if type is known to be a string.
        v_column_a_expr := CASE WHEN v_column_a_type IN (''VARCHAR'', ''STRING'',''TEXT'') THEN ''KANJI_TO_NUMERIC("'' || v_column_a || ''")'' ELSE ''"'' || v_column_a || ''"'' END;
        v_column_b_expr := CASE WHEN v_column_b_type IN (''VARCHAR'', ''STRING'',''TEXT'') THEN ''KANJI_TO_NUMERIC("'' || v_column_b || ''")'' ELSE ''"'' || v_column_b || ''"'' END;
        
        v_log_message := ''Using expression A: '' || v_column_a_expr || '', Expression B: '' || v_column_b_expr;
    EXCEPTION
        WHEN OTHER THEN
            v_error_message := ''Error getting column metadata (for TABLE) or forming expressions: '' || SQLERRM;
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
    END;

    v_comparison_operator := CASE WHEN v_or_equal THEN ''>='' ELSE ''>'' END;
    
    -- Where clause condition for failed rows (used later for insertion)
    v_where_clause_condition := ''( NOT ('' || v_column_a_expr || '' '' || v_comparison_operator || '' '' || v_column_b_expr || '') OR "'' || v_column_a || ''" IS NULL OR "'' || v_column_b || ''" IS NULL )'';

    IF (v_error_message IS NULL) THEN
        -- Main check query using dynamic FROM clause (v_from_clause contains the table name or the custom query)
        v_sql := ''SELECT
                      COUNT(*) AS total_count,
                      COUNT_IF("'' || v_column_a || ''" IS NULL OR "'' || v_column_b || ''" IS NULL) AS missing_count,
                      COUNT_IF(("'' || v_column_a || ''" IS NOT NULL AND "'' || v_column_b || ''" IS NOT NULL) AND NOT ('' || v_column_a_expr || '' '' || v_comparison_operator || '' '' || v_column_b_expr || '')) AS unexpected_count
                    FROM '' || v_from_clause;
        
        BEGIN
            v_result := (EXECUTE IMMEDIATE v_sql);
            LET v_cursor CURSOR FOR v_result;
            FOR record IN v_cursor DO
                v_total := COALESCE(record.total_count,0);
                v_missing_count := COALESCE(record.missing_count,0);
                v_unexpected := COALESCE(record.unexpected_count,0);
                BREAK;
            END FOR;
            
            -- Calculate percentages 
            v_missing_percent := CASE WHEN v_total = 0 THEN 0 ELSE (v_missing_count::FLOAT / v_total) END;
            v_unexpected_percent := CASE WHEN v_total = 0 THEN 0 ELSE (v_unexpected::FLOAT / v_total) END;
            v_unexpected_percent_total := v_unexpected_percent;
            v_unexpected_percent_nonmissing := CASE WHEN (v_total - v_missing_count) = 0 THEN 0 ELSE (v_unexpected::FLOAT / (v_total - v_missing_count)) END;
    
            v_percent := v_unexpected_percent_nonmissing; -- Used for the ''mostly'' check
            v_status_code := CASE WHEN v_percent <= (1.0 - v_allowed_deviation) THEN v_success_code ELSE v_failed_code END;
            
            v_log_message := ''Expectation results are captured'';
            
        EXCEPTION
            WHEN OTHER THEN
                v_error_message := ''Error in main query execution: '' || SQLERRM;
                v_status_code := v_execution_error;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
                RETURN v_status_code;
        END;
    END IF;
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Validation done. '' || :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    -- 4. Capture Failed Row Keys 
    v_step := ''CAPTURE_FAILED_KEYS'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Processing failed row keys'');
    IF ((v_unexpected + v_missing_count) > 0) THEN
        BEGIN
            -- Only attempt to find keys if it is a TABLE source
            IF (UPPER(v_dataset_type) = ''TABLE'') THEN
                v_sql := ''SELECT PRIMARY_KEY_COLUMNS, CANDIDATE_KEY_COLUMNS FROM DQ_DATASET WHERE DATASET_ID = ''|| :v_data_asset_id;
                v_result := (EXECUTE IMMEDIATE v_sql);
                LET v_pk_cursor CURSOR FOR v_result;
                FOR pk_record IN v_pk_cursor DO
                    -- FIX: Added PARSE_JSON() to handle VARCHAR to VARIANT conversion
                    v_pk_column_names := ARRAY_TO_STRING(PARSE_JSON(pk_record.PRIMARY_KEY_COLUMNS):primary_key, '','');
                    v_ck_column_names := pk_record.CANDIDATE_KEY_COLUMNS;
                    BREAK;
                END FOR;
            END IF;

            v_key_column_names := COALESCE(v_pk_column_names, v_ck_column_names);

            IF (v_key_column_names IS NOT NULL AND TRIM(v_key_column_names) != '''') THEN
                SELECT LISTAGG('''''''' || TRIM(value) || '''''''' || '', '' || TRIM(value), '', '') WITHIN GROUP (ORDER BY seq) INTO v_key_parts_list 
                FROM TABLE(SPLIT_TO_TABLE(:v_key_column_names, '',''));
                
                v_key_construct_expr := ''OBJECT_CONSTRUCT('' || v_key_parts_list || '')'';
                
                -- Use v_from_clause to query the source (table or query)
                v_sql := ''INSERT INTO DQ_FAILED_ROW_KEYS (DATASET_RUN_ID, RULE_CONFIG_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, FAILED_KEY) SELECT '' ||
                          v_run_id || '', '' || v_check_config_id || '', '''''' || COALESCE(v_database_name, ''N/A'') || '''''', '''''' || COALESCE(v_schema_name, ''N/A'') || '''''', '''''' || COALESCE(v_table_name, ''CUSTOM_QUERY'') || '''''', '' || v_key_construct_expr ||
                          '' FROM '' || v_from_clause || '' WHERE '' || v_where_clause_condition;
                EXECUTE IMMEDIATE v_sql;
                v_rows_inserted := SQLROWCOUNT;
                v_log_message := v_rows_inserted || '' keys of failed rows captured.'';
            ELSE
                v_log_message := ''No Primary/Candidate Key found or keys not applicable for query source. Skipping failed key capture.'';
            END IF;
        EXCEPTION
            WHEN OTHER THEN
                v_error_message := ''Error capturing failed row keys: '' || SQLERRM;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''WARNING'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
        END;
    ELSE
        v_log_message := ''No failed rows found, skipping failed key capture.'';
    END IF;
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    -- 5. Handle and log failed records
    v_step := ''INSERT_FAILED_RECORDS'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Processing failed records'');
    IF ((v_unexpected + v_missing_count) > 0) THEN
        v_failed_records_table := ''DQ_FAILED_RECORDS'';
        IF ((v_unexpected + v_missing_count) < v_failed_rows_threshold OR (v_failed_rows_cnt_limit IS NOT NULL AND v_failed_rows_cnt_limit > 0)) THEN
            -- Direct insert uses v_from_clause
            v_sql := ''INSERT INTO "'' || v_dq_db_name || ''"."'' || v_dq_schema_name || ''"."'' || v_failed_records_table || ''" (DATASET_RUN_ID, RULE_CONFIG_ID, FAILED_ROW, LOAD_TIMESTAMP) SELECT '' ||
                      v_run_id || '', '' || v_check_config_id || '', OBJECT_CONSTRUCT(*), CURRENT_TIMESTAMP() FROM '' ||
                      v_from_clause || '' WHERE '' || v_where_clause_condition ||
                      CASE WHEN v_failed_rows_cnt_limit > 0 THEN '' LIMIT '' || v_failed_rows_cnt_limit ELSE '''' END;
            EXECUTE IMMEDIATE v_sql;
            v_log_message := SQLROWCOUNT || '' rows inserted directly into failed records table.'';
        ELSE
            -- COPY INTO uses v_from_clause
            v_stage_path := v_dq_schema_name || ''.DQ_STAGE/DQ_FAILED_RECORDS/'' || v_run_id || ''_'' || v_check_config_id || ''.json'';
            v_sql := ''COPY INTO @'' || v_stage_path ||
                      '' FROM (SELECT OBJECT_CONSTRUCT(''''DATASET_RUN_ID'''', '' || v_run_id || '', ''''RULE_CONFIG_ID'''', '' || v_check_config_id || '', ''''FAILED_ROW'''', OBJECT_CONSTRUCT(*), ''''LOAD_TIMESTAMP'''', CURRENT_TIMESTAMP()) FROM '' ||
                      v_from_clause || '' WHERE '' || v_where_clause_condition || '') FILE_FORMAT = (TYPE = JSON COMPRESSION = AUTO) OVERWRITE = TRUE'';
            EXECUTE IMMEDIATE v_sql;
            v_log_message := ''Failed records unloaded to stage: '' || v_stage_path;
            
            v_sql := ''COPY INTO "'' || v_dq_db_name || ''"."'' || v_dq_schema_name || ''"."'' || v_failed_records_table || ''" FROM (SELECT $1:DATASET_RUN_ID, $1:RULE_CONFIG_ID, $1:FAILED_ROW, $1:LOAD_TIMESTAMP FROM @'' || v_stage_path || '') FILE_FORMAT = (TYPE = JSON)'';
            v_result := (EXECUTE IMMEDIATE v_sql);
        END IF;
    ELSE
        v_failed_records_table := ''No Failed Records'';
        v_log_message := ''No failed records found.'';
    END IF;
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    -- 6. Insert results into the DQ_RULE_RESULTS table
    v_step := ''INSERT_DQ_RESULTS_TABLE'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Loading results'');
    IF (v_error_message IS NULL) THEN
        
        LET results_json_str STRING := ''{'' ||
            ''"element_count": '' || COALESCE(v_total::STRING, ''null'') || '','' ||
            ''"missing_count": '' || COALESCE(v_missing_count::STRING, ''null'') || '','' ||
            ''"missing_percent": '' || COALESCE(v_missing_percent * 100::STRING, ''null'') || '','' ||
            ''"unexpected_count": '' || COALESCE(v_unexpected::STRING, ''null'') || '','' ||
            ''"unexpected_percent": '' || COALESCE(v_unexpected_percent * 100::STRING, ''null'') || '','' ||
            ''"unexpected_percent_total": '' || COALESCE(v_unexpected_percent_total * 100::STRING, ''null'') || '','' ||
            ''"unexpected_percent_nonmissing": '' || COALESCE(v_unexpected_percent_nonmissing * 100::STRING, ''null'') ||
        ''}'' ;
        
        LET details_json_str STRING := ''{}'';

        v_sql := ''INSERT INTO "'' || v_dq_db_name || ''"."'' || v_dq_schema_name || ''".DQ_RULE_RESULTS (
                     BATCH_ID, DATASET_RUN_ID, DATASET_ID, RULE_CONFIG_ID, EXPECTATION_ID, RUN_NAME, RUN_TIMESTAMP, DATASET_NAME,
                     EXPECATION_CONFIG, IS_SUCCESS, RESULTS, EXPECTATION_NAME, DETAILS, ELEMENT_COUNT, MISSING_COUNT, MISSING_PERCENT,
                     OBSERVED_VALUE, PARTIAL_UNEXPECTED_COUNTS, PARTIAL_UNEXPECTED_INDEX_LIST, PARTIAL_UNEXPECTED_LIST, UNEXPECTED_COUNT,
                     UNEXPECTED_PERCENT, UNEXPECTED_PERCENT_NONMISSING, UNEXPECTED_PERCENT_TOTAL, UNEXPECTED_ROWS, DATA_ROWS, FAILED_ROWS
                     )
                     SELECT 
                     '' || COALESCE(v_batch_id::STRING, ''null'') || '', '' ||
                     COALESCE(v_run_id::STRING, ''null'') || '', '' ||
                     COALESCE(v_data_asset_id::STRING, ''null'') || '', '' ||
                     COALESCE(v_check_config_id::STRING, ''null'') || '', '' ||
                     COALESCE(v_expectation_id::STRING, ''null'') || '', '''''' || REPLACE(COALESCE(v_run_name, ''null''), '''''''', '''''''''''') || '''''', CURRENT_TIMESTAMP(), '''''' || REPLACE(COALESCE(v_data_asset_name, ''null''), '''''''', '''''''''''') || '''''', PARSE_JSON('''''' || REPLACE(COALESCE(RULE::STRING, ''null''), '''''''', '''''''''''') || ''''''), '' || CASE WHEN v_status_code = v_success_code THEN ''TRUE'' ELSE ''FALSE'' END || '', PARSE_JSON('''''' || REPLACE(results_json_str, '''''''', '''''''''''') || ''''''), '''''' || REPLACE(COALESCE(v_expectation_name, ''null''), '''''''', '''''''''''') || '''''', PARSE_JSON('''''' || REPLACE(details_json_str, '''''''', '''''''''''') || ''''''), '' ||
                     COALESCE(v_total::STRING, ''null'') || '', '' ||
                     COALESCE(v_missing_count::STRING, ''null'') || '', '' ||
                     COALESCE(v_missing_percent * 100::STRING, ''null'') || 
                     '', NULL, NULL, NULL, NULL, '' || -- OBSERVED_VALUE and PARTIAL columns are NULL
                     COALESCE(v_unexpected::STRING, ''null'') || '', '' ||
                     COALESCE(v_unexpected_percent * 100::STRING, ''null'') || '', '' || 
                     COALESCE(v_unexpected_percent_nonmissing * 100::STRING, ''null'') || '', '' || 
                     COALESCE(v_unexpected_percent_total * 100::STRING, ''null'') ||
                     '', NULL, NULL, NULL''; -- UNEXPECTED_ROWS, DATA_ROWS and FAILED_ROWS are NULL

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
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Results loaded'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
    RETURN v_status_code;
EXCEPTION
    WHEN OTHER THEN
        v_error_message := ''Global exception in step '' || COALESCE(v_step, ''UNKNOWN'') || '': '' || SQLERRM;
        INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, END_TIMESTAMP, STATUS, ERROR_MESSAGE)
        VALUES (COALESCE(:v_run_id, -1), COALESCE(:v_check_config_id, -1), COALESCE(:v_procedure_name, ''SP_COLUMN_PAIR_A_GREATER_THAN_B''), COALESCE(:v_step, ''UNKNOWN''), CURRENT_TIMESTAMP(), ''FAILED'', :v_error_message);
        RETURN COALESCE(v_execution_error, 400);
END;
';;
