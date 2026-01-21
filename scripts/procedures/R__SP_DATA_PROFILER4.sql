USE DATABASE {{ snowflake_database }};
USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "SP_DATA_PROFILER4"(
    "RUN_ID" VARCHAR, "DATASET_ID" VARCHAR, "DATASET_NAME" VARCHAR, "DBNAME" VARCHAR, "SCHEMANAME" VARCHAR, "TABLENAME" VARCHAR, "CUSTOMSQL" VARCHAR
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'ydata-profiling', 'pandas', 'numpy')
HANDLER = 'run_profiler_sp'
EXECUTE AS CALLER
AS '
import warnings
import json
import re
import traceback
from datetime import datetime
import pandas as pd
import numpy as np
from ydata_profiling import ProfileReport
from snowflake.snowpark.functions import udf, col, cast, call_udf
from snowflake.snowpark.types import StringType, FloatType

# Suppress ydata_profiling warnings for cleaner log output
warnings.filterwarnings("ignore", category=FutureWarning)

# --- Helper Function for SQL Identifier Quoting ---
def q_ident(name: str) -> str:
    """Quotes an identifier for safe SQL use."""
    if name is None: return ''NULL''
    # Treat empty string as NULL for safer logging
    if not name or name.strip() == '''': return ''NULL'' 
    name = name.strip(''"'') 
    return ''"'' + name.replace(''"'', ''""'') + ''"''

def q_str(val: str) -> str:
    """Escapes single quotes for SQL string literals and handles None/empty string to SQL NULL."""
    if val is None: return ''NULL''
    if str(val).strip() == '''': return ''NULL'' 
    return "''" + str(val).replace("''", "''''") + "''"

# ==========================================
#  AUDIT LOGGING CONTEXT MANAGER
# ==========================================
class AuditLogManager:
    """Manages the insertion and updating of audit log records."""
    def __init__(self, session, fdb, fschema, audit_table, run_id, dataset_id, ds_name, db, schema, table_name, custom_sql):
        self.session = session
        self.full_table_path = f"{q_ident(fdb)}.{q_ident(fschema)}.{q_ident(audit_table)}"
        self.run_id = run_id
        self.dataset_id = dataset_id
        self.ds_name = ds_name
        self.db_name = db
        self.schema_name = schema
        self.table_name = table_name
        self.custom_sql = custom_sql

    def step(self, step_name):
        """Returns a context manager for a single audit step."""
        return AuditStep(self, step_name)

class AuditStep:
    """Context Manager for a single step in the SP execution."""
    def __init__(self, manager, step_name):
        self.mgr = manager
        self.step_name = step_name

    def __enter__(self):
        """Insert the log entry with STATUS = STARTED."""
        try:
            # q_str() ensures None/empty string inputs for DB/Schema/Table translate to SQL NULL.
            sql = f"""
                INSERT INTO {self.mgr.full_table_path} 
                (RUN_ID, DATASET_ID, DATASET_NAME, DATABASE_NAME, SCHEMA_NAME, 
                 TABLE_NAME, CUSTOM_SQL_QUERY, STEP, STATUS, MESSAGE, LOG_TIMESTAMP)
                VALUES (
                    {q_str(self.mgr.run_id)}, {q_str(self.mgr.dataset_id)}, {q_str(self.mgr.ds_name)},
                    {q_str(self.mgr.db_name)}, {q_str(self.mgr.schema_name)}, {q_str(self.mgr.table_name)},
                    {q_str(self.mgr.custom_sql)}, {q_str(self.step_name)}, ''STARTED'', NULL, CURRENT_TIMESTAMP()
                )
            """
            self.mgr.session.sql(sql).collect()
        except Exception as e:
            # If logging fails, print but do not crash the profiling run
            print(f"AUDIT INIT FAILED for step {self.step_name}: {str(e)}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Update the log entry to SUCCESS or FAILURE."""
        status = "SUCCESS"
        message = ""
        
        if exc_type:
            status = "FAILURE"
            # Format the exception traceback
            message = "".join(traceback.format_exception(exc_type, exc_val, exc_tb))
            message = message[:16777216] # Limit Snowflake VARCHAR max size

        try:
            # Update the log entry based on RUN_ID and STEP
            sql = f"""
                UPDATE {self.mgr.full_table_path}
                SET STATUS = {q_str(status)},
                    MESSAGE = {q_str(message)},
                    LOG_TIMESTAMP = CURRENT_TIMESTAMP()
                WHERE RUN_ID = {q_str(self.mgr.run_id)} 
                  AND STEP = {q_str(self.step_name)}
                  AND STATUS = ''STARTED'' -- Only update the START record
            """
            self.mgr.session.sql(sql).collect()
        except Exception as e:
            print(f"AUDIT UPDATE FAILED for step {self.step_name}: {str(e)}")

        # If an exception occurred in the block, return False to re-raise it
        if exc_type:
            return False 
        return True

# ==========================================
#  LOGIC HELPERS
# ==========================================

def _identify_kanji_numeral_columns(session, snowpark_df, sample_size: int = 1000) -> list:
    """
    Identifies String columns that likely contain Japanese Kanji numerals 
    and should be translated before profiling.
    """
    original_string_cols = [f.name for f in snowpark_df.schema.fields if isinstance(f.datatype, StringType)]
    if not original_string_cols: return []
    
    # Use a small sample for efficiency
    try:
        sample_df = snowpark_df.select(original_string_cols).sample(n=sample_size).to_pandas()
    except Exception: 
        return []
    
    # Pattern includes common Kanji numerals and related characters
    kanji_pattern = re.compile(r''[^一二三四五六七八九十百千万億兆0-9.,s・点〇零]'')
    col_name_map = {c.strip(''"''): c for c in original_string_cols}
    kanji_cols = []
    
    for col_pd in sample_df.columns:
        if col_pd in col_name_map:
            col_data = sample_df[col_pd].dropna().astype(str)
            if not col_data.empty:
                # Count records that contain characters *other than* the expected kanji/numeric chars
                non_compliant_count = col_data.apply(lambda x: bool(kanji_pattern.search(x))).sum()
                compliant_percentage = ((len(col_data) - non_compliant_count) / len(col_data)) * 100
                
                # If 100% of the sample appears to be kanji/numeric, flag for translation
                if compliant_percentage >= 100.0:
                    kanji_cols.append(col_name_map[col_pd])
                    
    return kanji_cols
    
def convert_num_to_kanji_str(session, num, framework_db: str, framework_schema: str):
    """Dynamically calls the NUMERIC_TO_KANJI UDF for summary stats translation."""
    if num is None: return ''NULL''
    try:
        # Check if number is actually float/int before calling UDF
        if isinstance(num, (int, float)) and not pd.isna(num):
            db_ident = q_ident(framework_db)
            schema_ident = q_ident(framework_schema)
            sql_query = f"SELECT {db_ident}.{schema_ident}.NUMERIC_TO_KANJI({num}) AS KANJI_VAL"
            res = session.sql(sql_query).collect()
            if res and ''KANJI_VAL'' in res[0]:
                return q_str(res[0][''KANJI_VAL''])
    except Exception:
        # If UDF call fails, return the original string representation
        pass
    return q_str(str(num))      

def _parse_and_restructure_report(session, audit_mgr, profile_data: dict, ds_name: str, dataset_id: str, run_id: str,
                                 dbname: str, schemaname: str, tablename: str, customsql: str,
                                 framework_db: str, framework_schema: str) -> (pd.DataFrame, pd.DataFrame):
    """
    Extracts column and table statistics from the ydata-profiling JSON report
    and structures them into two Pandas DataFrames for Snowflake insertion.
    Uses filtered metadata (dbname, schemaname, tablename) to populate results.
    """
    # We use the audit manager passed down to log sub-steps
    with audit_mgr.step("Parse Profiling JSON"):
        column_data = []
        for idx, (col_name, stats) in enumerate(profile_data.get("variables", {}).items(), 1):
            col_level = {"column_name": col_name, **stats}
            alert_msg = None
            
            # Find any alerts associated with the current column
            for msg in profile_data.get("alerts", []):
                if col_name in msg: 
                    alert_msg = msg
                    break

            # Use the filtered input parameters
            col_level.update({
                "dataset_name": ds_name, "dataset_id": dataset_id, "run_id": run_id,
                "database_name": dbname, "schema_name": schemaname,    
                "table_name": tablename, "custom_sql": customsql,       
                "column_id": idx,
                "profile_start_timestamp": profile_data.get("analysis", {}).get("date_start"),
                "profile_end_timestamp": profile_data.get("analysis", {}).get("date_end"),
                "profiled_at": profile_data.get("analysis", {}).get("date_end"),
                "column_alert": alert_msg,    
            })
            column_data.append(col_level)

        # Mapping ydata-profiling keys to target table column names
        column_mapping = {
            "type": "data_type", "n_distinct": "column_distinct_count", "p_distinct": "column_distinct_percentage",
            "n_unique": "column_unique_count", "p_unique": "column_unique_percent", "n_missing": "column_missing_count",
            "p_missing": "column_missing_percentage", "n_zeros": "column_zero_count", "p_zeros": "column_zero_percentage",
            "mean": "column_mean_value", "std": "column_std_dev", "min": "column_min_value", "max": "column_max_value",
            "sum": "column_sum_value", "memory_size": "column_memory_size_bytes", "column_alert": "column_alert"  
        }
        
        table = profile_data.get("table", {})
        correlations = profile_data.get("correlations", {})
        
        # Use the filtered input parameters
        table_mapping = {
            "run_id": run_id, "dataset_id": dataset_id,
            "database_name": dbname, "schema_name": schemaname, "table_name": tablename,  
            "custom_sql": customsql, "dataset_name": ds_name,
            "total_rows": table.get("n"), "total_columns": table.get("n_var"),
            "null_count": table.get("n_cells_missing"), "null_percent": table.get("p_cells_missing"),
            "duplicate_count": table.get("n_duplicates"),
            "correlation_matrix": json.dumps(correlations, ensure_ascii=False) if correlations else None,
            "missing_values_count": table.get("n_cells_missing"),
            "profile_start_timestamp": profile_data.get("analysis", {}).get("date_start"),
            "profile_end_timestamp": profile_data.get("analysis", {}).get("date_end"),
            "profiled_at": profile_data.get("analysis", {}).get("date_end"),
            
            # Kanji conversions for high-level summary stats
            "total_rows_ja": convert_num_to_kanji_str(session,table.get("n"), framework_db, framework_schema),
            "total_columns_ja": convert_num_to_kanji_str(session,table.get("n_var"), framework_db, framework_schema),
            "missing_values_count_ja": convert_num_to_kanji_str(session, table.get("n_cells_missing"), framework_db, framework_schema),
            "null_count_ja": convert_num_to_kanji_str(session, table.get("n_cells_missing"), framework_db, framework_schema),
            "duplicate_count_ja": convert_num_to_kanji_str(session,table.get("n_duplicates"), framework_db, framework_schema),
            "null_percent_ja": convert_num_to_kanji_str(session,table.get("p_cells_missing"), framework_db, framework_schema),
        }

        # Helper to convert nested Python objects (lists/dicts) into JSON strings
        def convert_complex(df):
            for c in df.columns:
                if df[c].apply(lambda x: isinstance(x, (list, dict))).any():
                    df[c] = df[c].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else x)
            return df

        columns_df = pd.DataFrame(column_data).rename(columns=column_mapping)
        table_df = pd.DataFrame([table_mapping])
        
        columns_df = convert_complex(columns_df)
        table_df = convert_complex(table_df)

    return columns_df, table_df

# ==========================================
#  MAIN STORED PROCEDURE HANDLER
# ==========================================
def run_profiler_sp(session, run_id: str, dataset_id: str, dataset_name: str, dbname: str, schemaname: str, tablename: str, customsql: str) -> str:
    
    # --- Internal Configuration ---
    column_stats_table = "PROFILER_ATTRIBUTE_STATS"
    table_summary_table = "PROFILER_DATASET_STATS"
    audit_log_table = "PROFILER_AUDIT_LOGS"  
    translate_kanji_numerals = True
    
    framework_db = session.get_current_database().strip(''"'') 
    framework_schema = session.get_current_schema().strip(''"'')
    
    dataset_name_for_report = dataset_name if dataset_name else (tablename if tablename else "custom_query")

    # --- Conditional Metadata Exclusion ---
    # If custom query is present, set DB/Schema/Table names to None so q_str converts them to SQL NULL.
    if customsql:
        db_name_log = None
        schema_name_log = None
        table_name_log = None
    else:
        # For table mode, use the provided inputs
        db_name_log = dbname
        schema_name_log = schemaname
        table_name_log = tablename


    # Initialize Audit Manager using conditional metadata
    audit = AuditLogManager(session, framework_db, framework_schema, audit_log_table, 
                            run_id, dataset_id, dataset_name_for_report, db_name_log, schema_name_log, table_name_log, customsql)

    # 1. Ensure Audit Table Exists (Critical for immediate insertion)
    try:
        create_audit_sql = f"""
        CREATE TABLE IF NOT EXISTS {audit.full_table_path} (
            RUN_ID VARCHAR, DATASET_ID VARCHAR, DATASET_NAME VARCHAR, DATABASE_NAME VARCHAR,
            SCHEMA_NAME VARCHAR, TABLE_NAME VARCHAR, CUSTOM_SQL_QUERY VARCHAR,
            STEP VARCHAR, STATUS VARCHAR, MESSAGE VARCHAR, LOG_TIMESTAMP TIMESTAMP_NTZ
        )
        """
        session.sql(create_audit_sql).collect()
    except Exception as e:
        # If we cant log, we must return a critical failure
        return f"CRITICAL FAILURE: Could not access audit table at start. {str(e)}"

    # Variables for control flow
    temp_table_name = None 
    snowflake_column_types = {}

    # ============================================
    # EXECUTION BLOCKS WITH TRANSACTIONAL AUDITING
    # ============================================
    
    with audit.step("Process Start"):
        if (not tablename and not customsql) or (tablename and customsql):
            raise ValueError("Provide either ''TABLENAME'' or ''CUSTOMSQL'', but not both.")

    # --- 2. Create Snowpark DataFrame and Retrieve Schema ---
    with audit.step("Create Snowpark DF and Retrieve Schema"):
        
        if tablename:
            # --- Path A: Standard Table Profiling ---
            table_path = f"{q_ident(dbname)}.{q_ident(schemaname)}.{q_ident(tablename)}"
            snowpark_df = session.table(table_path)
            
            with audit.step("Retrieve Information Schema for Table"):
                schema_query = f"""
                    SELECT 
                        COLUMN_NAME, 
                        DATA_TYPE 
                    FROM {q_ident(dbname)}.INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = {q_str(schemaname)}
                      AND TABLE_NAME = {q_str(tablename)}
                    ORDER BY ORDINAL_POSITION
                """
                # Fetching the original Snowflake types
                result_df = session.sql(schema_query).to_pandas()
                snowflake_column_types = {
                    col.strip(''"''): dtype.upper()
                    for col, dtype in zip(result_df[''COLUMN_NAME''], result_df[''DATA_TYPE''])
                }
                
        else: 
            # --- Path B: Custom Query Profiling (using Temp Table for Schema) ---
            # Generate a unique temp table name
            temp_table_name = f"TEMP_PROFILE_{run_id}_{datetime.now().strftime(''%Y%m%d%H%M%S'')}"
            temp_table_path = f"{q_ident(framework_db)}.{q_ident(framework_schema)}.{q_ident(temp_table_name)}"
            
            # 2.1. Create Temporary Table 
            with audit.step("Create Temp Table from Query"):
                # Use the custom SQL to populate a temporary table
                create_temp_sql = f"CREATE TEMPORARY TABLE {temp_table_path} AS ({customsql})"
                session.sql(create_temp_sql).collect()

            # 2.2. Get Schema from Information Schema
            with audit.step("Retrieve Temp Table Schema"):
                schema_query = f"""
                    SELECT 
                        COLUMN_NAME, 
                        DATA_TYPE 
                    FROM {q_ident(framework_db)}.INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = {q_str(framework_schema)}
                      AND TABLE_NAME = {q_str(temp_table_name)}
                    ORDER BY ORDINAL_POSITION
                """
                result_df = session.sql(schema_query).to_pandas()
                snowflake_column_types = {
                    col.strip(''"''): dtype.upper()
                    for col, dtype in zip(result_df[''COLUMN_NAME''], result_df[''DATA_TYPE''])
                }

            # 2.3. Create Snowpark DF from the Temporary Table
            snowpark_df = session.table(temp_table_path)

    # --- 3. Optional Kanji Translation ---
    if translate_kanji_numerals:
        with audit.step("Kanji Numeral Translation"):
            kanji_cols_to_translate = _identify_kanji_numeral_columns(session, snowpark_df)
            if kanji_cols_to_translate:
                # Use the KANJI_TO_NUMERIC UDF to convert identified string columns to numeric (Float)
                udf_path = f"{q_ident(framework_db)}.{q_ident(framework_schema)}.KANJI_TO_NUMERIC"
                for col_name in kanji_cols_to_translate:
                    snowpark_df = snowpark_df.withColumn(
                        col_name, 
                        cast(call_udf(udf_path, col(col_name)), FloatType())
                    )

    # --- 4. Core Profiling Execution ---
    with audit.step("Convert to Pandas"):
        # This executes the Snowpark query and fetches the data
        pandas_df = snowpark_df.to_pandas()

    with audit.step("Generate ydata-profiling Report"):
        if pandas_df.empty:
            raise ValueError("Dataset is empty. Cannot profile.")
        # Generate the minimal profile report
        profile = ProfileReport(pandas_df, title=f"Profile for {dataset_name_for_report}", minimal=True)
        report_json_data = json.loads(profile.to_json())

    # --- 5. Restructure and Load Results ---
    
    # Restructure the report using the filtered log variables (db_name_log, schema_name_log, table_name_log)
    column_stats_df, table_summary_df = _parse_and_restructure_report(
        session, audit, report_json_data, 
        ds_name=dataset_name_for_report, dataset_id=dataset_id, run_id=run_id, 
        dbname=db_name_log, schemaname=schema_name_log, tablename=table_name_log, 
        customsql=customsql, framework_db=framework_db, framework_schema=framework_schema
    )

    with audit.step(f"Upload Column Stats to {column_stats_table}"):
        table_path = f"{q_ident(framework_db)}.{q_ident(framework_schema)}.{q_ident(column_stats_table)}"
        
        # Filter columns to match target table schema
        existing_cols = [c.upper() for c in session.table(table_path).columns]       
        upload_cols_df = column_stats_df[[c for c in column_stats_df.columns if c.upper() in existing_cols]].copy()
        
        # Populate the inferred type using the Information Schema/Result Set schema
        upload_cols_df[''column_name_key''] = upload_cols_df[''column_name''].apply(lambda x: x.strip(''"''))
        upload_cols_df["column_inferred_data_type"] = upload_cols_df["column_name_key"].map(snowflake_column_types)
        del upload_cols_df[''column_name_key'']
        
        # Prepare for Snowflake upload
        upload_cols_df.columns = [c.upper() for c in upload_cols_df.columns]

        # Explicitly coerce numeric columns to ensure correct Snowflake types (e.g., FLOAT/NUMBER)
        numeric_stat_cols = ["COLUMN_DISTINCT_COUNT", "COLUMN_UNIQUE_COUNT", "COLUMN_MISSING_COUNT", 
                             "COLUMN_ZERO_COUNT", "COLUMN_MEAN_VALUE", "COLUMN_STD_DEV", 
                             "COLUMN_MIN_VALUE", "COLUMN_MAX_VALUE", "COLUMN_DISTINCT_PERCENTAGE", 
                             "COLUMN_MISSING_PERCENTAGE", "COLUMN_ZERO_PERCENTAGE"]
                             
        for stat_col in numeric_stat_cols:
            if stat_col in upload_cols_df.columns:
                upload_cols_df[stat_col] = pd.to_numeric(upload_cols_df[stat_col], errors=''coerce'')

        # Write data to the column statistics table
        session.write_pandas(upload_cols_df, table_name=column_stats_table, 
                             database=framework_db, schema=framework_schema, 
                             auto_create_table=False, overwrite=False)

    with audit.step(f"Upload Table Summary to {table_summary_table}"):
        upload_summary_df = table_summary_df.copy()
        upload_summary_df.columns = [c.upper() for c in upload_summary_df.columns]
        
        summary_numeric_cols = ["TOTAL_ROWS", "TOTAL_COLUMNS", "NULL_COUNT", "NULL_PERCENT", 
                                "DUPLICATE_COUNT", "MISSING_VALUES_COUNT"]
        for summary_col in summary_numeric_cols:
            if summary_col in upload_summary_df.columns:
                upload_summary_df[summary_col] = pd.to_numeric(upload_summary_df[summary_col], errors=''coerce'')

        # Write data to the table summary table
        session.write_pandas(upload_summary_df, table_name=table_summary_table, 
                             database=framework_db, schema=framework_schema, 
                             overwrite=False, auto_create_table=False)
                             
    # --- 6. Cleanup ---
    # Drop the temporary table if one was created for a custom query
    if temp_table_name:
        temp_table_path = f"{q_ident(framework_db)}.{q_ident(framework_schema)}.{q_ident(temp_table_name)}"
        with audit.step("Drop Temporary Table"):
            try:
                drop_temp_sql = f"DROP TABLE IF EXISTS {temp_table_path}"
                session.sql(drop_temp_sql).collect()
            except Exception as e:
                # Log a warning but let the procedure finish successfully
                print(f"WARNING: Failed to drop temporary table {temp_table_name}. {str(e)}")

    with audit.step("Process End"):
         # Final audit step indicating overall success
        pass

    return "SUCCESS: Profiling complete."
';;
