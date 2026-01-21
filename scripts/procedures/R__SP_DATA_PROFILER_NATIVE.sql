USE DATABASE {{ snowflake_database }};
USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "SP_DATA_PROFILER_NATIVE"("RUN_ID" VARCHAR, "DATASET_ID" VARCHAR, "DATASET_NAME" VARCHAR, "DBNAME" VARCHAR, "SCHEMANAME" VARCHAR, "TABLENAME" VARCHAR, "CUSTOMSQL" VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python','pandas','numpy')
HANDLER = 'run_profiler_native'
EXECUTE AS CALLER
AS '
import json
import traceback
from datetime import datetime
import pandas as pd
import snowflake.snowpark.functions as F
from snowflake.snowpark.types import StringType, IntegerType, DoubleType, DecimalType, LongType, DateType, TimestampType

# --- Helper Functions ---
def q_ident(name: str) -> str:
    """Quotes identifiers."""
    if not name or name.strip() == '''': return ''NULL'' 
    name = name.strip(''"'') 
    return ''"'' + name.replace(''"'', ''""'') + ''"''

def q_str(val: str) -> str:
    """Escapes strings for SQL."""
    if val is None: return ''NULL''
    if str(val).strip() == '''': return ''NULL''
    return "''" + str(val).replace("''", "''''") + "''"

def get_approx_domain(df, col_name, k=5):
    """Uses APPROX_TOP_K for fast frequent value analysis."""
    try:
        top_k_res = df.select(F.call_function("APPROX_TOP_K", F.col(col_name), k, 50)).collect()[0][0]
        if top_k_res:
            domain_list = [str(item["value"]) for item in json.loads(top_k_res)]
            return json.dumps(domain_list)
        return "[]"
    except:
        return "[]"

# ==========================================
#  AUDIT LOGGING CONTEXT MANAGER (RESTORED)
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
        return AuditStep(self, step_name)

class AuditStep:
    """Context Manager for a single step in the SP execution."""
    def __init__(self, manager, step_name):
        self.mgr = manager
        self.step_name = step_name

    def __enter__(self):
        try:
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
            print(f"AUDIT INIT FAILED for step {self.step_name}: {str(e)}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        status = "SUCCESS"
        message = ""
        
        if exc_type:
            status = "FAILURE"
            message = "".join(traceback.format_exception(exc_type, exc_val, exc_tb))
            message = message[:16777216] 

        try:
            sql = f"""
                UPDATE {self.mgr.full_table_path}
                SET STATUS = {q_str(status)},
                    MESSAGE = {q_str(message)},
                    LOG_TIMESTAMP = CURRENT_TIMESTAMP()
                WHERE RUN_ID = {q_str(self.mgr.run_id)} 
                  AND STEP = {q_str(self.step_name)}
                  AND STATUS = ''STARTED'' 
            """
            self.mgr.session.sql(sql).collect()
        except Exception as e:
            print(f"AUDIT UPDATE FAILED for step {self.step_name}: {str(e)}")

        if exc_type: return False 
        return True

# ==========================================
#  MAIN HANDLER
# ==========================================
def run_profiler_native(session, run_id: str, dataset_id: str, dataset_name: str, dbname: str, schemaname: str, tablename: str, customsql: str) -> str:
    
    # --- Configuration ---
    STATS_TABLE = "PROFILER_ATTRIBUTE_STATS_1"
    SUMMARY_TABLE = "PROFILER_DATASET_STATS_1"
    AUDIT_TABLE = "PROFILER_AUDIT_LOGS_1"
    
    framework_db = session.get_current_database().strip(''"'')
    framework_schema = session.get_current_schema().strip(''"'')
    
    # Setup Audit Logging
    audit_db_log = dbname if not customsql else None
    audit_schema_log = schemaname if not customsql else None
    audit_table_log = tablename if not customsql else None
    
    audit = AuditLogManager(session, framework_db, framework_schema, AUDIT_TABLE, 
                            run_id, dataset_id, dataset_name, audit_db_log, audit_schema_log, audit_table_log, customsql)

    # --- 1. Load Data & Create DF ---
    with audit.step("Load Data & Schema"):
        if tablename:
            # Standard Table Mode
            path = f"{q_ident(dbname)}.{q_ident(schemaname)}.{q_ident(tablename)}"
            df = session.table(path)
            
            # Variables for Metadata Query later
            meta_db = dbname
            meta_schema = schemaname
            meta_table = tablename
            
        elif customsql:
            # Custom SQL Mode
            temp_table_name = f"TEMP_PROFILE_{run_id}_{datetime.now().strftime(''%Y%m%d%H%M%S'')}"
            temp_table_path = f"{q_ident(framework_db)}.{q_ident(framework_schema)}.{q_ident(temp_table_name)}"
            
            with audit.step("Create Temp Table"):
                # Materialize SQL to Temp Table first (Required for Information Schema Size lookup)
                session.sql(f"CREATE TEMPORARY TABLE {temp_table_path} AS ({customsql})").collect()
            
            df = session.table(temp_table_path)
            
            # Variables for Metadata Query later
            meta_db = framework_db
            meta_schema = framework_schema
            meta_table = temp_table_name
        else:
            raise ValueError("Provide TABLENAME or CUSTOMSQL")
            
        # Cache row count
        total_rows = df.count()
        if total_rows == 0: return "SKIPPED: Empty Dataset"
        
        # Prefetch sample for "First Few Rows"
        sample_rows = df.limit(5).collect()

    # --- 2. Calculate Column Metrics (Native Aggregations) ---
    stats_output = []
    
    with audit.step("Calculate Native Column Stats"):
        for field in df.schema.fields:
            col_name = field.name
            col_type = field.datatype
            
            # Base Aggregations
            aggs = [
                F.count(F.col(col_name)).alias("NON_NULL_COUNT"),
                F.count_distinct(F.col(col_name)).alias("DISTINCT_COUNT"),
                F.min(F.col(col_name)).alias("MIN_VAL"),
                F.max(F.col(col_name)).alias("MAX_VAL")
            ]
            
            # Type Specific Aggregations
            is_numeric = isinstance(col_type, (IntegerType, DoubleType, DecimalType, LongType))
            is_string = isinstance(col_type, StringType)
            
            if is_numeric:
                aggs.extend([
                    F.avg(F.col(col_name)).alias("MEAN_VAL"),
                    F.stddev(F.col(col_name)).alias("STD_DEV"),
                    F.percentile_cont(0.25).within_group(F.col(col_name)).alias("P25"),
                    F.percentile_cont(0.50).within_group(F.col(col_name)).alias("P50"),
                    F.percentile_cont(0.75).within_group(F.col(col_name)).alias("P75"),
                    F.percentile_cont(0.95).within_group(F.col(col_name)).alias("P95"),
                    F.sum(F.iff(F.col(col_name) == 0, 1, 0)).alias("ZERO_COUNT")
                ])
            
            if is_string:
                aggs.extend([
                    F.avg(F.length(F.col(col_name))).alias("AVG_LENGTH"),
                    # FIX: Use try_cast(col, DateType()) instead of try_to_date
                    F.sum(F.iff(
                        (F.try_cast(F.col(col_name), DateType()).is_null()) & (F.col(col_name).is_not_null()), 
                        1, 0
                    )).alias("INVALID_DATE_COUNT")
                ])

            # Execute Query (Pushdown)
            res = df.select(aggs).collect()[0]
            
            # Derived Metrics
            non_null = res["NON_NULL_COUNT"]
            missing_count = total_rows - non_null
            distinct_count = res["DISTINCT_COUNT"]
            
            missing_pct = (missing_count / total_rows * 100) if total_rows > 0 else 0
            distinct_pct = (distinct_count / total_rows * 100) if total_rows > 0 else 0
            
            # Sample & Domain
            first_rows = [str(row[col_name]) for row in sample_rows if row[col_name] is not None]
            first_rows_str = json.dumps(first_rows[:5])
            domain_str = get_approx_domain(df, col_name, k=5)
            
            # Alerting
            alerts = []
            if missing_pct > 80: alerts.append("High Nulls")
            if distinct_pct == 100: alerts.append("Unique Key")
            alert_str = ", ".join(alerts) if alerts else "OK"

            row_stats = {
                "RUN_ID": run_id,
                "DATASET_ID": dataset_id,
                "COLUMN_NAME": col_name,
                "DATA_TYPE": str(col_type),
                "COLUMN_INFERRED_DATA_TYPE": str(col_type),
                "COLUMN_TOTAL_COUNT": total_rows,
                "COLUMN_NON_NULL_COUNT": non_null,
                "COLUMN_MISSING_COUNT": missing_count,
                "COLUMN_DISTINCT_COUNT": distinct_count,
                "COLUMN_ZERO_COUNT": res["ZERO_COUNT"] if is_numeric else 0,
                "COLUMN_MISSING_PERCENTAGE": missing_pct,
                "COLUMN_DISTINCT_PERCENTAGE": distinct_pct,
                "COLUMN_MIN_VALUE": str(res["MIN_VAL"]) if res["MIN_VAL"] is not None else None,
                "COLUMN_MAX_VALUE": str(res["MAX_VAL"]) if res["MAX_VAL"] is not None else None,
                "COLUMN_AVG_LENGTH": res["AVG_LENGTH"] if is_string else None,
                "COLUMN_STD_DEV": res["STD_DEV"] if is_numeric else None,
                "COLUMN_INVALID_DATE_COUNT": res["INVALID_DATE_COUNT"] if is_string else 0,
                "COLUMN_PERCENTILE_25": res["P25"] if is_numeric else None,
                "COLUMN_PERCENTILE_50": res["P50"] if is_numeric else None,
                "COLUMN_PERCENTILE_75": res["P75"] if is_numeric else None,
                "COLUMN_PERCENTILE_95": res["P95"] if is_numeric else None,
                "COLUMN_FIRST_FEW_ROWS": first_rows_str,
                # "COLUMN_DOMAIN": domain_str,
                "COLUMN_ALERT": alert_str,
                # "LOG_TIMESTAMP": datetime.now()
            }
            stats_output.append(row_stats)

    # --- 3. Dataset Summary Stats ---
    with audit.step("Calculate Dataset Summary"):
        # A. Calculate Duplicates (Hash Aggregate)
        try:
            dup_query = df.select(F.count("*") - F.count_distinct(F.hash(*df.columns))).collect()
            duplicate_count = dup_query[0][0]
        except: duplicate_count = -1 
        
        # B. Fetch Data Size from Information Schema (User Requested Logic)
        try:
            size_sql = f"""
                SELECT round(bytes / 1024 / 1024 / 1024, 4) AS SIZE_GB
                FROM {q_ident(meta_db)}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = {q_str(meta_schema)}
                  AND TABLE_NAME = {q_str(meta_table)}
            """
            size_res = session.sql(size_sql).collect()
            size_gb = float(size_res[0]["SIZE_GB"]) if size_res else 0.0
        except:
            size_gb = 0.0

        total_cols = len(df.columns)
        total_missing = sum([x["COLUMN_MISSING_COUNT"] for x in stats_output])
        total_cells = total_rows * total_cols
        null_percent = (total_missing / total_cells * 100) if total_cells > 0 else 0
        unique_values_sum = sum([x["COLUMN_DISTINCT_COUNT"] for x in stats_output])

        summary_stats = [{
            "RUN_ID": run_id,
            "DATASET_ID": dataset_id,
            "DATASET_NAME": dataset_name,
            "TOTAL_ROWS": total_rows,
            "TOTAL_COLUMNS": total_cols,
            "CORRELATION_MATRIX": size_gb, 
            "MISSING_VALUES_COUNT": total_missing,
            "NULL_COUNT": total_missing,
            "NULL_PERCENT": null_percent,
            "UNIQUE_VALUES_COUNT": unique_values_sum,
            "DUPLICATE_COUNT": duplicate_count,
            # "COLUMN_DOMAIN": "See Attribute Stats",
            "PROFILED_AT": datetime.now()
        }]

    # --- 4. Write Results ---
    with audit.step("Write Results to Table"):
        # Save Attribute Stats
        df_stats = pd.DataFrame(stats_output)
        df_stats.columns = [c.upper() for c in df_stats.columns]
        
        session.write_pandas(df_stats, table_name=STATS_TABLE, database=framework_db, 
                             schema=framework_schema, auto_create_table=False, overwrite=False)
        
        # Save Dataset Stats
        df_summary = pd.DataFrame(summary_stats)
        df_summary.columns = [c.upper() for c in df_summary.columns]
        
        session.write_pandas(df_summary, table_name=SUMMARY_TABLE, database=framework_db, 
                             schema=framework_schema, overwrite=False, auto_create_table=False)
        
    # Cleanup Temp Table if used
    if customsql and ''temp_table_name'' in locals():
        with audit.step("Cleanup Temp Table"):
            try:
                session.sql(f"DROP TABLE IF EXISTS {q_ident(framework_db)}.{q_ident(framework_schema)}.{q_ident(temp_table_name)}").collect()
            except Exception as e:
                print(f"Warning cleanup failed: {str(e)}")

    return "SUCCESS: Profiling Complete"
';;
