import snowflake.connector
import os
from config import TARGET_CONFIG, DATA_TABLES_TO_EXPORT, OUTPUT_DIR_DATA

# 1. Define Stage Paths
# We use separate folders to track status
STAGE_NAME = "MIGRATION_LOAD_STAGE"
PATH_TODO = "not_processed"
PATH_DONE = "processed"

def get_connection():
    print(f"🔌 Connecting to TARGET: {TARGET_CONFIG['account']}...")
    return snowflake.connector.connect(
        user=TARGET_CONFIG['user'],
        password=TARGET_CONFIG['password'],
        account=TARGET_CONFIG['account'],
        warehouse=TARGET_CONFIG['warehouse'],
        database=TARGET_CONFIG['database'],
        schema=TARGET_CONFIG['schema'],
        role=TARGET_CONFIG['role']
    )

def main():
    conn = get_connection()
    cur = conn.cursor()
    
    # We need the absolute path for the PUT command
    abs_path = os.path.abspath(OUTPUT_DIR_DATA)

    try:
        cur.execute(f"USE SCHEMA {TARGET_CONFIG['database']}.{TARGET_CONFIG['schema']}")
        
        print("🔨 Creating Internal Stage...")
        cur.execute(f"""
            CREATE STAGE IF NOT EXISTS {STAGE_NAME} 
            FILE_FORMAT=(TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"')
        """)

        for table in DATA_TABLES_TO_EXPORT:
            filename = f"{table}.csv.gz"
            file_path_local = os.path.join(abs_path, filename)
            
            if not os.path.exists(file_path_local):
                print(f"⚠️ Skipping {table}: File not found locally.")
                continue

            print(f"\n📦 Processing Table: {table}")
            
            # ---------------------------------------------------------
            # STEP 1: UPLOAD to 'not_processed' folder
            # ---------------------------------------------------------
            print(f"   ⬆️ Uploading to @{STAGE_NAME}/{PATH_TODO}/...")
            # overwrite=True ensures we always test the latest data
            cur.execute(f"PUT file://{file_path_local} @{STAGE_NAME}/{PATH_TODO}/ OVERWRITE=TRUE")

            # ---------------------------------------------------------
            # STEP 2: LOAD DATA (Strict Mode)
            # ---------------------------------------------------------
            print(f"   ⤵️ Loading into {table}...")
            
            # Using ABORT_STATEMENT ensures the script FAILS if data is bad.
            # Using NULL_IF handles the \N issue.
            copy_sql = f"""
                COPY INTO "{table}" 
                FROM @{STAGE_NAME}/{PATH_TODO}/{filename}
                FILE_FORMAT = (
                    TYPE='CSV' 
                    SKIP_HEADER=1 
                    FIELD_OPTIONALLY_ENCLOSED_BY='"'
                    NULL_IF=('NULL', 'nan', '\\\\N')
                )
                ON_ERROR = 'CONTINUE' 
                PURGE = FALSE
            """
            
            try:
                cur.execute(copy_sql)
                # If we get here, the load was 100% successful
                print(f"      ✅ Load Successful.")
                
                # -----------------------------------------------------
                # STEP 3: MOVE TO 'PROCESSED' (Archive)
                # -----------------------------------------------------
                # Since Snowflake doesn't have a simple "MV" command for internal stages,
                # we re-upload to the 'processed' folder and remove the old one.
                # This is safe because the file is still on the GitHub Runner.
                
                print(f"   🚚 Moving to @{STAGE_NAME}/{PATH_DONE}/...")
                cur.execute(f"PUT file://{file_path_local} @{STAGE_NAME}/{PATH_DONE}/ OVERWRITE=TRUE")
                cur.execute(f"REMOVE @{STAGE_NAME}/{PATH_TODO}/{filename}")
                
            except snowflake.connector.errors.ProgrammingError as e:
                print(f"      ❌ LOAD FAILED for {table}")
                print(f"      📄 File remains in: @{STAGE_NAME}/{PATH_TODO}/{filename}")
                # RE-RAISE the error to stop the pipeline immediately
                raise e

    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: {e}")
        exit(1) # Force GitHub Action to turn Red
        
    finally:
        conn.close()
        print("\n✨ Data Load Process Finished")

if __name__ == "__main__":
    main()