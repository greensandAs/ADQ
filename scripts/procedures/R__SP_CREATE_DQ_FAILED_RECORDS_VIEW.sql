USE DATABASE {{ snowflake_database }};
USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "SP_CREATE_DQ_FAILED_RECORDS_VIEW"("DATABASE_NAME" VARCHAR, "SCHEMA_NAME" VARCHAR, "TABLE_NAME" VARCHAR, "VIEW_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
  // 1. Define the full table path and the view path
  var full_table_name = DATABASE_NAME + ''.'' + SCHEMA_NAME + ''.'' + TABLE_NAME;
  var full_view_name = DATABASE_NAME + ''.'' + SCHEMA_NAME + ''.'' + VIEW_NAME;

  // 2. Query to extract all unique keys using FLATTEN
  var key_query = `
    SELECT DISTINCT
        f.key
    FROM ` + full_table_name + ` t,
    LATERAL FLATTEN(input => t.FAILED_ROW) f
    ORDER BY f.key;
  `;

  var key_stmt = snowflake.createStatement({ sqlText: key_query });
  var key_rs = key_stmt.execute();

  var select_columns = [];
  var column_count = 0;

  // 3. Iterate through results and build the list of projected columns
  while (key_rs.next()) {
    var key_name = key_rs.getColumnValue(1);
    // Use the JSON notation (:) and cast to STRING, aliasing with the key name
    select_columns.push(''FAILED_ROW:"'' + key_name + ''"::STRING AS "'' + key_name + ''"'');
    column_count++;
  }

  // Handle the case where the table is empty
  if (column_count === 0) {
      return "SUCCESS: Table " + full_table_name + " is empty. No view created.";
  }

  // 4. Construct the final CREATE VIEW statement
  var view_sql = `
    CREATE OR REPLACE VIEW ` + full_view_name + ` AS
    SELECT
        DATASET_RUN_ID,
        RULE_CONFIG_ID,
        ` + select_columns.join('',\\n        '') + `,
        LOAD_TIMESTAMP
    FROM ` + full_table_name + `;
  `;

  // 5. Execute the dynamically generated SQL
  var create_view_stmt = snowflake.createStatement({ sqlText: view_sql });
  create_view_stmt.execute();

  return "SUCCESS: View " + full_view_name + " created/replaced with " + column_count + " dynamic columns.";
';;
