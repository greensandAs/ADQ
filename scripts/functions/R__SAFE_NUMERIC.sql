USE DATABASE {{ snowflake_database }};
USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE FUNCTION "SAFE_NUMERIC"("VAL" VARCHAR)
RETURNS FLOAT
LANGUAGE SQL
AS '
CASE
    WHEN val IS NULL THEN NULL
    WHEN REGEXP_LIKE(val, ''^[+\\-]?[0-9]+(\\.[0-9]+)?([eE][+\\-]?[0-9]+)?$'')
         AND REGEXP_LIKE(val, ''[eE]'') THEN NULL
    WHEN TRY_TO_DOUBLE(val) IS NULL THEN NULL
    WHEN TRY_TO_DOUBLE(val) > 1e38 THEN NULL
    WHEN TRY_TO_DOUBLE(val) < -1e38 THEN NULL
    ELSE TRY_TO_DOUBLE(val)
END
';;
