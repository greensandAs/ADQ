-- Set the context (Ensure the session is pointed to the correct location)
USE DATABASE {{ snowflake_database }};
USE SCHEMA {{ snowflake_schema }};

-- DDL for PROCEDURE: DQ_FRAMEWORK.METADATA.GENERATE_NUMERIC_CORRELATION_RULES(VARCHAR, ARRAY, FLOAT)
-- Extracted on: 2025-10-27 10:25:01
--------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE GENERATE_NUMERIC_CORRELATION_RULES(
    "DATASET_NAME" VARCHAR, "NUMERIC_COLS" ARRAY, "CORR_THRESHOLD" FLOAT DEFAULT 0.7
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('pandas', 'numpy', 'snowflake-snowpark-python')
HANDLER = 'generate_numeric_correlation_rules_handler'
EXECUTE AS CALLER
AS '
import snowflake.snowpark as sp
from snowflake.snowpark import Session
import pandas as pd
import numpy as np

def find_correlated_columns(derived_columns, data, corr_thres):
    print(f"Computing Spearman correlation matrix for columns: {derived_columns} with threshold {corr_thres}")
    correlation_matrix = data[derived_columns].corr(method="spearman")
    relationships = []

    for column in derived_columns:
        related_columns = correlation_matrix[column][
            (correlation_matrix[column].abs() >= corr_thres) & (correlation_matrix[column].abs() < 1)
        ]
        if not related_columns.empty:
            correlated_features = []
            for feature, corr_value in related_columns.items():
                correlated_features.append({
                    "feature": feature,
                    "correlation": round(corr_value, 4)  # Round to 4 decimals
                })
            relationships.append({
                "target_variable": column,
                "correlated_features": correlated_features
            })
    print(f"Total correlated sets found: {len(relationships)}")
    return relationships

def generate_numeric_correlation_rules_handler(session: Session, dataset_name: str, numeric_cols: list, corr_threshold: float = 0.7):
    try:
        print(f"Loading dataset: {dataset_name}")
        df = session.table(dataset_name).to_pandas()
        print(f"Dataset loaded. Total rows: {len(df)}, columns: {df.columns.tolist()}")

        if df.empty:
            return {"status": "success", "message": f"Dataset {dataset_name} is empty."}

        numeric_cols = [col for col in numeric_cols if col in df.columns]
        if not numeric_cols:
            return {"status": "success", "message": "No numeric columns found."}

        # Sample large datasets
        sample_size = min(len(df), max(10000, int(len(df)*0.2)))
        df_sample = df[numeric_cols].sample(sample_size, random_state=42)

        correlated_relationships = find_correlated_columns(numeric_cols, df_sample, corr_threshold)

        return {
            "status": "success",
            "message": f"Found {len(correlated_relationships)} correlated columns sets.",
            "correlations": correlated_relationships
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}
';
