USE DATABASE {{ snowflake_database }};
USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "GENERATE_DQ_CATEGORICAL_RELATIONSHIP_RULES"("DATASET_NAME" VARCHAR, "DQ_CR_RULES_TABLE" VARCHAR, "FINAL_CAT_COL" ARRAY)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('pandas','scipy','numpy','snowflake-snowpark-python')
HANDLER = 'generate_dq_categorical_relationship_rules_handler'
EXECUTE AS CALLER
AS '
import snowflake.snowpark as sp
from snowflake.snowpark import Session
import itertools
import pandas as pd
from scipy.stats import chi2_contingency
from datetime import datetime

def get_low_cardinality_categoricals(df, max_unique=100):
    return [col for col in df.columns if df[col].nunique() <= max_unique]

def cramers_v(x, y):
    confusion_matrix = pd.crosstab(x, y)
    if confusion_matrix.empty:
        return 0.0
    chi2 = chi2_contingency(confusion_matrix)[0]
    n = confusion_matrix.sum().sum()
    r, k = confusion_matrix.shape
    min_rk = min(k - 1, r - 1)
    if n * min_rk == 0:
        return 0.0
    return (chi2 / (n * min_rk)) ** 0.5

def cramers_v_matrix(df, columns):
    matrix = pd.DataFrame(index=columns, columns=columns)
    for col1, col2 in itertools.combinations_with_replacement(columns, 2):
        value = cramers_v(df[col1], df[col2])
        matrix.loc[col1, col2] = value
        matrix.loc[col2, col1] = value
    return matrix.astype(float)

def get_strong_categorical_pairs(corr_matrix, threshold=0.5):
    seen = set()
    pairs = []
    for col1 in corr_matrix.columns:
        for col2 in corr_matrix.columns:
            if col1 != col2:
                key = tuple(sorted((col1, col2)))
                if key not in seen and corr_matrix.loc[col1, col2] >= threshold:
                    pairs.append((col1, col2, corr_matrix.loc[col1, col2]))
                    seen.add(key)
    return pairs

def generate_raw_rules(df, col1, col2, dataset_name, score, min_confidence=0.7):
    rules = []
    try:
        mapping = pd.crosstab(df[col1], df[col2], normalize=''index'')
    except Exception as e:
        return []

    timestamp = datetime.now()
    for val1 in mapping.index:
        most_likely = mapping.loc[val1].idxmax()
        confidence = mapping.loc[val1].max()
        if confidence >= min_confidence:
            rules.append({
                "dataset_name": dataset_name,
                "if_col": col1,
                "if_val": str(val1),
                "then_col": col2,
                "then_val": str(most_likely),
                "confidence": round(confidence, 3),
                "score": round(score, 2),
                "columns": [col1, col2],
                "created_at": timestamp.isoformat()
            })
    return rules

def generate_dq_categorical_relationship_rules_handler(session: Session, dataset_name: str, dq_cr_rules_table: str, final_cat_col: list):
    try:
        df = session.table(dataset_name).to_pandas()
        
        if df.empty:
            return {"status": "success", "message": f"Dataset {dataset_name} is empty or has no data."}

        # Filter for valid columns
        final_cat_col = [col for col in final_cat_col if col in df.columns]
        sample_df = df[final_cat_col]

        low_card_cols = get_low_cardinality_categoricals(sample_df)
        if not low_card_cols:
            return {"status": "success", "message": "No low cardinality columns found to generate rules."}

        corr_matrix = cramers_v_matrix(sample_df, low_card_cols)
        strong_pairs = get_strong_categorical_pairs(corr_matrix)

        final_dq_rules = []
        for col1, col2, score in strong_pairs:
            rules = generate_raw_rules(sample_df, col1, col2, dataset_name, score)
            final_dq_rules.extend(rules)

        # Save rules to table
        if final_dq_rules:
            rules_df = pd.DataFrame(final_dq_rules)
            snowpark_df = session.create_dataframe(rules_df)
            snowpark_df.write.mode("append").save_as_table(dq_cr_rules_table)

        return {"status": "success", "message": f"Successfully generated {len(final_dq_rules)} DQ rules.", "rules": final_dq_rules}

    except Exception as e:
        return {"status": "error", "message": str(e)}
';;
