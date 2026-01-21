USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "BUILD_AND_EVALUATE_LASSO_MODEL"("DATASET_NAME" VARCHAR, "FEATURE_LIST" ARRAY, "TARGET_COL" VARCHAR, "RELATION" ARRAY, "DATA_CONFIG" VARCHAR, "COMPUTED_VAR" VARCHAR, "CODE" VARCHAR, "EQUATION" VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','scikit-learn','pandas','numpy')
HANDLER = 'build_and_evaluate_lasso_model_handler'
EXECUTE AS CALLER
AS '
import json
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LassoCV
from sklearn.metrics import r2_score, mean_absolute_percentage_error


def make_serializable(obj):
    """Convert numpy/pandas types to native Python for JSON serialization."""
    if isinstance(obj, (np.integer, np.floating)):
        return obj.item()
    if isinstance(obj, (np.ndarray, pd.Series)):
        return obj.tolist()
    return obj


def lasso_get_relations(target_variable, feature_variables, sample_data):
    """
    Build Lasso regression model and derive the mathematical relationship.
    """
    X = sample_data[feature_variables]
    y = sample_data[target_variable]

    # Train-test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Train model
    model = LassoCV(cv=5, random_state=42)
    model.fit(X_train, y_train)

    # Predict and evaluate
    y_pred = model.predict(X_test)
    r2 = round(float(r2_score(y_test, y_pred)) * 100, 2)
    mape = round(float(mean_absolute_percentage_error(y_test, y_pred)) * 100, 2)

    # Generate equation
    intercept = model.intercept_
    equation_terms = [f"{intercept:.2f}"]
    for coef, var in zip(model.coef_, feature_variables):
        if abs(coef) > 1e-6:
            equation_terms.append(f"({coef:.2f} * {var})")
    equation = f"{target_variable} = " + " + ".join(equation_terms)

    return {
        "Source": "Data Relations",
        "Relation": "Numerical",
        "Target Variable": target_variable,
        "Feature Variables": feature_variables,
        "Equation": equation,
        "R2 Score": r2,
        "MAPE": mape,
        "Accuracy": ""
    }


def build_and_evaluate_lasso_model_handler(
    session, dataset_name: str, feature_list: list,
    target_col: str, relation: list, data_config: str,
    computed_var: str, code: str, equation: str
):
    """
    Builds a Lasso model and evaluates numerical relationships on the dataset.
    """
    # Parse configuration
    config = json.loads(data_config)
    params = config.get(''parameters'', {})

    # Extract thresholds
    r2_threshold = params.get(''r2_threshold'', 90)
    mape_threshold = params.get(''mape'', 10)
    relative_tolerance = params.get(''relative_tolerance'', 0.01)

    # Load dataset from Snowflake
    data = session.table(dataset_name).to_pandas()
    if data.empty:
        return {"status": "error", "message": f"Dataset ''{dataset_name}'' is empty."}

    # Step 1: Generate relationship using Lasso
    lasso_relations = lasso_get_relations(target_col, feature_list, data)

    # Step 2: Clean data for evaluation
    cleaned_data = data.loc[~(data[feature_list + [target_col]] == 0).any(axis=1)].copy()
    cleaned_data.replace([np.inf, -np.inf], np.nan, inplace=True)
    cleaned_data.dropna(subset=feature_list + [target_col], inplace=True)
    cleaned_data = cleaned_data[(cleaned_data[feature_list].applymap(np.isfinite)).all(axis=1)]

    # Step 3: Execute computation logic safely
    try:
        exec(code, {"cleaned_data": cleaned_data, "np": np})
    except KeyError as e:
        return {"status": "error", "message": f"Missing column in dataset: {str(e)}"}
    except Exception as e:
        return {"status": "error", "message": f"Error executing code: {str(e)}"}

    # Step 4: Check computed column
    if computed_var not in cleaned_data.columns:
        return {"status": "error", "message": f"Computed column ''{computed_var}'' not found."}

    # Step 5: Calculate evaluation metrics
    r2 = round(float(r2_score(cleaned_data[target_col], cleaned_data[computed_var])) * 100, 2)
    mape = round(float(mean_absolute_percentage_error(cleaned_data[target_col], cleaned_data[computed_var])) * 100, 2)

    tol = relative_tolerance * cleaned_data[target_col].std()
    approximate_matches = (np.abs(cleaned_data[target_col] - cleaned_data[computed_var]) <= tol)
    accuracy = round(float((approximate_matches.sum() / len(cleaned_data)) * 100), 2)

    valid_relations = {
        "Source": "Data Relations",
        "Relation": "Numerical",
        "Target Variable": target_col,
        "Feature Variables": feature_list,
        "Relationships": relation,
        "Equation": equation,
        "R2 Score": r2,
        "MAPE": mape,
        "Total Records": int(len(cleaned_data)),
        "Failed Records": int(len(cleaned_data) - approximate_matches.sum()),
        "Accuracy": accuracy,
        "Is Valid": bool(r2 >= r2_threshold and mape <= mape_threshold)
    }

    # Step 6: Return JSON-safe output
    return {
        "lasso_relations": {k: make_serializable(v) for k, v in lasso_relations.items()},
        "validation_metrics": {k: make_serializable(v) for k, v in valid_relations.items()}
    }

';