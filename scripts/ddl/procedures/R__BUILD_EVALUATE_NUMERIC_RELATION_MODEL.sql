USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "BUILD_EVALUATE_NUMERIC_RELATION_MODEL"("DATASET_NAME" VARCHAR, "FEATURE_LIST" ARRAY, "TARGET_COL" VARCHAR, "NUMERIC_COLS_ARRAY" ARRAY, "DATA_CONFIG" VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','scikit-learn','pandas','numpy')
HANDLER = 'build_and_evaluate_model_handler'
EXECUTE AS CALLER
AS '
import json
import numpy as np
import pandas as pd
from sklearn.linear_model import Lasso
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_absolute_percentage_error

def build_and_evaluate_model_handler(session, dataset_name: str, feature_list: list, target_col: str, numeric_cols_array: list, data_config: str):

    # Parse data_config JSON string to dict
    config = json.loads(data_config)
    params = config.get(''parameters'', {})

    r2_threshold = params.get(''r2_threshold'', 90)
    wmape_threshold = params.get(''wmape'', 10)
    mape_threshold = params.get(''mape'', 10)
    relative_tolerance = params.get(''relative_tolerance'', 0.01)

    results = {
        "Features": [],
        "Target": [],
        "Equation": [],
        "R2": [],
        "WMAPE": [],
        "MAPE": [],
        "Intercept Contribution": [],
        "R2 All Data": [],
        "WMAPE All Data": [],
        "MAPE All Data": [],
        "Total Data": [],
        "Accuracy": [],
    }

    data = session.table(dataset_name).to_pandas()
    if data.empty:
        return {"status": "error", "message": f"Dataset ''{dataset_name}'' is empty."}

    numeric_cols = [c for c in numeric_cols_array if c in data.columns]
    if not numeric_cols:
        return {"status": "error", "message": "No numeric columns found."}

    valid_feats = [c for c in feature_list if c in data.columns and c != target_col]
    if not valid_feats or target_col not in data.columns:
        return {"status": "error", "message": "Features or target column missing."}

    sub = data[valid_feats + [target_col]].replace([np.inf, -np.inf], np.nan).dropna()
    if sub.empty:
        return {"status": "error", "message": "No valid data after cleaning."}

    X = sub[valid_feats]
    y = sub[target_col]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
    model = Lasso(alpha=0.001)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    r2 = round(r2_score(y_test, y_pred) * 100, 2)
    mape = mean_absolute_percentage_error(y_test, y_pred) * 100
    wmape = (np.sum(np.abs(y_test - y_pred)) / np.sum(y_test)) * 100
    intercept = model.intercept_
    coefs = model.coef_

    equation = f"{target_col} = {intercept:.2f}"
    for coef, feat in zip(coefs, valid_feats):
        if round(coef, 2) != 0:
            equation += f" + ({coef:.2f} * {feat})"

    intercept_contribution = (intercept * len(y_test)) / np.sum(y_test) * 100

    results["Features"].append(valid_feats)
    results["Target"].append(target_col)
    results["Equation"].append(equation)
    results["R2"].append(r2)
    results["WMAPE"].append(wmape)
    results["MAPE"].append(mape)
    results["Intercept Contribution"].append(intercept_contribution)

    if r2 >= r2_threshold and wmape <= wmape_threshold and mape <= mape_threshold:
        y_pred_all = model.predict(X)
        r2_all = round(r2_score(y, y_pred_all) * 100, 2)
        wmape_all = round((np.sum(np.abs(y - y_pred_all)) / np.sum(y)) * 100, 2)
        mape_all = round(mean_absolute_percentage_error(y, y_pred_all) * 100, 2)

        tolerance = relative_tolerance * np.std(y)
        accuracy = round(((np.abs(y - y_pred_all) <= tolerance).sum() / len(y)) * 100, 2)

        results["R2 All Data"].append(r2_all)
        results["WMAPE All Data"].append(wmape_all)
        results["MAPE All Data"].append(mape_all)
        results["Total Data"].append(sub.shape[0])
        results["Accuracy"].append(accuracy)
    else:
        results["R2 All Data"].append(None)
        results["WMAPE All Data"].append(None)
        results["MAPE All Data"].append(None)
        results["Total Data"].append(None)
        results["Accuracy"].append(None)

    results["status"] = "success"
    return results
';