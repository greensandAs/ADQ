USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "RUN_MICROSEGMENT_PROFILER"("RUN_ID" VARCHAR, "MICROSEGMENT_DEF_ID" NUMBER(38,0), "MICROSEGMENT_DEF_NAME" VARCHAR, "DATASET_ID" NUMBER(38,0), "KPI_ID" NUMBER(38,0), "DBNAME" VARCHAR, "SCHEMANAME" VARCHAR, "TABLENAME" VARCHAR, "CUSTOMSQL" VARCHAR, "CREATED_BY" VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python','ydata-profiling','pandas')
HANDLER = 'run_microsegment_profiler_sp'
EXECUTE AS CALLER
AS '
import pandas as pd
import json
from datetime import datetime
from ydata_profiling import ProfileReport
from functions import col, cast, call_udf
from types import StringType, FloatType

def run_microsegment_profiler_sp(
    session,
    run_id,
    microsegment_def_id,
    microsegment_def_name,
    dataset_id,
    kpi_id,
    dbname,
    schemaname,
    tablename,
    customsql,
    created_by
):
    try:
        # --- 1. Load Snowpark DataFrame ---
        df = session.sql(customsql)

        # --- 2. Sample rows for profiling ---
        sample_size = 50000
        if sample_size > 0:
            df = df.sample(n=sample_size)

        # --- 3. Convert to Pandas for profiling ---
        pandas_df = df.to_pandas()
        print(f"retrieved results len: {len(pandas_df)}")

        # --- 4. Run ydata profiling ---
        profile = ProfileReport(pandas_df, title=f"Profile for {microsegment_def_name}", correlations=None)
        report_json = json.loads(profile.to_json())

        execution_date_start = report_json.get("analysis", {}).get("date_start")
        execution_date_end = report_json.get("analysis", {}).get("date_end")
        print(f"Execution date: {execution_date_start} to {execution_date_end}")

        # --- 5. Push aggregated records to microsegment_details table ---
        rows = []
        for idx, row in pandas_df.iterrows():
            profile_json = json.dumps(row.to_dict(), default=str)
            rows.append({
                "dataset_id": dataset_id,
                "microsegment_execution_date_start": execution_date_start,
                "microsegment_execution_date_end": execution_date_end,
                "microsegment_def_id": microsegment_def_id,
                "microsegment_id": idx + 1,
                "microsegment_profile": profile_json,
                "kpi_id": kpi_id,
                "microsegment_profiler_snapshot_id": run_id
            })
        final_df = pd.DataFrame(rows)
        final_df.columns = [c.upper() for c in final_df.columns]
        print(f"Final microsegment details rows: {len(final_df)}")

        session.write_pandas(
            final_df,
            table_name="MICROSEGMENT_DETAILS",
            auto_create_table=False,
            overwrite=False
        )
        print("Successfully written to MICROSEGMENT_DETAILS table")

        # --- 6. Prepare dataset summary ---
        table_summary = report_json.get("table", {})
        dataset_summary = pd.DataFrame([{
            "microsegment_def_id": microsegment_def_id,
            "dataset_id": dataset_id,
			"microsegment_profiler_snapshot_id": run_id,
            "kpi_id": kpi_id,
            "microsegment_total_records": table_summary.get("n"),
            "microsegment_total_columns": table_summary.get("n_var"),
            "microsegment_memory_size_bytes": table_summary.get("memory_size"),
            "microsegment_avg_record_size_bytes": table_summary.get("record_size"),
            "microsegment_missing_values_count": table_summary.get("n_cells_missing"),
            "microsegment_columns_with_missing_values": table_summary.get("n_vars_with_missing"),
            "microsegment_columns_fully_missing": table_summary.get("n_vars_all_missing"),
            "microsegment_missing_values_percentage": table_summary.get("p_cells_missing"),
            "microsegment_column_data_types": json.dumps(table_summary.get("types", {})),
            "microsegment_duplicate_records_count": table_summary.get("n_duplicates"),
            "microsegment_duplicate_records_percentage": table_summary.get("p_duplicates"),
            "microsegment_profiler_execution_date_start": execution_date_start,
            "microsegment_profiler_execution_date_end": execution_date_end,
            "created_by": created_by,
            "created_timestamp": execution_date_start,
            "updated_by": created_by,
            "updated_timestamp": execution_date_start
        }])

        # --- 7. Prepare attribute summary ---
        columns_data = []

        for idx, (col_name, stats) in enumerate(report_json.get("variables", {}).items(), 1):
            def safe_get(key, default=None):
                value = stats.get(key, default)
                numeric_keys = [
                    "n_distinct", "n_unique", "n_missing", "n", "count", "memory_size", "max_length",
                    "mean_length", "median_length", "min_length", "n_negative", "n_infinite",
                    "n_zeros", "mean", "std", "variance", "kurtosis", "skewness",
                    "sum", "mad", "5%", "25%", "50%", "75%", "95%", "iqr", "cv", "p_zeros",
                    "p_infinite", "monotonic_trend", "invalid_dates_total", "invalid_dates_percentage"
                ]
                boolean_keys = ["is_unique", "hashable", "ordering"]
                string_keys = [
                    "type", "value_counts_without_nan", "value_counts_index_sorted", "first_rows",
                    "chi_squared", "length_histogram", "character_counts", "category_alias_values",
                    "block_aliases", "block_alias_frequencies", "block_alias_char_counts", "script_frequencies",
                    "script_char_counts", "category_alias_frequencies", "word_counts", "cast_type",
                    "monotonic_increase", "monotonic_decrease", "monotonic_increase_strict",
                    "monotonic_decrease_strict", "histogram", "attribute_alert", "min_value", "max_value",
                    "value_range", "value_histogram", "min","max"
                ]
                if key in numeric_keys:
                    return 0 if value is None else value
                elif key in boolean_keys:
                    return False if value is None else value
                elif key in string_keys:
                    return "" if value is None else str(value)   # <-- convert everything to stri
                return value

            columns_data.append({
                "microsegment_profiler_snapshot_id": run_id,
				"microsegment_column_id": idx,
				"microsegment_def_id": microsegment_def_id,
				"dataset_id": dataset_id,
				"microsegment_attribute_distinct_count": safe_get("n_distinct", 0),
				"microsegment_attribute_distinct_percentage": safe_get("p_distinct", 0.0),
				"microsegment_attribute_is_unique": safe_get("is_unique", False),
				"microsegment_attribute_unique_count": safe_get("n_unique", 0),
				"microsegment_attribute_unique_percentage": safe_get("p_unique", 0.0),
				"microsegment_attribute_data_type": safe_get("type", ""),
				"microsegment_attribute_is_hashable": safe_get("hashable", False),
				"microsegment_value_counts_non_null": json.dumps(safe_get("value_counts_without_nan", [])),
				"microsegment_attribute_value_counts_sorted": json.dumps(safe_get("value_counts_index_sorted", [])),
				"microsegment_attribute_is_ordered": safe_get("ordering", False),
				"microsegment_attribute_missing_count": safe_get("n_missing", 0),
				"microsegment_attribute_total_count": safe_get("n", 0),
				"microsegment_attribute_missing_percentage": safe_get("p_missing", 0.0),
				"microsegment_attribute_non_null_count": safe_get("count", 0),
				"microsegment_attribute_memory_size_bytes": safe_get("memory_size", 0),
				"microsegment_attribute_first_few_rows": json.dumps(safe_get("first_rows", [])),
				"microsegment_attribute_max_length": safe_get("max_length", 0),
				"microsegment_attribute_avg_length": safe_get("mean_length", 0),
				"microsegment_attribute_median_length": safe_get("median_length", 0),
				"microsegment_attribute_min_length": safe_get("min_length", 0),
				"microsegment_attribute_length_distribution": json.dumps(safe_get("length_histogram", [])),
				"microsegment_attribute_histogram_length": json.dumps(safe_get("histogram_length", [])),
				"microsegment_attribute_distinct_character_count": safe_get("n_characters_distinct", 0),
				"microsegment_attribute_total_character_count": safe_get("n_characters", 0),
				"microsegment_attribute_character_frequencies": json.dumps(safe_get("character_counts", {})),
				"microsegment_category_aliases": json.dumps(safe_get("category_alias_values", {})),
				"microsegment_block_aliases": json.dumps(safe_get("block_aliases", {})),
				"microsegment_block_alias_frequencies": json.dumps(safe_get("block_alias_frequencies", {})),
				"microsegment_block_alias_count": safe_get("n_block_alias", 0),
				"microsegment_block_alias_character_frequencies": json.dumps(safe_get("block_alias_char_counts", {})),
				"microsegment_script_frequencies": json.dumps(safe_get("script_frequencies", {})),
				"microsegment_script_count": safe_get("n_scripts", 0),
				"microsegment_script_character_frequencies": json.dumps(safe_get("script_char_counts", {})),
				"microsegment_category_alias_frequencies": json.dumps(safe_get("category_alias_frequencies", {})),
				"microsegment_category_count": safe_get("n_category", 0),
				"microsegment_category_alias_character_frequencies": json.dumps(safe_get("category_alias_char_counts", {})),
				"microsegment_attribute_word_frequencies": json.dumps(safe_get("word_counts", {})),
				"microsegment_attribute_inferred_data_type": safe_get("cast_type", ""),
				"microsegment_execution_date_start": execution_date_start,
				"microsegment_execution_date_end": execution_date_end,
				"microsegment_attribute_alert": safe_get("attribute_alert", ""),
				"microsegment_attribute_negative_count": safe_get("n_negative", 0),
				"microsegment_attribute_negative_percentage": safe_get("p_negative", 0.0),
				"microsegment_attribute_infinite_count": safe_get("n_infinite", 0),
				"microsegment_attribute_zero_count": safe_get("n_zeros", 0),
				"microsegment_attribute_mean_value": safe_get("mean", 0.0),
				"microsegment_attribute_std_dev": safe_get("std", 0.0),
				"microsegment_attribute_variance_value": safe_get("variance", 0.0),
				"microsegment_attribute_min_value": safe_get("min", ""),
				"microsegment_attribute_max_value": safe_get("max", ""),
				"microsegment_attribute_kurtosis_value": safe_get("kurtosis", 0.0),
				"microsegment_attribute_skewness_value": safe_get("skewness", 0.0),
				"microsegment_attribute_sum_value": safe_get("sum", 0.0),
				"microsegment_attribute_mean_absolute_deviation": safe_get("mad", 0.0),
				"microsegment_attribute_chi_squared_test": safe_get("chi_squared", ""),
				"microsegment_attribute_value_range": safe_get("value_range", ""),
				"microsegment_attribute_percentile_5": safe_get("5%", 0.0),
				"microsegment_attribute_percentile_25": safe_get("25%", 0.0),
				"microsegment_attribute_percentile_50": safe_get("50%", 0.0),
				"microsegment_attribute_percentile_75": safe_get("75%", 0.0),
				"microsegment_attribute_percentile_95": safe_get("95%", 0.0),
				"microsegment_attribute_interquartile_range": safe_get("iqr", 0.0),
				"microsegment_attribute_coefficient_of_variation": safe_get("cv", 0.0),
				"microsegment_attribute_zero_percentage": safe_get("p_zeros", 0.0),
				"microsegment_attribute_infinite_percentage": safe_get("p_infinite", 0.0),
				"microsegment_attribute_monotonic_increasing": safe_get("monotonic_increase", ""),
				"microsegment_attribute_monotonic_decreasing": safe_get("monotonic_decrease", ""),
				"microsegment_attribute_strictly_monotonic_increasing": safe_get("monotonic_increase_strict", ""),
				"microsegment_attribute_strictly_monotonic_decreasing": safe_get("monotonic_decrease_strict", ""),
				"microsegment_attribute_monotonic_trend": safe_get("monotonic", 0.0),
				"microsegment_attribute_value_histogram": json.dumps(safe_get("histogram", {})),
				"microsegment_attribute_data_imbalance": safe_get("imbalance", 0.0),
				"microsegment_attribute_invalid_date_count": safe_get("invalid_dates", 0),
				"microsegment_attribute_invalid_dates_total": safe_get("n_invalid_dates", 0.0),
				"microsegment_attribute_invalid_dates_percentage": safe_get("p_invalid_dates", 0.0),
				"microsegment_profiler_execution_date_start": execution_date_start,
				"microsegment_profiler_execution_date_end": execution_date_end,
				"created_by": created_by,
				"created_timestamp": execution_date_start,
				"updated_by": created_by,
                "updated_timestamp": execution_date_start
            })

        attribute_summary = pd.DataFrame(columns_data)
        print(f"Attribute summary rows: {len(attribute_summary)}")

        # --- 8. Upload results to Snowflake ---
        dataset_summary.columns = [c.upper() for c in dataset_summary.columns]
        attribute_summary.columns= [c.upper() for c in attribute_summary.columns]
        session.write_pandas(dataset_summary,
                             table_name="MICROSEGMENT_PROFILER_DATASET_SUMMARY",
                             auto_create_table=False,
                             overwrite=False)

        session.write_pandas(attribute_summary,
                             table_name="MICROSEGMENT_PROFILER_ATTRIBUTE_SUMMARY",
                             auto_create_table=False,
                             overwrite=False)

        return "SUCCESS: Microsegment profiling completed."

    except Exception as e:
        return f"FAILURE: {str(e)}"
';