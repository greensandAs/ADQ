USE DATABASE {{ snowflake_database }};

 USE SCHEMA {{ snowflake_schema }};

create or replace view MICROSEGMENT_CONTEXT_ANOMALY(
	KPI_ID,
	DATASET_ID,
	MICROSEGMENT_DEF_ID,
	MICROSEGMENT_PROFILER_SNAPSHOT_ID,
	MICROSEGMENT_ID,
	MICROSEGMENT_EXECUTION_DATE_START,
	MICROSEGMENT_EXECUTION_DATE_END,
	MICROSEGMENT_PROFILE,
	MICROSEGMENT_CONTEXT,
	IS_ANOMALY,
	ANOMALY_DETECTION_METHOD,
	IS_WITHIN_ANOMALY,
	IS_ACROSS_ANOMALY,
	WITHIN_MICROSEGMENT_ANOMALY_REASON,
	ACROSS_MICROSEGMENT_ANOMALY_REASON,
	WITHIN_MICROSEGMENT_ANOMALY_CONFIDENCE_SCORE,
	ACROSS_MICROSEGMENT_ANOMALY_CONFIDENCE_SCORE,
	ANOMALY_CREATED_TIMESTAMP,
	ANOMALY_UPDATED_TIMESTAMP
) as

/* 1. Get latest profiler snapshot per dataset + KPI + microsegment definition */
WITH latest_snapshot AS (
    SELECT
        dataset_id,
        kpi_id,
        microsegment_def_id,
        MAX(microsegment_profiler_snapshot_id) AS max_snapshot_id
    FROM microsegment_details
    GROUP BY
        dataset_id,
        kpi_id,
        microsegment_def_id
),

/* 2. Filter microsegment details to only the latest snapshot */
filtered_microsegment_details AS (
    SELECT
        md.dataset_id,
        md.kpi_id,
        md.microsegment_def_id,
        md.microsegment_profiler_snapshot_id,
        md.microsegment_id,
        md.microsegment_execution_date_start,
        md.microsegment_execution_date_end,
        md.microsegment_profile
    FROM microsegment_details md
    JOIN latest_snapshot ls
        ON
            md.dataset_id = ls.dataset_id
            AND md.kpi_id = ls.kpi_id
            AND md.microsegment_def_id = ls.microsegment_def_id
            AND md.microsegment_profiler_snapshot_id = ls.max_snapshot_id
),

/* 3. Build microsegment context from definition + attributes */
microsegment_context AS (
    SELECT
        md.microsegment_def_id,
        OBJECT_CONSTRUCT(
            'definition',
            OBJECT_CONSTRUCT(
                'microsegment_def_name', md.microsegment_def_name,
                'microsegment_def_description', md.microsegment_def_description,
                'kpi_id', md.kpi_id,
                'dataset_id', md.dataset_id
            ),
            'attributes',
            ARRAY_AGG(
                OBJECT_CONSTRUCT(
                    'column_name', ma.column_name,
                    'column_type', ma.column_type,
                    'aggregation_type', ma.aggregation_type,
                    'filter_condition', ma.filter_condition,
                    'explanation', ma.explanation
                )
            )
        ) AS microsegment_context
    FROM microsegment_definition md
    LEFT JOIN microsegment_attribute ma
        ON md.microsegment_def_id = ma.microsegment_def_id
    GROUP BY
        md.microsegment_def_id,
        md.microsegment_def_name,
        md.microsegment_def_description,
        md.kpi_id,
        md.dataset_id
)

/* 4. Final projection */
SELECT
    fmd.kpi_id,
    fmd.dataset_id,
    fmd.microsegment_def_id,
    fmd.microsegment_profiler_snapshot_id,
    fmd.microsegment_id,

    /* execution window */
    fmd.microsegment_execution_date_start,
    fmd.microsegment_execution_date_end,

    /* profiler output */
    fmd.microsegment_profile,

    /* merged definition + attributes */
    mc.microsegment_context,

    /* anomaly flag */
    CASE
        WHEN ma.microsegment_id IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_anomaly,

    /* anomaly details (NULL when not anomalous) */
    ma.anomaly_detection_method,
    ma.is_within_anomaly,
    ma.is_across_anomaly,
    ma.within_microsegment_anomaly_reason,
    ma.across_microsegment_anomaly_reason,
    ma.within_microsegment_anomaly_confidence_score,
    ma.across_microsegment_anomaly_confidence_score,
    ma.created_timestamp AS anomaly_created_timestamp,
    ma.updated_timestamp AS anomaly_updated_timestamp

FROM filtered_microsegment_details fmd
JOIN microsegment_context mc
    ON fmd.microsegment_def_id = mc.microsegment_def_id
LEFT JOIN microsegment_anomalies ma
    ON
        fmd.dataset_id = ma.dataset_id
        AND fmd.kpi_id = ma.kpi_id
        AND fmd.microsegment_def_id = ma.microsegment_def_id
        AND fmd.microsegment_id = ma.microsegment_id
        AND fmd.microsegment_profiler_snapshot_id = ma.microsegment_profiler_snapshot_id;;