WITH schedule_types AS (
    SELECT
        DISTINCT UNNEST(
            string_to_array(job_schedule_type, ',')
        ) AS raw_type
    FROM {{ ref('stg_jobs') }}
    WHERE job_schedule_type IS NOT NULL
),

cleaned_schedule_types AS (
    SELECT
        INITCAP(TRIM(raw_type)) AS name
    FROM schedule_types
)

SELECT
    ROW_NUMBER() OVER (ORDER BY name) AS id,
    name
FROM (
    SELECT DISTINCT name FROM cleaned_schedule_types
) data_cleaned