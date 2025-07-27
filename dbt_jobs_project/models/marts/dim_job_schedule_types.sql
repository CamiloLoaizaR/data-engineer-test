WITH job_schedule_types AS (
    SELECT
        id AS job_id,
        UNNEST(string_to_array(job_schedule_type, ',')) AS raw_type
    FROM {{ ref('stg_jobs') }}
    WHERE job_schedule_type IS NOT NULL
),

cleaned_schedule_types AS (
    SELECT
        job_id,
        INITCAP(TRIM(raw_type)) AS name
    FROM job_schedule_types
)

SELECT
    ROW_NUMBER() OVER () AS id,
    cst.job_id,
    dst.id AS schedule_type_id
FROM cleaned_schedule_types cst
LEFT JOIN {{ ref('dim_schedule_types') }} dst
    ON dst.name = cst.name