WITH categories AS (
    SELECT DISTINCT
        TRIM(job_title_short) AS name
    FROM {{ ref('stg_jobs') }}
    WHERE job_title_short IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY name) AS id,
    name
FROM categories