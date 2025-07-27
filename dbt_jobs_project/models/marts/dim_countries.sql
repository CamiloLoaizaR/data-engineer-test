WITH countries AS (
    SELECT DISTINCT
        job_country AS name
    FROM {{ ref('stg_jobs') }}
    WHERE job_country IS NOT NULL AND TRIM(job_country) <> ''
),

countries_deduplicated AS (
    SELECT DISTINCT name
    FROM countries
)

SELECT
    ROW_NUMBER() OVER (ORDER BY name) AS id,
    name
FROM countries_deduplicated