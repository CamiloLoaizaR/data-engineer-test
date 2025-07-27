WITH salary_rates AS (
    SELECT DISTINCT
        TRIM(salary_rate) AS name
    FROM {{ ref('stg_jobs') }}
    WHERE salary_rate IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY name) AS id,
    name
FROM salary_rates