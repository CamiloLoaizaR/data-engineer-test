WITH companies AS (
    SELECT DISTINCT
        TRIM(company_name) AS name
    FROM {{ ref('stg_jobs') }}
    WHERE company_name IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY name) AS id,
    name
FROM companies