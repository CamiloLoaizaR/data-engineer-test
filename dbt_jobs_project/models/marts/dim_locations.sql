WITH raw_locations AS (
    SELECT DISTINCT
        job_location AS location,
        search_location,
        job_country AS country_name
    FROM {{ ref('stg_jobs') }}
    WHERE job_location IS NOT NULL AND job_location <> ''
),

locations_with_country AS (
    SELECT
        rl.location,
        rl.search_location,
        dc.id AS country_id
    FROM raw_locations rl
    LEFT JOIN {{ ref('dim_countries') }} dc
        ON dc.name = rl.country_name
)

SELECT
    ROW_NUMBER() OVER (ORDER BY location) AS id,
    location,
    search_location,
    country_id
FROM locations_with_country