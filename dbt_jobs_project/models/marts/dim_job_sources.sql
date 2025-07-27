WITH sources AS (
    SELECT DISTINCT
        TRIM(job_via) AS publication_source
    FROM {{ ref('stg_jobs') }}
    WHERE job_via IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY publication_source) AS id,
    publication_source
FROM sources