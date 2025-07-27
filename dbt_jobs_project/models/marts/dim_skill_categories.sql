WITH skill_categories AS (
    SELECT DISTINCT
        jsonb_object_keys(job_type_skills) AS category
    FROM {{ ref('stg_jobs') }}
    WHERE job_type_skills IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY category) AS id,
    category AS name
FROM skill_categories