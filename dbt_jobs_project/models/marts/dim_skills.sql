WITH raw_skills AS (
    SELECT DISTINCT
        lower(trim(jsonb_array_elements_text(job_skills))) AS original_skill
    FROM {{ ref('stg_jobs') }}
    WHERE job_skills IS NOT NULL
),

skills_with_normalization AS (
    SELECT
        r.original_skill,
        COALESCE(n.normalized_value, r.original_skill) AS normalized_skill
    FROM raw_skills r
    LEFT JOIN {{ ref('job_skills_normalized') }} n
        ON r.original_skill = n.original
)

SELECT
    ROW_NUMBER() OVER (ORDER BY normalized_skill) AS id,
    normalized_skill AS name
FROM (
    SELECT DISTINCT normalized_skill
    FROM skills_with_normalization
) AS unique_values