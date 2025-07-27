WITH job_skills_raw AS (
    SELECT
        j.id AS job_id,
        lower(trim(category)) AS category,
        lower(trim(jsonb_array_elements_text(skills))) AS original_skill
    FROM {{ ref('stg_jobs') }} j,
         LATERAL jsonb_each(j.job_type_skills) AS cat(category, skills)
),

job_skills_normalized AS (
    SELECT
        jsr.job_id,
        COALESCE(n.normalized_value, jsr.original_skill) AS skill,
        jsr.category
    FROM job_skills_raw jsr
    LEFT JOIN {{ ref('job_skills_normalized') }} n
        ON jsr.original_skill = n.original
)

SELECT
    ROW_NUMBER() OVER () AS id,
    jn.job_id,
    s.id AS skill_id,
    c.id AS category_id
FROM job_skills_normalized jn
LEFT JOIN {{ ref('dim_skills') }} s
    ON s.name = jn.skill
LEFT JOIN {{ ref('dim_skill_categories') }} c
    ON lower(c.name) = jn.category
WHERE s.id IS NOT NULL AND c.id IS NOT NULL
