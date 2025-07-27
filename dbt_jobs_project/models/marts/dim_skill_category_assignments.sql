WITH category_assignments_raw AS (
    SELECT DISTINCT
        lower(trim(jsonb_array_elements_text(value))) AS skill,
        lower(trim(key)) AS category
    FROM {{ ref('stg_jobs') }},
         LATERAL jsonb_each(job_type_skills)
    WHERE job_type_skills IS NOT NULL
),

category_assignments AS (
    SELECT
        COALESCE(n.normalized_value, r.skill) AS normalized_skill,
        r.category
    FROM category_assignments_raw r
    LEFT JOIN {{ ref('job_skills_normalized') }} n
        ON r.skill = n.original
),

normalized_joined AS (
    SELECT DISTINCT
        s.id AS skill_id,
        c.id AS category_id
    FROM category_assignments ca
    LEFT JOIN {{ ref('dim_skills') }} s
        ON s.name = ca.normalized_skill
    LEFT JOIN {{ ref('dim_skill_categories') }} c
        ON lower(c.name) = ca.category
    WHERE s.id IS NOT NULL AND c.id IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER () AS id,
    skill_id,
    category_id
FROM normalized_joined