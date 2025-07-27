WITH jobs_base AS (
    SELECT *
    FROM {{ source('public', 'jobs') }}
),

job_via_map AS (
    SELECT DISTINCT ON (original)
        TRIM(LOWER(original)) AS original,
        normalized_value
    FROM {{ ref('job_via_normalized') }}
    ORDER BY original, normalized_value
),

job_schedule_type_map AS (
    SELECT DISTINCT ON (TRIM(LOWER(original)))
        TRIM(LOWER(original)) AS original,
        normalized_value
    FROM {{ ref('job_schedule_type_normalized') }}
    ORDER BY TRIM(LOWER(original)), normalized_value
),

company_map AS (
    SELECT DISTINCT ON (TRIM(LOWER(original)))
        TRIM(LOWER(original)) AS original,
        normalized_value
    FROM {{ ref('company_normalized') }}
    ORDER BY TRIM(LOWER(original)), normalized_value
)

SELECT
    j.id,

    INITCAP(TRIM(j.job_title_short)) AS job_title_short,
    INITCAP(TRIM(j.job_title)) AS job_title,

    REGEXP_REPLACE(REGEXP_REPLACE(TRIM(j.job_location), '\\s+', ' ', 'g'), '\\s*,\\s*', ', ', 'g') AS job_location,
    INITCAP(TRIM(j.search_location)) AS search_location,
    INITCAP(TRIM(j.job_country)) AS job_country,

    COALESCE(jvm.normalized_value, j.job_via) AS job_via,
    COALESCE(jstm.normalized_value, j.job_schedule_type) AS job_schedule_type,
    j.job_work_from_home,
    j.job_posted_date,
    j.job_no_degree_mention,
    j.job_health_insurance,
    TRIM(j.salary_rate) AS salary_rate,
    j.salary_year_avg,
    j.salary_hour_avg,
    COALESCE(cm.normalized_value, j.company_name) AS company_name,
    j.job_skills,
    j.job_type_skills

FROM jobs_base j
LEFT JOIN job_via_map jvm
    ON TRIM(LOWER(j.job_via)) = jvm.original
LEFT JOIN job_schedule_type_map jstm
    ON TRIM(LOWER(j.job_schedule_type)) = jstm.original
LEFT JOIN company_map cm
    ON TRIM(LOWER(j.company_name)) = cm.original