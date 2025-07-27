WITH base AS (
    SELECT
        j.id,
        j.job_title,
        TRIM(j.job_title_short) AS job_category_name,
        j.job_schedule_type,
        j.job_work_from_home,
        j.job_posted_date,
        j.job_no_degree_mention,
        j.job_health_insurance,
        j.salary_rate,
        j.salary_year_avg,
        j.salary_hour_avg,
        j.job_location,
        j.search_location,
        j.job_country,
        TRIM(j.company_name) AS company_name,
        j.job_via
    FROM {{ ref('stg_jobs') }} j
),

final AS (
    SELECT
        b.id,
        jc.id AS job_category_id,
        b.job_title,
        b.job_work_from_home,
        b.job_posted_date,
        b.job_no_degree_mention,
        b.job_health_insurance,
        srt.id AS salary_rate_id,
        b.salary_year_avg,
        b.salary_hour_avg,
        dl.id AS location_id,
        c.id AS company_id,
        js.id AS job_sources_id
    FROM base b
    LEFT JOIN {{ ref('dim_job_categories') }} jc ON jc.name = b.job_category_name
    LEFT JOIN {{ ref('dim_salary_rate_types') }} srt ON srt.name = b.salary_rate
    LEFT JOIN {{ ref('dim_companies') }} c ON c.name = b.company_name
    LEFT JOIN {{ ref('dim_job_sources') }} js ON js.publication_source = b.job_via
    LEFT JOIN {{ ref('dim_countries') }} dc ON dc.name = b.job_country
    LEFT JOIN {{ ref('dim_locations') }} dl ON (dl.location = b.job_location 
    AND dl.search_location = b.search_location AND dl.country_id=dc.id)
)

SELECT * FROM final