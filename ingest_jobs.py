import os
from dotenv import load_dotenv
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import ast
import re
import json

from logs import get_logger

logger = get_logger(__name__)

load_dotenv()

DB_PARAMS = {
    "dbname": os.getenv("DBT_DB_NAME"),
    "user": os.getenv("DBT_DB_USER"),
    "password": os.getenv("DBT_DB_PASSWORD"),
    "host": os.getenv("DBT_DB_HOST"),
    "port": int(os.getenv("DBT_DB_PORT", 5432)),
}

try:
    logger.info("Reading CSV file...")

    df = pd.read_csv("data_jobs.csv")

    logger.info(f"CSV file loaded with {len(df)} rows.")

    for col in ["job_skills", "job_type_skills"]:
        df[col] = df[col].apply(lambda x: None if re.findall('^\s*$',str(x)) else x)

    df["job_skills"] = df["job_skills"].apply(lambda x: json.dumps(ast.literal_eval(x))
                                            if pd.notnull(x) 
                                            else None)

    df["job_type_skills"] = df["job_type_skills"].apply(lambda x: json.dumps(ast.literal_eval(x))
                                                        if pd.notnull(x) 
                                                        else None)

    df = df.where(pd.notnull(df), None)

    logger.info("Connecting to the database...")
    
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    logger.info("Database connection established.")

    logger.info("Creating table 'jobs'...")
    cur.execute("""
        DROP TABLE IF EXISTS jobs CASCADE;
        CREATE TABLE jobs (
            id SERIAL PRIMARY KEY,
            job_title_short VARCHAR(50),
            job_title VARCHAR(200),
            job_location VARCHAR(100),
            job_via VARCHAR(150),
            job_schedule_type VARCHAR(50),
            job_work_from_home BOOLEAN,
            search_location VARCHAR(50),
            job_posted_date TIMESTAMP,
            job_no_degree_mention BOOLEAN,
            job_health_insurance BOOLEAN,
            job_country VARCHAR(50),
            salary_rate VARCHAR(10),
            salary_year_avg DOUBLE PRECISION,
            salary_hour_avg DOUBLE PRECISION,
            company_name TEXT,
            job_skills JSONB,
            job_type_skills JSONB
        );
        """)
    conn.commit()
    logger.info("Table 'jobs' created successfully.")

    records = list(df.itertuples(index=False, name=None))

    query = """
        INSERT INTO jobs (
            job_title_short, job_title, job_location, job_via,
            job_schedule_type, job_work_from_home, search_location,
            job_posted_date, job_no_degree_mention, job_health_insurance,
            job_country, salary_rate, salary_year_avg, salary_hour_avg, 
            company_name, job_skills, job_type_skills
        ) VALUES %s
    """

    logger.info(f"Starting insertion of {len(records)} records...")

    chunk_size = 10000
    for i in range(0, len(records), chunk_size):
        logger.info(f"Inserting records {i} to {i + chunk_size}...")
        execute_values(cur, query, records[i:i + chunk_size])


    conn.commit()
    logger.info("All records inserted successfully.")

except:
    logger.exception("An error occurred during pipeline execution.")
finally:
    if "cur" in locals():
        cur.close()
    if "conn" in locals():
        conn.close()
        logger.info("Database connection closed.")