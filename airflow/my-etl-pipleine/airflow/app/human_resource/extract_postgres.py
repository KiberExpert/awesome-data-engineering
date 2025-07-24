import pandas as pd
import psycopg2
import os
from utils.snowflake_uploader import upload_to_snowflake
from dotenv import load_dotenv

load_dotenv()

def extract_postgres():
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        dbname=os.getenv("PG_DB")
    )
    query = """ 
        select
            emp.id as employee_id,
            concat(emp.lastname, ' ', emp.firstname) as full_name,
            emp.position,
            emp.department,
            emp.event_date,
            emp.salary
        from public.employees emp
    """
    df = pd.read_sql(query, conn)
    conn.close()
    upload_to_snowflake(df, "human_resources")
