import pandas as pd
import sqlite3
from utils.snowflake_uploader import upload_to_snowflake
from dotenv import load_dotenv
import os

load_dotenv()

def extract_sqlite():
    conn = sqlite3.connect(os.getenv("SALARY_DB"))  # db path

    query = """
        select
            s.employee_id,
            s.base_salary,
            s.bonus,
            s.tax_deduction,
            s.total_salary,
            s.payment_date
        from main.salary s
    """
    df = pd.read_sql(query, conn)
    conn.close()
    upload_to_snowflake(df, "salarys")
