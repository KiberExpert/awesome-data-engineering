from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from app.human_resource.extract_postgres import extract_postgres
from app.salary.extract_sqlite import extract_sqlite
from app.shop.extract_csv import extract_csv

with DAG(
    dag_id="etl_to_snowflake",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:
    
    t1 = PythonOperator(
        task_id="extract_postgres",
        python_callable=extract_postgres
    )
    
    t2 = PythonOperator(
        task_id="extract_csv",
        python_callable=extract_csv
    )
    
    t3 = PythonOperator(
        task_id="extract_sqlite",
        python_callable=extract_sqlite
    )
    
    [t1, t2, t3]
