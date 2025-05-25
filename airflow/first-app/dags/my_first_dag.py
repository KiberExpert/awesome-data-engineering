from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def task1():
    print("Task 1 bajarildi!")

def task2():
    print("Task 2 bajarildi!")

def task3():
    print("Task 3 bajarildi!")

with DAG(
    dag_id="my_first_dag",
    start_date=datetime(2025, 5, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    t1 = PythonOperator(
        task_id="task1",
        python_callable=task1
    )

    t2 = PythonOperator(
        task_id="task2",
        python_callable=task2
    )

    t3 = PythonOperator(
        task_id="task3",
        python_callable=task3
    )

    t1 >> t2 >> t3  # tartibda bajariladi