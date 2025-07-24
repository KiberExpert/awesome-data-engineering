from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

def print_welcome():
    print('Welcome to Airflow!')

def print_date():
    print(f'Today is {datetime.today().date()}')

def print_random_quote():
    response = requests.get('https://jsonplaceholder.typicode.com/todos')
    todos = response.json()
    
    # Masalan, 1-elementni chiqarish:
    if todos:
        todo = todos[0]
        print(f'TODO title: "{todo.get("title", "No title")}"')
    else:
        print("No todos found.")

default_args = {
    'start_date': datetime.now() - timedelta(days=1)
}

with DAG(
    dag_id='welcome_dag',
    default_args=default_args,
    schedule='0 23 * * *',
    catchup=False,
    tags=['example', 'learning'],
    description='A simple DAG that prints messages and a random quote'
) as dag:

    print_welcome_task = PythonOperator(
        task_id='print_welcome',
        python_callable=print_welcome
    )

    print_date_task = PythonOperator(
        task_id='print_date',
        python_callable=print_date
    )

    print_random_quote_task = PythonOperator(
        task_id='print_random_quote',
        python_callable=print_random_quote
    )

    print_welcome_task >> print_date_task >> print_random_quote_task
