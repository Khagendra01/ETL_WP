from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 31),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the print_message function
def print_message(message):
    print(message)

# Instantiate the DAG object
dag = DAG(
    'example_dag',
    default_args=default_args,
    description='A simple DAG with three tasks',
    schedule_interval='@daily',
)

# Define the first task
task1 = PythonOperator(
    task_id='task_1',
    python_callable=print_message,
    op_kwargs={'message': 'Task 1 executed'},
    dag=dag,
)

# Define the second task
task2 = PythonOperator(
    task_id='task_2',
    python_callable=print_message,
    op_kwargs={'message': 'Task 2 executed'},
    dag=dag,
)

# Define the third task
task3 = PythonOperator(
    task_id='task_3',
    python_callable=print_message,
    op_kwargs={'message': 'Task 3 executed'},
    dag=dag,
)

# Define dependencies between tasks
task1 >> task2
task1 >> task3
