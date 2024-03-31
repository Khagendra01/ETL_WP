from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 31),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def print_message():
    print("task executed")


dag = DAG(
    'example_dag',
    default_args=default_args,
    description='A simple DAG with three tasks',
    schedule_interval='@daily',
)


task1 = PythonOperator(
    task_id='task_1',
    python_callable=print_message,
    op_kwargs={'message': 'Task 1 executed'},
    dag=dag,
)


task2 = PythonOperator(
    task_id='task_2',
    python_callable=print_message,
    op_kwargs={'message': 'Task 2 executed'},
    dag=dag,
)


task3 = PythonOperator(
    task_id='task_3',
    python_callable=print_message,
    op_kwargs={'message': 'Task 3 executed'},
    dag=dag,
)


task1 >> task2
task1 >> task3
