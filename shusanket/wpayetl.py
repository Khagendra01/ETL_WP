import textwrap
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
import pandas as pd


def extract_data():
    # Code to extract data from a CSV file
    df = pd.read_csv("./carddetail_spendbonuscategory.csv")
    return df


def transform_data(**context):
    # Retrieve the DataFrame from the previous task using XCom
    df = context["task_instance"].xcom_pull(task_ids="extract_data")

    # Code to transform the extracted data
    df["random"] = 1
    return df


def create_csv(**context):
    # Retrieve the DataFrame from the previous task using XCom
    df = context["task_instance"].xcom_pull(task_ids="transform_data")

    # Code to create a new CSV file with transformed data
    df.to_csv("newfile.csv")
    return df


with DAG(
    "wpayetl",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
    description="A simple tutorial DAG",
    schedule=timedelta(minutes=5),
    start_date=datetime(2024, 4, 7, 8, 55, 0),
    catchup=False,
    tags=["example"],
) as dag:

    t1 = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    t2 = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    t3 = PythonOperator(
        task_id="create_csv",
        python_callable=create_csv,
    )


t1 >> t2 >> t3
