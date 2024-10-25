from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# import psycopg2
# import psycopg2.extras
# import json
# import requests
# from collections import defaultdict

default_args = {
    'owner': 'penta',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}
def print_hello_world(): 
    print("hello world")

with DAG(
    dag_id='test_airflow_v1',
    default_args=default_args,
    start_date=datetime(2024, 10, 25),
    schedule_interval=timedelta(minutes=1)
) as dag:

    
    test = PythonOperator(
        task_id='print_hello_world',
        python_callable=print_hello_world,
        provide_context=True
    )

    test