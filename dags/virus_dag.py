# Import libary
from airflow import DAG
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from virus_crawl import run_etl_crawl
from virus_preprocess import run_etl_preprocess

# Define default argument
default_args = {
    'owner': 'GiaiVi',
    'depends_on_past': False,
    'start_date': datetime(2021, 11, 27),
    'email': ['phanvigiaii@gmail..com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initiate DAG
dag = DAG(
    'virus_dag',
    default_args=default_args,
    description='Run crawling data virus and preprocessing',
    schedule_interval=timedelta(days=1)
)

# Our task
run_crawl = PythonOperator(
    task_id = 'crawl_data',
    python_callable = run_etl_crawl,
    dag = dag
)

run_preprocess = PythonOperator(
    task_id = 'preprocess_data',
    python_callable = run_etl_preprocess,
    dag = dag
)

# Task dependencies
run_crawl >> run_preprocess