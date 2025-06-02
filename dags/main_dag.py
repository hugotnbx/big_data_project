from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import sys
sys.path.append('/opt/airflow/scripts')

from fetch_data import fetch_data_from_nba_api

with DAG(
    'main_pipeline',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='NBA data ingestion pipeline (only raw)',
    schedule_interval='@daily',
    start_date=datetime(2025, 6, 2),
    catchup=False,
) as dag:

    ingestion_task = PythonOperator(
        task_id='source_to_raw',
        python_callable=fetch_data_from_nba_api,
    )
