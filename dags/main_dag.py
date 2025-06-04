from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

import sys
sys.path.append('/opt/airflow/scripts')

from ingestion.fetch_games import fetch_games
from ingestion.fetch_players import fetch_players
from ingestion.fetch_boxscores import fetch_boxscores

with DAG(
    'main_pipeline',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Pipeline for the Big Data project',
    schedule_interval='@daily',
    start_date=datetime(2025, 6, 4),
    catchup=False,
) as dag:

    # fetch_games_task = PythonOperator(
    #     task_id='fetch_games',
    #     python_callable=fetch_games
    # )

    # fetch_players_task = PythonOperator(
    #     task_id='fetch_players',
    #     python_callable=fetch_players
    # )

    # fetch_boxscores_task = PythonOperator(
    #     task_id='fetch_boxscores',
    #     python_callable=fetch_boxscores
    # )

    format_games = BashOperator(
        task_id="format_games",
        bash_command="docker exec spark spark-submit --master spark://spark:7077 --deploy-mode client /opt/spark/scripts/formatting/format_games.py"
    )