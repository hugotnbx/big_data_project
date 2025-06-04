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

    fetch_games_task = PythonOperator(
        task_id='fetch_games',
        python_callable=fetch_games
    )

    fetch_players_task = PythonOperator(
        task_id='fetch_players',
        python_callable=fetch_players
    )

    fetch_boxscores_task = PythonOperator(
        task_id='fetch_boxscores',
        python_callable=fetch_boxscores
    )

    format_games_task = BashOperator(
        task_id="format_games",
        bash_command="docker exec spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/spark/scripts/formatting/format_games.py"
    )

    format_boxscores_task = BashOperator(
        task_id="format_boxscores",
        bash_command="docker exec spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/spark/scripts/formatting/format_boxscores.py"
    )

    format_players_task = BashOperator(
        task_id="format_players",
        bash_command="docker exec spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/spark/scripts/formatting/format_players.py"
    )

    merge_games_files_task = BashOperator(
        task_id="merge_games_files",
        bash_command="docker exec spark-master python3 /opt/spark/scripts/formatting/merge_games_files.py"
    )

    merge_boxscores_files_task = BashOperator(
        task_id="merge_boxscores_files",
        bash_command="docker exec spark-master python3 /opt/spark/scripts/formatting/merge_boxscores_files.py"
    )

    merge_players_files_task = BashOperator(
        task_id="merge_players_files",
        bash_command="docker exec spark-master python3 /opt/spark/scripts/formatting/merge_players_files.py"
    )

    fetch_games_task >> format_games_task >> merge_games_files_task
    fetch_boxscores_task >> format_boxscores_task >> merge_boxscores_files_task
    fetch_players_task >> format_players_task >> merge_players_files_task

   

