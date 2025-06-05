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
    start_date=datetime(2025, 6, 5),
    catchup=False,
) as dag:

    # Tasks

    # Ingestion
    fetch_games_task = PythonOperator(
        task_id='fetch_games',
        python_callable=fetch_games
    )

    fetch_boxscores_task = PythonOperator(
        task_id='fetch_boxscores',
        python_callable=fetch_boxscores
    )

    fetch_players_task = PythonOperator(
        task_id='fetch_players',
        python_callable=fetch_players
    )

    # Formatting
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

    clean_games_files_task = BashOperator(
        task_id="clean_games_files",
        bash_command="docker exec spark-master python3 /opt/spark/scripts/formatting/clean_games_files.py"
    )

    clean_boxscores_files_task = BashOperator(
        task_id="clean_boxscores_files",
        bash_command="docker exec spark-master python3 /opt/spark/scripts/formatting/clean_boxscores_files.py"
    )

    clean_players_files_task = BashOperator(
        task_id="clean_players_files",
        bash_command="docker exec spark-master python3 /opt/spark/scripts/formatting/clean_players_files.py"
    )

    # Combination
    combine_datasources_task = BashOperator(
        task_id="combine_datasources",
        bash_command="docker exec spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/spark/scripts/combination/combine_datasources.py"
    )

    clean_combine_files_task = BashOperator(
        task_id="clean_combine_files",
        bash_command="docker exec spark-master python3 /opt/spark/scripts/combination/clean_combine_files.py"
    )

    # Indexing
    index_data_to_elastic_task = BashOperator(
        task_id="index_data_to_elastic",
        bash_command="docker exec spark-master spark-submit --master spark://spark-master:7077 --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0 --deploy-mode client /opt/spark/scripts/indexing/index_data_to_elastic.py"
    )

    # Dependencies
    fetch_games_task >> format_games_task
    fetch_boxscores_task >> format_boxscores_task
    fetch_players_task >> format_players_task

    format_games_task >> clean_games_files_task
    format_boxscores_task >> clean_boxscores_files_task
    format_players_task >> clean_players_files_task

    combine_datasources_task.set_upstream([clean_games_files_task, clean_boxscores_files_task, clean_players_files_task])

    combine_datasources_task >> clean_combine_files_task

    clean_combine_files_task >> index_data_to_elastic_task