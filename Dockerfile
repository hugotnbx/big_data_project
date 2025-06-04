FROM apache/airflow:2.8.0
RUN pip install nba_api
RUN pip install apache-airflow-providers-apache-spark

