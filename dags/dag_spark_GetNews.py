from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

with DAG('spark_job_get_news', default_args=default_args, schedule='@daily') as dag:

    submit_spark_job = SparkSubmitOperator(
        task_id='submit_spark_job',
        application='/opt/airflow/dags/service/GetNewsJSON.py',  # chemin de ton script PySpark
        conn_id='spark_default',  # connexion à configurer dans Airflow
        executor_memory='1g',
        driver_memory='1g',
        application_args=['--param1', 'value1'],  # si tu as des arguments à passer
    )
