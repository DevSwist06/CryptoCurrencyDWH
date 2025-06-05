from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'spark_process',
    default_args=default_args,
    schedule_interval='@daily'
)

spark_job = SparkSubmitOperator(
    task_id='spark_process_task',
    application='/opt/airflow/dags/spark_script.py',
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark:7077',
    },
    dag=dag
)