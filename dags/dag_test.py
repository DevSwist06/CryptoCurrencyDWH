from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="dag_test",
    start_date=datetime(2025, 1, 1),
    schedule='@daily', #attention à la nouvelle version DAG
    catchup=False,
    tags=["test"],

) as dag:

    dire_bonjour = BashOperator(
        task_id="dire_bonjour",
        bash_command='echo "Hello depuis Airflow !"',
    )
