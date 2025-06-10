from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone
from pyspark.sql import SparkSession
import requests
import os
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

def run_pyspark_job():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("spark_job_get_crypto") \
        .getOrCreate()

    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {"ids": "monero", "vs_currencies": "usd"}

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        now_utc = datetime.now(timezone.utc)
        data['timestamp_utc'] = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Convertir en DataFrame Spark
        df = spark.read.json(spark.sparkContext.parallelize([json.dumps(data)]))

        output_dir = "/opt/airflow/data/crypto_raw/"
        os.makedirs(output_dir, exist_ok=True)

        output_path = os.path.join(output_dir, f"monero_price_{now_utc.strftime('%Y%m%dT%H%M%S')}")
        df.coalesce(1).write.mode("overwrite").json(output_path)

        print(f"✅ Fichier Spark JSON écrit dans : {output_path}")

    except requests.RequestException as e:
        print(f"❌ Erreur lors de l’appel API : {e}")

    finally:
        spark.stop()

with DAG(
    dag_id='GetCrypto',
    default_args=default_args,
    schedule='@hourly',
    catchup=False
) as dag:
    run_spark_local = PythonOperator(
        task_id='run_spark_local',
        python_callable=run_pyspark_job
    )
