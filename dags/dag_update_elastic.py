from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import requests
import os

def load_json_to_elasticsearch():
    file_path = "/opt/airflow/data/formatted/bdd_crypto_test.json"
    es_url = "http://elasticsearch:9200/crypto_prices/_bulk"

    bulk_data = ""

    with open(file_path, "r", encoding="utf-8") as f:
        for line_number, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue  # ignore lignes vides
            try:
                data = json.loads(line)
                monero_price = data["monero"]["usd"]
                timestamp = data["timestamp_utc"]

                bulk_data += json.dumps({"index": {}}) + "\n"
                bulk_data += json.dumps({
                    "symbol": "XMR",
                    "price_usd": monero_price,
                    "timestamp": timestamp
                }) + "\n"

            except KeyError as e:
                print(f"Ligne {line_number} ignorée, clé manquante: {e}")
            except json.JSONDecodeError as e:
                print(f"Ligne {line_number} ignorée, erreur JSON: {e}")

    if not bulk_data:
        print("Aucune donnée à envoyer.")
        return

    headers = {"Content-Type": "application/x-ndjson"}
    response = requests.post(es_url, headers=headers, data=bulk_data)

    if response.status_code >= 200 and response.status_code < 300:
        print("Import réussi dans Elasticsearch.")
    else:
        print("Erreur d'import:", response.status_code, response.text)


with DAG(
    dag_id="inject_crypto_data_to_es",
    start_date=datetime(2025, 6, 1),
    schedule="@daily",
    catchup=False,
    tags=["elasticsearch", "crypto"],
) as dag:

    inject_task = PythonOperator(
        task_id="load_json_to_es",
        python_callable=load_json_to_elasticsearch
    )
