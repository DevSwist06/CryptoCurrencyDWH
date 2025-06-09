from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import json
from glob import glob

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

def update_crypto_bdd():
    raw_dir = "/opt/airflow/data/crypto_raw/"
    bdd_path = "/opt/airflow/data/formatted/bdd_crypto.json"
    os.makedirs(os.path.dirname(bdd_path), exist_ok=True)

    # Trouver le sous-dossier le plus récent
    subdirs = [os.path.join(raw_dir, d) for d in os.listdir(raw_dir) if os.path.isdir(os.path.join(raw_dir, d))]
    if not subdirs:
        raise FileNotFoundError("Aucun dossier trouvé dans crypto_raw/")
    latest_dir = max(subdirs, key=os.path.getmtime)

    # Trouver le fichier JSON dans ce dossier
    json_files = glob(os.path.join(latest_dir, "*.json"))
    if not json_files:
        raise FileNotFoundError(f"Aucun fichier JSON trouvé dans {latest_dir}")
    latest_file = json_files[0]

    # Lire la ligne JSON
    with open(latest_file, "r") as f:
        lines = [json.loads(line) for line in f if line.strip()]

    if not lines:
        raise ValueError("Fichier JSON vide")
    new_entry = lines[0]

    # Ajouter à la BDD (format JSONL)
    with open(bdd_path, "a") as bdd_file:
        bdd_file.write(json.dumps(new_entry) + "\n")

    print(f"✅ Donnée ajoutée à {bdd_path}")

with DAG(
    dag_id='dag_spark_update_crypto',
    default_args=default_args,
    schedule='@daily',
    catchup=False
) as dag:

    update_crypto = PythonOperator(
        task_id='append_latest_crypto_to_bdd',
        python_callable=update_crypto_bdd
    )
