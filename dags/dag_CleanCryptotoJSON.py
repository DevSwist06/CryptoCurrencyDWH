from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime
import os
from glob import glob
from pyspark.sql import SparkSession

# --- Configuration des chemins ---
RAW_DATA_BASE_DIR = "/opt/airflow/data/crypto_raw/"
FORMATTED_BBD_PATH = "/opt/airflow/data/formatted/bdd_crypto.json"


# --- Tâches Python Callables ---

def find_latest_folder_callable(**kwargs):
    """
    Trouve le sous-dossier le plus récent dans le répertoire brut.
    Retourne son chemin pour le passer via XCom.
    """
    subdirs = [os.path.join(RAW_DATA_BASE_DIR, d) for d in os.listdir(RAW_DATA_BASE_DIR) if
               os.path.isdir(os.path.join(RAW_DATA_BASE_DIR, d))]

    if not subdirs:
        print("Aucun dossier trouvé dans crypto_raw/, la tâche est ignorée.")
        raise AirflowSkipException("Aucun dossier à traiter.")

    latest_dir = max(subdirs, key=os.path.getmtime)
    print(f"Dossier le plus récent trouvé : {latest_dir}")
    return latest_dir  # Cette valeur sera passée via XCom


def append_with_pyspark_callable(**kwargs):
    """
    Utilise PySpark pour lire le JSON du dossier source et l'ajouter au fichier BDD.
    """
    ti = kwargs['ti']
    source_folder = ti.xcom_pull(task_ids='find_latest_folder_task', key='return_value')

    if not source_folder:
        print("Aucun chemin de dossier reçu de la tâche précédente. Arrêt.")
        raise AirflowSkipException("Chemin du dossier source non fourni.")

    # Initialisation de la session Spark
    spark = SparkSession.builder.appName("AppendCryptoToBDD").getOrCreate()

    try:
        # Chemin vers le ou les fichiers JSON dans le dossier source
        json_files_path = os.path.join(source_folder, "*.json")

        # Lecture des données JSON avec Spark
        df = spark.read.json(json_files_path)

        if df.rdd.isEmpty():
            print(f"Aucune donnée à traiter dans {source_folder}.")
            spark.stop()
            return

        print(f"Schéma des données lues par Spark :")
        df.printSchema()

        # Conversion du DataFrame en une liste de chaînes JSON (pour l'ajout au format JSONL)
        # .collect() ramène les données sur le driver. OK pour de petits volumes.
        lines_to_append = df.toJSON().collect()

        # Créer le dossier de destination s'il n'existe pas
        os.makedirs(os.path.dirname(FORMATTED_BBD_PATH), exist_ok=True)

        # Ajout des nouvelles lignes au fichier BDD
        with open(FORMATTED_BBD_PATH, "a") as bdd_file:
            for line in lines_to_append:
                bdd_file.write(line + "\n")

        print(f"✅ {len(lines_to_append)} ligne(s) ajoutée(s) à {FORMATTED_BBD_PATH} avec PySpark.")

    finally:
        # Arrêt de la session Spark
        spark.stop()


# --- Définition du DAG ---

with DAG(
        dag_id='CleanCrypto_to_JSON',
        default_args={'owner': 'airflow'},
        start_date=datetime(2023, 1, 1),
        schedule='@daily',
        catchup=False
) as dag:
    # Tâche 1: Trouver le dossier le plus récent à traiter
    find_latest_folder_task = PythonOperator(
        task_id='find_latest_folder_task',
        python_callable=find_latest_folder_callable,
    )

    # Tâche 2: Lire les données avec PySpark et les ajouter à la BDD
    append_with_pyspark_task = PythonOperator(
        task_id='append_with_pyspark_task',
        python_callable=append_with_pyspark_callable,
    )

    # Tâche 3: Supprimer le dossier source une fois le traitement terminé
    # Utilise Jinja templating pour récupérer la valeur de XCom
    delete_source_folder_task = BashOperator(
        task_id='delete_source_folder_task',
        bash_command="rm -rf {{ ti.xcom_pull(task_ids='find_latest_folder_task') }}",
    )

    # --- Définition des dépendances ---
    # La chaîne est linéaire : trouver -> traiter -> supprimer
    find_latest_folder_task >> append_with_pyspark_task >> delete_source_folder_task