from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

def run_pyspark_job():
    spark = SparkSession.builder.master("local[*]").appName("spark_job_get_crypto").getOrCreate()
    # Importer et exécuter ton script PySpark ici, ou mettre le code directement
    # Exemple minimal : lire ton script et exécuter les fonctions dedans

    # Si tu veux exécuter ton script Python externe, tu peux l'importer ou l'exécuter via exec()
    # Exemple très simple d'exécution inline :
    import sys
    sys.path.insert(0, '/opt/airflow/dags/service/')
    import GetCryptoJSON  # supposant que c’est un module Python (pas un script standalone)

    # Appelle une fonction principale dans GetCryptoJSON si elle existe
    if hasattr(GetCryptoJSON, 'main'):
        GetCryptoJSON.main(spark)
    else:
        print("Le module GetCryptoJSON n'a pas de fonction main(spark) à appeler.")

    spark.stop()

with DAG('spark_job_get_crypto', default_args=default_args, schedule='@daily') as dag:
    run_spark_local = PythonOperator(
        task_id='run_spark_local',
        python_callable=run_pyspark_job
    )
