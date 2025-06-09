from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from pathlib import Path
import json
import requests
from googleapiclient.discovery import build
from dotenv import load_dotenv

# === CHARGEMENT DES VARIABLES D'ENVIRONNEMENT ===
load_dotenv(dotenv_path="/opt/airflow/.env")  # Spécifie le chemin si nécessaire
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
KEYWORD = "monero"
BASE_PATH = "/opt/airflow/data/trends"


# === UTILS ===
def save_json(data, filepath):
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)


# === TÂCHES ===
def fetch_youtube_trends(**kwargs):
    service = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
    now = datetime.now().strftime("%Y%m%dT%H%M%S")
    output_path = Path(BASE_PATH) / "youtube" / f"{KEYWORD}_youtube_{now}.json"

    request = service.search().list(
        q=KEYWORD,
        part="snippet",
        maxResults=10,
        order="date"
    )
    response = request.execute()
    save_json(response, output_path)


def fetch_twitter_trends(**kwargs):
    now = datetime.now().strftime("%Y%m%dT%H%M%S")
    output_path = Path(BASE_PATH) / "twitter" / f"{KEYWORD}_twitter_{now}.json"

    url = f"https://nitter.net/search?f=tweets&q={KEYWORD}&since=&until=&near="
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers)

    data = {
        "url": url,
        "status": resp.status_code,
        "length": len(resp.text),
        "raw_html_snippet": resp.text[:500]
    }
    save_json(data, output_path)


def fetch_linkedin_trends(**kwargs):
    now = datetime.now().strftime("%Y%m%dT%H%M%S")
    output_path = Path(BASE_PATH) / "linkedin" / f"{KEYWORD}_linkedin_{now}.json"

    search_url = f"https://www.linkedin.com/search/results/content/?keywords={KEYWORD}"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(search_url, headers=headers)

    data = {
        "url": search_url,
        "status": response.status_code,
        "html_preview": response.text[:500]
    }
    save_json(data, output_path)


# === DAG ===
with DAG(
        dag_id="trend_monitoring_dag",
        start_date=datetime(2024, 1, 1),
        schedule="@daily",
        catchup=False
) as dag:
    youtube_task = PythonOperator(
        task_id="fetch_youtube_trends",
        python_callable=fetch_youtube_trends
    )

    twitter_task = PythonOperator(
        task_id="fetch_twitter_trends",
        python_callable=fetch_twitter_trends
    )

    linkedin_task = PythonOperator(
        task_id="fetch_linkedin_trends",
        python_callable=fetch_linkedin_trends
    )

    [youtube_task, twitter_task, linkedin_task]
