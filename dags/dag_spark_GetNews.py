from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import json
import requests
from googleapiclient.discovery import build
from dotenv import load_dotenv

# === CHARGEMENT DES VARIABLES D'ENVIRONNEMENT ===
load_dotenv(dotenv_path="/opt/airflow/.env")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
KEYWORD = "monero"
BASE_PATH = "/opt/airflow/data/trends"

# === UTILS ===
def save_json(data, filepath):
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

def read_json(filepath):
    with open(filepath, "r") as f:
        return json.load(f)

# === TÂCHES ===
def fetch_youtube_trends(**kwargs):
    service = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

    now = datetime.now(timezone.utc)
    time_24_hours_ago = now - timedelta(days=1)
    since = time_24_hours_ago.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

    output_path = Path(BASE_PATH) / "youtube" / f"{KEYWORD}_youtube_{now.strftime('%Y%m%dT%H%M%S')}.json"

    request = service.search().list(
        q=KEYWORD,
        part="snippet",
        maxResults=25,
        order="date",
        publishedAfter=since
    )
    response = request.execute()
    save_json(response, output_path)
    kwargs['ti'].xcom_push(key='youtube_path', value=str(output_path))

def fetch_twitter_trends(**kwargs):
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    output_path = Path(BASE_PATH) / "twitter" / f"{KEYWORD}_twitter_{now}.json"

    url = f"https://nitter.net/search?f=tweets&q={KEYWORD}&since=&until="
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers)

    data = {
        "url": url,
        "status": resp.status_code,
        "length": len(resp.text),
        "raw_html_snippet": resp.text[:500]
    }
    save_json(data, output_path)
    kwargs['ti'].xcom_push(key='twitter_path', value=str(output_path))

def fetch_linkedin_trends(**kwargs):
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    output_path = Path(BASE_PATH) / "linkedin" / f"{KEYWORD}_linkedin_{now}.json"

    search_url = f"https://www.linkedin.com/search/results/content/?keywords={KEYWORD}"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(search_url, headers=headers)

    data = {
        "url": search_url,
        "status": response.status_code,
        "length": len(response.text),
        "html_preview": response.text[:500]
    }
    save_json(data, output_path)
    kwargs['ti'].xcom_push(key='linkedin_path', value=str(output_path))

def aggregate_trends(**kwargs):
    ti = kwargs['ti']
    youtube_path = Path(ti.xcom_pull(task_ids="fetch_youtube_trends", key="youtube_path"))
    twitter_path = Path(ti.xcom_pull(task_ids="fetch_twitter_trends", key="twitter_path"))
    linkedin_path = Path(ti.xcom_pull(task_ids="fetch_linkedin_trends", key="linkedin_path"))

    # Extraction de données simples
    yt_data = read_json(youtube_path)
    yt_score = len(yt_data.get("items", []))

    tw_data = read_json(twitter_path)
    tw_score = tw_data.get("length", 0)

    li_data = read_json(linkedin_path)
    li_score = li_data.get("length", 0)

    summary = {
        "timestamp": datetime.now().isoformat(),
        "keyword": KEYWORD,
        "scores": {
            "youtube": yt_score,
            "twitter_html_length": tw_score,
            "linkedin_html_length": li_score
        }
    }

    # === Sauvegarde dans bdd_trend.json ===
    bdd_path = Path("/opt/airflow/data/formatted/bdd_trend.json")
    bdd_path.parent.mkdir(parents=True, exist_ok=True)

    if bdd_path.exists():
        with open(bdd_path, "r") as f:
            try:
                bdd_data = json.load(f)
                if not isinstance(bdd_data, list):
                    bdd_data = [bdd_data]
            except json.JSONDecodeError:
                bdd_data = []
    else:
        bdd_data = []

    bdd_data.append(summary)

    with open(bdd_path, "w") as f:
        json.dump(bdd_data, f, indent=2)

# === DAG ===
with DAG(
    dag_id="GetNews",
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

    aggregate_task = PythonOperator(
        task_id="aggregate_trends",
        python_callable=aggregate_trends
    )

    [youtube_task, twitter_task, linkedin_task] >> aggregate_task
