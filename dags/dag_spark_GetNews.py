from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import json
from googleapiclient.discovery import build
from dotenv import load_dotenv

# === ENV ===
load_dotenv(dotenv_path="/opt/airflow/.env")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
KEYWORD = "monero"
BASE_PATH = "/opt/airflow/data/trends_raw"
OUTPUT_PATH = Path("/opt/airflow/data/formatted/bdd_trend.json")

# === UTILS ===
def save_json(data, filepath):
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

def read_json(filepath):
    if filepath.exists():
        try:
            with open(filepath, "r") as f:
                return json.load(f)
        except json.JSONDecodeError:
            return []
    return []

# === TÂCHE 1 : FETCH YOUTUBE ===
def fetch_youtube_trends(**kwargs):
    service = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
    now = datetime.now(timezone.utc)
    since = (now - timedelta(days=1)).isoformat(timespec='seconds').replace('+00:00', 'Z')

    search_response = service.search().list(
        q=KEYWORD,
        part="id",
        maxResults=50,
        order="date",
        publishedAfter=since,
        type="video"
    ).execute()

    video_ids = [item['id']['videoId'] for item in search_response.get("items", [])]

    detailed_data = []
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i+50]
        stats_response = service.videos().list(
            part="snippet,statistics",
            id=",".join(chunk)
        ).execute()
        detailed_data.extend(stats_response.get("items", []))

    # Sauvegarde fichier brut, optionnel ici
    raw_path = Path(BASE_PATH) / "youtube" / f"{KEYWORD}_{now.strftime('%Y%m%dT%H%M%S')}.json"
    save_json(detailed_data, raw_path)

    # Passe le chemin pour la prochaine tâche
    kwargs['ti'].xcom_push(key='raw_path', value=str(raw_path))

# === TÂCHE 2 : FORMATTER ET AJOUTER AU JSON HISTORIQUE ===
def process_youtube_trends(**kwargs):
    ti = kwargs['ti']
    raw_path_str = ti.xcom_pull(task_ids='fetch_youtube_trends', key='raw_path')
    raw_path = Path(raw_path_str)

    data = read_json(raw_path)
    if not data:
        return

    total_views = 0
    total_likes = 0
    total_comments = 0
    video_count = len(data)

    for video in data:
        stats = video.get("statistics", {})
        total_views += int(stats.get("viewCount", 0))
        total_likes += int(stats.get("likeCount", 0))
        total_comments += int(stats.get("commentCount", 0))

    summary = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "keyword": KEYWORD,
        "video_count": video_count,
        "total_views": total_views,
        "total_likes": total_likes,
        "total_comments": total_comments
    }

    history = read_json(OUTPUT_PATH)
    if not isinstance(history, list):
        history = []

    history.append(summary)
    save_json(history, OUTPUT_PATH)

# === DAG ===
with DAG(
    dag_id="GetYoutubeTrendsSimple",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["youtube", "trends"]
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_youtube_trends",
        python_callable=fetch_youtube_trends
    )

    process_task = PythonOperator(
        task_id="process_youtube_trends",
        python_callable=process_youtube_trends
    )

    fetch_task >> process_task
