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
BASE_PATH = "/opt/airflow/data/trends"

# === UTILS ===
def save_json(data, filepath):
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

def read_json(filepath):
    with open(filepath, "r") as f:
        return json.load(f)

# === FETCH YOUTUBE TRENDS ===
def fetch_youtube_trends(**kwargs):
    service = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

    now = datetime.now(timezone.utc)
    time_24_hours_ago = now - timedelta(days=1)
    since = time_24_hours_ago.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

    # Étape 1: recherche des vidéos
    search_response = service.search().list(
        q=KEYWORD,
        part="id",
        maxResults=50,
        order="date",
        publishedAfter=since,
        type="video"
    ).execute()

    video_ids = [item['id']['videoId'] for item in search_response.get("items", [])]

    # Étape 2: récupération des stats détaillées
    detailed_data = []
    for i in range(0, len(video_ids), 50):  # gestion par batch de 50
        chunk = video_ids[i:i+50]
        stats_response = service.videos().list(
            part="snippet,statistics,contentDetails",
            id=",".join(chunk)
        ).execute()
        detailed_data.extend(stats_response.get("items", []))

    output_path = Path(BASE_PATH) / "youtube" / f"{KEYWORD}_youtube_{now.strftime('%Y%m%dT%H%M%S')}.json"
    save_json(detailed_data, output_path)
    kwargs['ti'].xcom_push(key='youtube_path', value=str(output_path))

# === AGGREGATION SIMPLE ===
def aggregate_trends(**kwargs):
    ti = kwargs['ti']
    youtube_path = Path(ti.xcom_pull(task_ids="fetch_youtube_trends", key="youtube_path"))

    yt_data = read_json(youtube_path)

    summary = {
        "timestamp": datetime.now().isoformat(),
        "keyword": KEYWORD,
        "video_count": len(yt_data),
        "total_views": sum(int(video["statistics"].get("viewCount", 0)) for video in yt_data),
        "total_likes": sum(int(video["statistics"].get("likeCount", 0)) for video in yt_data if "likeCount" in video["statistics"]),
        "video_samples": [
            {
                "title": video["snippet"]["title"],
                "channel": video["snippet"]["channelTitle"],
                "views": video["statistics"].get("viewCount"),
                "likes": video["statistics"].get("likeCount"),
                "comments": video["statistics"].get("commentCount"),
                "publishedAt": video["snippet"]["publishedAt"],
                "url": f"https://www.youtube.com/watch?v={video['id']}"
            }
            for video in yt_data[:5]  # échantillon des 5 premiers
        ]
    }

    bdd_path = Path("/opt/airflow/data/formatted/bdd_trend.json")
    bdd_path.parent.mkdir(parents=True, exist_ok=True)

    if bdd_path.exists():
        try:
            with open(bdd_path, "r") as f:
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
    dag_id="GetYoutubeTrends",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    youtube_task = PythonOperator(
        task_id="fetch_youtube_trends",
        python_callable=fetch_youtube_trends
    )

    aggregate_task = PythonOperator(
        task_id="aggregate_trends",
        python_callable=aggregate_trends
    )

    youtube_task >> aggregate_task
