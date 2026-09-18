from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime
import json
import requests
from typing import Dict,Any
from scripts.constants import BASE_URL

@dag(
    schedule=None,
    dag_id="data_extraction",
    catchup=False,
)
def extraction_dag():
    @task
    def get_channel_playlist_id(
        api_key:str,
        handle:str
    ):
        handle = handle.lstrip("@")
        params = {
            "part": "snippet,contentDetails,statistics",
            "forHandle": handle,
            "key": api_key,
        }
        response = requests.get(
            f"{BASE_URL}/channels",
            params=params,
            timeout=30,
        )

        response.raise_for_status()

        data = response.json()

        if not data.get("items"):
            raise ValueError(f"Channel not found: @{handle}")

        return data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    
    @task
    def get_playlist_videos(
        playlist_id:str,
        api_key:str
    ):
        
        video_ids = []
        next_page_token = None

        while True:

            params = {
                "part": "snippet,contentDetails",
                "playlistId": playlist_id,
                "maxResults": 50,
                "key": api_key,
            }

            if next_page_token:
                params["pageToken"] = next_page_token

            response = requests.get(
                f"{BASE_URL}/playlistItems",
                params=params,
                timeout=30,
            )

            response.raise_for_status()

            data = response.json()

            for item in data.get("items", []):

                video_ids.append(item["contentDetails"]["videoId"])

            next_page_token = data.get("nextPageToken")

            if not next_page_token:
                break

        videos = []
        
        for i in range(0, len(video_ids), 50):
        
                batch = video_ids[i:i + 50]
        
                params = {
                    "part": "snippet,contentDetails,statistics",
                    "id": ",".join(batch),
                    "key": api_key,
                }
        
                response = requests.get(
                    f"{BASE_URL}/videos",
                    params=params,
                    timeout=30,
                )
        
                response.raise_for_status()
        
                data = response.json()
        
                for item in data.get("items", []):
        
                    statistics = item.get("statistics", {})
        
                    videos.append({
                        "video_id": item["id"],
                        "title": item["snippet"]["title"],
                        "description": item["snippet"].get("description"),
                        "published_at": item["snippet"].get("publishedAt"),
                        "duration": item["contentDetails"].get("duration"),
                        "views": statistics.get("viewCount", 0),
                        "likes": statistics.get("likeCount", 0),
                        "comments": statistics.get("commentCount", 0),
                    })
        return videos
            
    @task
    def save_json(videos:list[Dict[str,Any]]):
        filename = f"/opt/airflow/data/{datetime.now().strftime('%Y-%m-%d %H:%M')}.json"
        with open(f"{filename}", "w") as file:
            json.dump(videos, file, indent=4)
    
  
    api_key = Variable.get("API_KEY")
    handle = Variable.get("CHANNEL_HANDLE")
    
    playlist_id = get_channel_playlist_id(api_key,handle)
    videos = get_playlist_videos(playlist_id,api_key)
    
    next_dag = TriggerDagRunOperator(
                    task_id="trigger_data_loading",                    
                    trigger_dag_id="data_loading",
                    wait_for_completion=True,
                )
    save_json(videos) >> next_dag

extraction_dag()