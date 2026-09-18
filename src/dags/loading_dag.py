from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from pathlib import Path
from scripts.utils import yt_duration_format
from typing import Dict,Any
from scripts.constants import CONN_ID,JSON_DIR


@dag(
    dag_id="data_loading",
)
def loading():
    @task
    def prepare_staging_db():
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        hook.run(
            """
                CREATE SCHEMA IF NOT EXISTS staging;
                CREATE TABLE IF NOT EXISTS staging.videos (
                    video_id       VARCHAR(50) PRIMARY KEY,
                    title          TEXT,
                    description    TEXT,
                    published_at   TIMESTAMPTZ,
                    duration       VARCHAR(50),
                    views          BIGINT,
                    likes          BIGINT,
                    comments       BIGINT,
                    loaded_at      TIMESTAMPTZ DEFAULT NOW()
                );
        """
        )
    @task
    def load_json_data()->list[Dict[str,Any]]:

        directory = Path(JSON_DIR)

        json_files = list(directory.glob("*.json"))

        if not json_files:
            raise FileNotFoundError(
                f"No JSON files found in {JSON_DIR}"
            )

        latest_file = max(
            json_files,
            key=lambda file: file.stat().st_mtime
        )
        with latest_file.open("r", encoding="utf-8") as file:
            data = json.load(file)
        return data

    @task
    def synchronize_staging(videos:list[Dict[str,Any]]):

        hook = PostgresHook(postgres_conn_id=CONN_ID)

        rows = [
            (
                video.get("video_id"),
                video.get("title"),
                video.get("description"),
                video.get("published_at"),
                video.get("duration"),
                video.get("views"),
                video.get("likes"),
                video.get("comments"),
            )
            for video in videos
        ]
        hook.insert_rows(
            table="staging.videos",
            rows=rows,
            target_fields=[
                "video_id",
                "title",
                "description",
                "published_at",
                "duration",
                "views",
                "likes",
                "comments",
            ],
            replace=True,
            replace_index="video_id"
        )

        hook = PostgresHook(
                    postgres_conn_id=CONN_ID
                )
        rows = hook.get_records("""
            SELECT video_id
                FROM staging.videos;
        """)
        
        old_video_ids = [row[0] for row in rows]
        new_video_ids = [video["video_id"] for video in videos]
        ids_to_delete = [old_id for old_id in old_video_ids if old_id not in new_video_ids]
        hook.run("""
            DELETE FROM staging.videos
                WHERE video_id = ANY(%s);;
            """,parameters=(ids_to_delete,),)
        
    @task
    def prepare_core_db():
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        
        sql = """
                CREATE SCHEMA IF NOT EXISTS core;
                CREATE TABLE IF NOT EXISTS core.videos (
                    video_id       VARCHAR(50) PRIMARY KEY,
                    title          TEXT,
                    description    TEXT,
                    published_at   TIMESTAMPTZ,
                    duration       BIGINT,
                    views          BIGINT,
                    likes          BIGINT,
                    comments       BIGINT,
                    loaded_at      TIMESTAMPTZ DEFAULT NOW()
                );
                """
        
        hook.run(sql)
    
    @task
    def transform_data()->list[Dict[str,Any]]:
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        records = hook.get_records(
                    sql="SELECT v.* name FROM staging.videos v"
                )
        return list(map(lambda video: {
            "video_id":video[0],
            "title":video[1],
            "description":video[2],
            "published_at":video[3],
            "duration":yt_duration_format(video[4]),
            "views":video[5],
            "likes":video[6],
            "comments":video[7]} ,records))
 
    
    @task
    def synchronize_core(videos:list[Dict[str,Any]]):
        rows = [(
                    video.get("video_id"),
                    video.get("title"),
                    video.get("description"),
                    video.get("published_at"),
                    video.get("duration"),
                    video.get("views"),
                    video.get("likes"),
                    video.get("comments"),
                )
                for video in videos
        ]
        
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        hook.insert_rows(
                    table="core.videos",
                    rows=rows,
                    target_fields=[
                        "video_id",
                        "title",
                        "description",
                        "published_at",
                        "duration",
                        "views",
                        "likes",
                        "comments",
                    ],
                    replace=True,
                    replace_index="video_id"
                )
        rows = hook.get_records("""
                    SELECT video_id
                    FROM core.videos;
                """)
        
        old_video_ids = [row[0] for row in rows]
        new_video_ids = [video["video_id"] for video in videos]
        ids_to_delete = [old_id for old_id in old_video_ids if old_id not in new_video_ids]
        
        
        sql = """
            DELETE FROM core.videos
                WHERE video_id = ANY(%s);;
            """
        
        hook.run(sql,parameters=(ids_to_delete,),)
                
    videos = load_json_data()
    transformed = transform_data()
    prepare_staging_db() >>  synchronize_staging(videos) >> prepare_core_db() >> synchronize_core(transformed)

loading()