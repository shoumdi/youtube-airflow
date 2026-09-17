from airflow.decorators import dag, task

from datetime import datetime
import json
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dotenv import load_dotenv
import os

load_dotenv()

JSON_DIR = "/opt/airflow/data"
CONN_ID = "postgres_db_yt_elt"

with DAG(
    dag_id="data_staging_dag",
    catchup=False,
) as dag:

    @task
    def create_tables():
        hook = PostgresHook(postgres_conn_id=CONN_ID)

        sql = """
        CREATE SCHEMA IF NOT EXISTS staging;
        CREATE TABLE IF NOT EXISTS staging.youtube_videos (
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

        hook.run(sql)
    @task
    def get_latest_videos():

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
    def upsert(videos):

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
            table="staging.youtube_videos",
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
        )

        sql = """
        INSERT INTO staging.youtube_videos (
            video_id,
            title,
            description,
            published_at,
            duration,
            views,
            likes,
            comments
        )
        SELECT
            video_id,
            title,
            description,
            published_at,
            duration,
            views,
            likes,
            comments
        FROM staging.youtube_videos

        ON CONFLICT (video_id)
        DO UPDATE SET
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            published_at = EXCLUDED.published_at,
            duration = EXCLUDED.duration,
            views = EXCLUDED.views,
            likes = EXCLUDED.likes,
            comments = EXCLUDED.comments,
            updated_at = NOW()
        """
        hook.run(sql)


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
            for video in data
        ]

        if rows:
            hook.insert_rows(
                table="staging.youtube_videos",
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
            )
    
    @task
    def delete_missing_rows(videos):
        hook = PostgresHook(
            postgres_conn_id=CONN_ID
        )
        rows = hook.get_records("""
            SELECT video_id
            FROM staging.youtube_videos;
        """)

        old_video_ids = [row[0] for row in rows]
        new_video_ids = [video["video_id"] for video in videos]
        ids_to_delete = list(old_video_ids - new_video_ids)


        sql = """
        DELETE FROM staging.youtube_videos
            WHERE video_id = ANY(%s);;
        """

        hook.run(sql,parameters=(ids_to_delete,),)
    
    videos_list_task = get_latest_videos()
    
    upsert_task = upsert(videos_list_task)
    
    delete_task = delete_missing_rows(videos_list_task)

    create_tables() >>  videos_list_task >> upsert_task >> delete_task
