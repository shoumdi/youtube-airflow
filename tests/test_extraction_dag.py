import importlib
from unittest.mock import MagicMock, patch

import pytest


MODULE = "dags.data_extraction"


@pytest.fixture
def dag_module():
    with patch("airflow.models.Variable.get") as variable_get:
        variable_get.side_effect = lambda key: {
            "API_KEY": "test-api-key",
            "CHANNEL_HANDLE": "@test-channel",
        }[key]

        module = importlib.import_module(MODULE)

    return module


@pytest.fixture
def extraction_dag(dag_module):
    return dag_module.extraction_dag()


def test_dag_tasks(extraction_dag):
    task_ids = {task.task_id for task in extraction_dag.tasks}

    assert task_ids == {
        "get_channel_playlist_id",
        "get_playlist_videos",
        "save_json",
        "trigger_data_loading",
    }


def test_task_dependencies(extraction_dag):
    get_playlist = extraction_dag.get_task("get_playlist_videos")
    save_json = extraction_dag.get_task("save_json")
    trigger = extraction_dag.get_task("trigger_data_loading")

    assert save_json.task_id in get_playlist.downstream_task_ids
    assert trigger.task_id in save_json.downstream_task_ids



def test_get_channel_playlist_id_success(dag_module):
    response = MagicMock()
    response.json.return_value = {
        "items": [
            {
                "contentDetails": {
                    "relatedPlaylists": {
                        "uploads": "UPLOADS_PLAYLIST_ID"
                    }
                }
            }
        ]
    }

    with patch.object(dag_module.requests, "get", return_value=response) as mock_get:
        result = dag_module.extraction_dag().get_task(
            "get_channel_playlist_id"
        ).python_callable(
            "test-api-key",
            "@test-channel",
        )

    assert result == "UPLOADS_PLAYLIST_ID"

    mock_get.assert_called_once_with(
        f"{dag_module.BASE_URL}/channels",
        params={
            "part": "snippet,contentDetails,statistics",
            "forHandle": "test-channel",
            "key": "test-api-key",
        },
        timeout=30,
    )

    response.raise_for_status.assert_called_once()

def test_save_json(dag_module):
    videos = [
        {
            "video_id": "video-1",
            "title": "Test video",
            "views": "100",
        }
    ]

    with patch.object(dag_module, "export_json") as mock_export:
        result = dag_module.extraction_dag().get_task(
            "save_json"
        ).python_callable(videos)

    assert result is None
    mock_export.assert_called_once_with(videos)
