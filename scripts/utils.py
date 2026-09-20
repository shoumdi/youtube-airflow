import re
import json
from datetime import datetime
from pathlib import Path
from .constants import JSON_DIR
from typing import Dict,Any
def yt_duration_format(value: str) -> int:
    match = re.fullmatch(
        r"P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?",
        value
    )

    if not match:
        raise ValueError(f"Invalid YouTube duration: {value}")

    days = int(match.group(1) or 0)
    hours = int(match.group(2) or 0)
    minutes = int(match.group(3) or 0)
    seconds = int(match.group(4) or 0)

    return (
        days * 86400
        + hours * 3600
        + minutes * 60
        + seconds
    )

def export_json(data):
    filename = f"/opt/airflow/data/{datetime.now().strftime('%Y-%m-%d %H:%M')}.json"
    with open(f"{filename}", "w",encoding="utf-8") as file:
        json.dump(data, file, indent=4)
    return True

def import_latest_json()->list[Dict[str,Any]]:
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