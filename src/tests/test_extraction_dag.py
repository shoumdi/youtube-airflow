import pytest
from dags.extraction_dag import extraction_dag
from scripts.utils import export_json
from constants import DATA_LIST

def test_json_export():
    videos = DATA_LIST
    
    res = export_json(videos)
    
    assert res == True
    