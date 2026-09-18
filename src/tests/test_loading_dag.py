import pytest

from scripts.utils import import_latest_json
from constants import DATA_LIST

def test_load_data():
    oldData = DATA_LIST
    newData = import_latest_json()
    assert len(newData) == len(oldData)

