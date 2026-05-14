import os

from pathlib import Path

import pytest

from airflow.dag_processing.dagbag import DagBag

dag_dir = Path(__file__).resolve().parent.parent.parent / "mokelumne/dags"

@pytest.fixture()
def dagbag() -> DagBag:
    return DagBag(dag_folder=dag_dir.resolve(), include_examples=False)
