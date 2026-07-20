"""Utilities for loading Airflow Dags in tests."""

from pathlib import Path
from airflow.dag_processing.dagbag import DagBag

_DAG_DIR = Path(__file__).resolve().parent.parent.parent / "mokelumne" / "dags"

_DAG_BAG = DagBag(
    dag_folder=_DAG_DIR.resolve(),
)

def get_dag(dag_id: str):
    """Fetch a dag by name."""
    dag = _DAG_BAG.get_dag(dag_id)

    assert dag is not None, (
        f"Could not load DAG '{dag_id}'. "
        f"Import errors: {_DAG_BAG.import_errors}"
    )

    return dag
