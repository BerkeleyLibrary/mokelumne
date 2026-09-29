"""Structure tests for the GOBI SFTP fetch Dag."""

from datetime import timedelta
from pathlib import Path

import pytest
from airflow.dag_processing.dagbag import DagBag

DAG_FILE = (
    Path(__file__).resolve().parent.parent.parent
    / "mokelumne"
    / "dags"
    / "fetch_gobi_order_file.py"
)


@pytest.fixture(scope="module")
def dag_bag() -> DagBag:
    """Load the GOBI fetch Dag."""

    return DagBag(dag_folder=DAG_FILE)


@pytest.fixture(scope="module")
def gobi_fetch_dag(dag_bag: DagBag):
    """Return the parsed GOBI fetch Dag."""

    return dag_bag.dags.get("fetch_gobi_order_file")


def test_gobi_fetch_dag_loads(dag_bag, gobi_fetch_dag) -> None:
    assert dag_bag.import_errors == {}
    assert gobi_fetch_dag is not None


def test_gobi_fetch_dag_is_independent(gobi_fetch_dag) -> None:
    assert set(gobi_fetch_dag.task_ids) == {"fetch_file"}
    task = gobi_fetch_dag.get_task("fetch_file")
    assert task.upstream_list == []
    assert task.downstream_list == []


def test_gobi_fetch_dag_schedule_and_safety(gobi_fetch_dag) -> None:
    assert gobi_fetch_dag.schedule == "0 5 * * *"
    assert str(gobi_fetch_dag.timezone) == "America/Los_Angeles"
    assert gobi_fetch_dag.catchup is False
    assert gobi_fetch_dag.max_active_runs == 1
    assert gobi_fetch_dag.is_paused_upon_creation is True


def test_gobi_fetch_task_retry_policy(gobi_fetch_dag) -> None:
    task = gobi_fetch_dag.get_task("fetch_file")
    assert task.retries == 2
    assert task.retry_delay == timedelta(minutes=15)


def test_gobi_fetch_dag_filename_parameter(gobi_fetch_dag) -> None:
    assert set(gobi_fetch_dag.params) == {"filename"}
    assert gobi_fetch_dag.params["filename"] is None


def test_gobi_fetch_dag_tags(gobi_fetch_dag) -> None:
    assert {"gobi", "sftp", "recurring"}.issubset(gobi_fetch_dag.tags)
