"""Structure tests for the LBNL SFTP fetch Dag."""

from datetime import timedelta
from pathlib import Path

import pytest
from airflow.dag_processing.dagbag import DagBag

DAG_FILE = (
    Path(__file__).resolve().parent.parent.parent
    / "mokelumne"
    / "dags"
    / "fetch_lbnl_patron_file.py"
)


@pytest.fixture(scope="module")
def dag_bag() -> DagBag:
    """Load the LBNL fetch Dag."""

    return DagBag(dag_folder=DAG_FILE)


@pytest.fixture(scope="module")
def lbnl_fetch_dag(dag_bag: DagBag):
    """Return the parsed LBNL fetch Dag."""

    return dag_bag.dags.get("fetch_lbnl_patron_file")


def test_lbnl_fetch_dag_loads(dag_bag, lbnl_fetch_dag) -> None:
    assert dag_bag.import_errors == {}
    assert lbnl_fetch_dag is not None


def test_lbnl_fetch_dag_is_independent(lbnl_fetch_dag) -> None:
    assert set(lbnl_fetch_dag.task_ids) == {"fetch_file"}
    task = lbnl_fetch_dag.get_task("fetch_file")
    assert task.upstream_list == []
    assert task.downstream_list == []


def test_lbnl_fetch_dag_schedule_and_safety(lbnl_fetch_dag) -> None:
    assert lbnl_fetch_dag.schedule == "0 1 * * 2"
    assert str(lbnl_fetch_dag.timezone) == "America/Los_Angeles"
    assert lbnl_fetch_dag.catchup is False
    assert lbnl_fetch_dag.max_active_runs == 1
    assert lbnl_fetch_dag.is_paused_upon_creation is True


def test_lbnl_fetch_task_retry_policy(lbnl_fetch_dag) -> None:
    task = lbnl_fetch_dag.get_task("fetch_file")
    assert task.retries == 2
    assert task.retry_delay == timedelta(minutes=15)


def test_lbnl_fetch_dag_filename_parameter(lbnl_fetch_dag) -> None:
    assert set(lbnl_fetch_dag.params) == {"filename"}
    assert lbnl_fetch_dag.params["filename"] is None


def test_lbnl_fetch_dag_tags(lbnl_fetch_dag) -> None:
    assert {"lbnl", "sftp", "recurring"}.issubset(lbnl_fetch_dag.tags)
