"""Structure tests for the GOBI order processing Dag."""

from pathlib import Path

import pytest

from airflow.dag_processing.dagbag import DagBag

DAG_FILE = (
    Path(__file__).resolve().parent.parent.parent
    / "mokelumne"
    / "dags"
    / "process_gobi_orders.py"
)


@pytest.fixture(scope="module")
def dag_bag() -> DagBag:
    """Load the GOBI Dag after its Airflow Variables have been mocked."""

    return DagBag(dag_folder=DAG_FILE)


@pytest.fixture(scope="module")
def gobi_dag(dag_bag: DagBag):
    """Return the parsed GOBI Dag."""

    return dag_bag.dags.get("process_gobi_orders")


def test_gobi_dag_loads(dag_bag, gobi_dag):
    assert dag_bag.import_errors == {}
    assert gobi_dag is not None
    assert gobi_dag.dag_id == "process_gobi_orders"


def test_gobi_dag_has_expected_tasks_and_dependency(gobi_dag):
    discover = gobi_dag.get_task("discover_order_files")
    process = gobi_dag.get_task("process_one_order_file")

    assert set(gobi_dag.task_ids) == {"discover_order_files", "process_one_order_file"}
    assert discover in process.upstream_list
    assert process.is_mapped


def test_gobi_dag_prevents_overlapping_runs_and_catchup(gobi_dag):
    assert gobi_dag.max_active_runs == 1
    assert gobi_dag.catchup is False
    assert gobi_dag.start_date.utcoffset().total_seconds() == 0


def test_gobi_dag_has_directory_parameters(gobi_dag, gobi_dag_variables):
    assert set(gobi_dag.params) == {
        "input_directory",
        "output_directory",
        "processed_directory",
    }
    assert (
        gobi_dag.params["input_directory"]
        == gobi_dag_variables["process_gobi_orders_input_dir"]
    )
    assert (
        gobi_dag.params["output_directory"]
        == gobi_dag_variables["process_gobi_orders_output_dir"]
    )
    assert (
        gobi_dag.params["processed_directory"]
        == gobi_dag_variables["process_gobi_orders_processed_dir"]
    )


def test_gobi_dag_runs_every_thirty_minutes_by_default(gobi_dag):
    assert gobi_dag.schedule == "*/30 * * * *"


def test_gobi_dag_is_tagged_as_recurring(gobi_dag):
    assert "recurring" in gobi_dag.tags
