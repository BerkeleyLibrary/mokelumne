"""Structure tests for the GOBI order processing Dag."""

from pathlib import Path

from airflow.dag_processing.dagbag import DagBag

DAG_FILE = (
    Path(__file__).resolve().parent.parent.parent
    / "mokelumne"
    / "dags"
    / "process_gobi_orders.py"
)
DAG_BAG = DagBag(dag_folder=DAG_FILE)
DAG = DAG_BAG.dags.get("process_gobi_orders")


def test_gobi_dag_loads():
    assert DAG_BAG.import_errors == {}
    assert DAG is not None
    assert DAG.dag_id == "process_gobi_orders"


def test_gobi_dag_has_expected_tasks_and_dependency():
    discover = DAG.get_task("discover_order_files")
    process = DAG.get_task("process_one_order_file")

    assert set(DAG.task_ids) == {"discover_order_files", "process_one_order_file"}
    assert discover in process.upstream_list
    assert process.is_mapped


def test_gobi_dag_prevents_overlapping_runs_and_catchup():
    assert DAG.max_active_runs == 1
    assert DAG.catchup is False
    assert DAG.start_date.utcoffset().total_seconds() == 0


def test_gobi_dag_has_directory_parameters():
    assert set(DAG.params) == {
        "input_directory",
        "output_directory",
        "processed_directory",
    }
    assert DAG.params["input_directory"] == "/srv/alma/gobi-ebook-eocr-input"


def test_gobi_dag_runs_every_thirty_minutes_by_default():
    assert DAG.schedule == "*/30 * * * *"
