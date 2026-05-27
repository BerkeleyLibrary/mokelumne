"""Tests for gen_llm_image_descriptions DAG."""
# pylint: disable=redefined-outer-name

from unittest.mock import patch, MagicMock
import pytest

from airflow.dag_processing.dagbag import DagBag
from pathlib import Path
from tind_client.errors import TooManyRequestsError

dag_dir = Path(__file__).resolve().parent.parent.parent / "mokelumne/dags"


@pytest.fixture(scope="module")
def fetch_fn():
    """Fixture to get the fetch function from the DAG."""
    dagbag = DagBag(dag_folder=dag_dir.resolve(), include_examples=False)
    dag = dagbag.get_dag("gen_llm_image_descriptions")
    return dag.get_task("fetch_images.fetch_image_to_record_directory").python_callable


def _mock_context(try_number: int, max_tries: int) -> dict:
    mock_ti = MagicMock()
    mock_ti.try_number = try_number
    mock_ti.max_tries = max_tries
    return {"params": {"max_width": 8000, "max_height": 8000}, "run_id": "test", "ti": mock_ti}


def test_429_causes_task_retry(fetch_fn):
    """If retries remain, a TindClient's TooManyRequestsError (429) triggers a retry."""
    mock_fetcher = MagicMock()
    mock_fetcher.get_metadata_for_record.side_effect = TooManyRequestsError()

    with patch(
        f"{fetch_fn.__module__}.get_current_context",
        return_value=_mock_context(try_number=1, max_tries=3),
    ):
        with pytest.raises(TooManyRequestsError):
            fetch_fn("test_run", mock_fetcher, "12345")


def test_429_on_final_attempt_returns_failed_status(fetch_fn):
    """If last retry gets a TooManyRequestsError, the task returns a failed status."""
    mock_fetcher = MagicMock()
    mock_fetcher.get_metadata_for_record.side_effect = TooManyRequestsError()

    with patch(
        f"{fetch_fn.__module__}.get_current_context",
        return_value=_mock_context(try_number=4, max_tries=3),
    ):
        result = fetch_fn("test_run", mock_fetcher, "12345")
        assert result.tind_id == "12345"
        assert result.status == "failed"
