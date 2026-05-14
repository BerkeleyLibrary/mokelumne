from unittest.mock import patch, MagicMock
import pytest
import requests

from airflow.dag_processing.dagbag import DagBag
from pathlib import Path

dag_dir = Path(__file__).resolve().parent.parent / "mokelumne/dags"

def test_429_causes_task_retry():
    """Make sure that if the TIND API returns a 429, our task retries instead of failing."""
    dagbag = DagBag(dag_folder=dag_dir.resolve(), include_examples=False)
    dag = dagbag.get_dag("gen_llm_image_descriptions")
    fetch_fn = dag.get_task("fetch_images.fetch_image_to_record_directory").python_callable

    mock_context = {"params": {"max_width": 8000, "max_height": 8000}, "run_id": "test"}
    mock_fetcher = MagicMock()
    response = requests.Response()
    response.status_code = 429
    mock_fetcher.get_metadata_for_record.side_effect = requests.HTTPError(response=response)

    with patch(f"{fetch_fn.__module__}.get_current_context",
               return_value=mock_context):
        with pytest.raises(requests.HTTPError) as exc_info:
            fetch_fn("test_run", mock_fetcher, "12345")
        assert exc_info.value.response.status_code == 429
