"""Run the pdf_creation DAG using fixture data."""

import shutil
from collections.abc import Generator
from pathlib import Path

import pytest

from airflow_client.client import ApiClient
from airflow_client.client.api.dag_run_api import DagRunApi
from airflow_client.client.models.dag_run_response import DAGRunResponse
from airflow_client.client.models.trigger_dag_run_post_body import TriggerDAGRunPostBody

pytestmark = pytest.mark.live_ocr


@pytest.fixture
def clean_generated_outputs() -> Generator[None, None, None]:
    """Clear fixture outputs while preserving the pre-existing skipped PDF."""
    outputs = Path(__file__).resolve().parents[1] / "fixtures/pdf_creation/outputs"
    for output in outputs.iterdir():
        if output.name == "skipped.pdf":
            continue
        if output.is_dir() and not output.is_symlink():
            shutil.rmtree(output)
        else:
            output.unlink()
    yield


@pytest.fixture
def completed_pdf_run(
    api_client: ApiClient,
    dag_run_id: str,
    clean_generated_outputs: None,
) -> DAGRunResponse:
    """Trigger the fixture Dag, wait for completion, and return its run details."""
    dag_runs = DagRunApi(api_client)
    run = dag_runs.trigger_dag_run(
        dag_id="pdf_creation",
        trigger_dag_run_post_body=TriggerDAGRunPostBody(
            dag_run_id=dag_run_id,
            note=f"Created by pytest: {dag_run_id}",
            logical_date=None,
            conf={
                "source": "/opt/airflow/test/fixtures/pdf_creation/inputs",
                "destination": "/opt/airflow/test/fixtures/pdf_creation/outputs",
                "language": "eng",
                "max_resolution": 200,
            },
        ),
    )
    assert run.dag_id == "pdf_creation"
    assert run.dag_run_id == dag_run_id

    dag_runs.wait_dag_run_until_finished(
        dag_id="pdf_creation", dag_run_id=dag_run_id,
        interval=1, _request_timeout=180.0,
    )
    return dag_runs.get_dag_run(dag_id="pdf_creation", dag_run_id=dag_run_id)


@pytest.mark.usefixtures("completed_pdf_run")
def test_create_pdf_from_fixtures() -> None:
    """Write only the successful PDF alongside the pre-existing skipped PDF."""
    outputs = Path(__file__).resolve().parents[1] / "fixtures/pdf_creation/outputs"
    assert {output.name for output in outputs.iterdir()} == {"successful.pdf", "skipped.pdf"}
    assert all(output.is_file() for output in outputs.iterdir())


def test_fixture_run_failed(completed_pdf_run: DAGRunResponse) -> None:
    """Fail the run because the fixtures include a Tesseract processing error."""
    assert completed_pdf_run.state == "failed"
