"""Test document preparation branches and dependencies in the PDF DAG."""

import json
from pathlib import Path
from unittest.mock import Mock, call

import pytest
import pyvips
from airflow.sdk.exceptions import AirflowSkipException

from mokelumne.util.pdf_utils import SubmitOcrJobOperator
from test.conftest import mock_ti
from test.util.dag_helper import get_dag


DAG = get_dag("pdf_creation")


def task_function(name: str):
    """Return a document-processing task's callable."""
    return DAG.get_task(f"process_document.{name}").python_callable


def test_prepare_and_submit_new_document(tmp_path: Path, monkeypatch) -> None:
    """Prepare a real source image and submit its job through the HTTP hook."""
    source = tmp_path / "document"
    source.mkdir()
    pyvips.Image.black(100, 100).tiffsave(str(source / "page.tif"))
    destination = tmp_path / "pdfs"
    destination.mkdir()
    document = {"source": str(source), "output": "document.pdf"}

    task_function("check_if_processed")(document, str(destination), Mock())
    workspace = task_function("prepare_workspace")(document, str(tmp_path / "run"))
    language = task_function("determine_language")(document, "eng+fra")
    file_list = task_function("prepare_images")(document, workspace, 200)
    response = Mock(status_code=202)
    response.json.return_value = {
        "job_id": "job-123", "job_status": "jobs/job-123", "status": "PENDING",
    }
    sent_requests = []

    def send_request(_session, request, **_kwargs):
        sent_requests.append(request)
        return response

    monkeypatch.setenv("AIRFLOW__API__INSTANCE_NAME", "test")
    monkeypatch.setenv("AIRFLOW_CONN_QUIABO_DEFAULT", "http://quiabo.test")
    monkeypatch.setattr("requests.Session.send", send_request)
    submit = SubmitOcrJobOperator(
        task_id="submit", document=document, destination=str(destination),
        language=language, file_list_path=file_list,
    )

    ti = mock_ti(
        dag_id="pdf_creation", run_id="test_run",
        task_id="process_document.submit_ocr_job", map_index=1,
    )
    assert submit.execute({"ti": ti}) == "jobs/job-123"

    assert len(sent_requests) == 1
    request = sent_requests[0]
    assert request.method == "POST"
    assert request.url == "http://quiabo.test/jobs"
    assert request.headers["Content-Type"] == "application/json"
    assert json.loads(request.body) == {
        "filelist": file_list,
        "languages": ["eng", "fra"],
        "output": str(destination / "document.pdf"),
        "correlation_id": "test/pdf_creation/test_run/process_document.submit_ocr_job/1",
    }
    image_path = Path(workspace) / "00000001.tif"
    assert Path(file_list).read_text(encoding="utf-8") == f"{image_path}\n"
    assert image_path.is_file()


def test_existing_pdf_records_status_and_skips(tmp_path: Path) -> None:
    """Record the existing PDF's path before skipping its processing."""
    output = tmp_path / "document.pdf"
    output.write_bytes(b"existing PDF")
    document = {"source": "/source/document", "output": output.name}
    ti = Mock()

    with pytest.raises(AirflowSkipException, match="output PDF already exists"):
        task_function("check_if_processed")(document, str(tmp_path), ti)

    ti.xcom_push.assert_called_once_with(
        key="document_status",
        value={
            "document": document,
            "status": "PDF_ALREADY_EXISTS",
            "output_path": str(output),
            "job_id": None,
            "date_done": None,
            "error": None,
        },
    )


def test_return_document_status_recovers_skip_status() -> None:
    """Retrieve the skip status from the check task for the same mapped document."""
    document = {"source": "/source/document", "output": "document.pdf"}
    skipped = {"document": document, "status": "PDF_ALREADY_EXISTS"}
    ti = Mock(map_index=2)
    ti.xcom_pull.side_effect = [None, None, None, skipped]

    assert task_function("return_document_status")(document, ti) == skipped
    assert ti.xcom_pull.call_args_list == [
        call(task_ids="process_document.publish_pdf", key="document_status", map_indexes=2),
        call(task_ids="process_document.validate_pdf", key="document_status", map_indexes=2),
        call(task_ids="process_document.check_job_status", key="document_status", map_indexes=2),
        call(task_ids="process_document.check_if_processed", key="document_status", map_indexes=2),
    ]


def test_language_and_images_can_run_in_parallel() -> None:
    """Gate both branches on the skip check and join their results for submission."""
    check = DAG.get_task("process_document.check_if_processed")
    language = DAG.get_task("process_document.determine_language")
    images = DAG.get_task("process_document.prepare_images")
    request = DAG.get_task("process_document.submit_ocr_job")

    assert {"process_document.prepare_workspace", language.task_id} <= check.downstream_task_ids
    assert "process_document.prepare_workspace" in images.upstream_task_ids
    assert language.task_id not in images.get_flat_relative_ids(upstream=True)
    assert images.task_id not in language.get_flat_relative_ids(upstream=True)
    assert {language.task_id, images.task_id} <= request.upstream_task_ids
