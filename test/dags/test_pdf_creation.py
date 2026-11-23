"""Test the pdf_creation DAG."""

import json
import hashlib
from copy import copy
from unittest.mock import Mock

import pytest
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException

from test.util.dag_helper import get_dag


DAG = get_dag("pdf_creation")
check_ocr_job_status = DAG.get_task("process_document.check_job_status").python_callable
validate_pdf = DAG.get_task("process_document.validate_pdf").python_callable
publish_pdf = DAG.get_task("process_document.publish_pdf").python_callable
check_for_errors = DAG.get_task("check_for_errors").python_callable
cleanup_run = DAG.get_task("cleanup_run").python_callable


class TestPDFCreationDag:
    """Tests for the pdf_creation DAG."""

    def test_dag_tags(self):
        """Verify the PDF creation DAG has the expected tags."""
        assert set(DAG.tags) == {"dlp", "pdf", "creation"}

    def test_validate_inputs_task_exists(self):
        """Ensure validate_inputs task exists."""
        assert DAG.get_task("validate_inputs")

    def test_discover_documents_task_exists(self):
        """Ensure discover_documents task exists."""
        assert DAG.get_task("discover_documents")

    def test_expected_params_exist(self):
        """Ensure expected DAG parameters exist."""
        assert "source_volume" in DAG.params
        assert "source_subdirectory" in DAG.params
        assert "destination_volume" in DAG.params
        assert "destination_subdirectory" in DAG.params
        assert "language" in DAG.params
        assert "max_poll_interval" in DAG.params
        assert "max_total_poll_time" in DAG.params
        assert "max_resolution" in DAG.params

    def test_submission_polling_and_publication_order(self):
        """Submit prepared requests, poll their jobs, then publish completed PDFs."""
        assert {"process_document.prepare_images", "process_document.determine_language"} <= (
            DAG.get_task("process_document.submit_ocr_job").upstream_task_ids
        )
        assert "process_document.submit_ocr_job" in DAG.get_task(
            "process_document.await_job_completion"
        ).upstream_task_ids
        assert "process_document.await_job_completion" in DAG.get_task(
            "process_document.check_job_status"
        ).upstream_task_ids
        assert "process_document.check_job_status" in DAG.get_task(
            "process_document.validate_pdf"
        ).upstream_task_ids
        assert "process_document.validate_pdf" in DAG.get_task(
            "process_document.publish_pdf"
        ).upstream_task_ids

    def test_documents_are_processed_in_a_mapped_group(self):
        """Map the document-processing group over discovery results."""
        group = DAG.task_group.children["process_document"]
        assert {task.task_id for task in group.iter_mapped_dependencies()} == {
            "discover_documents",
        }
        assert "prepare_run_directory" in DAG.get_task("discover_documents").upstream_task_ids
        assert "validate_inputs" in DAG.get_task("prepare_run_directory").upstream_task_ids

    def test_document_status_waits_for_publication_even_after_failure(self):
        """Collect each document's status after its processing tasks finish."""
        status = DAG.get_task("process_document.return_document_status")
        assert status.trigger_rule == "all_done"
        assert "process_document.publish_pdf" in status.upstream_task_ids

    def test_prepare_run_directory(self, monkeypatch, tmp_path):
        """Create and return the scratch directory for the supplied run ID."""
        task = DAG.get_task("prepare_run_directory").python_callable
        monkeypatch.setenv("AIRFLOW_VAR_PDF_CREATION_SCRATCH_BASE", str(tmp_path))

        result = task(run_id="test_run")

        assert result == str(tmp_path / "test_run")
        assert (tmp_path / "test_run").is_dir()

    def test_cleanup_run_removes_validated_run_directory(
        self, monkeypatch, tmp_path
    ):
        """Remove the directory after validating it against the scratch base."""
        scratch_base = tmp_path / "scratch"
        scratch_base.mkdir()
        run_directory = scratch_base / "run-1"
        run_directory.mkdir()
        (run_directory / "page.tif").write_text("test data")

        variable = Mock()
        variable.get.return_value = str(scratch_base)
        monkeypatch.setitem(cleanup_run.__globals__, "Variable", variable)

        cleanup_run(str(run_directory), "run-1")

        variable.get.assert_called_once_with("pdf_creation_scratch_base")
        assert not run_directory.exists()

    @pytest.mark.parametrize("cleanup_after_successful_run", [True, False])
    def test_cleanup_run_condition(self, cleanup_after_successful_run):
        """Allow cleanup when enabled and skip it when disabled."""
        cleanup_task = DAG.get_task("cleanup_run")
        pre_execute_hook = cleanup_task._pre_execute_hook
        assert pre_execute_hook is not None
        context = {"params": {"cleanup_after_successful_run": cleanup_after_successful_run}}

        if cleanup_after_successful_run:
            pre_execute_hook(context)
        else:
            with pytest.raises(AirflowSkipException, match="Skipping cleanup"):
                pre_execute_hook(context)

    def test_cleanup_run_skips_already_removed_directory(
        self, monkeypatch, tmp_path
    ):
        """Do not attempt removal when validation finds no run directory."""
        scratch_base = tmp_path / "scratch"
        scratch_base.mkdir()
        run_directory = scratch_base / "run-1"

        variable = Mock()
        variable.get.return_value = str(scratch_base)
        monkeypatch.setitem(cleanup_run.__globals__, "Variable", variable)
        remove_directory = Mock()
        monkeypatch.setitem(
            cleanup_run.__globals__, "remove_run_directory", remove_directory
        )

        cleanup_run(str(run_directory), "run-1")

        remove_directory.assert_not_called()

    def test_cleanup_run_rejects_directory_outside_scratch_base(
        self, monkeypatch, tmp_path
    ):
        """Reject an outside directory and leave it untouched."""
        scratch_base = tmp_path / "scratch"
        scratch_base.mkdir()
        outside_directory = tmp_path / "run-1"
        outside_directory.mkdir()

        variable = Mock()
        variable.get.return_value = str(scratch_base)
        monkeypatch.setitem(cleanup_run.__globals__, "Variable", variable)
        remove_directory = Mock()
        monkeypatch.setitem(
            cleanup_run.__globals__, "remove_run_directory", remove_directory
        )

        with pytest.raises(ValueError, match="Unexpected scratch directory"):
            cleanup_run(str(outside_directory), "run-1")

        remove_directory.assert_not_called()
        assert outside_directory.is_dir()

    def test_cleanup_run_follows_success_check(self):
        """Run cleanup only after the final error check succeeds."""
        cleanup_task = DAG.get_task("cleanup_run")

        assert cleanup_task.upstream_task_ids == {
            "check_for_errors",
            "prepare_run_directory",
        }
        assert cleanup_task.trigger_rule == "all_success"

    @pytest.mark.parametrize("status", ["FAILURE", "REVOKED"])
    def test_check_job_status_rejects_unsuccessful_jobs(self, status):
        """Record failed OCR jobs and fail without trying to publish them."""
        document = {"source": "/src/document", "output": "document.pdf"}
        ti = Mock()

        error = f"Job 'job-123' {status}: Tesseract failed"
        with pytest.raises(AirflowFailException, match=error):
            check_ocr_job_status(
                document,
                {"id": "job-123", "status": status, "result": "Tesseract failed"},
                ti,
            )

        assert ti.xcom_push.call_args.kwargs["value"]["status"] == "OCR_JOB_FAILED"
        assert ti.xcom_push.call_args.kwargs["value"]["error"] == error

    def test_successful_job_is_validated_and_published(self, tmp_path):
        """Validate and publish an actual PDF file, recording its publication path."""
        output_path = tmp_path / "output.pdf"
        output_path.write_bytes(b"%PDF-1.4\n%%EOF\n")
        destination = tmp_path / "published"
        destination.mkdir()
        document = {"source": "/src/document", "output": "output.pdf"}
        job = {
            "id": "job-123", "status": "SUCCESS",
            "result": {
                "output_path": str(output_path),
                "sha256": hashlib.sha256(output_path.read_bytes()).hexdigest(),
            },
            "date_done": "2026-10-08T12:00:00",
        }
        ti = Mock()

        successful_job = check_ocr_job_status(document, job, ti)
        validated_job = validate_pdf(document, successful_job, ti)
        assert publish_pdf(document, validated_job, str(destination), ti) == document

        assert (destination / "output.pdf").read_bytes() == b"%PDF-1.4\n%%EOF\n"
        ti.xcom_push.assert_called_once_with(
            key="document_status",
            value={
                "document": document, "job_id": "job-123", "status": "PUBLISHED",
                "output_path": str(destination / "output.pdf"),
                "date_done": "2026-10-08T12:00:00", "error": None,
            },
        )

    def test_validate_pdf_reports_invalid_pdf(self, tmp_path):
        """Reject a checksum mismatch without publishing the PDF."""
        output = tmp_path / "output.pdf"
        output.write_bytes(b"%PDF-1.4\n%%EOF\n")
        destination = tmp_path / "published"
        destination.mkdir()
        job = {
            "id": "job-123", "status": "SUCCESS",
            "result": {"output_path": str(output), "sha256": "wrong-checksum"},
        }
        ti = Mock()

        with pytest.raises(AirflowFailException, match="hash mismatch"):
            validate_pdf(
                {"source": "/src/a", "output": "output.pdf"},
                job | {"date_done": "2026-10-08T12:00:00"},
                ti,
            )

        assert list(destination.iterdir()) == []
        assert ti.xcom_push.call_args.kwargs["value"]["status"] == "PDF_VALIDATION_FAILED"

    def test_publish_pdf_reports_publication_error(self, monkeypatch, tmp_path):
        """Record publication errors before failing the task."""
        monkeypatch.setattr(
            "mokelumne.util.pdf_utils.publish_pdf",
            Mock(side_effect=OSError("destination unavailable")),
        )
        ti = Mock()
        job = {
            "id": "job-123", "status": "SUCCESS",
            "result": {"output_path": str(tmp_path / "output.pdf"), "sha256": "abc123"},
        }

        with pytest.raises(AirflowFailException, match="destination unavailable"):
            publish_pdf(
                {"source": "/src/a", "output": "output.pdf"},
                job | {"date_done": "2026-10-08T12:00:00"},
                str(tmp_path),
                ti,
            )

        status = ti.xcom_push.call_args.kwargs["value"]
        assert status["status"] == "PUBLISH_FAILED"
        assert status["error"] == "destination unavailable"

    @pytest.mark.parametrize("results", [
        {"job_id": "job-123", "status": "PUBLISHED"},
        [{"job_id": "job-123", "status": "PUBLISHED"}],
    ])
    def test_check_for_errors_succeeds_when_all_publications_succeed(self, results):
        """Accept successful processing as either a singleton or mapped results."""
        assert check_for_errors(results) is None

    def test_check_for_errors_accepts_skips_alongside_publication(self):
        """Succeed when at least one document was published and the others were skipped."""
        assert check_for_errors([
            {"status": "PUBLISHED"},
            {"status": "PDF_ALREADY_EXISTS"},
        ]) is None

    def test_check_for_errors_accepts_all_skipped_documents(self):
        """Succeed when discovered documents already have output PDFs."""
        assert check_for_errors([
            {"status": "PDF_ALREADY_EXISTS"},
            {"status": "PDF_ALREADY_EXISTS"},
        ]) is None

    @pytest.mark.parametrize("status", ["PDF_VALIDATION_FAILED", "PUBLISH_FAILED"])
    def test_check_for_errors_fails_for_unsuccessful_publication(self, status):
        """Fail with document details when a publication did not succeed."""
        failed = {"job_id": "job-123", "status": status, "error": "publication error"}
        with pytest.raises(AirflowFailException) as exc_info:
            check_for_errors([
                {"status": "PUBLISHED"},
                {"status": "PDF_ALREADY_EXISTS"},
                failed,
            ])

        assert json.loads(str(exc_info.value)) == {
            "message": "Some documents failed to publish", "unpublished": [failed],
        }

    def test_check_for_errors_fails_when_no_documents_are_discovered(self):
        """Fail when discovery produced no document results."""
        with pytest.raises(AirflowFailException, match="No documents were discovered"):
            check_for_errors([])

    def test_summary_follows_document_statuses(self):
        """Render collected document statuses before the final error check."""
        assert "process_document.return_document_status" in (
            DAG.get_task("generate_summary").upstream_task_ids
        )
        assert "generate_summary" in DAG.get_task("check_for_errors").upstream_task_ids

    def test_generate_summary_writes_index_html(self, monkeypatch, tmp_path):
        """Render published and failed document statuses to the run's index page."""
        generate_summary = DAG.get_task("generate_summary").python_callable
        monkeypatch.setitem(
            generate_summary.__globals__, "static_files_run_dir",
            lambda dag_id, run_id: tmp_path,
        )
        document_results = [
            {
                "document": {"source": "/src/b", "output": "b.pdf"},
                "job_id": None, "status": "PROCESSING_FAILED", "output_path": None,
                "date_done": None, "error": "boom",
            },
            {
                "document": {"source": "/src/a", "output": "a.pdf"},
                "job_id": "job-1", "status": "PUBLISHED", "output_path": "/dest/a.pdf",
                "date_done": "2026-11-04T00:00:00+00:00", "error": None,
            },
        ]

        output_dir = generate_summary(document_results, "/src", "/dest", "eng", "pdf_creation", "run-1")

        html = (tmp_path / "index.html").read_text(encoding="utf-8")
        assert output_dir == str(tmp_path)
        assert "2 total: 1 published, 0 skipped, 1 failed" in html
        assert "boom" in html


def test_map_index_uses_document_directory_name() -> None:
    """Label mapped documents with the source directory name rather than its full path."""
    document = {
        "source": "/opt/airflow/test/fixtures/pdf_creation/inputs/tesseract-failure",
        "output": "tesseract-failure.pdf",
    }
    named_tasks = [
        task for task in DAG.tasks
        if "op_kwargs" in task.template_fields and task.map_index_template is not None
    ]
    assert named_tasks
    for task in named_tasks:
        assert "document" in task.op_kwargs
        rendered_task = copy(task)
        rendered_task.op_kwargs = task.op_kwargs | {"document": document}
        assert rendered_task.render_template(
            task.map_index_template, {"task": rendered_task},
        ) == "tesseract-failure"


@pytest.mark.parametrize("task_id", [
    "process_document.submit_ocr_job",
    "process_document.await_job_completion",
])
def test_http_task_map_index_uses_document_directory_name(task_id: str) -> None:
    """Label HTTP submission and polling with the same document directory name."""
    task = copy(DAG.get_task(task_id))
    assert "document" in task.template_fields
    assert task.document is not None
    task.document = {
        "source": "/opt/airflow/test/fixtures/pdf_creation/inputs/tesseract-failure",
        "output": "tesseract-failure.pdf",
    }

    assert task.render_template(task.map_index_template, {"task": task}) == "tesseract-failure"
