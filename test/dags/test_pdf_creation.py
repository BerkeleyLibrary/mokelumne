"""Test the pdf_creation DAG."""

import json
from unittest.mock import Mock
from test.util.dag_helper import get_dag

import pytest



DAG = get_dag("pdf_creation")
check_job_status = DAG.get_task("wait_for_pdf").partial_kwargs["response_check"]
valid_submission = DAG.get_task("submit_ocr_job").partial_kwargs["response_check"]
status_endpoint = DAG.get_task("submit_ocr_job").partial_kwargs["response_filter"]
select_successful_job = DAG.get_task("select_successful_job").python_callable
validate_and_publish = DAG.get_task("validate_and_publish").python_callable
process_document = DAG.get_task("process_document").python_callable
airflow_skip_exception = process_document.__globals__["AirflowSkipException"]


class TestPDFCreationDag:
    """Tests for the pdf_creation DAG."""

    def test_valid_submission_accepts_pending_job(self):
        """Accept a valid HTTP 202 job submission response."""
        response = Mock()
        response.status_code = 202
        response.json.return_value = {
            "job_id": "job-123",
            "job_status": "jobs/job-123",
            "status": "PENDING",
        }

        assert valid_submission(response) is True

    @pytest.mark.parametrize(
        "status_code,payload",
        [
            (
                200,
                {
                    "job_id": "job-123",
                    "job_status": "jobs/job-123",
                    "status": "PENDING",
                },
            ),
            (202, {"job_status": "jobs/job-123", "status": "PENDING"}),
            (202, {"job_id": "job-123", "status": "PENDING"}),
            (
                202,
                {
                    "job_id": "job-123",
                    "job_status": "jobs/job-123",
                    "status": "STARTED",
                },
            ),
        ],
        ids=["wrong-http-status", "missing-job-id", "missing-job-status", "not-pending"],
    )
    def test_valid_submission_rejects_invalid_response(self, status_code, payload):
        """Reject responses that do not match Quiabo's submission contract."""
        response = Mock()
        response.status_code = status_code
        response.json.return_value = payload

        assert valid_submission(response) is False

    def test_status_endpoint_returns_job_status_path(self):
        """Use Quiabo's job status path for subsequent HTTP polling."""
        response = Mock()
        response.json.return_value = {"job_status": "jobs/job-123"}

        assert status_endpoint(response) == "jobs/job-123"

    @pytest.mark.parametrize("status", ["PENDING", "STARTED"])
    def test_check_job_status_continues_polling(self, status):
        """Keep polling while a job is in a nonterminal state."""
        response = Mock()
        response.json.return_value = {"id": "job-123", "status": status}

        result = check_job_status(response)

        assert result.is_done is False
        assert result.xcom_value is None

    @pytest.mark.parametrize(
        "payload",
        [
            {
                "id": "job-123",
                "status": "SUCCESS",
                "result": {
                    "output_path": "/srv/test/output.pdf",
                    "sha256": "abc123",
                },
                "date_done": "2026-10-08T12:00:00",
            },
            {
                "id": "job-123",
                "status": "FAILURE",
                "result": "Tesseract failed",
                "traceback": "Traceback: Tesseract failed",
            },
            {"id": "job-123", "status": "REVOKED"},
        ],
        ids=["success", "failure", "revoked"],
    )
    def test_check_job_status_returns_terminal_payload(self, payload):
        """Return the response payload when a job reaches a terminal state."""
        response = Mock()
        response.json.return_value = payload

        result = check_job_status(response)

        assert result.is_done is True
        assert result.xcom_value == payload

    def test_check_job_status_rejects_unknown_status(self):
        """Reject statuses that Celery does not recognize."""
        response = Mock()
        response.json.return_value = {"status": "UNKNOWN"}

        with pytest.raises(ValueError, match="Unexpected job status: UNKNOWN"):
            check_job_status(response)

    def test_validate_inputs_task_exists(self):
        """Ensure validate_inputs task exists."""
        assert DAG.get_task("validate_inputs")

    def test_discover_documents_task_exists(self):
        """Ensure discover_documents task exists."""
        assert DAG.get_task("discover_documents")

    def test_expected_params_exist(self):
        """Ensure expected DAG parameters exist."""
        assert "source" in DAG.params
        assert "destination" in DAG.params
        assert "language" in DAG.params
        assert "max_poll_interval" in DAG.params
        assert "max_total_poll_time" in DAG.params
        assert "max_resolution" in DAG.params

    def test_task_order(self):
        """Ensure DAG tasks execute in the expected order."""
        validate_inputs = DAG.get_task("validate_inputs")
        prepare_run_directory = DAG.get_task("prepare_run_directory")
        discover_documents = DAG.get_task("discover_documents")
        process_document_task = DAG.get_task("process_document")
        submit_ocr_job = DAG.get_task("submit_ocr_job")
        wait_for_pdf = DAG.get_task("wait_for_pdf")
        select_successful_job_task = DAG.get_task("select_successful_job")
        validate_and_publish_task = DAG.get_task("validate_and_publish")

        assert [task.task_id for task in validate_inputs.downstream_list] == [
            "prepare_run_directory"
        ]
        assert {
            task.task_id for task in prepare_run_directory.downstream_list
        } == {
            "discover_documents",
            "process_document",
        }
        assert [task.task_id for task in discover_documents.downstream_list] == [
            "process_document"
        ]
        assert process_document_task.task_id in {
            task.task_id for task in submit_ocr_job.upstream_list
        }
        assert wait_for_pdf.task_id in {
            task.task_id for task in select_successful_job_task.upstream_list
        }
        assert select_successful_job_task.task_id in {
            task.task_id for task in validate_and_publish_task.upstream_list
        }

    def test_process_document_task_exists(self):
        """Ensure process_document task exists."""
        assert DAG.get_task("process_document")

    def test_process_document_prepares_job_payload(self, monkeypatch, tmp_path):
        """Prepare images and return the payload submitted to Quiabo."""
        source_path = tmp_path / "document_001"
        workspace_path = tmp_path / "run" / "document_001"
        file_list_path = workspace_path / "filelist.txt"
        context = {
            "params": {
                "destination": str(tmp_path / "destination"),
                "language": "",
                "max_resolution": 300,
            }
        }
        monkeypatch.setitem(
            process_document.__globals__,
            "get_current_context",
            lambda: context,
        )
        pdf_utils_module = process_document.__globals__["pdf_utils"]
        monkeypatch.setattr(pdf_utils_module, "output_exists", Mock(return_value=False))
        monkeypatch.setattr(
            pdf_utils_module,
            "prepare_workspace",
            Mock(return_value=workspace_path),
        )
        monkeypatch.setattr(
            pdf_utils_module,
            "determine_language",
            Mock(return_value="eng+fra"),
        )
        monkeypatch.setattr(
            pdf_utils_module,
            "prepare_images",
            Mock(return_value=file_list_path),
        )

        result = process_document(
            {"source": str(source_path), "output": "document_001.pdf"},
            str(tmp_path / "run"),
        )

        assert json.loads(result) == {
            "filelist": str(file_list_path),
            "languages": ["eng", "fra"],
            "output": str(tmp_path / "destination" / "document_001.pdf"),
        }

    def test_process_document_skips_existing_output(self, monkeypatch, tmp_path):
        """Skip processing when the expected output PDF already exists."""
        monkeypatch.setitem(
            process_document.__globals__,
            "get_current_context",
            lambda: {
                "params": {
                    "destination": str(tmp_path),
                    "language": "eng",
                    "max_resolution": 300,
                }
            },
        )
        pdf_utils_module = process_document.__globals__["pdf_utils"]
        monkeypatch.setattr(pdf_utils_module, "output_exists", Mock(return_value=True))

        with pytest.raises(
            airflow_skip_exception,
            match="Output PDF already exists",
        ):
            process_document(
                {"source": str(tmp_path / "document_001"), "output": "document_001.pdf"},
                str(tmp_path / "run"),
            )

    def test_polling_tasks_are_connected(self):
        """Ensure submissions flow through kwargs generation to the sensor."""
        submissions = DAG.get_task("submit_ocr_job")
        set_sensor_kwargs = DAG.get_task("set_sensor_kwargs")
        wait_for_pdf = DAG.get_task("wait_for_pdf")

        assert submissions in set_sensor_kwargs.upstream_list
        assert set_sensor_kwargs in wait_for_pdf.upstream_list

    def test_set_sensor_kwargs_converts_polling_params_to_seconds(self, monkeypatch):
        """Convert poll interval hours and total poll days to sensor seconds."""
        task = DAG.get_task("set_sensor_kwargs").python_callable
        monkeypatch.setitem(
            task.__globals__,
            "get_current_context",
            lambda: {
                "params": {
                    "max_poll_interval": 2,
                    "max_total_poll_time": 4,
                }
            },
        )

        result = task(["jobs/hash-id-1", "jobs/hash-id-2"])

        assert result == [
            {
                "endpoint": "jobs/hash-id-1",
                "max_wait": 2 * 60 * 60,
                "timeout": 4 * 24 * 60 * 60,
            },
            {
                "endpoint": "jobs/hash-id-2",
                "max_wait": 2 * 60 * 60,
                "timeout": 4 * 24 * 60 * 60,
            },
        ]

    def test_prepare_run_directory(self, monkeypatch, tmp_path):
        """Create and return the scratch directory for the Dag run."""
        task = DAG.get_task("prepare_run_directory").python_callable

        monkeypatch.setitem(
            task.__globals__,
            "get_current_context",
            lambda: {
                "run_id": "test_run",
            },
        )

        variable = Mock()
        variable.get.return_value = str(tmp_path)

        monkeypatch.setitem(
            task.__globals__,
            "Variable",
            variable,
        )

        result = task()

        assert result == str(tmp_path / "test_run")
        assert (tmp_path / "test_run").is_dir()
        variable.get.assert_called_once_with("pdf_creation_scratch_base")

    @pytest.mark.parametrize("status", ["FAILURE", "REVOKED"])
    def test_select_successful_job_filters_unsuccessful_jobs(self, status):
        """Do not pass failed or revoked OCR jobs to publication."""
        finished_job = {"id": "job-123", "status": status}

        assert select_successful_job(finished_job) is None

    def test_select_successful_job_returns_successful_job(self):
        """Pass successful OCR jobs to PDF validation and publication."""
        finished_job = {"id": "job-123", "status": "SUCCESS"}

        assert select_successful_job(finished_job) == finished_job

    def test_validate_and_publish_publishes_valid_pdf(self, monkeypatch, tmp_path):
        """Validate the Quiabo output and publish a valid PDF."""
        output_path = tmp_path / "output.pdf"
        published_path = tmp_path / "published" / "output.pdf"
        finished_job = {
            "id": "job-123",
            "status": "SUCCESS",
            "result": {"output_path": str(output_path), "sha256": "abc123"},
            "date_done": "2026-10-08T12:00:00",
        }
        monkeypatch.setitem(
            validate_and_publish.__globals__,
            "get_current_context",
            lambda: {"params": {"destination": str(tmp_path / "published")}},
        )
        validate_pdf = Mock(return_value=(True, None))
        publish_pdf = Mock(return_value=published_path)
        monkeypatch.setattr("mokelumne.util.pdf_utils.validate_pdf", validate_pdf)
        monkeypatch.setattr("mokelumne.util.pdf_utils.publish_pdf", publish_pdf)

        result = validate_and_publish(finished_job)

        validate_pdf.assert_called_once_with(output_path, "abc123")
        publish_pdf.assert_called_once_with(output_path, tmp_path / "published")
        assert result == {
            "job_id": "job-123",
            "status": "PUBLISHED",
            "output_path": str(published_path),
            "date_done": "2026-10-08T12:00:00",
            "error": None,
        }

    def test_validate_and_publish_reports_invalid_pdf(self, monkeypatch, tmp_path):
        """Return a validation failure without attempting publication."""
        output_path = tmp_path / "output.pdf"
        finished_job = {
            "id": "job-123",
            "result": {"output_path": str(output_path), "sha256": "abc123"},
        }
        monkeypatch.setitem(
            validate_and_publish.__globals__,
            "get_current_context",
            lambda: {"params": {"destination": str(tmp_path)}},
        )
        monkeypatch.setattr(
            "mokelumne.util.pdf_utils.validate_pdf",
            Mock(return_value=(False, "PDF hash mismatch")),
        )
        publish_pdf = Mock()
        monkeypatch.setattr("mokelumne.util.pdf_utils.publish_pdf", publish_pdf)

        result = validate_and_publish(finished_job)

        publish_pdf.assert_not_called()
        assert result["status"] == "VALIDATION_FAILED"
        assert result["output_path"] == str(output_path)
        assert result["error"] == "PDF hash mismatch"

    def test_validate_and_publish_reports_publication_error(self, monkeypatch, tmp_path):
        """Return a publication failure when moving the validated PDF fails."""
        output_path = tmp_path / "output.pdf"
        finished_job = {
            "id": "job-123",
            "result": {"output_path": str(output_path), "sha256": "abc123"},
        }
        monkeypatch.setitem(
            validate_and_publish.__globals__,
            "get_current_context",
            lambda: {"params": {"destination": str(tmp_path)}},
        )
        monkeypatch.setattr(
            "mokelumne.util.pdf_utils.validate_pdf",
            Mock(return_value=(True, None)),
        )
        monkeypatch.setattr(
            "mokelumne.util.pdf_utils.publish_pdf",
            Mock(side_effect=OSError("destination unavailable")),
        )

        result = validate_and_publish(finished_job)

        assert result["status"] == "PUBLISH_FAILED"
        assert result["output_path"] == str(output_path)
        assert result["error"] == "destination unavailable"
