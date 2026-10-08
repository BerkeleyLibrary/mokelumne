"""Test the pdf_creation DAG."""

from unittest.mock import Mock

import pytest

# from mokelumne.dags.pdf_creation import check_job_status
from test.util.dag_helper import get_dag


DAG = get_dag("pdf_creation")
check_job_status = DAG.get_task("wait_for_pdf").partial_kwargs["response_check"]
valid_submission = DAG.get_task("submit_ocr_job").partial_kwargs["response_check"]


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
        assert "run_base" in DAG.params
        assert "language" in DAG.params
        assert "max_poll_interval" in DAG.params
        assert "max_total_poll_time" in DAG.params
        assert "max_resolution" in DAG.params

    def test_task_order(self):
        """Ensure DAG tasks execute in the expected order."""
        # TODO: Update this test as additional tasks are implemented.
        validate_inputs = DAG.get_task("validate_inputs")
        discover_documents = DAG.get_task("discover_documents")

        assert [task.task_id for task in validate_inputs.downstream_list] == ["discover_documents"]
        assert [task.task_id for task in discover_documents.downstream_list] == ["process_document"]

    def test_process_document_task_exists(self):
        """Ensure process_document task exists."""
        assert DAG.get_task("process_document")

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
