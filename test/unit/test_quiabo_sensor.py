"""Test Quiabo job completion and runtime polling configuration."""

from datetime import timedelta
from unittest.mock import Mock

import pytest
from airflow.sdk import DAG

from mokelumne.util.pdf_utils import OcrJobSensor


check_job_status = OcrJobSensor.check_job_completion


class TestOcrJobSensor:
    """Recognize running and completed OCR jobs."""

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



def test_polling_limits_are_templated() -> None:
    """Apply per-run polling limits after rendering the sensor's inputs."""
    sensor = OcrJobSensor(
        task_id="poll", endpoint="jobs/job-123",
        document={"source": "/src/document", "output": "document.pdf"},
        max_poll_interval="{{ params.max_poll_interval }}",
        max_total_poll_time="{{ params.max_total_poll_time }}",
        dag=DAG("test_polling", schedule=None, render_template_as_native_obj=True),
    )
    context = {"params": {"max_poll_interval": 2, "max_total_poll_time": 5}}

    sensor.render_template_fields(context)
    sensor.pre_execute(context)

    assert sensor.max_wait == timedelta(hours=2)
    assert sensor.timeout == 432000
    assert sensor.mode == "reschedule"
    assert sensor.exponential_backoff is True


def test_sensor_polls_until_job_finishes(monkeypatch) -> None:
    """Poll through a running job to its final payload using the HTTP transport."""
    completed = {"id": "job-123", "status": "SUCCESS", "result": {"output_path": "/out/a.pdf"}}
    pending_response = Mock(status_code=200)
    pending_response.json.return_value = {"id": "job-123", "status": "STARTED"}
    completed_response = Mock(status_code=200)
    completed_response.json.return_value = completed
    responses = iter([pending_response, completed_response])
    sent_requests = []

    def send_request(_session, request, **_kwargs):
        sent_requests.append(request)
        return next(responses)

    monkeypatch.setenv("AIRFLOW_CONN_QUIABO_DEFAULT", "http://quiabo.test")
    monkeypatch.setattr("requests.Session.send", send_request)
    sensor = OcrJobSensor(
        task_id="poll", endpoint="jobs/job-123",
        document={"source": "/src/document", "output": "document.pdf"},
    )

    assert not sensor.poke({})
    result = sensor.poke({})

    assert result.is_done is True
    assert result.xcom_value == completed
    assert [request.url for request in sent_requests] == [
        "http://quiabo.test/jobs/job-123", "http://quiabo.test/jobs/job-123",
    ]
