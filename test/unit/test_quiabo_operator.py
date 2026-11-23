"""Test Quiabo operator payload construction and response validation."""

from unittest.mock import Mock

import pytest
from airflow.exceptions import AirflowException

from mokelumne.util.pdf_utils import SubmitOcrJobOperator


@pytest.fixture
def operator() -> SubmitOcrJobOperator:
    """Configure a submission with ordinary document inputs."""
    return SubmitOcrJobOperator(
        task_id="submit", document={"source": "/src/a", "output": "a.pdf"},
        language="eng+fra", file_list_path="/run/filelist.txt",
    )


def test_request_payload(operator: SubmitOcrJobOperator) -> None:
    """Submit the file list and languages without an explicit output path."""
    assert operator.request_payload() == {
        "filelist": "/run/filelist.txt", "languages": ["eng", "fra"],
    }


def test_accepted_response_returns_polling_endpoint(operator: SubmitOcrJobOperator) -> None:
    """Extract the polling endpoint from an accepted pending job."""
    response = Mock(status_code=202)
    response.json.return_value = {
        "job_id": "job-123", "job_status": "jobs/job-123", "status": "PENDING",
    }

    assert operator.process_response({}, response) == "jobs/job-123"


@pytest.mark.parametrize("status_code,payload", [
    (200, {"job_id": "job-123", "job_status": "jobs/job-123", "status": "PENDING"}),
    (202, {"job_status": "jobs/job-123", "status": "PENDING"}),
    (202, {"job_id": "job-123", "status": "PENDING"}),
    (202, {"job_id": "job-123", "job_status": "jobs/job-123", "status": "STARTED"}),
])
def test_invalid_response_is_rejected(operator, status_code, payload) -> None:
    """Reject HTTP or payload values outside Quiabo's submission contract."""
    response = Mock(status_code=status_code)
    response.json.return_value = payload

    with pytest.raises(AirflowException, match="invalid job submission response"):
        operator.process_response({}, response)

