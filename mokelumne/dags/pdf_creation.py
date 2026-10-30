"""DAG for creating searchable PDFs from directories of source images."""

import json
import logging
from http import HTTPStatus
from pathlib import Path

from celery.states import ALL_STATES, READY_STATES
from requests import Response
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sdk import Param, PokeReturnValue, Variable, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException

from mokelumne.util import pdf_utils, storage

logger = logging.getLogger(__name__)


def valid_submission(response: Response, **_context: object) -> bool:
    """Confirm Quiabo accepted the job submission."""
    if response.status_code != HTTPStatus.ACCEPTED:
        return False

    payload = response.json()
    accepted = (
        isinstance(payload, dict)
        and isinstance(payload.get("job_id"), str)
        and isinstance(payload.get("job_status"), str)
        and payload.get("status") == "PENDING"
    )
    if accepted:
        logger.info("Job submission accepted: %s", payload)
    return accepted


def status_endpoint(response: Response) -> str:
    """Return polling endpoint from the job submission response."""

    return response.json()["job_status"]


def check_job_status(response: Response) -> PokeReturnValue:
    """Check the status of a submitted OCR job."""
    payload = response.json()
    status = payload.get("status")

    if status not in ALL_STATES:
        raise ValueError(f"Unexpected job status: {status}")

    # SUCCESS, FAILURE, and REVOKED are all considered "done" states.
    done = status in READY_STATES
    return PokeReturnValue(
        is_done=done,
        xcom_value=payload if done else None,
    )


@dag(
    description="Creates searchable PDFs from directories of source images",
    schedule=None,
    catchup=False,
    params={
        "source": Param(
            default="tmp/srv/pa/collection1",
            type="string",
            title="Source directory",
            description="Directory containing the document subdirectories to process.",
        ),
        "destination": Param(
            default="tmp/srv/ocr/pdfs",
            type="string",
            title="Destination directory",
            description="Directory where the generated PDFs will be saved.",
        ),
        "language": Param(
            default="eng",
            type="string",
            minLength=0,
            title="OCR Language",
            description=(
                "Optional Tesseract language code to use instead of "
                "automatic language selection."
            ),
        ),
        "max_poll_interval": Param(
            default=1,
            type="integer",
            minimum=1,
            maximum=3,
            title="Maximum poll interval",
            description="This is the maximum time in hours between polling attempts.",
        ),
        "max_total_poll_time": Param(
            default=3,
            type="integer",
            minimum=1,
            maximum=7,
            title="Maximum total poll time",
            description="This is the maximum total time in days for all polling attempts.",
        ),
        "max_resolution": Param(
            default=200,
            type="integer",
            minimum=150,
            maximum=600,
            title="Maximum Resolution",
            description=(
                "Maximum image resolution in DPI. Images above this "
                "resolution will be downsampled."
            ),
        ),
    },
)
def pdf_creation(): # pylint: disable=too-many-locals
    """Create searchable PDFs from document directories."""

    @task
    def validate_inputs():
        """Validate source, destination, and source directory structure."""
        context = get_current_context()

        source_path = Path(context["params"]["source"])
        destination_path = Path(context["params"]["destination"])
        scratch_base = Path(Variable.get("pdf_creation_scratch_base"))

        pdf_utils.validate_source_path(source_path)
        pdf_utils.validate_destination_path(destination_path)
        pdf_utils.validate_source_structure(source_path)
        pdf_utils.validate_run_base(scratch_base)

    @task
    def prepare_run_directory() -> str:
        """Create and return the scratch directory for this Dag run."""
        context = get_current_context()
        scratch_base = Variable.get("pdf_creation_scratch_base")
        run_id = context["run_id"]

        run_path = storage.run_dir(run_id, base_dir=scratch_base)

        return str(run_path)

    @task
    def discover_documents():
        """Discover document directories and build work items."""
        context = get_current_context()
        source_path = Path(context["params"]["source"])

        return pdf_utils.discover_documents(source_path)

    @task
    def process_document(document: pdf_utils.DocumentWorkItem, run_path: str):
        """Process a document directory into a searchable PDF."""

        context = get_current_context()
        document_name = Path(document["source"]).name
        destination_path = Path(context["params"]["destination"])
        language = context["params"]["language"]
        max_resolution = context["params"]["max_resolution"]

        # 1 - Check if output PDF already exists (skip if it does)
        if pdf_utils.output_exists(destination_path, document["output"]):
            raise AirflowSkipException(
                f"Output PDF already exists: {destination_path / document['output']}"
            )

        # 2 - Prepare workspace!
        workspace_path = pdf_utils.prepare_workspace(
            Path(run_path),
            document_name,
        )

        # Log the workspace path for now; later stages will use it directly.
        logger.info("Prepared document workspace: %s", workspace_path)

        # 3 - Determine language
        language = pdf_utils.determine_language(
            language,
            document_name,
        )
        logger.info("Using OCR language(s): %s", language)

        # 4 - Prepare images (size/convert as necessary)
        file_list_path = pdf_utils.prepare_images(
            Path(document["source"]),
            workspace_path,
            max_resolution,
        )
        logger.info("Prepared Tesseract file list: %s", file_list_path)

        return json.dumps(
            {
                "filelist": str(file_list_path),
                "languages": language.split("+"),
                "output": str(destination_path / document["output"]),
            }
        )

    @task
    def set_sensor_kwargs(endpoints: list[str]) -> list[dict]:
        """Set kwargs for HttpSensor. This allows us to set runtime polling params."""
        context = get_current_context()
        max_wait = 60 * 60 * context["params"]["max_poll_interval"]
        timeout = 24 * 60 * 60 * context["params"]["max_total_poll_time"]

        return [
            {
                "endpoint": endpoint,
                "max_wait": max_wait,
                "timeout": timeout
            }
            for endpoint in endpoints
        ]

    validation = validate_inputs()
    run_path = prepare_run_directory()
    documents = discover_documents()
    processed_documents = process_document.partial(
        run_path=run_path
    ).expand(document=documents)

    # 5 - Submit OCR job
    # submissions will be a list of job_status endpoints to poll for completion.
    submissions = HttpOperator.partial(
        task_id="submit_ocr_job",
        http_conn_id="quiabo_default",
        endpoint="/jobs",
        method="POST",
        headers={"Content-Type": "application/json"},
        response_check=valid_submission,
        response_filter=status_endpoint,
        deferrable=False,
    ).expand(data=processed_documents)

    sensor_kwargs = set_sensor_kwargs(submissions.output)

    # 6 - Wait for OCR.....
    # Will poll for up to a week for success or failure.
    # xcom for each response will be a dict with the job_id, status, and result (if done).
    wait_for_pdf = HttpSensor.partial(
        task_id="wait_for_pdf",
        http_conn_id="quiabo_default",
        response_check=check_job_status,
        mode="reschedule",
        deferrable=False,
        poke_interval=15,
        exponential_backoff=True,
    ).expand_kwargs(sensor_kwargs)

    # 7 - Select successful OCR jobs for publication
    @task
    def select_successful_job(finished_job: dict) -> dict | None:
        """Return successful OCR jobs and filter non-successful jobs."""
        status = finished_job.get("status")

        if status != "SUCCESS":
            logger.warning(
                "OCR job %s completed with status %s; not publishing",
                finished_job.get("id", "UNKNOWN"),
                status,
            )
            return None

        return finished_job

    # 8 - Validate and publish each successful OCR job independently
    @task
    def validate_and_publish(finished_job: dict) -> dict:
        """Validate one generated PDF and publish it to the destination."""
        context = get_current_context()
        destination_path = Path(context["params"]["destination"])

        result = finished_job["result"]
        output_path = Path(result["output_path"])
        sha256 = result["sha256"]

        # Validate the PDF
        valid, error_msg = pdf_utils.validate_pdf(output_path, sha256)

        if not valid:
            logger.error("Failed to validate PDF: %s", error_msg)
            return {
                "job_id": finished_job.get("id", "UNKNOWN"),
                "status": "VALIDATION_FAILED",
                "output_path": str(output_path),
                "date_done": finished_job.get("date_done", "UNKNOWN"),
                "error": error_msg,
            }

        # Publish the PDF to the destination
        try:
            published_path = pdf_utils.publish_pdf(
                output_path,
                destination_path,
            )
        except OSError as e:
            logger.error("Failed to publish PDF: %s", e)
            return {
                "job_id": finished_job.get("id", "UNKNOWN"),
                "status": "PUBLISH_FAILED",
                "output_path": str(output_path),
                "date_done": finished_job.get("date_done", "UNKNOWN"),
                "error": str(e),
            }

        logger.info("Published PDF: %s", published_path)

        return {
            "job_id": finished_job.get("id", "UNKNOWN"),
            "status": "PUBLISHED",
            "output_path": str(published_path),
            "date_done": finished_job.get("date_done", "UNKNOWN"),
            "error": None,
        }

    successful_jobs = select_successful_job.expand(finished_job=wait_for_pdf.output)

    publication_results = validate_and_publish.expand(finished_job=successful_jobs)

    # 9 - Summarize results using finished jobs and published pdfs
    # should use trigger_rule="all_done"

    # 10 - Final status task to succeed or fail the DAG based on whether any jobs were published
    # successfully.  If no jobs were published, the DAG will fail.

    (
        validation
        >> run_path
        >> documents
        >> processed_documents
        >> submissions
        >> wait_for_pdf
        >> successful_jobs
        >> publication_results
    )

pdf_creation()
