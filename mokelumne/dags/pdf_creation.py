"""DAG for creating searchable PDFs from directories of source images."""

import json
import logging
from datetime import datetime, timedelta, timezone
from enum import Enum
from http import HTTPStatus
from pathlib import Path

from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sdk import Param, PokeReturnValue, Variable, dag, get_current_context, task, task_group
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException
from celery.states import ALL_STATES, READY_STATES
from requests import Response

from mokelumne.plugins.static_files.helpers import static_files_run_dir
from mokelumne.util import params, pdf_report, pdf_utils, storage

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


def configure_ocr_polling(context: dict) -> None:
    """Set sensor polling limits from the current Dag run's Params."""
    sensor = context["task"]
    sensor.max_wait = timedelta(hours=context["params"]["max_poll_interval"])
    sensor.timeout = timedelta(days=context["params"]["max_total_poll_time"]).total_seconds()


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
    tags=["dlp", "pdf", "creation"],
    params={
        "source": params.configurable_param(
            "pdf_creation_source_",
            default="tmp/srv/pa/collection1",
            type="string",
            title="Source directory",
            description="Directory containing the document subdirectories to process.",
        ),
        "destination": params.configurable_param(
            "pdf_creation_destination_",
            default="tmp/srv/ocr/pdfs",
            type="string",
            title="Destination directory",
            description="Directory where the generated PDFs will be saved.",
        ),
        "language": params.configurable_param(
            "pdf_creation_language_",
            default=pdf_utils.DEFAULT_OCR_LANGUAGES,
            type="string",
            minLength=0,
            title="OCR Language",
            description_md=(
                "Tesseract [language codes](https://tesseract-ocr.github.io/tessdoc/Data-Files-in-different-versions.html) "
                "to use instead of automatic language selection from an "
                "associated Alma record. Language codes should be delimited "
                "by a **+**."
            ),
        ),
        "max_poll_interval": params.configurable_param(
            "pdf_creation_max_poll_interval_",
            default=1,
            type="integer",
            minimum=1,
            maximum=3,
            title="Maximum poll interval",
            description="This is the maximum time in hours between polling attempts.",
        ),
        "max_total_poll_time": params.configurable_param(
            "pdf_creation_max_total_poll_time_",
            default=3,
            type="integer",
            minimum=1,
            maximum=7,
            title="Maximum total poll time",
            description="This is the maximum total time in days for all polling attempts.",
        ),
        "max_resolution": params.configurable_param(
            "pdf_creation_max_resolution_",
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

    @task_group
    def process_document(document: pdf_utils.DocumentWorkItem, run_path: str):
        """Processes a single document directory into a PDF using Quiabo"""


        class DocumentStatus(Enum):
            """Indicates valid end states for a given document"""
            ERROR_PDF_ALREADY_EXISTS = "PDF_ALREADY_EXISTS"
            ERROR_OCR_JOB_UNSUCCESSFUL = "OCR_JOB_FAILED"
            ERROR_PDF_INVALID = "PDF_VALIDATION_FAILED"
            ERROR_PUBLISHING = "PUBLISH_FAILED"
            ERROR_PROCESSING = "PROCESSING_FAILED"
            PUBLISHED = "PUBLISHED"


        def _push_document_status(
            context,
            document: pdf_utils.DocumentWorkItem,
            status: DocumentStatus,
            job_id: str | None = None,
            output_path: Path | None = None,
            date_done: str | None = None,
            error: str | None = None,
        ) -> None:
            """
            Push a standardized representation of processing status via xcom

            This helper provides a consistent way to indicate the processing
            status of a document. Sharing status via xcom allows the use of
            standard Airflow skip/error exceptions (changing pipeline visualization)
            without losing the specifics of the failure.

            `output_path` points to something different depending on status.
            On success or when the PDF already exists, it's the publication path;
            otherwise, it's the internal output_path reported by Quiabo, or None
            if nothing has been written.
            """

            context["ti"].xcom_push(
                key="document_status",
                value={
                    "document": document,
                    "job_id": job_id,
                    "status": status.value,
                    "output_path": str(output_path) if output_path is not None else None,
                    "date_done": date_done,
                    "error": error,
                },
            )

        @task
        def prepare_ocr_request(document: pdf_utils.DocumentWorkItem, run_path: str) -> str:
            """Prepare a document directory for OCR, returning Quiabo job submission args encoded as JSON"""

            context = get_current_context()
            document_name = Path(document["source"]).name
            destination_path = Path(context["params"]["destination"])
            language = context["params"]["language"]
            max_resolution = context["params"]["max_resolution"]

            if pdf_utils.output_exists(destination_path, document["output"]):
                existing_filepath = destination_path / document["output"]
                _push_document_status(
                    context=context,
                    document=document,
                    status=DocumentStatus.ERROR_PDF_ALREADY_EXISTS,
                    output_path=existing_filepath,
                )
                raise AirflowSkipException(
                    f"Skipping document because output PDF already exists: {existing_filepath}"
                )

            workspace_path = pdf_utils.prepare_workspace(
                Path(run_path),
                document_name,
            )
            logger.info("Prepared document workspace: %s", workspace_path)

            language = pdf_utils.determine_language(language, document_name)
            logger.info("Using OCR language(s): %s", language)

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

        def submit_ocr_job(data: str):
            """Submit job JSON to Quiabo for processing"""
            return HttpOperator(
                task_id="submit_ocr_job",
                http_conn_id="quiabo_default",
                endpoint="/jobs",
                method="POST",
                headers={"Content-Type": "application/json"},
                data=data,
                response_check=valid_submission,
                response_filter=status_endpoint,
                deferrable=False,
            )

        def await_job_completion(endpoint: str):
            """Wait for Quiabo to report that a job is done processing"""
            return HttpSensor(
                task_id="await_job_completion",
                http_conn_id="quiabo_default",
                endpoint=endpoint,
                response_check=check_job_status,
                mode="reschedule",
                deferrable=False,
                poke_interval=15,
                exponential_backoff=True,
                pre_execute=configure_ocr_polling,
            )


        @task
        def validate_and_publish(document: pdf_utils.DocumentWorkItem, job: dict) -> dict:
            """Validate one generated PDF and publish it to the destination."""

            context = get_current_context()
            job_id = job.get("id", "UNKNOWN")
            status = job.get("status")
            if status != "SUCCESS":
                errmsg = f"OCR job {job_id} completed with status {status}; not publishing"
                _push_document_status(
                    context=context,
                    document=document,
                    status=DocumentStatus.ERROR_OCR_JOB_UNSUCCESSFUL,
                    job_id=job_id,
                    error=errmsg,
                )
                raise AirflowFailException(errmsg)

            destination_path = Path(context["params"]["destination"])
            result = job["result"]
            output_path = Path(result["output_path"])
            sha256 = result["sha256"]
            date_done = job.get("date_done") or datetime.now(timezone.utc).isoformat()

            valid, errmsg = pdf_utils.validate_pdf(output_path, sha256)
            if not valid:
                _push_document_status(
                    context=context,
                    document=document,
                    status=DocumentStatus.ERROR_PDF_INVALID,
                    job_id=job_id,
                    date_done=date_done,
                    output_path=output_path,
                    error=errmsg,
                )
                raise AirflowFailException(errmsg)

            try:
                published_path = pdf_utils.publish_pdf(
                    output_path,
                    destination_path,
                )
            except OSError as e:
                _push_document_status(
                    context=context,
                    document=document,
                    status=DocumentStatus.ERROR_PUBLISHING,
                    job_id=job_id,
                    date_done=date_done,
                    output_path=output_path,
                    error=str(e),
                )
                raise AirflowFailException(e)

            logger.info("Published PDF: %s", published_path)
            _push_document_status(
                context=context,
                document=document,
                status=DocumentStatus.PUBLISHED,
                job_id=job_id,
                date_done=date_done,
                output_path=str(published_path),
            )
            return document

        @task(trigger_rule="all_done")
        def return_document_status(document: pdf_utils.DocumentWorkItem) -> dict:
            """Return a standardized DocumentStatus summary for the doc

            Handles skips, exceptions, and success-cases."""
            context = get_current_context()
            ti = context["ti"]

            for task_id in (
                "process_document.validate_and_publish",
                "process_document.prepare_ocr_request",
            ):
                status = ti.xcom_pull(
                    task_ids=task_id,
                    key="document_status",
                    map_indexes=ti.map_index,
                )
                if status is not None:
                    return status

            return {
                "document": document,
                "job_id": None,
                "status": DocumentStatus.ERROR_PROCESSING.value,
                "output_path": None,
                "date_done": datetime.now(timezone.utc).isoformat(),
                "error": "Processing ended without a recorded status; see task logs.",
            }

        ocr_request = prepare_ocr_request(document, run_path)
        submitted_job = submit_ocr_job(data=ocr_request)
        completed_job = await_job_completion(endpoint=submitted_job.output)
        publication = validate_and_publish(
            document=document,
            job=completed_job.output,
        )
        document_status = return_document_status(document)

        [ocr_request, submitted_job, completed_job, publication] >> document_status

        return document_status

    @task(trigger_rule="all_done")
    def normalize_results(results) -> list[dict]:
        """Collect document statuses into a consistent list.

        This works around a bug in Airflow <3.4.0 whereby tasks mapped over a one-element
        input return a single scalar output rather than a list with one item.

        This is fixed in 3.4.0, see:
        - https://airflow.apache.org/docs/apache-airflow/3.4.0/release_notes.html#airflow-3-4-0-2026-11-06).
        - https://github.com/apache/airflow/pull/69075
        """
        if isinstance(results, dict):
            return [results]
        return list(results or [])

    def generate_summary(results) -> str:
        """Write index.html summarizing published and failed jobs; return its directory."""
        context = get_current_context()
        params = context["params"]

        html = pdf_report.render_results_html(
            source=params["source"],
            destination=params["destination"],
            language=params["language"],
            results=results or [],
        )

        output_dir = static_files_run_dir(context["dag"].dag_id, context["run_id"])
        (output_dir / "index.html").write_text(html, encoding="utf-8")
        return str(output_dir)

    def check_for_errors(results) -> None:
        """Mark the DAG successful only if all jobs succeeded"""

        if isinstance(results, dict):
            results = [results]

        if not results:
            raise AirflowFailException("No documents were published successfully.")

        unpublished = [
            r for r in results
                if r["status"] != "PUBLISHED"
        ]
        if unpublished:
            raise AirflowFailException(
                json.dumps(
                    {
                        "message": "Some documents failed to publish",
                        "unpublished": unpublished,
                    }, indent=2,
                )
            )

        return None


    validation = validate_inputs()
    run_path = prepare_run_directory()
    documents = discover_documents()
    validation >> run_path >> documents

    document_results = normalize_results(
        process_document.partial(
            run_path=run_path
        ).expand(document=documents)
    )

    generate_summary(document_results) >> check_for_errors(document_results)


pdf_creation()
