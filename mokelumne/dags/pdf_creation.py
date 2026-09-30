"""DAG for creating searchable PDFs from directories of source images."""

import json
import logging
from pathlib import Path
from requests import Response

from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sdk import Param, PokeReturnValue, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException

from mokelumne.util import pdf_utils, storage

logger = logging.getLogger(__name__)


def check_job_status(response: Response) -> PokeReturnValue:
    """Check the status of a submitted OCR job."""
    payload = response.json()
    status = payload.get("status")

    if status not in {"PENDING", "STARTED", "SUCCESS", "FAILURE"}:
        raise ValueError(f"Unexpected job status: {status}")

    done = status in {"SUCCESS", "FAILURE"}
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
            type="string",
            title="Source directory",
            description="Directory containing the document subdirectories to process.",
        ),
        "destination": Param(
            type="string",
            title="Destination directory",
            description="Directory where the generated PDFs will be saved.",
        ),
        "language": Param(
            default="",
            type="string",
            title="OCR Language",
            description=(
                "Optional Tesseract language code to use instead of "
                "automatic language selection."
            ),
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
def pdf_creation():
    """Create searchable PDFs from document directories."""

    @task
    def validate_inputs():
        """Validate source, destination, and source directory structure."""
        context = get_current_context()

        source_path = Path(context["params"]["source"])
        destination_path = Path(context["params"]["destination"])

        pdf_utils.validate_source_path(source_path)
        pdf_utils.validate_destination_path(destination_path)
        pdf_utils.validate_source_structure(source_path)

    @task
    def discover_documents():
        """Discover document directories and build work items."""
        context = get_current_context()
        source_path = Path(context["params"]["source"])

        return pdf_utils.discover_documents(source_path)

    @task
    def process_document(document: pdf_utils.DocumentWorkItem):
        """Process a document directory into a searchable PDF."""

        context = get_current_context()
        document_name = Path(document["source"]).name
        destination_path = Path(context["params"]["destination"])
        run_id = context["run_id"]
        language = context["params"]["language"]
        max_resolution = context["params"]["max_resolution"]

        # 1 - Check if output PDF already exists (skip if it does)
        if pdf_utils.output_exists(destination_path, document["output"]):
            raise AirflowSkipException(
                f"Output PDF already exists: {destination_path / document['output']}"
            )

        # 2 - Prepare workspace!
        run_path = storage.run_dir(run_id)
        workspace_path = pdf_utils.prepare_workspace(
            run_path,
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

    validation = validate_inputs()
    documents = discover_documents()
    processed_documents = process_document.expand(document=documents)

    # 5 - Submit OCR job
    # submissions will be a list of job_status endpoints to poll for completion.
    submissions = HttpOperator.partial(
        task_id="submit_ocr_job",
        http_conn_id="quiabo_default",
        endpoint="/jobs",
        method="POST",
        headers={"Content-Type": "application/json"},
        # response_check=valid_submission,
        response_check=lambda response: response.json()["status"] == "PENDING",
        response_filter=lambda response: response.json()["job_status"],
        deferrable=False,
    ).expand(data=processed_documents)

    # 6 - Wait for OCR.....
    # Will poll for up to a week for success or failure.
    # xcom for each submission will be a dict with keys "output_path" and "sha256" if successful, or "status" and "result" if failed?
    wait_for_pdf = HttpSensor.partial(
        task_id="wait_for_pdf",
        http_conn_id="quiabo_default",
        response_check=check_job_status,
        mode="reschedule",
        deferrable=False,
        poke_interval=30,
        exponential_backoff=True,
        max_wait=60 * 60, 
        timeout=7 * 24 * 60 * 60,
    ).expand(endpoint=submissions)

    # 7 - Validate and publish
    #   TODO: Fail and log any jobs that returned a "FAILURE" status 

    # 8 - Cleanup


    validation >> documents >> processed_documents >> submissions >> wait_for_pdf


pdf_creation()
