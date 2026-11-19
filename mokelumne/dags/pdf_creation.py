"""DAG for creating searchable PDFs from directories of source images."""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import TaskInstance, Variable, dag, task, task_group
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException

from mokelumne.plugins.static_files.helpers import static_files_run_dir
from mokelumne.util import params, pdf_report, pdf_utils, storage
from mokelumne.util.pdf_creation_cleanup_utils import remove_run_directory, validate_cleanup_directory

logger = logging.getLogger(__name__)


@dag(
    description="Creates searchable PDFs from directories of source images",
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
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
        "cleanup_after": params.configurable_param(
            "pdf_creation_cleanup_after_",
            default=True,
            type="boolean",
            title="Cleanup scratch directory after run?",
            description="If true, the scratch directory will be deleted after a successful run.",
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
    def validate_inputs(source: str, destination: str) -> None:
        """Validate source, destination, and source directory structure."""
        source_path = Path(source)
        destination_path = Path(destination)
        scratch_base = Path(Variable.get("pdf_creation_scratch_base"))

        storage.validate_path(path=source_path, kind="source")
        storage.validate_path(path=destination_path, kind="destination")
        pdf_utils.validate_source_structure(source_path)
        storage.validate_path(path=scratch_base, kind="run base")

    @task
    def prepare_run_directory(run_id: str) -> str:
        """Create and return the scratch directory for this Dag run."""
        scratch_base = Variable.get("pdf_creation_scratch_base")

        run_path = storage.run_dir(run_id, base_dir=scratch_base)

        return str(run_path)

    @task
    def discover_documents(source: str) -> list[pdf_utils.DocumentWorkItem]:
        """Discover document directories and build work items."""
        source_path = Path(source)

        return pdf_utils.discover_documents(source_path)

    @task_group
    def process_document(document: pdf_utils.DocumentWorkItem, run_path: str):
        """Processes a single document directory into a PDF using Quiabo"""

        # Labels mapped tasks by their document's containing directory name.
        TASKFLOW_DOCUMENT_INDEX = (
            "{{ task.op_kwargs['document']['source'].rstrip('/').rsplit('/', 1)[-1] }}"
        )
        HTTP_DOCUMENT_INDEX = (
            "{{ task.document['source'].rstrip('/').rsplit('/', 1)[-1] }}"
        )

        @task(map_index_template=TASKFLOW_DOCUMENT_INDEX)
        def check_if_processed(
            document: pdf_utils.DocumentWorkItem,
            destination: str,
            ti: TaskInstance,
        ) -> None:
            """Skip documents whose destination PDF already exists."""
            destination_path = Path(destination)
            if pdf_utils.output_exists(destination_path, document["output"]):
                existing_filepath = destination_path / document["output"]
                ti.xcom_push(
                    **pdf_utils.DocumentStatus(
                        document=document,
                        status=pdf_utils.DocumentStatus.PDF_ALREADY_EXISTS,
                        output_path=existing_filepath,
                    )
                )
                raise AirflowSkipException(
                    f"Skipping document because output PDF already exists: {existing_filepath}"
                )

        @task(map_index_template=TASKFLOW_DOCUMENT_INDEX)
        def prepare_workspace(document: pdf_utils.DocumentWorkItem, run_path: str) -> str:
            """Create the document workspace and return its path."""
            workspace_path = pdf_utils.prepare_workspace(
                Path(run_path), Path(document["source"]).name,
            )
            logger.info("Prepared document workspace: %s", workspace_path)
            return str(workspace_path)

        @task(map_index_template=TASKFLOW_DOCUMENT_INDEX)
        def determine_language(document: pdf_utils.DocumentWorkItem, language: str) -> str:
            """Determine the document's OCR languages."""
            language = pdf_utils.determine_language(language, Path(document["source"]).name)
            logger.info("Using OCR language(s): %s", language)
            return language

        @task(map_index_template=TASKFLOW_DOCUMENT_INDEX)
        def prepare_images(
            document: pdf_utils.DocumentWorkItem,
            workspace_path: str,
            max_resolution: int,
        ) -> str:
            """Prepare derivative images and return the Tesseract file list path."""
            file_list_path = pdf_utils.prepare_images(
                Path(document["source"]), Path(workspace_path), max_resolution,
            )
            logger.info("Prepared Tesseract file list: %s", file_list_path)
            return str(file_list_path)

        @task(map_index_template=TASKFLOW_DOCUMENT_INDEX)
        def check_job_status(
            document: pdf_utils.DocumentWorkItem,
            job: dict,
            ti: TaskInstance,
        ) -> dict:
            """Fail unsuccessful OCR jobs and return successful jobs with a completion time."""

            if "SUCCESS" != (status := job.get("status", "UNKNOWN")):
                job_id = job.get("id", "UNKNOWN")
                result = job.get('result', 'UNKNOWN')
                errmsg = f"Job '{job_id}' {status}: {result}"
                ti.xcom_push(
                    **pdf_utils.DocumentStatus(
                        document=document,
                        status=pdf_utils.DocumentStatus.ERROR_OCR_JOB_UNSUCCESSFUL,
                        job_id=job_id,
                        error=errmsg,
                    )
                )
                raise AirflowFailException(errmsg)

            return job | {"date_done": job.get("date_done") or datetime.now(timezone.utc).isoformat()}

        @task(map_index_template=TASKFLOW_DOCUMENT_INDEX)
        def validate_pdf(
            document: pdf_utils.DocumentWorkItem,
            job: dict,
            ti: TaskInstance,
        ) -> dict:
            """Validate the generated PDF before allowing publication."""
            output_path = Path(job["result"]["output_path"])
            valid, errmsg = pdf_utils.validate_pdf(output_path, job["result"]["sha256"])
            if not valid:
                ti.xcom_push(
                    **pdf_utils.DocumentStatus(
                        document=document,
                        status=pdf_utils.DocumentStatus.ERROR_PDF_INVALID,
                        job_id=job.get("id", "UNKNOWN"),
                        date_done=job["date_done"],
                        output_path=output_path,
                        error=errmsg,
                    )
                )
                raise AirflowFailException(errmsg)

            return job

        @task(map_index_template=TASKFLOW_DOCUMENT_INDEX)
        def publish_pdf(
            document: pdf_utils.DocumentWorkItem,
            job: dict,
            destination: str,
            ti: TaskInstance,
        ) -> dict:
            """Move a validated PDF to its destination and record publication status."""
            output_path = Path(job["result"]["output_path"])
            try:
                published_path = pdf_utils.publish_pdf(
                    output_path,
                    Path(destination),
                )
            except OSError as e:
                ti.xcom_push(
                    **pdf_utils.DocumentStatus(
                        document=document,
                        status=pdf_utils.DocumentStatus.ERROR_PUBLISHING,
                        job_id=job.get("id", "UNKNOWN"),
                        date_done=job["date_done"],
                        output_path=output_path,
                        error=str(e),
                    )
                )
                raise AirflowFailException(e)

            logger.info("Published PDF: %s", published_path)
            ti.xcom_push(
                **pdf_utils.DocumentStatus(
                    document=document,
                    status=pdf_utils.DocumentStatus.PUBLISHED,
                    job_id=job.get("id", "UNKNOWN"),
                    date_done=job["date_done"],
                    output_path=published_path,
                )
            )
            return document

        @task(map_index_template=TASKFLOW_DOCUMENT_INDEX, trigger_rule="all_done")
        def return_document_status(
            document: pdf_utils.DocumentWorkItem,
            ti: TaskInstance
        ) -> dict:
            """Return a standardized pdf_utils.DocumentStatus summary for the doc

            Handles skips, exceptions, and success-cases."""

            for task_id in (
                "process_document.publish_pdf",
                "process_document.validate_pdf",
                "process_document.check_job_status",
                "process_document.check_if_processed",
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
                "status": pdf_utils.DocumentStatus.ERROR_PROCESSING,
                "output_path": None,
                "date_done": datetime.now(timezone.utc).isoformat(),
                "error": "Processing ended without a recorded status; see task logs.",
            }

        processed_check = check_if_processed(
            document=document,
            destination="{{ params.destination }}",
        )
        workspace = prepare_workspace(document=document, run_path=run_path)
        language = determine_language(document=document, language="{{ params.language }}")
        file_list = prepare_images(
            document=document,
            workspace_path=workspace,
            max_resolution="{{ params.max_resolution }}",
        )
        processed_check >> [workspace, language]
        submitted_job = pdf_utils.SubmitOcrJobOperator(
            task_id="submit_ocr_job",
            map_index_template=HTTP_DOCUMENT_INDEX,
            document=document,
            destination="{{ params.destination }}",
            language=language,
            file_list_path=file_list,
        )
        completed_job = pdf_utils.OcrJobSensor(
            task_id="await_job_completion",
            map_index_template=HTTP_DOCUMENT_INDEX,
            document=document,
            endpoint=submitted_job.output,
            max_poll_interval="{{ params.max_poll_interval }}",
            max_total_poll_time="{{ params.max_total_poll_time }}",
        )
        successful_job = check_job_status(document=document, job=completed_job.output)
        validated_job = validate_pdf(document=document, job=successful_job)
        published_doc = publish_pdf(
            document=document,
            job=validated_job,
            destination="{{ params.destination }}",
        )
        document_status = return_document_status(document=document)

        [language, file_list, published_doc] >> document_status

        return document_status

    @task
    def generate_summary(
        results: list[dict],
        source: str,
        destination: str,
        language: str,
        dag_id: str,
        run_id: str,
    ) -> str:
        """Write index.html summarizing published and failed jobs; return its directory."""

        results = pdf_utils.normalize_results(results)

        html = pdf_report.render_results_html(
            source=source,
            destination=destination,
            language=language,
            results=results or [],
        )

        output_dir = static_files_run_dir(dag_id, run_id)
        (output_dir / "index.html").write_text(html, encoding="utf-8")
        return str(output_dir)

    @task
    def check_for_errors(results) -> None:
        """Mark success/failure based on job results

        A run is successful iff at least one document was discovered and none failed.
        Skipped documents are considered acceptable.
        """

        results = pdf_utils.normalize_results(results)

        if not results:
            raise AirflowFailException("No documents were discovered.")

        errors = [
            r for r in results if r["status"] not in {
                pdf_utils.DocumentStatus.PDF_ALREADY_EXISTS,
                pdf_utils.DocumentStatus.PUBLISHED
            }
        ]
        if errors:
            raise AirflowFailException(
                json.dumps(
                    {
                        "message": "Some documents failed to publish",
                        "unpublished": errors,
                    }, indent=2,
                )
            )

    @task
    def cleanup_run(scratch_path: str, run_id: str, cleanup_after: bool) -> None:
        """Delete the scratch directory for this Dag run when enabled."""
        if not cleanup_after:
            return

        if validate_cleanup_directory(scratch_path, Variable.get("pdf_creation_scratch_base"), run_id):
            remove_run_directory(scratch_path)

    validation = validate_inputs(
        source="{{ params.source }}",
        destination="{{ params.destination }}",
    )
    run_path = prepare_run_directory()
    documents = discover_documents(source="{{ params.source }}")
    validation >> run_path >> documents

    document_results = process_document.partial(
        run_path=run_path
    ).expand(document=documents)

    (
        generate_summary(
            results=document_results,
            source="{{ params.source }}",
            destination="{{ params.destination }}",
            language="{{ params.language }}",
            dag_id="{{ dag.dag_id }}",
        )
        >> check_for_errors(document_results)
        >> cleanup_run(
            run_path, run_id="{{ run_id }}", cleanup_after="{{ params.cleanup_after }}"
        )
    )


pdf_creation()
