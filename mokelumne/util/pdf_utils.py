"""Utilities and Airflow integrations for PDF creation."""

import hashlib
import re
import shutil
from collections.abc import Iterable
from datetime import timedelta
from http import HTTPStatus
from pathlib import Path
from typing import Any, cast

from airflow.exceptions import AirflowException
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sdk import Context, PokeReturnValue
from airflow.sdk.configuration import conf
from celery.states import ALL_STATES, READY_STATES
import pyvips  # type: ignore[import-untyped]
from requests import Response

from mokelumne.providers.alma.hooks.alma import AlmaHook
from mokelumne.util import marc, observability

DEFAULT_OCR_LANGUAGES = "eng+spa+fra+ita+deu"
IMAGE_EXTENSIONS = {".tif", ".tiff", ".jpg", ".jpeg"}
MAX_DOCUMENT_IMAGES = 99_999_999
MM_PER_INCH = 25.4

DocumentWorkItem = dict[str, str]


class DocumentStatus(dict[str, object]):
    """XCom arguments describing a document's processing status."""
    ERROR_OCR_JOB_UNSUCCESSFUL = "OCR_JOB_FAILED"
    ERROR_PDF_INVALID = "PDF_VALIDATION_FAILED"
    ERROR_PROCESSING = "PROCESSING_FAILED"
    ERROR_PUBLISHING = "PUBLISH_FAILED"
    PDF_ALREADY_EXISTS = "PDF_ALREADY_EXISTS"
    PUBLISHED = "PUBLISHED"

    def __init__(
        self,
        document: DocumentWorkItem,
        status: str,
        job_id: str | None = None,
        output_path: Path | None = None,
        date_done: str | None = None,
        error: str | None = None,
    ) -> None:
        """Build the common XCom key and serializable status value.

        :param document: Document being processed.
        :param status: Processing status code.
        :param job_id: Quiabo job identifier, if submitted.
        :param output_path: Publication path on success or an existing PDF;
            otherwise, the internal output path reported by Quiabo, if any.
        :param date_done: Processing completion timestamp.
        :param error: Processing failure details, if any.
        """
        super().__init__(
            key="document_status",
            value={
                "document": document,
                "job_id": job_id,
                "status": status,
                "output_path": str(output_path) if output_path is not None else None,
                "date_done": date_done,
                "error": error,
            },
        )


def normalize_results(results: dict | Iterable[dict] | None) -> list[dict]:
    """Normalize mapped document results to a list.

    This works around a bug (fixed in Airflow 3.4.0) whereby tasks mapped over a one-element
    input return a single scalar output rather than a list with one item. See:
    - https://airflow.apache.org/docs/apache-airflow/3.4.0/release_notes.html#airflow-3-4-0-2026-11-06
    - https://github.com/apache/airflow/pull/69075

    :param results: A singleton result, iterable of results, or no results.
    :returns: Document results as a list.
    """
    return [results] if isinstance(results, dict) else list(results or [])


def validate_source_structure(source_path: Path) -> None:
    """Validate the source directory structure for PDF creation."""

    source_entries = list(source_path.iterdir())

    root_images = [
        path
        for path in source_entries
        if path.is_file() and path.suffix.lower() in IMAGE_EXTENSIONS
    ]

    # Thou shalt not have images in the root path!
    if root_images:
        raise ValueError(f"Source directory contains TIFF/JPEG files: {source_path}")

    document_dirs = [
        path
        for path in source_entries
        if path.is_dir()
    ]

    # Thou shalt not have nested subdirectories!!
    for document_dir in document_dirs:
        nested_image_dirs = _directories_with_images(document_dir)

        if nested_image_dirs:
            raise ValueError(
                f"Document directory contains nested TIFF/JPEG directories: {document_dir}"
            )

    valid_document_dirs = _directories_with_images(source_path)

    # Thou shalt contain at least one subdirectory with TIFFs or JPEGs!!!
    if not valid_document_dirs:
        raise ValueError(f"No TIFF/JPEG document directories found: {source_path}")


def validate_pdf(pdf_path: Path, sha256_hash: str) -> tuple[bool, str | None]:
    """Validate a PDF file and return a tuple of (is_valid, error_message)."""
    try:
        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF file does not exist: {pdf_path}")

        if not pdf_path.is_file():
            raise ValueError(f"PDF path is not a file: {pdf_path}")

        actual_hash = hashlib.sha256(pdf_path.read_bytes()).hexdigest()

        if actual_hash != sha256_hash:
            raise ValueError(
                f"PDF hash mismatch for {pdf_path}: "
                f"expected {sha256_hash}, got {actual_hash}"
            )
    except (OSError, ValueError) as e:
        return False, str(e)

    return True, None


def publish_pdf(pdf_path: Path, destination_path: Path) -> Path:
    """Move a valid pdf to a destination directory and return the new path."""

    published_path = destination_path / pdf_path.name

    # Move the PDF to the destination, overwriting if it already exists.
    # Questions: do we want to overwrite existing files?
    # Also, should we use a `.part` extension during the move to avoid false positives?
    shutil.move(pdf_path, published_path)

    return published_path


def discover_documents(source_path: Path) -> list[DocumentWorkItem]:
    """Build work items for document subdirectories in the source directory."""

    # TODO: Consider natural sorting if document directory names require it.
    document_dirs = sorted(_directories_with_images(source_path))

    work_items = [
        {
            "source": str(document_dir),
            "output": f"{document_dir.name}.pdf",
        }
        for document_dir in document_dirs
    ]

    return work_items


def _directories_with_images(source_path: Path) -> list[Path]:
    """Return immediate subdirectories containing TIFF/JPEG images."""

    return [
        path
        for path in source_path.iterdir()
        if (
            path.is_dir()
            and any(
                child.is_file()
                and child.suffix.lower() in IMAGE_EXTENSIONS
                for child in path.iterdir()
            )
        )
    ]


def output_exists(destination_path: Path, output_filename: str) -> bool:
    """Return whether the expected output PDF already exists."""

    output_path = destination_path / output_filename

    if output_path.is_file():
        return True

    if not output_path.exists():
        return False

    raise FileExistsError(
        f"Output path exists and is not a regular file: {output_path}"
    )


def prepare_workspace(run_path: Path, document_name: str) -> Path:
    """Create and return a clean workspace for a document."""

    workspace_path = run_path / document_name

    if workspace_path.exists():
        shutil.rmtree(workspace_path)

    workspace_path.mkdir(parents=True)

    return workspace_path


def _source_images(source_path: Path) -> list[Path]:
    """Return TIFF/JPEG source images in page order."""

    return sorted(
        path
        for path in source_path.iterdir()
        if (
            path.is_file()
            and not path.name.startswith(".")
            and path.suffix.lower() in IMAGE_EXTENSIONS
        )
    )


def _prepare_image(source_path: Path, output_path: Path, max_resolution: int) -> None:
    """Normalize a source image to TIFF for OCR."""

    image = pyvips.Image.new_from_file(
        str(source_path),
        access="sequential",
    )

    resolution = image.xres * MM_PER_INCH
    target_xres = image.xres
    target_yres = image.yres

    if resolution > max_resolution:
        scale = max_resolution / resolution
        image = image.resize(scale)
        target_xres = max_resolution / MM_PER_INCH
        target_yres = max_resolution / MM_PER_INCH

    image.tiffsave(
        str(output_path),
        compression="lzw",
        xres=target_xres,
        yres=target_yres,
    )


def prepare_images(source_path: Path, workspace_path: Path, max_resolution: int) -> Path:
    """Prepare source images for OCR and return the Tesseract file list path."""

    source_images = _source_images(source_path)

    if len(source_images) > MAX_DOCUMENT_IMAGES:
        raise ValueError(
            f"Document contains {len(source_images):,} images "
            f"(maximum allowed: {MAX_DOCUMENT_IMAGES:,})"
        )

    prepared_images = []

    for sequence, source_image in enumerate(source_images, start=1):
        output_path = workspace_path / f"{sequence:08}.tif"

        _prepare_image(source_image, output_path, max_resolution)

        prepared_images.append(output_path)

    file_list_path = workspace_path / "filelist.txt"

    file_list_path.write_text(
        "".join(f"{path}\n" for path in prepared_images),
        encoding="utf-8",
    )

    return file_list_path


def extract_mms_id(document_name: str) -> str | None:
    """Return an Alma MMS ID from a document name when present."""

    mms_id_match = r'9\d{17}'
    match = re.match(mms_id_match, document_name)

    if match:
        return match.group(0)

    return None


def determine_language(
    requested_language: str,
    document_name: str,
) -> str:
    """Determine the Tesseract language codes for a document."""

    fallback_language = requested_language if requested_language else DEFAULT_OCR_LANGUAGES

    mms_id = extract_mms_id(document_name)

    if not mms_id:
        return fallback_language

    record_xml = AlmaHook().get_record_by_mms_id(mms_id)
    language = marc.derive_tesseract_codes_from_marc(record_xml)

    if not language:
        return fallback_language

    return language


class SubmitOcrJobOperator(HttpOperator):
    """Submit prepared document images to Quiabo and return the polling endpoint."""

    template_fields = (
        *HttpOperator.template_fields,
        "document", "destination", "language", "file_list_path",
    )

    def __init__(
        self,
        *,
        document: dict[str, str],
        destination: str,
        language: str,
        file_list_path: str,
        http_conn_id: str = "quiabo_default",
        **kwargs: Any,
    ) -> None:
        """Configure submission without performing runtime work.

        The document is passed in, but not actually used by this code,
        so that it's available for templating (e.g. via map_index_template).

        :param document: Document to process.
        :param destination: Final PDF directory.
        :param language: Tesseract language codes separated by plus signs.
        :param file_list_path: Path to the prepared Tesseract file list.
        :param http_conn_id: Airflow connection for Quiabo.
        :param kwargs: Additional Airflow operator arguments.
        """
        super().__init__(
            method="POST", endpoint="/jobs", http_conn_id=http_conn_id,
            deferrable=False, **kwargs,
        )
        self.document = document
        self.destination = destination
        self.language = language
        self.file_list_path = file_list_path

    def request_payload(self) -> dict[str, object]:
        """Build the submission body from the rendered inputs.

        :returns: Quiabo job submission arguments.
        """
        return {
            "filelist": self.file_list_path,
            "languages": self.language.split("+"),
            "output": str(Path(self.destination) / self.document["output"]),
        }

    def execute(self, context: Context) -> str:
        """Submit the JSON payload using the inherited HTTP machinery.

        :param context: Airflow task execution context.
        :returns: Endpoint for polling the submitted job.
        """
        payload = self.request_payload()
        payload["correlation_id"] = observability.task_instance_correlation_id(
            ti=context["ti"],
            instance_name=conf.get("api", "instance_name", fallback="mokelumne"),
        )
        self.request_kwargs["json"] = payload
        return cast(str, super().execute(context))

    def process_response(self, context: Context, response: Any) -> str:
        """Validate Quiabo's submission response and extract its polling endpoint.

        :param context: Airflow task execution context.
        :param response: HTTP response from Quiabo.
        :returns: Endpoint for polling the submitted job.
        """
        if response.status_code != HTTPStatus.ACCEPTED:
            raise AirflowException("Quiabo returned an invalid job submission response")
        payload = response.json()
        if not (
            isinstance(payload, dict)
            and isinstance(payload.get("job_id"), str)
            and isinstance(payload.get("job_status"), str)
            and payload.get("status") == "PENDING"
        ):
            raise AirflowException("Quiabo returned an invalid job submission response")
        self.log.info("Job submission accepted: %s", payload)
        return cast(str, payload["job_status"])


class OcrJobSensor(HttpSensor):
    """Poll a Quiabo job until it finishes and return its final payload."""

    template_fields = (
        *HttpSensor.template_fields, "document", "max_poll_interval", "max_total_poll_time",
    )

    def __init__(
        self,
        *,
        endpoint: str,
        document: dict[str, str],
        max_poll_interval: int | str = 1,
        max_total_poll_time: int | str = 3,
        http_conn_id: str = "quiabo_default",
        **kwargs: Any,
    ) -> None:
        """Configure rescheduled polling with exponential backoff.

        :param endpoint: Job status endpoint returned by submission.
        :param document: Document associated with the job, used for mapped task labels.
        :param max_poll_interval: Maximum interval between polls, in hours.
        :param max_total_poll_time: Maximum polling duration, in days.
        :param http_conn_id: Airflow connection for Quiabo.
        :param kwargs: Additional Airflow sensor arguments.
        """
        super().__init__(
            endpoint=endpoint, http_conn_id=http_conn_id,
            response_check=self.check_job_completion,
            mode="reschedule", deferrable=False,
            poke_interval=15, exponential_backoff=True, **kwargs,
        )
        self.document = document
        self.max_poll_interval = max_poll_interval
        self.max_total_poll_time = max_total_poll_time

    def pre_execute(self, context: Context) -> None:
        """Apply rendered polling limits before each execution.

        :param context: Airflow task execution context.
        """
        self.max_wait = timedelta(hours=int(self.max_poll_interval))
        self.timeout = timedelta(days=int(self.max_total_poll_time)).total_seconds()
        super().pre_execute(context)

    @staticmethod
    def check_job_completion(response: Response) -> PokeReturnValue:
        """Recognize terminal Celery states and preserve the final job payload.

        :param response: HTTP response from the job status endpoint.
        :returns: Completion state and the final payload when the job is done.
        """
        payload = response.json()
        status = payload.get("status")
        if status not in ALL_STATES:
            raise ValueError(f"Unexpected job status: {status}")
        done = status in READY_STATES
        return PokeReturnValue(is_done=done, xcom_value=payload if done else None)
