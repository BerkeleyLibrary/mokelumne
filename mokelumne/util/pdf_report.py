"""Build the static HTML summary for the ``pdf_creation`` Dag."""

from collections.abc import Iterable
from typing import NotRequired, TypedDict

from mokelumne.templates import render

PUBLISHED = "PUBLISHED"
OCR_SUCCESS = "SUCCESS"

class FinishedOcrJob(TypedDict):
    """Final status payload from the Quiabo job endpoint."""

    status: str
    id: NotRequired[str]
    result: NotRequired[object]
    date_done: NotRequired[str | None]


class JobResult(TypedDict):
    """One normalized row in the PDF creation report."""

    job_id: str
    status: str
    output_path: str | None
    date_done: str | None
    error: str | None


def failed_ocr_results(finished_jobs: Iterable[FinishedOcrJob | None]) -> list[JobResult]:
    """Convert finished OCR jobs that did not succeed into failure results.

    :param finished_jobs: Final status payloads returned by the Quiabo job endpoint.
    :type finished_jobs: Iterable[FinishedOcrJob | None]
    :returns: One result per non-successful job, in the publication result shape.
    :rtype: list[JobResult]
    """
    failures: list[JobResult] = []
    for job in finished_jobs:
        if not job or job["status"] == OCR_SUCCESS:
            continue
        result = job.get("result")
        failures.append(
            {
                "job_id": job.get("id", "UNKNOWN"),
                "status": job["status"],
                "output_path": None,
                "date_done": job.get("date_done"),
                "error": None if result is None else str(result),
            }
        )
    return failures


def render_results_html(
    source: str,
    destination: str,
    language: str,
    results: Iterable[JobResult | None],
) -> str:
    """Render the run summary page.

    :param source: Source directory for the run.
    :type source: str
    :param destination: Destination directory for the run.
    :type destination: str
    :param language: OCR language code, or an empty string for automatic selection.
    :type language: str
    :param results: Job results with ``job_id``, ``status``, ``output_path``,
        ``date_done`` and ``error`` keys; ``None`` entries are ignored.
    :type results: Iterable[JobResult | None]
    :returns: The rendered HTML document.
    :rtype: str
    """
    jobs = [job for job in results if job]
    published = [job for job in jobs if job["status"] == PUBLISHED]
    failed = [job for job in jobs if job["status"] != PUBLISHED]
    return render(
        "pdf_creation/results.html",
        source=source,
        destination=destination,
        language=language,
        total=len(jobs),
        published=published,
        failed=failed,
    )
