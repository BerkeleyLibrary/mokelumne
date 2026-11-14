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
    published = [job for job in results if job["status"] == PUBLISHED]
    failed = [job for job in results if job["status"] != PUBLISHED]
    return render(
        "pdf_creation/results.html",
        source=source,
        destination=destination,
        language=language,
        total=len(results),
        published=published,
        failed=failed,
    )
