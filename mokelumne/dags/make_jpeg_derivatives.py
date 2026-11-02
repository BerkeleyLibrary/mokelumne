"""Create JPEG derivatives of arbitrarily nested TIFF files on shared storage."""

from __future__ import annotations

import logging
from typing import Any

from airflow.sdk import Param, dag, get_current_context, task

from mokelumne.util import jpeg_derivatives as derivatives

logger = logging.getLogger(__name__)


@task
def validate_inputs() -> str:
    """Validate run parameters, shared storage, and native image capabilities.

    :return: Run report directory on shared storage.
    :rtype: str
    """
    context = get_current_context()
    params = context["params"]
    return derivatives.initialize_run(
        params["input_root"], params["output_root"], params["log_name"], context["run_id"],
    )


@task
def discover_inputs(report_path: str) -> list[dict[str, str]]:
    """Discover TIFFs and persist bounded batch manifests on shared storage.

    :param str report_path: Validated run report directory.
    :return: Manifest arguments for mapped conversion tasks.
    :rtype: list[dict[str, str]]
    """
    return derivatives.discover_inputs(report_path)


@task(max_active_tis_per_dag=2)
def convert_batch(manifest_path: str) -> None:
    """Convert a batch with durable checkpoints and portable diagnostics.

    :param str manifest_path: Shared-storage path to the batch manifest.
    """
    derivatives.convert_batch(manifest_path)


@task(trigger_rule="all_done", multiple_outputs=False)
def finalize_reports() -> dict[str, Any]:
    """Publish reports even when conversion tasks fail.

    :return: Portable summary of the run's completed and incomplete work.
    :rtype: dict[str, Any]
    """
    context = get_current_context()
    params = context["params"]
    reports = derivatives.report_directory(
        params["output_root"], params["log_name"], context["run_id"],
    )
    summary = derivatives.finalize_reports(str(reports))
    if summary["state"] == "incomplete":
        raise RuntimeError(f"Incomplete JPEG conversion; see {reports / 'summary.json'}")
    return summary


@task(trigger_rule="none_failed")
def complete(summary: dict[str, Any]) -> None:
    """Log completion after every preceding stage has finished without task failures.

    :param summary: Final portable summary.
    :type summary: dict[str, Any]
    """
    logger.info("JPEG derivatives %s: %s", summary["state"], summary["reports"])


@dag(
    dag_id="make_jpeg_derivatives",
    description="Create JPEG derivatives from a nested TIFF tree",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["images", "jpeg", "manual"],
    params={
        "input_root": Param(
            type="string", minLength=1, pattern=r"^/", title="Input root",
            description="Absolute shared directory containing the source TIFF tree.",
        ),
        "output_root": Param(
            type="string", minLength=1, pattern=r"^/", title="Output root",
            description="Absolute shared directory for JPEGs and portable run reports.",
        ),
        "log_name": Param(
            type="string", pattern=derivatives.LOG_NAME_PATTERN, title="Log name",
            description="Report label: 1–64 ASCII letters, digits, underscores or hyphens.",
        ),
    },
    doc_md="""
    Convert the first page of unsigned grayscale/RGB TIFFs to quality-70,
    progressive JPEGs. Existing outputs are preserved. Per-file errors and
    exclusions are reported as warnings; infrastructure failures fail the run.
    Portable reports are saved under `output_root/.mkderivs/log_name/run-key/`.
    See `docs/jpeg-derivatives-dag.md` for image policy and operating guidance.
    """,
)
def make_jpeg_derivatives() -> None:
    """Create and report on bounded batches of TIFF-to-JPEG conversions."""
    validation = validate_inputs()
    # The SDK's callable annotations do not include runtime-resolved XComArg inputs.
    discovery = discover_inputs(validation)  # type: ignore[arg-type]
    conversion = convert_batch.expand_kwargs(discovery)
    finalization = finalize_reports()
    completion = complete(finalization)  # type: ignore[arg-type]
    [validation, discovery, conversion] >> finalization
    [validation, discovery, conversion, finalization] >> completion


make_jpeg_derivatives()
