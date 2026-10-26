"""DAG for cleaning up PDF creation scratch directories."""

import logging
from datetime import datetime, timedelta, timezone

from airflow.sdk import Param, Variable, dag, get_current_context, task

from mokelumne.util.pdf_cleanup_utils import (
    find_cleanup_runs,
    remove_run_directory,
    validate_cleanup_directory,
)

logger = logging.getLogger(__name__)


@dag(
    dag_id="pdf_cleanup",
    schedule=None,
    catchup=False,
    params={
        "cleanup_window": Param(
            default=30,
            type="integer",
            minimum=1,
            title="Cleanup window (days)",
            description="Clean up completed PDF creation runs this many days old or older.",
        ),
        "dry_run": Param(
            default=True,
            type="boolean",
            title="Dry run",
            description="Log directories that would be removed without deleting them.",
        ),
    },
)
def pdf_cleanup():
    """Clean up scratch directories from completed PDF creation runs."""

    @task
    def calculate_cutoff() -> str:
        """Calculate the cutoff date for eligible PDF creation runs."""
        context = get_current_context()
        cleanup_window = context["params"]["cleanup_window"]

        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=cleanup_window)

        return cutoff.isoformat()

    @task
    def find_eligible_runs(cutoff: str) -> list[str]:
        """Find completed PDF creation runs eligible for cleanup."""
        cutoff_date = datetime.fromisoformat(cutoff)

        return find_cleanup_runs(cutoff_date)

    @task
    def find_run_directories(run_ids: list[str]) -> list[dict[str, str]]:
        """Retrieve scratch directories for eligible PDF creation runs."""
        ti = get_current_context()["ti"]
        run_directories = []

        for run_id in run_ids:
            run_directory = ti.xcom_pull(
                dag_id="pdf_creation",
                run_id=run_id,
                task_ids="prepare_run_directory",
                key="return_value",
                include_prior_dates=False,
            )

            if not run_directory:
                logger.warning(
                    "No recorded scratch directory found for PDF creation run %s",
                    run_id,
                )
                continue

            if not isinstance(run_directory, str):
                raise ValueError(
                    f"Unexpected scratch directory XCom value for run {run_id}"
                )

            run_directories.append({
                "run_id": run_id,
                "run_directory": run_directory,
            })

        return run_directories

    @task
    def validate_run_directories(
        run_directories: list[dict[str, str]],
    ) -> list[dict[str, str]]:
        """Verify that recorded scratch directories are safe to remove."""
        scratch_base = Variable.get("pdf_creation_scratch_base")
        safe_directories = []

        for run in run_directories:
            run_directory = run["run_directory"]
            run_id = run["run_id"]

            if not validate_cleanup_directory(run_directory, scratch_base, run_id):
                continue

            safe_directories.append(run)

        return safe_directories

    @task
    def remove_run_directories(
        run_directories: list[dict[str, str]],
    ) -> None:
        """Remove validated scratch directories unless running in dry-run mode."""
        context = get_current_context()
        dry_run = context["params"]["dry_run"]
        logger.info("DRY RUN ENABLED: %s", dry_run)
        scratch_base = Variable.get("pdf_creation_scratch_base")

        for run in run_directories:
            run_directory = run["run_directory"]
            run_id = run["run_id"]

            if not validate_cleanup_directory(run_directory, scratch_base, run_id):
                continue

            if dry_run:
                logger.info(
                    "DRY RUN: Would remove scratch directory: %s",
                    run_directory,
                )
                continue

            remove_run_directory(run_directory)
            logger.info("Removed scratch directory: %s", run_directory)

    cutoff = calculate_cutoff()
    eligible_runs = find_eligible_runs(cutoff)
    run_directories = find_run_directories(eligible_runs)
    safe_directories = validate_run_directories(run_directories)
    remove_run_directories(safe_directories)


pdf_cleanup()
