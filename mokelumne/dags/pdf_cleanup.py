"""DAG for cleaning up PDF creation scratch directories."""

import logging
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import Param, Variable, dag, get_current_context, task
from airflow.sdk.execution_time.xcom import XCom
from airflow.models.dagrun import DagRun
from airflow.utils.session import provide_session
from airflow.utils.state import State

logger = logging.getLogger(__name__)


@provide_session
def find_cleanup_runs(cutoff: datetime, session=None):
    """Find completed PDF creation runs that are eligible for cleanup."""
    return (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == "pdf_creation",
            DagRun.state.in_(State.finished_dr_states),
            DagRun.end_date <= cutoff,
        )
        .all()
    )

def get_run_directory(run_id: str) -> str | None:
    """Retrieve the recorded scratch directory for a PDF creation run."""
    return XCom.get_one(
        key="return_value",
        dag_id="pdf_creation",
        task_id="prepare_run_directory",
        run_id=run_id,
    )

def safe_run_directory(
    run_directory: str,
    scratch_base: str,
    run_id: str,
) -> bool:
    """Return whether a recorded run directory is safe to remove."""
    run_path = Path(run_directory).resolve()
    scratch_path = Path(scratch_base).resolve()

    return (
        run_path != scratch_path
        and run_path.is_relative_to(scratch_path)
        and run_path == scratch_path / run_id
    )

def remove_run_directory(run_directory: str) -> None:
    """Remove a scratch directory and its contents."""
    run_path = Path(run_directory)

    if not run_path.exists():
        logger.info("Scratch directory already removed: %s", run_directory)
        return

    shutil.rmtree(run_path)

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
        runs = find_cleanup_runs(cutoff_date)

        return [run.run_id for run in runs]

    @task
    def find_run_directories(run_ids: list[str]) -> list[dict[str, str]]:
        """Retrieve recorded scratch directories for eligible PDF creation runs."""
        run_directories = []

        for run_id in run_ids:
            run_directory = get_run_directory(run_id)

            if not run_directory:
                logger.warning(
                    "No recorded scratch directory found for PDF creation run %s",
                    run_id,
                )
                continue

            run_directories.append(
                {
                    "run_id": run_id,
                    "run_directory": run_directory,
                }
            )

        return run_directories

    @task
    def validate_run_directories(
        run_directories: list[dict[str, str]],
    ) -> list[dict[str, str]]:
        """Verify that recorded scratch directories are safe to remove."""
        scratch_base = Variable.get("pdf_creation_scratch_base")
        safe_directories = []

        for run in run_directories:
            if not safe_run_directory(
                run["run_directory"],
                scratch_base,
                run["run_id"],
            ):
                raise ValueError(
                    f"Unsafe scratch directory for run {run['run_id']}: "
                    f"{run['run_directory']}"
                )

            safe_directories.append(run)

        return safe_directories

    @task
    def remove_run_directories(
        run_directories: list[dict[str, str]],
    ) -> None:
        """Remove validated scratch directories."""
        for run in run_directories:
            remove_run_directory(run["run_directory"])

    cutoff = calculate_cutoff()
    eligible_runs = find_eligible_runs(cutoff)
    run_directories = find_run_directories(eligible_runs)
    safe_directories = validate_run_directories(run_directories)
    remove_run_directories(safe_directories)

pdf_cleanup()
