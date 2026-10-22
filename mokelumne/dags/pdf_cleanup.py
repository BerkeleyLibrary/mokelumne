"""DAG for cleaning up PDF creation scratch directories."""

import logging
import requests
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import Param, Variable, dag, get_current_context, task

logger = logging.getLogger(__name__)


def get_api_session() -> tuple[str, requests.Session]:
    """Create an authenticated session for the Airflow REST API."""
    from airflow.sdk.bases.hook import BaseHook

    conn = BaseHook.get_connection("airflow_api")
    base_url = f"{conn.host}:{conn.port}"

    session = requests.Session()

    response = session.post(
        f"{base_url}/auth/token",
        json={
            "username": conn.login,
            "password": conn.password,
        },
        timeout=10,
    )
    response.raise_for_status()

    token = response.json()["access_token"]
    session.headers.update({"Authorization": f"Bearer {token}"})

    return base_url, session

def find_cleanup_runs(cutoff: datetime) -> list[str]:
    """Find completed PDF creation runs eligible for cleanup via the API."""
    base_url, session = get_api_session()

    eligible_runs = []

    # Fetch all pages of DAG runs
    cursor = None

    with session:
        while True:
            params = {"limit": 100}

            if cursor:
                params["cursor"] = cursor

            response = session.get(
                f"{base_url}/api/v2/dags/pdf_creation/dagRuns",
                params=params,
                timeout=30,
            )
            response.raise_for_status()

            payload = response.json()

            for run in payload["dag_runs"]:
                if run["state"] not in ("success", "failed"):
                    continue

                if not run["end_date"]:
                    continue

                end_date = datetime.fromisoformat(
                    run["end_date"].replace("Z", "+00:00")
                )

                if end_date <= cutoff:
                    eligible_runs.append(run["dag_run_id"])

            cursor = payload.get("next_cursor")
            if not cursor:
                break

    logger.info("Found %d eligible PDF creation runs", len(eligible_runs))

    return eligible_runs

def get_run_directory(run_id: str, base_url: str, session: requests.Session) -> str | None:
    """Retrieve the recorded scratch directory via the Airflow REST API."""
    from urllib.parse import quote

    # Retrieve the prepare_run_directory return-value XCom
    encoded_run_id = quote(run_id, safe="")
    url = (
        f"{base_url}/api/v2/dags/pdf_creation/dagRuns/"
        f"{encoded_run_id}/taskInstances/"
        "prepare_run_directory/xcomEntries/return_value"
    )

    response = session.get(
        url,
        timeout=30,
    )

    # Older runs may not have a scratch directory XCom.
    if response.status_code == 404:
        logger.info("No scratch directory XCom for run %s", run_id)
        return None

    response.raise_for_status()

    run_directory = response.json().get("value")

    if not isinstance(run_directory, str):
        raise ValueError(
            f"Unexpected scratch directory XCom value for run {run_id}"
        )

    return run_directory

def expected_run_directory(
    run_directory: str,
    scratch_base: str,
    run_id: str,
) -> bool:
    """Check that the path matches the expected scratch directory."""
    raw_path = Path(run_directory)
    scratch_path = Path(scratch_base).resolve()

    return (
        raw_path.is_absolute()
        and raw_path.parent == scratch_path
        and raw_path.name == run_id
        and raw_path != scratch_path
    )

def safe_run_directory(
    run_directory: str,
    scratch_base: str,
    run_id: str,
) -> bool:
    """Return whether a recorded run directory is safe to remove."""
    raw_path = Path(run_directory)

    return (
        expected_run_directory(run_directory, scratch_base, run_id)
        and not raw_path.is_symlink()
        and raw_path.is_dir()
        and raw_path.resolve() == Path(scratch_base).resolve() / run_id
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
            minimum=0,
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
        """Retrieve recorded scratch directories for eligible PDF creation runs."""
        run_directories = []

        base_url, session = get_api_session()

        with session:
            for run_id in run_ids:
                run_directory = get_run_directory(run_id, base_url, session)

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
            run_directory = run["run_directory"]
            run_id = run["run_id"]
            run_path = Path(run_directory)

            # Validate the expected path before checking existence.
            if not expected_run_directory(run_directory, scratch_base, run_id):
                raise ValueError(
                    f"Unexpected scratch directory for run {run_id}: "
                    f"{run_directory}"
                )

            # Already deleted? Skip it safely.
            if not run_path.exists() and not run_path.is_symlink():
                logger.info(
                    "Scratch directory already removed for run %s: %s",
                    run_id,
                    run_directory,
                )
                continue

            # Existing directories must pass all safety checks.
            if not safe_run_directory(run_directory, scratch_base, run_id):
                raise ValueError(
                    f"Unsafe scratch directory for run {run_id}: "
                    f"{run_directory}"
                )

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
            if not safe_run_directory(
                run["run_directory"],
                scratch_base,
                run["run_id"],
            ):
                raise ValueError(
                    f"Unsafe scratch directory for run {run['run_id']}: "
                    f"{run['run_directory']}"
                )

            if dry_run:
                logger.info(
                    "DRY RUN: Would remove scratch directory: %s",
                    run["run_directory"],
                )
                continue

            remove_run_directory(run["run_directory"])
    
    cutoff = calculate_cutoff()
    eligible_runs = find_eligible_runs(cutoff)
    run_directories = find_run_directories(eligible_runs)
    safe_directories = validate_run_directories(run_directories)
    remove_run_directories(safe_directories)

pdf_cleanup()
