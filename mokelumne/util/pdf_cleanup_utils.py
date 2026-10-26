"""Utilities for cleaning up PDF creation scratch directories."""

import logging
import shutil
from datetime import datetime
from pathlib import Path

import requests

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
        and Path(run_id).name == run_id
        and run_id not in (".", "..")
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


def validate_cleanup_directory(
    run_directory: str,
    scratch_base: str,
    run_id: str,
) -> bool:
    """Validate a scratch directory, returning False if already removed."""
    run_path = Path(run_directory)

    if not expected_run_directory(run_directory, scratch_base, run_id):
        raise ValueError(
            f"Unexpected scratch directory for run {run_id}: {run_directory}"
        )

    if not run_path.exists() and not run_path.is_symlink():
        logger.info(
            "Scratch directory already removed for run %s: %s",
            run_id,
            run_directory,
        )
        return False

    if not safe_run_directory(run_directory, scratch_base, run_id):
        raise ValueError(
            f"Unsafe scratch directory for run {run_id}: {run_directory}"
        )

    return True


def remove_run_directory(run_directory: str) -> None:
    """Remove a scratch directory and its contents."""
    run_path = Path(run_directory)

    if not run_path.exists():
        logger.info("Scratch directory already removed: %s", run_directory)
        return

    shutil.rmtree(run_path)
