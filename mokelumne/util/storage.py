"""Provides storage management routines."""

import os

from pathlib import Path


def storage_dir() -> Path:
    """Retrieve the base directory path to use for storage."""
    base_dir = os.environ.get("MOKELUMNE_TIND_DOWNLOAD", "/opt/airflow/download")
    storage_path = Path(base_dir)
    storage_path.mkdir(parents=True, exist_ok=True)
    return storage_path


def run_dir(run_id: str) -> Path:
    """Retrieve the directory path to use for a given run's storage.

    :note: As a side effect, calling this method will create the directory if it
    does not already exist.  It is immediately usable for storage upon return."""
    run_path = storage_dir() / run_id
    run_path.mkdir(exist_ok=True)
    return run_path


def record_dir(run_id: str, record_id: str) -> Path:
    """Retrieve the directory path to use for a given record during a given run."""
    record_path = run_dir(run_id) / record_id
    record_path.mkdir(exist_ok=True)
    return record_path
