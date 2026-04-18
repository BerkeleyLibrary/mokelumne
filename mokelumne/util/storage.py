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
    record_path = run_dir(run_id) / record_id[0:2] / record_id
    record_path.mkdir(exist_ok=True, parents=True)
    return record_path


def public_dir() -> Path:
    """Retrieve the base directory path to use for *public* storage."""
    public_env = os.environ.get("MOKELUMNE_PUBLIC_STORAGE", "/opt/airflow/public")
    public_path = Path(public_env)
    public_path.mkdir(exist_ok=True)
    return public_path


def public_path_to_url(public_path: Path) -> str:
    """Take a public path, as a subdirectory of +public_dir()+, and return a publicly accessible
    HTTPS URL."""
    top = public_dir()
    location = public_path.relative_to(top)
    url_base = os.environ.get("MOKELUMNE_PUBLIC_URL", "http://localhost:8080/public/")
    return f"{url_base}{str(location)}"
