"""Provides storage management routines."""

import os

from pathlib import Path
from typing import Optional

def storage_dir(base_dir: Optional[str] = None) -> Path:
    """Retrieve the base directory path to use for storage."""
    if not base_dir:
        base_dir = os.environ.get("MOKELUMNE_TIND_DOWNLOAD", "/opt/airflow/download")
    storage_path = Path(base_dir)
    storage_path.mkdir(parents=True, exist_ok=True)
    return storage_path


def run_dir(run_id: str, base_dir: Optional[str] = None) -> Path:
    """Retrieve the directory path to use for a given run's storage.

    :note: As a side effect, calling this method will create the directory if it
    does not already exist.  It is immediately usable for storage upon return."""
    run_path = storage_dir(base_dir=base_dir) / run_id
    run_path.mkdir(exist_ok=True)
    return run_path


def record_dir(record_id: str) -> Path:
    """Retrieve the directory path to use for a given record during a given run."""
    record_path = storage_dir() / "tind_records" / record_id[0:2] / record_id
    record_path.mkdir(exist_ok=True, parents=True)
    return record_path


def validate_path(path: Path, kind: str | None = None) -> None:
    """
    Validate that a given path exists and is a directory.

    :param path: The path to validate.
    :type path: Path
    :param kind: The kind of path to be verified (used in exception messages).
    :type kind: str | None
    :rtype: None
    """

    label = str(kind or "specified").capitalize()

    if not path.exists():
        raise FileNotFoundError(
            " ".join([label, "directory does not exist:", str(path)])
        )

    if not path.is_dir():
        raise ValueError(
            " ".join([label, "path is not a directory:", str(path)])
        )
