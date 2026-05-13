from pathlib import Path
from urllib.parse import urlencode

from .config import (
    STATIC_FILES_BASE_URL,
    STATIC_FILES_EMBED_PARAM,
    STATIC_FILES_ROOT,
    STATIC_FILES_URL_PREFIX,
)


def static_file_path(subpath: Path | str = "") -> Path:
    """Resolve ``subpath`` against the storage root, rejecting traversal escapes."""
    root = static_files_root()
    local_path = (root / subpath).resolve()
    if not local_path.is_relative_to(root):
        raise ValueError(f"Subpath resolved unsafely outside of the storage root: {local_path}")
    return local_path


def static_files_root() -> Path:
    """Return the static-files storage root, ensuring the directory exists."""
    STATIC_FILES_ROOT.mkdir(parents=True, exist_ok=True)
    return STATIC_FILES_ROOT


def static_files_run_dir(dag_id: str, run_id: str) -> Path:
    """Return (and create) the per-DAG-run output directory.

    The plugin's ``external_views`` href templates on ``{DAG_ID}/{RUN_ID}``,
    so any task that wants its outputs surfaced in the DAG-run "Files" tab
    must write under this path.
    """
    path = static_files_root() / dag_id / run_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def static_path_to_url(p: Path, embed: bool = False) -> str:
    """
    Map a path under static_files_root() to its public HTTP URL.

    When ``embed`` is True the URL includes ``?embed=1``, signaling to the
    route handler that the response will be rendered inside Airflow's
    sandboxed iframe (which blocks downloads — see ``serve_static_file``).
    Omit the argument for normal "open in browser / email link" usage.

    Raises ValueError if path is outside of the root.
    """
    base_url = STATIC_FILES_BASE_URL.rstrip("/")
    url_prefix = STATIC_FILES_URL_PREFIX.strip("/")
    subpath = p.relative_to(static_files_root()).as_posix()
    query = f"?{urlencode({STATIC_FILES_EMBED_PARAM: int(embed)})}" if embed else ""
    return f"{base_url}/{url_prefix}/{subpath}{query}"
