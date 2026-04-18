"""
Airflow plugin: DAG Run File Browser

Serves files from `/{public_dir}/{dag_id}/{run_id}`

Users must have permissions to view the given DAG in order to view its files.
"""

from __future__ import annotations

import mimetypes
import os
from pathlib import Path

from airflow.api_fastapi.core_api.security import requires_access_dag
from airflow.plugins_manager import AirflowPlugin
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.responses import FileResponse, Response

from mokelumne.util.storage import public_dir


# Intercepts HTTP requests at this prefix. Cannot conflict with AirFlow's routes.
URL_PREFIX = os.getenv("MOKELUMNE_PUBLIC_URL_PREFIX", "/public")


app = FastAPI(
    title="DAG Run File Browser",
    version="0.0.1",
    dependencies=[
        Depends(requires_access_dag(method="GET"))
    ]
)

@app.get("/{dag_id}/{run_id}/{subpath:path}")
def show(dag_id: str, run_id: str, subpath: str) -> Response:
    filepath = _resolve(dag_id, run_id, subpath)

    # Downloads are blocked when rendered via the DAG Run tab, which is in an iframe.
    # Recasting the media_type to text/plain tells the browser to render it inline,
    # so left-click behavior works. The user can still right-click/Save As to download it.
    media_type = mimetypes.guess_type(filepath)[0] or "application/octet-stream"
    if media_type == "text/csv":
        media_type = "text/plain; charset=utf-8"

    return FileResponse(filepath, media_type=media_type)


def _resolve(dag_id: str, run_id: str, subpath: str) -> Path:
    """
    Returns the local Path to the given subpath in the DAG run's public directory

    If the Path resolves to a directory, it's mapped to an `index.html` file in that directory.

    Raises 404 if that path does not exist.
    """

    root = public_dir().resolve()
    subpath = subpath.rstrip("/")
    target = (root / dag_id / run_id / subpath).resolve()
    try:
        target.relative_to(root)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)

    if target.is_dir():
        target = target / "index.html"

    if not target.exists():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)

    return target


class DagFilebrowserPlugin(AirflowPlugin):
    name = "dag_run_file_browser"
    fastapi_apps = [
        {
            "app": app,
            "name": "DAG File Browser",
            "url_prefix": URL_PREFIX,
        },
    ]
    external_views = [
        # Show files for a specific DAG_Run in a new tab on the run view
        {
            "name": "Files",
            "destination": "dag_run",
            "href": f"{URL_PREFIX}/{{DAG_ID}}/{{RUN_ID}}/",
            "url_route": "dag_run_file_browser_show",
        },
    ]
