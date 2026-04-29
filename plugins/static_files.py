"""
Airflow plugin: Static Files

Serves files to authenticated users from FILESERVER_ROOT (default: /opt/airflow/storage)
at FILESERVER_URL_PREFIX (default: /storage).

Adds a tab to the DAG Run view showing files under `{root}/{dag_id}/{run_id}`.
"""

from __future__ import annotations

import os

from airflow.api_fastapi.core_api.security import requires_access_dag
from airflow.plugins_manager import AirflowPlugin
from fastapi import Depends, FastAPI, Request
from fastapi.staticfiles import StaticFiles


# Comma-separated list of media types to return as plaintext, allowing them to be viewed in the DAG Run tab.
FORCE_PLAINTEXT = os.getenv("FILESERVER_FORCE_PLAINTEXT", "text/csv").split(',')
# Root directory from which to serve files
ROOT_DIR = os.getenv("FILESERVER_ROOT", "/opt/airflow/storage")
# Intercepts HTTP requests at this prefix. Cannot conflict with AirFlow's routes.
URL_PREFIX = os.getenv("FILESERVER_URL_PREFIX", "/storage")


app = FastAPI(
    title="Static Files",
    version="0.0.1",
    dependencies=[
        Depends(requires_access_dag(method="GET"))
    ]
)

# Mount at the root of the _plugin_, which is mounted at URL_PREFIX in AirFlow.
app.mount("/", StaticFiles(directory=ROOT_DIR, html=True), name="files")

@app.middleware("http")
async def _force_plaintext(request: Request, call_next):
    """
    Force some Content-Types to "text/plain" so they render properly in sandboxed Airflow iFrames
    """
    response = await call_next(request)

    content_type, flags = response.headers.get("Content-Type", "").split(";")
    if content_type in FORCE_PLAINTEXT:
        if flags is None:
            response.headers["Content-Type"] = f"text/plain"
        else:
            response.headers["Content-Type"] = f"text/plain; {flags}"

    return response


class StaticFilesPlugin(AirflowPlugin):
    name = "static_files"
    fastapi_apps = [
        {
            "app": app,
            "name": "Static Files",
            "url_prefix": URL_PREFIX,
        },
    ]
    external_views = [
        {
            "name": "Files",
            "destination": "dag_run",
            "href": f"{URL_PREFIX}/{{DAG_ID}}/{{RUN_ID}}/",
            "url_route": "static_files",
        },
    ]
