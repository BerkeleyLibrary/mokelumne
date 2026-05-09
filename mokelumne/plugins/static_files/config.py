import os

from pathlib import Path

from airflow.configuration import conf


STATIC_FILES_EMBED_PARAM = "embed"
"""Name of the query param that, if truthy, tells the plugin to render downloadable files inline."""

STATIC_FILES_DAG_RUN_TAB_LABEL = os.getenv('STATIC_FILES_DAG_RUN_TAB_LABEL', 'Files')
"""Name of the tab attached to the DAG run view."""

STATIC_FILES_BASE_URL = (
    os.getenv('STATIC_FILES_BASE_URL')
    or conf.get('api', 'base_url', fallback='http://localhost:8080')
)
"""The root URL from which static files are served. Defaults to the AirFlow API base URL."""

STATIC_FILES_INLINE_MIMES = tuple(os.getenv(
    'STATIC_FILES_INLINE_MIMES',
    'image/,audio/,video/,application/pdf,text/html,text/plain'
).split(','))
"""
Tuple of MIME types the plugin will pass-thru to the browser when embedded.

When serving files from the AirFlow sandboxed iFrame that have MIME types other than these
prefixes, the plugin returns them as `Content-Type: text/plain` to ensure they're rendered
inline, preventing the browser from trying to download them, which fails silently within
the iFrame.
"""

STATIC_FILES_PLUGIN_NAME = os.getenv('STATIC_FILES_PLUGIN_NAME', 'Static Files API')
"""Name of the plugin."""

STATIC_FILES_ROOT = Path(os.getenv('STATIC_FILES_ROOT', '/opt/airflow/files')).resolve()
"""The local file system root path from which to serve files."""

STATIC_FILES_ROUTE = os.getenv('STATIC_FILES_ROUTE', 'files')
"""Name of the route added to the DAG Run view linked to the file server."""

STATIC_FILES_URL_PREFIX = os.getenv('STATIC_FILES_URL_PREFIX', '/files')
"""The URL prefix to which the plugin's routes are registered in AirFlow."""
