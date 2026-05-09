from airflow.plugins_manager import AirflowPlugin

from .config import (
    STATIC_FILES_DAG_RUN_TAB_LABEL,
    STATIC_FILES_EMBED_PARAM,
    STATIC_FILES_PLUGIN_NAME,
    STATIC_FILES_ROOT,
    STATIC_FILES_ROUTE,
    STATIC_FILES_URL_PREFIX,
)
from .helpers import static_file_path, static_files_root, static_path_to_url
from .routes import files_router


class StaticFilesPlugin(AirflowPlugin):
    name = STATIC_FILES_PLUGIN_NAME

    fastapi_apps = [
        {
            "app": files_router,
            "name": STATIC_FILES_PLUGIN_NAME,
            "url_prefix": STATIC_FILES_URL_PREFIX,
        }
    ]

    external_views = [
        {
            "name": STATIC_FILES_DAG_RUN_TAB_LABEL,
            "destination": "dag_run",
            "url_route": STATIC_FILES_ROUTE,
            "href": f"{STATIC_FILES_URL_PREFIX}/{{DAG_ID}}/{{RUN_ID}}/?{STATIC_FILES_EMBED_PARAM}=1",
        }
    ]

    # Adds macros allowing the rest of AirFlow to address files served by this plugin.
    macros = [
        static_file_path,
        static_files_root,
        static_path_to_url,
    ]
