# pyright: reportTypedDictNotRequiredAccess=false
# pylint: disable=duplicate-code

"""Fetch the weekly LBNL patron file from SFTP."""

from __future__ import annotations

import logging
import os
from datetime import timedelta

import pendulum
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException

from mokelumne.util.sftp import (
    DownloadStatus,
    download_sftp_file,
    lbnl_filename_for,
    require_host_key_verification,
    validate_lbnl_filename,
    validate_lbnl_prefix,
)

logger = logging.getLogger(__name__)

CONN_ID = os.environ.get("MOKELUMNE_LBNL_SFTP_CONN_ID", "lbnl_sftp")
DESTINATION = os.environ.get(
    "MOKELUMNE_LBNL_DOWNLOAD_DIR", "/srv/alma/patron_lbl"
)
FILENAME_PREFIX = validate_lbnl_prefix(
    os.environ.get("MOKELUMNE_LBNL_FILENAME_PREFIX", "lbnl_people")
)
PACIFIC = pendulum.timezone("America/Los_Angeles")


@dag(
    dag_id="fetch_lbnl_patron_file",
    description="Fetch the weekly LBNL patron file over SFTP",
    schedule="0 1 * * 2",
    start_date=pendulum.datetime(2025, 1, 1, tz=PACIFIC),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    params={
        "filename": Param(
            default=None,
            type=["null", "string"],
            pattern=rf"^{FILENAME_PREFIX}_[0-9]{{8}}[.]zip$",
            title="LBNL patron filename",
            description=(
                "Optional historical filename. Scheduled runs use the most recent "
                "Monday in America/Los_Angeles."
            ),
        )
    },
    tags=["lbnl", "patrons", "sftp", "recurring"],
)
def fetch_lbnl_patron_file():
    """Fetch one LBNL patron file as an independent workflow."""

    @task(task_id="fetch_file", retries=2, retry_delay=timedelta(minutes=15))
    def fetch_file() -> dict[str, object]:
        """Download and atomically publish the expected LBNL file."""

        context = get_current_context()
        requested_filename = context["params"].get("filename")
        if requested_filename is None:
            interval_end = context["data_interval_end"]
            if interval_end is None:
                raise ValueError("The Dag run has no data interval end")
            scheduled_for = interval_end.astimezone(PACIFIC)
            filename = lbnl_filename_for(scheduled_for, FILENAME_PREFIX)
        elif isinstance(requested_filename, str):
            filename = validate_lbnl_filename(requested_filename, FILENAME_PREFIX)
        else:
            raise ValueError("filename must be a string or null")

        require_host_key_verification(CONN_ID)
        result = download_sftp_file(
            SFTPHook(ssh_conn_id=CONN_ID),
            filename,
            DESTINATION,
            filename,
            recognize_old_markers=True,
        )
        if result.status is not DownloadStatus.DOWNLOADED:
            logger.info(result.message)
            raise AirflowSkipException(result.message)
        return result.to_dict()

    fetch_file()


fetch_lbnl_patron_file()  # pyright: ignore[reportUnusedExpression]
