# pyright: reportTypedDictNotRequiredAccess=false
# pylint: disable=duplicate-code

"""Fetch the daily GOBI order file from SFTP."""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime, timedelta
from pathlib import PurePosixPath

import pendulum
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException

from mokelumne.util.sftp import (
    DownloadStatus,
    download_sftp_file,
    gobi_filename_for,
    require_host_key_verification,
    validate_gobi_filename,
)

logger = logging.getLogger(__name__)

CONN_ID = os.environ.get("MOKELUMNE_GOBI_SFTP_CONN_ID", "gobi_sftp")
DESTINATION = os.environ.get(
    "MOKELUMNE_GOBI_DOWNLOAD_DIR", "/srv/alma/gobi-ebook-eocr-input"
)
PACIFIC = pendulum.timezone("America/Los_Angeles")


@dag(
    dag_id="fetch_gobi_order_file",
    description="Fetch the daily GOBI MARC order file over SFTP",
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz=PACIFIC),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    params={
        "filename": Param(
            default=None,
            type=["null", "string"],
            pattern=r"^ebook[0-9]{4}[.]ord$",
            title="GOBI order filename",
            description=(
                "Optional historical ebookMMDD.ord filename. Scheduled runs use "
                "the run date in America/Los_Angeles."
            ),
        )
    },
    tags=["gobi", "sftp", "recurring"],
)
def fetch_gobi_order_file():
    """Fetch one GOBI order file without triggering downstream processing."""

    @task(task_id="fetch_file", retries=2, retry_delay=timedelta(minutes=15))
    def fetch_file() -> dict[str, object]:
        """Download and atomically publish the expected GOBI file."""

        context = get_current_context()
        requested_filename = context["params"].get("filename")
        if requested_filename is None:
            interval_end = context["data_interval_end"]
            if interval_end is None:
                raise ValueError("The Dag run has no data interval end")
            scheduled_for = interval_end.astimezone(PACIFIC)
            filename = gobi_filename_for(scheduled_for)
            minimum_modified_at = datetime.now(UTC) - timedelta(days=10)
        elif isinstance(requested_filename, str):
            filename = validate_gobi_filename(requested_filename)
            minimum_modified_at = None
        else:
            raise ValueError("filename must be a string or null")

        require_host_key_verification(CONN_ID)
        result = download_sftp_file(
            SFTPHook(ssh_conn_id=CONN_ID),
            str(PurePosixPath("/gobiord") / filename),
            DESTINATION,
            filename,
            minimum_modified_at=minimum_modified_at,
        )
        if result.status is not DownloadStatus.DOWNLOADED:
            logger.info(result.message)
            raise AirflowSkipException(result.message)
        return result.to_dict()

    fetch_file()


fetch_gobi_order_file()  # pyright: ignore[reportUnusedExpression]
