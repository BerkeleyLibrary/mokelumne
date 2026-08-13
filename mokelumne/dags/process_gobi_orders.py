# pyright: reportTypedDictNotRequiredAccess=false

"""Process incoming GOBI MARC order files into provider-specific files."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from mokelumne.util.gobi import find_order_files, process_order_file, require_directory

logger = logging.getLogger(__name__)


@dag(
    dag_id="process_gobi_orders",
    description="Split GOBI MARC order files into provider-specific files",
    schedule="*/30 * * * *",  # every 30 minutes
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    params={
        "input_directory": Param(
            default="/srv/alma/gobi-ebook-eocr-input",
            type="string",
            title="Incoming order directory",
            description="Shared directory containing GOBI .ord files.",
        ),
        "output_directory": Param(
            default="/opt/airflow/files/gobi/gobi_processed",
            type="string",
            title="Provider output directory",
            description="Shared directory where provider-specific files are written.",
        ),
        "processed_directory": Param(
            default="/opt/airflow/files/gobi/incoming/processed",
            type="string",
            title="Processed order directory",
            description="Shared directory where original .ord files are archived.",
        ),
    },
    tags=["gobi", "marc", "recurring"],
)
def process_gobi_orders():
    """Discover and process each currently pending GOBI order file."""

    @task
    def discover_order_files() -> list[str]:
        """Find the pending order files at task runtime."""

        context = get_current_context()
        params = context["params"]
        try:
            require_directory(params["output_directory"], "Output")
            require_directory(params["processed_directory"], "Processed")
            order_files = find_order_files(params["input_directory"])
        except Exception as ex:
            raise AirflowFailException(f"Could not scan for GOBI order files: {ex}") from ex

        logger.info("Found %s GOBI order file(s)", len(order_files))
        return order_files

    @task(max_active_tis_per_dag=4)
    def process_one_order_file(order_file: str) -> dict[str, object]:
        """Process and archive one order file."""
        context = get_current_context()
        params = context["params"]
        try:
            return process_order_file(
                order_file,
                params["output_directory"],
                params["processed_directory"],
            )
        except Exception as ex:
            raise AirflowFailException(
                f"Could not process GOBI order file {order_file}: {ex}"
            ) from ex

    order_files = discover_order_files()
    process_one_order_file.expand(order_file=order_files)


process_gobi_orders()  # pyright: ignore[reportUnusedExpression]
