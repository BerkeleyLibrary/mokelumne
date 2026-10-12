# pyright: reportTypedDictNotRequiredAccess=false

"""Process incoming GOBI MARC order files into provider-specific files."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from airflow.sdk import Param, Variable, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from mokelumne.util.gobi import (
    build_staging_directory,
    find_order_files,
    process_order_file,
    require_directory,
)

input_dir = Variable.get("process_gobi_orders_input_dir")
output_dir = Variable.get("process_gobi_orders_output_dir")
processed_dir = Variable.get("process_gobi_orders_processed_dir")
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
            default=input_dir,
            type="string",
            title="Incoming order directory",
            description="Shared directory containing GOBI .ord files.",
        ),
        "output_directory": Param(
            default=output_dir,
            type="string",
            title="Provider output directory",
            description="Shared directory where provider-specific files are written.",
        ),
        "processed_directory": Param(
            default=processed_dir,
            type="string",
            title="Processed order directory",
            description="Shared directory where original .ord files are archived.",
        ),
    },
    tags=["gobi", "marc", "recurring"],
)
def process_gobi_orders():
    """
    Discover and process each currently pending GOBI order file.

    The directory parameters get their default values from these Airflow
    Variables:

    * ``process_gobi_orders_input_dir`` for incoming ``.ord`` files
    * ``process_gobi_orders_output_dir`` for provider-specific output files
    * ``process_gobi_orders_processed_dir`` for archived source files

    Airflow Variables are used instead of literal strings for the parameter
    defaults so the same Dag code can run in staging and production while each
    environment supplies its own shared-volume paths. Scheduled runs use these
    environment-specific defaults, while manually triggered runs can still
    override the directory parameters. All three Variables must exist before
    Airflow parses the Dag.
    """

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
            staging_directory = build_staging_directory(
                order_file,
                params["output_directory"],
                context["dag"].dag_id,
                context["run_id"],
            )
            return process_order_file(
                order_file,
                params["output_directory"],
                params["processed_directory"],
                staging_directory,
            )
        except Exception as ex:
            raise AirflowFailException(
                f"Could not process GOBI order file {order_file}: {ex}"
            ) from ex

    order_files = discover_order_files()
    process_one_order_file.expand(order_file=order_files)


process_gobi_orders()  # pyright: ignore[reportUnusedExpression]
