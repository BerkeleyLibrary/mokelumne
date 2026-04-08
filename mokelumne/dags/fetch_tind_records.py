"""fetch_tind_records.py

DAG that fetches TIND records matching a query and writes them to an XML file.
"""
from __future__ import annotations
import logging

from airflow.sdk import Asset, dag, task, Param, get_current_context
from airflow.exceptions import AirflowFailException, AirflowSkipException

from mokelumne.batch_image.assets import records_xml
from mokelumne.util.fetch_tind import FetchTind
from mokelumne.util.storage import run_dir

logger = logging.getLogger(__name__)


@dag(
    schedule=None,
    catchup=False,
    params={"tind_query": Param("", type="string")},
    tags=["tind", "records", "batch-image", "xml"]
)
def fetch_tind_records():
    """Fetch TIND records matching a query and write them to an XML file."""

    @task
    def validate_params() -> None:
        """Validate that the tind_query parameter is not empty.

        :raises AirflowFailException: If the tind_query parameter is empty.
        """

        context = get_current_context()
        val = context["params"].get("tind_query", "")
        if not val.strip():
            raise AirflowFailException("Parameter tind_query cannot be empty")

    @task(outlets=[records_xml])
    def write_query_results_to_xml() -> int:
        """Fetch records matching the query and write them to an XML file.

        :raises AirflowSkipException: If no records are found.
        :raises AirflowFailException: If it fails during the fetching or writing process.
        :return int: The number of records written.
        """

        context = get_current_context()
        run_id = context["run_id"]
        tind_query = context["params"]["tind_query"]
        fetch_tind = FetchTind(run_id)

        try:
            records_written = fetch_tind.write_query_results_to_xml(tind_query, "tind_bulk.xml")
        except Exception as ex:
            raise AirflowFailException(f"Failed to write query results to XML: {ex}") from ex

        if records_written == 0:
            raise AirflowSkipException(f"No records found for query: {tind_query}")

        actual_path = run_dir(run_id) / "tind_bulk.xml"
        logger.info("Query results written to XML file at: %s", actual_path)

        context["outlet_events"][records_xml].add(Asset(f"file://{actual_path}"))

        return records_written

    validate_params() >> write_query_results_to_xml()


fetch_tind_records()
