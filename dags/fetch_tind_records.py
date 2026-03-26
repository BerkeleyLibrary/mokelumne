from __future__ import annotations
import logging

from airflow.sdk import Asset, AssetAlias, dag, task, Param, get_current_context
from airflow.exceptions import AirflowFailException, AirflowSkipException

from helpers.fetch_tind import FetchTind
from mokelumne.util.storage import run_dir

logger = logging.getLogger(__name__)

# use new util/storage.py helper with run_dir()
tind_bulk_xml_alias = AssetAlias("tind_bulk_xml")

@dag(
    schedule=None,
    catchup=False,
    params={"tind_query": Param("", type="string")},
    tags=["tind records"]
)
def fetch_tind_records():

    @task
    def validate_params(**context):
        val = context['params'].get('tind_query')
        if not val.strip():
            raise AirflowFailException("Parameter tind_query cannot be empty")

    @task(outlets=[tind_bulk_xml_alias])
    def write_query_results_to_xml(tind_query: str):
        context = get_current_context()
        run_id = context["run_id"]
        fetch_tind = FetchTind(run_id)

        try:
            records_written = fetch_tind.write_query_results_to_xml(tind_query, 'tind_bulk.xml')
        except Exception as ex:
            raise AirflowFailException(f"Failed to write query results to XML: {str(ex)}") from ex

        if records_written == 0:
            raise AirflowSkipException(f"No records found for query: {tind_query}")

        actual_path = run_dir(run_id) / 'tind_bulk.xml'
        logger.info(f"Query results written to XML file at: {actual_path}")

        context["outlet_events"][tind_bulk_xml_alias].add(Asset(f"file://{actual_path}"))

        return records_written

    validate_params() >> write_query_results_to_xml("{{ params.tind_query }}")


fetch_tind_records()
