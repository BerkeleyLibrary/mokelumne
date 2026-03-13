from __future__ import annotations
import sys
import logging
from typing import List
from pathlib import Path
from airflow.sdk import dag, task, Param, get_current_context
from airflow.exceptions import AirflowFailException, AirflowSkipException

sys.path.insert(0, str(Path(__file__).resolve().parent))
from helpers.fetch_tind import FetchTind

logger = logging.getLogger(__name__)


@dag(
    schedule=None,
    catchup=False,
    params={
            "tind_query": "",
            "batch_size": Param(1, type="integer", minimum=1)},
    tags=["tind collection"],
)
def fetch_tind_collection():
    def get_fetch_tind() -> FetchTind:
        context = get_current_context()
        return FetchTind(context["run_id"])

    @task
    def validate_params(**context):
        val = context['params'].get('tind_query')
        if not val.strip():
            raise AirflowFailException("Parameter tind_query cannot be empty")

    @task
    def validate_tind_ids(ids: List[str], tind_query: str) -> List[str]:
        """
        Airflow skips downstream tasks silently if the TIND IDs list is empty.
        This task explicitly logs the query and raises AirflowSkipException here to:
        1. Provide a clear log audit (Invalid query vs. No records found).
        2. Prevent silent task skip in Airflow UI.
        """
        if not ids:
            msg = (f"No TIND IDs retrieved. All downstream tasks have been skipped. Please check to see "
                   f"if the 'tind_query' is valid: {tind_query}")
            logger.warning(msg)
            raise AirflowSkipException(msg)
        return ids

    @task
    def get_tind_ids(tind_query: str) -> List[str]:
        """Fetch record IDs from TIND collection.  """
        try:
            fetch_tind = get_fetch_tind()
            return fetch_tind.get_ids(tind_query)
        except Exception as ex:
            raise AirflowFailException(f"Failed to fetch TIND IDs: {str(ex)}") from ex

    @task
    def chunk_tind_ids(ids: List[str], batch_size: str) -> List[List[str]]:
        batch_size = int(batch_size)
        logger.info("Chunking %d IDs into batches of %d", len(ids), batch_size)
        batches = [
            ids[i:i + batch_size]
            for i in range(0, len(ids), batch_size)
        ]
        logger.info("Created %d batches", len(batches))
        return batches

    @task
    def process_tind_fetch_batch(batch: List[str]):
        logger.info("Processing batch of %d records: %r", len(batch), batch)
        fetch_tind = get_fetch_tind()
        for tind_id in batch:
            logger.info("Processing record: %s", tind_id)
            fetch_tind.download_metadata_file(tind_id)

    @task
    def save_tind_ids_to_csv_file(ids: List[str]):
        fetch_tind = get_fetch_tind()
        fetch_tind.save_tind_ids_file(ids)

    query = "{{ params.tind_query }}"
    ids = get_tind_ids(query)
    validated_ids = validate_tind_ids(ids, tind_query=query)
    batches = chunk_tind_ids(validated_ids, batch_size="{{ params.batch_size }}")

    validate_params() >> process_tind_fetch_batch.expand(batch=batches) >> save_tind_ids_to_csv_file(ids)


fetch_tind_collection()
