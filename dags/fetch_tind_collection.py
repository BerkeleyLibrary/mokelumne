from __future__ import annotations
import sys
import logging
from typing import List
from pathlib import Path
from airflow.sdk import dag, task, Param
from airflow.exceptions import AirflowFailException

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
    fetch_tind = FetchTind()

    @task
    def validate_params(**context):
        val = context['params'].get('tind_query')
        if not val.strip():
            raise AirflowFailException("Parameter tind_qury cannot be empty")

    @task
    def get_ids(tind_qury: str) -> List[str]:
        """Fetch record IDs from TIND collection.
        """
        try:
            return fetch_tind.get_ids(tind_qury)
        except Exception as ex:
            raise AirflowFailException(f"Failed to fetch TIND IDs: {str(ex)}")
            
    @task
    def chunk_ids(ids: List[str], batch_size: str) -> List[List[str]]:
        batch_size = int(batch_size)
        logger.info(f"Chunking {len(ids)} IDs into batches of {batch_size}")
        batches = [
            ids[i:i + batch_size]
            for i in range(0, len(ids), batch_size)
        ]
        logger.info(f"Created {len(batches)} batches")
        return batches
    
    @task
    def process_batch(batch: List[str]):
        logger.info(f"Processing batch of {len(batch)} records: {batch}")

        for id in batch:
            logger.info(f"Processing record: {id}")
            fetch_tind.download_metadata_file(id)
            
    ids = get_ids("{{ params.tind_query }}")
    batches = chunk_ids(ids, batch_size="{{ params.batch_size }}")
    validate_params() >> process_batch.expand(batch=batches)
    

fetch_tind_collection()