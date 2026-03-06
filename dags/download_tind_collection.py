from __future__ import annotations
import os
import logging
from typing import List
from pathlib import Path
from airflow.sdk import dag, task, Param
from tind_client import TINDClient
from lxml import etree

from pymarc import Record
from typing import Union

logger = logging.getLogger(__name__)
BASE_OUTPUT_DIR = "/opt/airflow/download"

@dag(
    schedule=None,
    catchup=False,
    params={
            "collection_name": "Johan Hagemeyer Photographs", 
            "batch_size": Param(2, type="integer", minimum=1)},
    tags=["tind collection"],
)

def download_tind_collection():

    def _get_tind_client() -> TINDClient:
        storage_dir = Path("/opt/airflow/download")
        storage_dir.mkdir(parents=True, exist_ok=True)
        return TINDClient(default_storage_dir=storage_dir)

    def _result_dir(id: str) -> str:
        record_dir = os.path.join(BASE_OUTPUT_DIR, id)
        os.makedirs(record_dir, exist_ok=True)
        return record_dir

    def _download_image_file(client: TINDClient, id: str) -> None:
        record = client.fetch_file_metadata(id)     
        download_url = record[0]["url"]
    
        record_dir = os.path.join(BASE_OUTPUT_DIR, id)
        os.makedirs(record_dir, exist_ok=True)
        
        client.fetch_file(download_url, record_dir )
        logger.info(f"Successfully downloaded: {record_dir }")

    def _write_record_to_xml(record: Record, file_path: Union[str, Path]) -> None:
        parser = etree.XMLParser(remove_blank_text=True)
        tree = etree.parse(str(file_path), parser)
        tree.write(
            str(file_path),
            encoding="utf-8",
            xml_declaration=True,
            pretty_print=True,
        )

    def _download_metadata_file(client: TINDClient, id: str) -> None:
        record = client.fetch_metadata(id)
        record_dir = _result_dir(id)
        file_path = Path(f"{record_dir}/{id}.xml")
        _write_record_to_xml(record, file_path)

    @task
    def get_ids(collection_name: str) -> List[str]:
        client = _get_tind_client()
        query = "collection:'{0}'".format(collection_name)
        ids = client.fetch_ids_search(query)
        return ids[:6]
    
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
        client = _get_tind_client()

        for id in batch:
            logger.info(f"Processing record: {id}")
            _download_image_file(client, id)
            _download_metadata_file(client, id)
            
    ids = get_ids("{{ params.collection_name }}")
    batches = chunk_ids(ids, batch_size="{{ params.batch_size }}")
    process_batch.expand(batch=batches)
    

download_tind_collection()