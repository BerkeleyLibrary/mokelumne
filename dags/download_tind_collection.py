from __future__ import annotations
import os
import logging
from typing import List
from pathlib import Path
from airflow.sdk import dag, task
from tind_client import TINDClient

logger = logging.getLogger(__name__)
BASE_OUTPUT_DIR = "/opt/airflow/download"

@dag(
    schedule=None,
    catchup=False,
    params={"collection_name": "Johan Hagemeyer Photographs"},
    tags=["tind collection"],
)

def download_tind_collection():

    @task
    def get_ids(collection_name: str) -> List[str]:
        storage_dir = Path("/opt/airflow/download")
        storage_dir.mkdir(parents=True, exist_ok=True)
        client = TINDClient(default_storage_dir=storage_dir)
        query = "collection:'{0}'".format(collection_name)
        ids = client.fetch_ids_search(query)
        return ids[:1]
    
    @task
    def process_batch(ids: List[str]):
        """
        Process a batch of record IDs
        """
        logger.info(f"Processing batch of {len(ids)} records: {ids}")
        storage_dir = Path("/opt/airflow/download")
        storage_dir.mkdir(parents=True, exist_ok=True)
        client = TINDClient(default_storage_dir=storage_dir)

        for record_id in ids:
            logger.info(f"Processing record: {record_id}")
            record = client.fetch_file_metadata(record_id)
            record_xml = ''        
            download_url = record[0]["url"]
            logger.info(f"Download URL: {download_url}")

            record_dir = os.path.join(BASE_OUTPUT_DIR, record_id)
            os.makedirs(record_dir, exist_ok=True)
            logger.info(f"Created directory: {record_dir}")

            # Save XML
            # xml_path = os.path.join(record_dir, f"{record_id}.xml")
            # with open(xml_path, "w", encoding="utf-8") as f:
            #     f.write(record_xml)

            client.fetch_file(download_url, record_dir )
            logger.info(f"Successfully downloaded: {record_dir }")


    ids = get_ids("{{ params.collection_name }}")
    process_batch(ids)
    

download_tind_collection()