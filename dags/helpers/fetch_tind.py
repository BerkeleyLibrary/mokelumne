import os
import logging
from lxml import etree
from pymarc import Record, XMLWriter
from typing import Union
from pathlib import Path
from typing import List
from tind_client import TINDClient

class FetchTind:
    def __init__(self):
        self.client = self._tind_client()

    def get_ids(self, tind_query: str) -> List[str]:
        return  self.client.fetch_ids_search(tind_query)
        
    # to be used in later Jira ticket
    # def download_image_file(self, id: str) -> None:
    #     record = self.client.fetch_file_metadata(id)     
    #     download_url = record[0]["url"]    
    #     record_dir = self._record_dir(id)        
    #     self.client.fetch_file(download_url, record_dir)
    
    def download_metadata_file(self, id: str) -> None:
        record = self.client.fetch_metadata(id)
        record_dir = self._record_dir(id) 
        file_path = Path(f"{record_dir}/{id}.xml")
        self._write_record_to_xml(record, file_path)

    def _tind_client(self) -> TINDClient:
        base_dir = os.environ.get("MOKELUMNE_TIND_DOWNLOAD", "/opt/airflow/download")     
        storage_dir = Path(base_dir)
        storage_dir.mkdir(parents=True, exist_ok=True)
        return TINDClient(default_storage_dir=storage_dir)
     
    def _record_dir(self, id: str) -> str:
        record_dir = os.path.join(self.client.default_storage_dir, id)
        os.makedirs(record_dir, exist_ok=True)
        return record_dir

    def _write_record_to_xml(self, record: Record, file_path: Union[str, Path]) -> None:
        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        with file_path.open("wb") as f:
            writer = XMLWriter(f)
            writer.write(record)
            writer.close()

        parser = etree.XMLParser(remove_blank_text=True)
        tree = etree.parse(str(file_path), parser)
        tree.write(
            str(file_path),
            encoding="utf-8",
            xml_declaration=True,
            pretty_print=True,
        )
