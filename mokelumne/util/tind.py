import os
from lxml import etree
from pymarc import Record
from typing import Union
from pathlib import Path
from typing import List
from tind_client import TINDClient

class Tind:
    def __init__(self):
        self.client = self._tind_client()
        
    def get_ids(self, collection_name: str, extract_num: int = None) -> List[str]:
        query = "collection:'{0}'".format(collection_name)
        ids = self.client.fetch_ids_search(query)
        return ids[:extract_num] if extract_num is not None else ids
  
    def download_image_file(self, id: str) -> None:
        record = self.client.fetch_file_metadata(id)     
        download_url = record[0]["url"]
    
        record_dir = self._record_dir(id)        
        self.client.fetch_file(download_url, record_dir)
       
    
    def download_metadata_file(self, id: str) -> None:
        record = self.client.fetch_metadata(id)
        record_dir = self._record_dir(id) 
        file_path = Path(f"{record_dir}/{id}.xml")
        self._write_record_to_xml(record, file_path)

     def _tind_client(self) -> TINDClient:
        base_dir = os.environ.get("MOKELUMNE_TIND_DOWNLOAD", "")
        if base_dir:
            storage_dir = Path(base_dir)
            storage_dir.mkdir(parents=True, exist_ok=True)
            return TINDClient(default_storage_dir=storage_dir)
        else:
            return TINDClient()
     
    def _record_dir(self, id: str) -> str:
        record_dir = os.path.join(self.client.default_storage_dir, id)
        os.makedirs(record_dir, exist_ok=True)
        return record_dir

    
    def _write_record_to_xml(record: Record, file_path: Union[str, Path]) -> None:
        parser = etree.XMLParser(remove_blank_text=True)
        tree = etree.parse(str(file_path), parser)
        tree.write(
            str(file_path),
            encoding="utf-8",
            xml_declaration=True,
            pretty_print=True,
        )