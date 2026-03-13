import os
import csv

from pathlib import Path
from typing import List, Union

from lxml import etree
from pymarc import Record, XMLWriter
from tind_client import TINDClient


class FetchTind:
    """Helper methods for fetching items from TIND using TINDClient."""
    def __init__(self, run_id: str):
        self.client = self._tind_client()
        self.batch_dir = self._batch_dir(run_id)

    def get_ids(self, tind_query: str) -> List[str]:
        return  self.client.fetch_ids_search(tind_query)

    # to be used in later Jira ticket
    def download_image_file(self, tind_id: str) -> None:
        record = self.client.fetch_file_metadata(tind_id)
        download_url = record[0]["url"]
        record_dir = self._record_dir(tind_id)
        self.client.fetch_file(download_url, record_dir)

    def download_metadata_file(self, tind_id: str) -> None:
        record = self.client.fetch_metadata(tind_id)
        file_path = Path(f"{self._record_dir(tind_id)}/{tind_id}.xml")
        self._write_record_to_xml(record, file_path)

    def save_tind_ids_file(self, ids: List[str]) -> None:
        file_path = Path(f"{self.batch_dir}/ids.csv")
        file_path.parent.mkdir(parents=True, exist_ok=True)

        with file_path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(["TIND_ID"])
            for tind_id in ids:
                writer.writerow([tind_id])

    def _tind_client(self) -> TINDClient:
        base_dir = os.environ.get("MOKELUMNE_TIND_DOWNLOAD", "/opt/airflow/download")
        storage_dir = Path(base_dir)
        storage_dir.mkdir(parents=True, exist_ok=True)
        return TINDClient(default_storage_dir=str(storage_dir))

    def _record_dir(self, tind_id: str) -> str:
        record_dir = os.path.join(self.batch_dir, tind_id)
        os.makedirs(record_dir, exist_ok=True)
        return record_dir

    def _batch_dir(self, run_id: str) -> str:
        return os.path.join(self.client.default_storage_dir, run_id)

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
