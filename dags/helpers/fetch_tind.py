import csv

from pathlib import Path

from lxml import etree
from pymarc import Record, XMLWriter
from tind_client import TINDClient

from mokelumne.util.storage import storage_dir, run_dir, record_dir


class FetchTind:
    """Helper methods for fetching items from TIND using TINDClient."""
    def __init__(self, _run_id: str):
        self.run_id = _run_id
        self.client = TINDClient(default_storage_dir=str(run_dir(_run_id)))

    def get_ids(self, tind_query: str) -> list[str]:
        return self.client.fetch_ids_search(tind_query)

    # to be used in later Jira ticket
    def download_image_file(self, tind_id: str) -> None:
        record = self.client.fetch_file_metadata(tind_id)
        download_url = record[0]["url"]
        record_path = record_dir(self.run_id, tind_id)
        self.client.fetch_file(download_url, str(record_path))

    def download_metadata_file(self, tind_id: str) -> None:
        record = self.client.fetch_metadata(tind_id)
        file_path = record_dir(self.run_id, tind_id) / f"{tind_id}.xml"
        self._write_record_to_xml(record, file_path)

    def save_tind_ids_file(self, ids: list[str]) -> None:
        file_path = run_dir(self.run_id) / "ids.csv"

        with file_path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(["TIND_ID"])
            for tind_id in ids:
                writer.writerow([tind_id])

    def write_query_results_to_xml(self, tind_query: str, file_name: str = "") -> int:
        records_written = self.client.write_search_results_to_file(tind_query, file_name)
        return int(records_written)

    def _write_record_to_xml(self, record: Record, file_path: Path) -> None:
        with file_path.open("wb") as f:
            writer = XMLWriter(f)
            writer.write(record)
            writer.close()

        parser = etree.XMLParser(remove_blank_text=True)
        tree = etree.parse(str(file_path), parser)
        tree.write(str(file_path), encoding="utf-8", xml_declaration=True, pretty_print=True)
