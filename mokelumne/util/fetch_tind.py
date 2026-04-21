"""Provides a helper class, FetchTind, to download information from TIND."""

import csv

from tind_client import TINDClient

from mokelumne.util.storage import run_dir, record_dir


class FetchTind:
    """Helper methods for fetching items from TIND using TINDClient."""
    def __init__(self, _run_id: str):
        self.run_id = _run_id
        self.client = TINDClient(default_storage_dir=str(run_dir(_run_id)))

    def get_ids(self, tind_query: str) -> list[str]:
        """Return the TIND IDs that match a given query."""
        return self.client.fetch_ids_search(tind_query)

    def download_image_file(self, tind_id: str) -> str:
        """Download the first file attachment for a given TIND ID."""
        record = self.client.fetch_file_metadata(tind_id)
        download_url = record[0]["url"]
        record_path = record_dir(self.run_id, tind_id)
        return self.client.fetch_file(download_url, str(record_path))

    def write_query_results_to_xml(self, tind_query: str, file_name: str = "") -> int:
        """Download the XML results of a search query from TIND."""
        records_written = self.client.write_search_results_to_file(tind_query, file_name)
        return int(records_written)
