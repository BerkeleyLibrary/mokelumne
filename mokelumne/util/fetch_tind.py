"""Provides a helper class, FetchTind, to download information from TIND."""

import logging
from os import environ as ENV
from typing import Any

import requests
from piffle.image import IIIFImageClient
from piffle.load_iiif import load_iiif_presentation
from tind_client import TINDClient, TINDError

from mokelumne.util.storage import run_dir, record_dir


logger = logging.getLogger(__name__)
"""The TIND Fetcher logger."""


class FetchTind:
    """Helper methods for fetching items from TIND using TINDClient."""
    def __init__(self, _run_id: str):
        self.run_id = _run_id
        self.client = TINDClient(default_storage_dir=str(run_dir(_run_id)))

    def get_ids(self, tind_query: str) -> list[str]:
        """Return the TIND IDs that match a given query."""
        return self.client.fetch_ids_search(tind_query)

    def get_first_file_metadata(self, tind_id: str) -> dict[str, Any]:
        """Return the file metadata for a given TIND ID."""
        record = self.client.fetch_file_metadata(tind_id)
        if not record or not record[0]:
            return {}
        return record[0]

    def download_image_file(self, tind_id: str) -> str:
        """Download the first file attachment for a given TIND ID."""
        metadata = self.get_first_file_metadata(tind_id)
        download_url = metadata["url"]
        record_path = record_dir(self.run_id, tind_id)
        return self.client.fetch_file(download_url, str(record_path))

    def download_image_from_record_sized(self, tind_id: str, width: int, height: int) -> str:
        """Download the first image for a given TIND ID with the given size.

        :param tind_id: The TIND record ID.
        :param int width: The desired width of the image in pixels.
        :param int height: The desired height of the image in pixels.
        :returns: The path where the file was saved.
        """
        url = ENV.get(
            "TIND_IIIF_MANIFEST_URL_PATTERN",
            "https://digicoll.lib.berkeley.edu/record/{tind_id}/export/iiif_manifest"
        ).format(tind_id=tind_id)

        manifest = load_iiif_presentation(url)
        canvases = len(manifest.items)
        if canvases != 1:
            logger.warning("%s: manifest has invalid number of canvases: %d; crash may follow",
                           tind_id, canvases)

        # Manifest -> Canvas -> AnnotationPage -> Annotation -> Image
        image_id = manifest.items[0].items[0].items[0].body["service"][0]["id"]
        image = IIIFImageClient(*image_id.rsplit("/", 1))

        data = requests.get(str(image.size(width=width, height=height, exact=True)))
        data.raise_for_status()

        output_path = record_dir(self.run_id, tind_id) / image.image_id
        with output_path.open('wb') as out_f:
            for chunk in data.iter_content():
                out_f.write(chunk)

        return str(output_path)

    def write_query_results_to_xml(self, tind_query: str, file_name: str = "") -> int:
        """Download the XML results of a search query from TIND."""
        records_written = self.client.write_search_results_to_file(tind_query, file_name)
        return int(records_written)
