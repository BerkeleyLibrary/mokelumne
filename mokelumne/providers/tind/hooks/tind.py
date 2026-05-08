"""Provides a TindHook to interact with a TINDClient instance using Airflow Connections."""

from __future__ import annotations

import logging
from functools import cached_property
from os import environ as ENV
from typing import Any

import requests
from airflow.sdk import BaseHook
from piffle.image import IIIFImageClient
from piffle.load_iiif import load_iiif_presentation
from tind_client import TINDClient

from mokelumne.util.storage import record_dir

logger = logging.getLogger(__name__)


class TindHook(BaseHook):
    """
    Interact with a TINDClient instance.

    :param conn_id: Airflow Connection ID `tind_default` or a Connection object with
    the TIND API URL as the host and the API key as the password.
    """

    conn_type = "tind"
    conn_name_attr = "conn_id"
    default_conn_name = "tind_default"
    hook_name = "TIND"

    def __init__(self, conn_id: str = "tind_default") -> None:
        super().__init__()
        self.conn_id = conn_id

    def get_conn(self) -> TINDClient:
        """Return a new TINDClient build from the Airflow Connection."""
        connection = self.get_connection(self.conn_id)
        return TINDClient(
            api_url=connection.host,
            api_key=connection.password,
        )

    @cached_property
    def conn(self) -> TINDClient:
        """Return a cached TINDClient instance."""
        return self.get_conn()

    def test_connection(self) -> tuple[bool, str]:
        """Test the connection to the TIND API."""
        try:
            # need a better connection test call
            self.conn.fetch_ids_search("farm mokelumne")
            return True, "Connection successful!"
        except Exception as e:
            return False, str(e)

    def get_ids(self, tind_query: str) -> list[str]:
        """Return the TIND IDs that match a given query."""
        return self.conn.fetch_ids_search(tind_query)

    def get_file_metadata(self, tind_id: str) -> list[dict[str, Any]]:
        """Return the file metadata for a given TIND ID."""
        record = self.conn.fetch_file_metadata(tind_id)
        if not record:
            return []
        return record

    def get_first_file_metadata(self, tind_id: str) -> dict[str, Any]:
        """Return the file metadata for a given TIND ID."""
        metadata = self.get_file_metadata(tind_id)
        if len(metadata) == 0:
            return {}
        return metadata[0]

    def download_image_file(self, tind_id: str, run_id: str) -> str:
        """Download the first file attachment for a given TIND ID."""
        metadata = self.get_first_file_metadata(tind_id)
        download_url = metadata["url"]
        record_path = record_dir(run_id, tind_id)
        return self.conn.fetch_file(download_url, str(record_path))

    def download_image_from_record_sized(
        self, tind_id: str, run_id: str, width: int, height: int
    ) -> str:
        """Download the first image for a given TIND ID with the given size.

        :param tind_id: The TIND record ID.
        :param run_id: The ID of the run.
        :param int width: The desired width of the image in pixels.
        :param int height: The desired height of the image in pixels.
        :returns: The path where the file was saved.
        """
        url = ENV.get(
            "TIND_IIIF_MANIFEST_URL_PATTERN",
            "https://digicoll.lib.berkeley.edu/record/{tind_id}/export/iiif_manifest",
        ).format(tind_id=tind_id)

        manifest = load_iiif_presentation(url)
        canvases = len(manifest.items)
        if canvases != 1:
            logger.warning(
                "%s: manifest has invalid number of canvases: %d; crash may follow",
                tind_id,
                canvases,
            )

        # Manifest -> Canvas -> AnnotationPage -> Annotation -> Image
        image_id = manifest.items[0].items[0].items[0].body["service"][0]["id"]
        image = IIIFImageClient(*image_id.rsplit("/", 1))

        data = requests.get(
            str(image.size(width=width, height=height, exact=True)), timeout=30
        )
        data.raise_for_status()

        output_path = record_dir(run_id, tind_id) / image.image_id
        with output_path.open("wb") as out_f:
            for chunk in data.iter_content():
                out_f.write(chunk)

        return str(output_path)

    def write_query_results_to_xml(
        self, tind_query: str, file_name: str = "", output_dir: str = ""
    ) -> int:
        """Download the XML results of a search query from TIND."""
        records_written = self.conn.write_search_results_to_file(
            tind_query, file_name, output_dir=output_dir
        )
        return int(records_written)
