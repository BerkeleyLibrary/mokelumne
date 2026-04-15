# pyright: reportTypedDictNotRequiredAccess=false

"""Given a list of records, fetch their corresponding image and save to storage."""

import csv
import logging
import os

from collections import namedtuple
from math import ceil, trunc
from pathlib import Path
from typing import List

from airflow.sdk import dag, task, Asset, get_current_context

from mokelumne.batch_image.assets import to_process_csv, fetched_csv
from mokelumne.util.fetch_tind import FetchTind
from mokelumne.util.storage import run_dir

logger = logging.getLogger(__name__)

RunStatus = namedtuple('RunStatus', ('tind_id', 'status', 'path'))

SUPPORTED_IMAGE_TYPES = {"image/jpeg", "image/png", "image/gif", "image/webp"}
"""The supported image MIME types we will fetch."""

SIZE_LIMIT: int = 3750000
"""The upper bound for a size of image in bytes that we will fetch."""


def base64_size(s: int | float) -> int:
    """Determine the base64-encoded size of an object given its original size.

    :param s: The original size of the object.
    :returns: The base64-encoded size of the object.
    :note: The result of this function has the same unit as its parameter.
        For example, passing 4.5 MiB will result in 8 (MiB).
    """
    return ceil((s / 3) * 4)


@dag(schedule=[to_process_csv], catchup=False, tags=["tind", "fetch", "batch-image"])
def fetch_images():
    """Fetches images from a filtered list of TIND records."""
    @task(inlets=[to_process_csv], multiple_outputs=True)
    def read_csv_to_process() -> dict[str, List[str] | str]:
        """Read the to_process.csv file to determine our target TIND records."""
        context = get_current_context()
        events = context["triggering_asset_events"][to_process_csv]

        csv_path = Path(events[0].asset.uri.replace("file://", ""))
        original_run_id = csv_path.parent.name
        record_ids: list[str] = []
        records: dict[str, list[str]] = {}

        with csv_path.open("r", encoding="utf-8") as csv_file:
            reader = csv.reader(csv_file)
            for row in reader:
                record_ids.append(row[0])
                records[row[0]] = row
        return {
            "record_ids": record_ids[1:],
            "records": records,
            "original_run_id": original_run_id,
        }

    @task
    def load_record_ids(processed: dict[str, List[str] | str]) -> List[str]:
        """Load the record IDs from the to_process job."""
        return processed["record_ids"]

    @task
    def load_records(
        processed: dict[str, List[str] | str | dict[str, list[str]]],
    ) -> dict[str, list[str]]:
        """Load the records from the to_process job."""
        return processed["records"]

    @task
    def fetch_image_to_record_directory(orig_run_id: str, tind_id: str) -> RunStatus:
        """Fetch an image from TIND to the target record's storage directory."""
        try:
            client = FetchTind.from_connection(orig_run_id, conn="tind_default")
            filemd = client.get_first_file_metadata(tind_id)
            if not filemd.get("mime") in SUPPORTED_IMAGE_TYPES:
                return RunStatus(
                    tind_id=tind_id,
                    path="",
                    status=f"skipped: Unsupported file type {filemd.get('mime')}",
                )

            target_width = width = filemd.get("width", 0)
            target_height = height = filemd.get("height", 0)
            size = filemd.get("size", 0)
            b64_size = base64_size(size)
            logger.warning("b64_size = %d, size = %d", b64_size, size)
            if b64_size > SIZE_LIMIT:
                factor = float(SIZE_LIMIT) / b64_size
                target_width *= factor
                target_height *= factor

            if target_width > 8000:
                factor = 8000.0 / width
                target_width *= factor
                target_height *= factor

            if target_height > 8000:
                factor = 8000.0 / height
                target_width *= factor
                target_height *= factor

            target_width = int(trunc(target_width))
            target_height = int(trunc(target_height))

            if (width != target_width) or (height != target_height):
                # downsample using IIIF
                path = client.download_image_from_record_sized(tind_id, target_width, target_height)

                # TIND resampling may cause the image to be larger than original.  recalculate.
                # in my testing, 290827 resampled to 5998x8000 went from 2.5 MB to 4.9 MB(!)
                new_size = os.stat(path).st_size
                b64_size = base64_size(new_size)
                if b64_size > SIZE_LIMIT:
                    factor = float(SIZE_LIMIT) / b64_size
                    target_width *= factor
                    target_height *= factor
                    target_width = int(trunc(target_width))
                    target_height = int(trunc(target_height))
                    # Only re-download if it actually does exceed the limit.
                    path = client.download_image_from_record_sized(tind_id, target_width, target_height)
            else:
                path = client.download_image_file(tind_id)
        except Exception as ex:  # pylint: disable=broad-exception-caught
            logger.warning("Fetcher encountered exception", exc_info=ex)
            return RunStatus(tind_id=tind_id, status=f'failed: {str(ex)}', path='')

        return RunStatus(tind_id=tind_id, status="fetched", path=path)

    @task(outlets=[fetched_csv])
    def write_status_to_fetched_csv(
        orig_run_id: str, records: dict[str, list[str]], statuses: List[RunStatus]
    ) -> None:
        """Write the status of processed records to a CSV file."""
        context = get_current_context()

        fetched_path = run_dir(orig_run_id) / "fetched.csv"
        with fetched_path.open("w", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow((*records["Record ID"], "Image Path"))

            status_col = records["Record ID"].index("Status")

            for status in statuses:
                record = [*records[status[0]], *status[2:]]
                record[status_col] = status[1]
                writer.writerow(record)

        context["outlet_events"][fetched_csv].add(Asset(f"file://{fetched_path}"))

    processed = read_csv_to_process()
    orig_run_id = processed["original_run_id"]
    fetch_partial = fetch_image_to_record_directory.partial(orig_run_id=orig_run_id)
    results = fetch_partial.expand(tind_id=load_record_ids(processed))
    write_status_to_fetched_csv(orig_run_id, load_records(processed), results)


fetch_images()
