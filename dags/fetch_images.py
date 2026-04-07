"""Given a list of records, fetch their corresponding image and save to storage."""

import csv
import logging

from collections import namedtuple
from pathlib import Path
from typing import List

from airflow.sdk import dag, task, Asset, get_current_context

from mokelumne.util.storage import run_dir
from mokelumne.batch_image.assets import to_process_csv, fetched_csv

from mokelumne.util.fetch_tind import FetchTind

logger = logging.getLogger(__name__)

RunStatus = namedtuple('RunStatus', ('tind_id', 'status', 'path'))


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

        with csv_path.open('r', encoding='utf-8') as csv_file:
            reader = csv.reader(csv_file)
            for row in reader:
                record_ids.append(row[0])
                records[row[0]] = row
        return {"record_ids": record_ids[1:], "records": records,
                "original_run_id": original_run_id}

    @task
    def load_record_ids(processed: dict[str, List[str] | str]) -> List[str]:
        """Load the record IDs from the to_process job."""
        return processed["record_ids"]

    @task
    def load_records(processed: dict[str, List[str] | str | dict[str, list[str]]])\
            -> dict[str, list[str]]:
        """Load the records from the to_process job."""
        return processed["records"]

    @task
    def fetch_image_to_record_directory(orig_run_id: str, tind_id: str) -> RunStatus:
        """Fetch an image from TIND to the target record's storage directory."""
        try:
            client = FetchTind(orig_run_id)
            path = client.download_image_file(tind_id)
        except Exception as ex:  # pylint: disable=broad-exception-caught
            return RunStatus(tind_id=tind_id, status=f'failed: {str(ex)}', path='')

        return RunStatus(tind_id=tind_id, status='fetched', path=path)

    @task(outlets=[fetched_csv])
    def write_status_to_fetched_csv(orig_run_id: str, records: dict[str, list[str]],
                                    statuses: List[RunStatus]) -> None:
        """Write the status of processed records to a CSV file."""
        context = get_current_context()

        fetched_path = run_dir(orig_run_id) / 'fetched.csv'
        with fetched_path.open('w', encoding='utf-8') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow((*records['Record ID'], 'Image Path'))

            status_col = records['Record ID'].index('Status')

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
