"""Collate the CSV output of the jobs and upload them to an object store."""

from datetime import datetime, UTC
from itertools import filterfalse
from pathlib import Path
from shutil import copyfile

import csv
import json

from airflow.sdk import dag, task, get_current_context, Asset, DAG

from mokelumne.batch_image.assets import processed_csv
from mokelumne.batch_image.assets import public_dir as public_dir_asset
from mokelumne.util.storage import public_dir, public_path_to_url


FILENAME_TEMPLATE: str = 'imagedesc_%s_{timestamp}.csv'
"""The template for the generated filenames."""


@dag(schedule=[processed_csv], catchup=False, tags=["batch-image", "csv", "summary"])
def summarise_job():
    """Summarise the DAG runs for the job."""

    @task
    def generate_id(dag: DAG, run_id: str) -> str:
        """Creates the local storage path for the current DAG run"""
        path = public_dir() / dag.dag_id / run_id
        path.mkdir(exist_ok=True, parents=True)
        # Cast to str because AirFlow can't serialize Path
        return str(path)

    @task(inlets=[processed_csv])
    def collate_csvs(out_str: str) -> str:
        """Gather the CSVs from the DAG runs and place them in the given directory."""
        context = get_current_context()
        asset = context['triggering_asset_events'][processed_csv][0].asset
        asset_path = Path(asset.uri.replace("file://", "")).parent
        out_path = Path(out_str)

        timestamp = datetime.now(UTC).strftime('%Y%m%d_%H%M%S')
        template_name = FILENAME_TEMPLATE.format(timestamp=timestamp)

        for csv in ('processed', 'fetched', 'skipped'):
            copyfile(asset_path / f'{csv}.csv', out_path / (template_name % csv))
        copyfile(asset_path / 'params.json', out_path / 'params.json')

        return timestamp

    @task(outlets=[public_dir_asset])
    def generate_summary(collated_str: str, timestamp: str) -> str:
        """Generate a summary of the files in the collated path."""
        collated_path = Path(collated_str)

        template_name = FILENAME_TEMPLATE.format(timestamp=timestamp)
        proc_path = collated_path / (template_name % 'processed')
        fetched_path = collated_path / (template_name % 'fetched')
        skipped_path = collated_path / (template_name % 'skipped')
        with (collated_path / 'params.json').open(encoding='utf-8') as fp:
            params = json.load(fp)

        with skipped_path.open(encoding='utf-8') as skipped:
            skip_count = len(skipped.readlines()) - 1

        with fetched_path.open(encoding='utf-8') as fetched:
            reader = csv.reader(fetched)
            header = next(reader)
            all_fetched = list(reader)
            fetch_count = len(all_fetched)
            status_col = header.index('Status')
            fetch_failures = len(list(filterfalse(lambda x: x == 'fetched',
                                                  (row[status_col] for row in all_fetched))))
            fetch_success = fetch_count - fetch_failures

        with proc_path.open(encoding='utf-8') as processed:
            reader = csv.reader(processed)
            header = next(reader)
            all_processed = list(reader)
            proc_count = len(all_processed)
            status_col = header.index('Status')
            proc_failures = len(list(filterfalse(lambda x: x == 'success',
                                                 (row[status_col] for row in all_processed))))

        template_html = f"""
        <html><head><title>Batch image description results</title></head><body>
        <h1>Batch image description results</h1>
        <p>Query: {params['tind_query']}</p>
        <dl>
            <dt><a href="{public_path_to_url(proc_path)}">{proc_count} images processed</a></dt>
            <dd>{proc_count - proc_failures} succeeded, {proc_failures} failed</dd>
            <dt><a href="{public_path_to_url(fetched_path)}">{fetch_count} images fetched</a></dt>
            <dd>{fetch_success} succeeded, {fetch_failures} failed</dd>
            <dt><a href="{public_path_to_url(skipped_path)}">{skip_count} records skipped</a></dt>
            <dd>Records did not match filter criteria</dd>
        </dl>
        </body></html>
        """

        with (collated_path / 'index.html').open('w') as html:
            html.write(template_html)

        context = get_current_context()
        context["outlet_events"][public_dir_asset].add(Asset(f"file://{collated_str}"))

        return collated_str

    directory = generate_id()
    csv_timestamp = collate_csvs(directory)
    generate_summary(directory, csv_timestamp)


summarise_job()
