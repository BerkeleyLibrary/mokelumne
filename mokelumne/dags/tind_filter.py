"""Filter TIND records on conditions matching the batch image processing workflow."""

from __future__ import annotations

import logging
from pathlib import Path

from pymarc.marcxml import map_xml
from airflow.sdk import dag, task, Asset, get_current_context

from mokelumne.batch_image.assets import records_xml, to_process_csv, skipped_csv
from mokelumne.util.tind_csv_writer import TindCsvWriter

logger = logging.getLogger(__name__)

@dag(
    schedule=[records_xml],
    catchup=False,
    tags=["tind", "filter"],
)
def tind_filter():
    """DAG that filters TIND records into to-process and skipped CSVs."""

    @task(inlets=[records_xml], outlets=[to_process_csv, skipped_csv])
    def filter_records() -> None:
        """Read TIND records from inlet XML asset, filter, and write two CSVs as outlet assets."""

        context = get_current_context()

        inlet_events = context["inlet_events"][records_xml]

        xml_file = Path(inlet_events[-1].asset.uri.replace("file://", ""))
        batch_dir = xml_file.parent

        with TindCsvWriter(batch_dir) as csv_writer:
            map_xml(csv_writer.process_tind_record, xml_file)

        logger.info(
            "Tind filter complete. to_process=%s (%s records), skipped=%s (%s records)",
            csv_writer.csv_p,
            csv_writer.count_p,
            csv_writer.csv_s,
            csv_writer.count_s
        )

        context["outlet_events"][to_process_csv].add(Asset(f"file://{csv_writer.csv_p}"))
        context["outlet_events"][skipped_csv].add(Asset(f"file://{csv_writer.csv_s}"))

    filter_records()


tind_filter()
