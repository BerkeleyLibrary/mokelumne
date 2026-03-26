from __future__ import annotations

import logging
import sys
from pathlib import Path

from pymarc.marcxml import map_xml
from airflow.sdk import Param, dag, task

sys.path.insert(0, str(Path(__file__).resolve().parent))
from helpers.tind_filter_util import TindCsvWriter

logger = logging.getLogger(__name__)

@dag(
    schedule=None,
    catchup=False,
    params={
        "batch_dir_from_fetch_tind_dag": Param("/opt/airflow/download", type="string")
    },
    tags=["tind", "filter"],
)
def tind_filter():
    @task
    def filter_records(batch_dir: str) -> str:
        xml_file = f"{batch_dir}/search.xml"

        with TindCsvWriter(batch_dir) as csv_writer:
            map_xml(csv_writer.process_tind_record, xml_file)

        logger.info(
            "Tind filter complete. to_process=%s (%s records), skipped=%s (%s records)",
            csv_writer.csv_p,
            csv_writer.count_p,
            csv_writer.csv_s,
            csv_writer.count_s
        )

        return str(csv_writer.csv_p)

    filter_records("{{ params.batch_dir_from_fetch_tind_dag }}")

tind_filter()
