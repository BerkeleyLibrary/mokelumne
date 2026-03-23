from __future__ import annotations

import logging
import sys
from pathlib import Path
from pymarc.marcxml import parse_xml

from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, dag, task

sys.path.insert(0, str(Path(__file__).resolve().parent))
from helpers.tind_xml_handler import TindXmlHandler

logger = logging.getLogger(__name__)

# Note: 
# 1. The parameter "batch_dir_from_fetch_tind_dag" will be replaced with something else
#    provided by an upstream DAG or task.
# 2. Currently, the DAG returns the file path of the "to_processed.csv" file.
#    This will be updated based on final requirements.
@dag(
    schedule=None,
    catchup=False,
    params={
             "batch_dir_from_fetch_tind_dag": Param("/opt/airflow/download/manual__2026-03-12T18:51:57.880502+00:00", type="string")
    },
    tags=["tind", "filter"],
)
def tind_filter():
    @task
    def validate_inputs(**context) -> dict[str, str]:
        batch_dir = context['params'].get('batch_dir_from_fetch_tind_dag')
        xml_file = f"{batch_dir}/search.xml"

        xml_path = Path(xml_file)
        batch_path = Path(batch_dir)

        # The fetch dag will handle the case when tind xml returns no records or error?
        if not xml_path.exists() or not xml_path.is_file():
            raise AirflowFailException(f"XML file not found: {xml_path}")

        if not batch_path.exists() or not batch_path.is_dir():
            raise AirflowFailException(f"Batch directory not found: {batch_path}")

        return {
            "xml_file": xml_file,
            "batch_dir": batch_dir
        }

    @task
    def filter_records(inputs: dict[str, str]) -> dict[str,str,int,int]:
        xml_path = Path(inputs["xml_file"])
        batch_dir = inputs["batch_dir"]

        with TindXmlHandler(batch_dir) as handler:
            parse_xml(xml_path, handler)

        result = {
            "to_process_file": str(handler.csv_p),
            "skipped_file": str(handler.csv_s),
            "to_process_count": handler.count_p,
            "skipped_count": handler.count_s,
        }

        to_process_file_path = Path(result["to_process_file"])
        to_process_file_path = Path(result["skipped_file"])
        if not to_process_file_path.exists() or not to_process_file_path.exists():
            raise AirflowFailException(
                f"Expected output CSVs were not written: {str(to_process_file_path)}, {str(to_process_file_path)}"
            )

        return result

    @task
    def outputs(result: dict[str, str | int]) -> str:
        file_p = result["to_process_file"]
        logger.info(
            "Tind Filter complete. to_proces_file =%s (%s records), skipped=%s (%s records)" %(
            file_p,
            result["to_process_count"],
            result["skipped_file"],
            result["skipped_count"])
        )

        return file_p

    inputs = validate_inputs()
    outputs(filter_records(inputs))


tind_filter()
