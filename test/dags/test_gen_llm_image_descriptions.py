"""Test the gen_llm_image_descriptions DAG."""

from pathlib import Path
from shutil import copyfile
from typing import Callable, Optional

from airflow.dag_processing.dagbag import DagBag
from mokelumne.dags import gen_llm_image_descriptions
from test.unit.test_langfuse import MockLangfuse


FIXTURE_PATH: Path = Path(__file__).parent.parent / "fixtures"
"""The directory that contains our fixtures."""


DAG_DIR: Path = Path(__file__).resolve().parent.parent.parent / "mokelumne" / "dags"
"""The location of the DAGs."""


class MockTindHook:
    """Implements a TindHook that we can manipulate for test purposes."""
    def __init__(self):
        pass

    def write_query_results_to_xml(self, query: str, filename: str, output_dir: Path) -> int:
        """Pretend to write query results."""
        assert query == 'Test query, do not use'
        copyfile(FIXTURE_PATH / 'bulk_query_success.xml', output_dir / filename)
        return 3


class MockImageFetcher:
    """Implements an ImageFetcher that we can manipulate for test purposes."""

    def __init__(self, client,
                 max_w: Optional[int] = None,
                 max_h: Optional[int] = None,
                 max_size: Optional[int] = None,
                 size_transform: Callable[[int | float], int] = None):
        self.client = client
        self.max_w = max_w
        self.max_h = max_h
        self.max_size = max_size
        self.size_transform = size_transform

    def fetch_one_image_for_record(self, _record_id: str,
                                   _run_id: str,
                                   _index: int = 0) -> Optional[Path]:
        """Pretend to fetch an image for a record."""
        return FIXTURE_PATH / 'test1.jpg'

    def fetch_images_for_record(self, _record_id: str, _run_id: str) -> list[Optional[Path]]:
        """Pretend to fetch multiple images for a record."""
        return [FIXTURE_PATH / 'test1.jpg', FIXTURE_PATH / 'test2.jpg']


class MockImageDescriber:
    """Implements an ImageDescriber that we can manipulate for test purposes."""

    def __init__(self, model, prompt: str):
        self.model = model
        self.prompt = prompt

    def describe(self, record: dict[str, str]) -> dict[str, str]:
        """Pretend to describe the image described in +record+."""
        record["Status"] = "success"
        record["Description"] = "An example description of an image."
        return record


class TestGenLLMImageDescriptionsDAG:
    """Test the gen_llm_image_descriptions DAG."""

    def test_dag_tasks(self):
        """Ensure tasks have the expected downstreams."""

        definitions: dict[str, set[str]] = {
            "fetch_tind_records.validate_params": {"fetch_tind_records.write_query_results_to_xml"},
            "fetch_tind_records.write_query_results_to_xml": {"tind_filter.filter_records"},
            "tind_filter.filter_records": {"fetch_images.read_csv_to_process",
                                           "summarise_job.collate_csvs"},
            "fetch_images.read_csv_to_process": {"fetch_images.load_records",
                                                 "fetch_images.load_record_ids"},
            "fetch_images.load_records": {"fetch_images.write_status_to_fetched_csv"},
            "fetch_images.load_record_ids": {"fetch_images.fetch_image_to_record_directory"},
            "fetch_images.fetch_image_to_record_directory": {"fetch_images.write_status_to_fetched_csv"},
            "fetch_images.write_status_to_fetched_csv": {"generate_descriptions.read_and_batch_csv",
                                                         "summarise_job.collate_csvs"},
            "generate_descriptions.read_and_batch_csv": {"generate_descriptions.invoke_llm_on_batch_with_prompt",
                                                         "generate_descriptions.write_output_csv"},
            "generate_descriptions.get_prompt": {"generate_descriptions.invoke_llm_on_batch_with_prompt",
                                                 "generate_descriptions.transform_results"},
            "generate_descriptions.invoke_llm_on_batch_with_prompt": {"generate_descriptions.transform_results"},
            "generate_descriptions.transform_results": {"generate_descriptions.write_output_csv"},
            "generate_descriptions.write_output_csv": {"summarise_job.collate_csvs"},
            "summarise_job.generate_id": {"summarise_job.collate_csvs", "summarise_job.generate_summary"},
            "summarise_job.collate_csvs": {"summarise_job.generate_summary"},
            "summarise_job.generate_summary": {"notify_user.render_email_template"},
            "notify_user.render_email_template": {"notify_user.send_email"},
            "notify_user.send_email": set()
        }
        dag = gen_llm_image_descriptions.gen_llm_image_descriptions()

        assert dag.task_dict.keys() == definitions.keys()
        for task_id, downstream_list in definitions.items():
            assert dag.has_task(task_id)
            task = dag.get_task(task_id)
            assert task.downstream_task_ids == downstream_list

    def test_full_run(self, monkeypatch):
        """Test a full end-to-end run of the DAG."""
        dagbag = DagBag(dag_folder=DAG_DIR.resolve(), include_examples=False)
        dag = dagbag.get_dag("gen_llm_image_descriptions")
        # Any task would work; this is the shortest by length.
        module_name = dag.get_task("fetch_images.load_records").python_callable.__module__
        monkeypatch.setattr(f"{module_name}.TindHook", MockTindHook)
        #monkeypatch.setattr(f"{module_name}.langfuse.Langfuse", MockLangfuse)
        monkeypatch.setattr(f"{module_name}.ImageFetcher", MockImageFetcher)
        monkeypatch.setattr(f"{module_name}.ImageDescriber", MockImageDescriber)
        dag.test(use_executor=False, run_conf={'tind_query': 'Test query, do not use'})
