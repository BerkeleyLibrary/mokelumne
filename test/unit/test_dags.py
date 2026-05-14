"""
PyTest cases for Mokelumne DAGs
"""

import pytest

from airflow.dag_processing.dagbag import DagBag

from mokelumne.dags.gen_llm_image_descriptions import gen_llm_image_descriptions

@pytest.fixture
def gen_llm_image_descriptions(dagbag: DagBag):
    return dagbag.dags['gen_llm_image_descriptions']

def _downstream(dag, task_id: str) -> set[str]:
    return dag.get_task(task_id).downstream_task_ids

def test_dags_load_with_no_errors(dagbag: DagBag) -> None:
    assert dagbag.import_errors == {}, \
        ("Error(s) during Dag import: %s" % dagbag.import_errors)
    assert dagbag.size() == 1

class TestGenLlmImageDescriptionsDag:
    def test_tasks_are_wired_properly(self, gen_llm_image_descriptions) -> None:
        assert _downstream(gen_llm_image_descriptions, "fetch_tind_records.write_query_results_to_xml") == {
            "tind_filter.filter_records",
        }
        assert _downstream(gen_llm_image_descriptions, "tind_filter.filter_records") == {
            "fetch_images.read_csv_to_process",
            "summarise_job.collate_csvs",
        }
        assert _downstream(gen_llm_image_descriptions, "fetch_images.write_status_to_fetched_csv") == {
            "generate_descriptions.get_prompt",
            "generate_descriptions.read_and_batch_csv",
            "summarise_job.collate_csvs",
        }
        assert _downstream(gen_llm_image_descriptions, "generate_descriptions.write_output_csv") == {
            "summarise_job.collate_csvs",
        }
        assert _downstream(gen_llm_image_descriptions, "summarise_job.generate_summary") == {
            "notify_user.render_email_template",
        }
