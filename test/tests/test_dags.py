import os

from pathlib import Path

import pytest

from airflow.dag_processing.dagbag import DagBag

dag_dir = Path(__file__).resolve().parent.parent.parent / "mokelumne/dags"

@pytest.fixture()
def dagbag() -> DagBag:
    return DagBag(dag_folder=dag_dir.resolve(), include_examples=False)


def test_dags_load_with_no_errors(dagbag: DagBag) -> None:
    assert dagbag.import_errors == {}, \
        ("Error(s) during Dag import: %s" % dagbag.import_errors)


def test_fetch_ldc_corpus_files_structure(dagbag: DagBag) -> None:
    """Structure test for fetch_ldc_corpus_files."""
    dag = dagbag.dags.get("fetch_ldc_corpus_files")
    assert dag is not None

    expected_task_ids = {
        "get_available_ldc_corpora",
        "parse_corpora_metadata",
        "corpus_is_available",
        "download_corpus_file",
    }
    assert expected_task_ids.issubset(set(dag.task_ids))

    assert dag.get_task("get_available_ldc_corpora").downstream_task_ids == {
        "parse_corpora_metadata"
    }

    assert dag.get_task("parse_corpora_metadata").downstream_task_ids == {"corpus_is_available"}
    assert dag.get_task("corpus_is_available").downstream_task_ids == {"download_corpus_file"}
