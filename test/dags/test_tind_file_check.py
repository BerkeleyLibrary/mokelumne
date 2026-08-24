"""Tests for the tind_file_check DAG."""

from pathlib import Path

from test.util.dag_helper import get_dag


DAG = get_dag("tind_file_check")


class TestTindFileCheckDagStructure:
    """tests for the tind_file_check DAG."""

    def test_dag_loads(self):
        assert DAG is not None
        assert DAG.dag_id == "tind_file_check"

    def test_expected_tasks_exist(self):
        expected_task_ids = {
            "validated_source_dir",
            "retrieve_file_list",
            "has_files",
            "retrieve_856_filenames",
            "compare_to_tind",
            "write_results",
            "view_report_log",
        }

        assert expected_task_ids.issubset(set(DAG.task_ids))

    def test_compare_runs_after_filename_list(self):
        assert DAG.get_task("retrieve_856_filenames") in DAG.get_task("compare_to_tind").upstream_list

    def test_write_results_runs_after_compare(self):
        assert DAG.get_task("compare_to_tind") in DAG.get_task("write_results").upstream_list


class TestTindFileCheckTaskBehavior:
    """task-call tests for the DAG's core logic."""

    def test_compare_to_tind_deduplicates_and_splits_results(self):
        compare_task = DAG.get_task("compare_to_tind").python_callable

        results = compare_task(
            ["a.jpg", "a.jpg", "b.jpg", "c.jpg"],
            ["b.jpg", "d.jpg"],
            {"search_options": "Both"},
        )

        assert results["in_tind"] == ["b.jpg"]
        assert results["not_in_tind"] == ["a.jpg", "c.jpg"]

    def test_has_files_short_circuits_on_empty_list(self):
        short_circuit_task = DAG.get_task("has_files").python_callable

        assert short_circuit_task([], {"file_extension": "*"}) is False

    def test_has_files_allows_non_empty_list(self):
        short_circuit_task = DAG.get_task("has_files").python_callable

        assert short_circuit_task(["a.jpg"], {"file_extension": "*"}) is True

    def test_write_results_writes_report_file(self, tmp_path, monkeypatch):
        write_task = DAG.get_task("write_results").python_callable

        monkeypatch.setitem(
            write_task.__globals__,
            "get_current_context",
            lambda: {"dag": DAG, "run_id": "run-1"},
        )
        monkeypatch.setitem(
            write_task.__globals__,
            "static_files_run_dir",
            lambda _dag_id, _run_id: tmp_path,
        )

        report_path = write_task(
            {"in_tind": ["a.jpg"], "not_in_tind": ["b.jpg"]},
            {"search_options": "Both"},
        )

        assert report_path == str(Path(tmp_path) / "tind_report.txt")
        report_text = (tmp_path / "tind_report.txt").read_text(encoding="utf-8")
        assert "a.jpg" in report_text
        assert "b.jpg" in report_text
