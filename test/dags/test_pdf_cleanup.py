"""Tests for the PDF cleanup DAG."""

import pytest

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock

from test.util.dag_helper import get_dag

DAG = get_dag("pdf_cleanup")


class TestPdfCleanup:
    """Tests for the PDF cleanup DAG."""

    def test_calculate_cutoff(self, monkeypatch):
        """Calculate the cutoff using the configured cleanup window."""
        task = DAG.get_task("calculate_cutoff").python_callable

        monkeypatch.setitem(
            task.__globals__,
            "get_current_context",
            lambda: {
                "params": {
                    "cleanup_window": 30,
                },
            },
        )

        before = datetime.now(timezone.utc) - timedelta(days=30)
        result = datetime.fromisoformat(task())
        after = datetime.now(timezone.utc) - timedelta(days=30)

        assert before <= result <= after



    def test_find_eligible_runs(self, monkeypatch):
        """Return the IDs of completed runs eligible for cleanup."""
        task = DAG.get_task("find_eligible_runs").python_callable

        runs = ["run_1", "run_2"]
        find_cleanup_runs = Mock(return_value=runs)

        monkeypatch.setitem(
            task.__globals__,
            "find_cleanup_runs",
            find_cleanup_runs,
        )

        cutoff = "2026-09-14T12:00:00+00:00"

        result = task(cutoff)

        assert result == ["run_1", "run_2"]
        find_cleanup_runs.assert_called_once_with(
            datetime.fromisoformat(cutoff)
        )

    def test_find_run_directories(self, monkeypatch):
        """Retrieve recorded scratch directories using Airflow XCom."""
        task = DAG.get_task("find_run_directories").python_callable

        ti = Mock()
        ti.xcom_pull.side_effect = [
            "/srv/ocr/quiabo/run_1",
            "/srv/ocr/quiabo/run_2",
        ]

        monkeypatch.setitem(
            task.__globals__,
            "get_current_context",
            lambda: {"ti": ti},
        )

        result = task(["run_1", "run_2"])

        assert result == [
            {
                "run_id": "run_1",
                "run_directory": "/srv/ocr/quiabo/run_1",
            },
            {
                "run_id": "run_2",
                "run_directory": "/srv/ocr/quiabo/run_2",
            },
        ]

        assert ti.xcom_pull.call_count == 2

        ti.xcom_pull.assert_any_call(
            dag_id="pdf_creation",
            run_id="run_1",
            task_ids="prepare_run_directory",
            key="return_value",
            include_prior_dates=False,
        )

        ti.xcom_pull.assert_any_call(
            dag_id="pdf_creation",
            run_id="run_2",
            task_ids="prepare_run_directory",
            key="return_value",
            include_prior_dates=False,
        )

    def test_find_run_directories_skips_missing_directory(self, monkeypatch):
        """Skip runs that have no recorded scratch directory in XCom."""
        task = DAG.get_task("find_run_directories").python_callable

        ti = Mock()
        ti.xcom_pull.side_effect = [
            "/srv/ocr/quiabo/run_1",
            None,
            "/srv/ocr/quiabo/run_3",
        ]

        monkeypatch.setitem(
            task.__globals__,
            "get_current_context",
            lambda: {"ti": ti},
        )

        result = task(["run_1", "run_2", "run_3"])

        assert result == [
            {
                "run_id": "run_1",
                "run_directory": "/srv/ocr/quiabo/run_1",
            },
            {
                "run_id": "run_3",
                "run_directory": "/srv/ocr/quiabo/run_3",
            },
        ]

        assert ti.xcom_pull.call_count == 3


    def test_validate_run_directories(self, monkeypatch, tmp_path):
        """Return recorded scratch directories that are safe to remove."""
        task = DAG.get_task("validate_run_directories").python_callable

        scratch_base = tmp_path / "scratch"
        scratch_base.mkdir()

        run_1 = scratch_base / "run_1"
        run_2 = scratch_base / "run_2"
        run_1.mkdir()
        run_2.mkdir()

        variable = Mock()
        variable.get.return_value = str(scratch_base)

        monkeypatch.setitem(
            task.__globals__,
            "Variable",
            variable,
        )

        run_directories = [
            {
                "run_id": "run_1",
                "run_directory": str(run_1),
            },
            {
                "run_id": "run_2",
                "run_directory": str(run_2),
            },
        ]

        result = task(run_directories)

        assert result == run_directories

    def test_validate_run_directories_rejects_unsafe_directory(self, monkeypatch):
        """Reject recorded scratch directories that are unsafe to remove."""
        task = DAG.get_task("validate_run_directories").python_callable

        variable = Mock()
        variable.get.return_value = "/srv/ocr/quiabo"

        monkeypatch.setitem(
            task.__globals__,
            "Variable",
            variable,
        )

        run_directories = [
            {
                "run_id": "run_1",
                "run_directory": "/srv/ocr/quiabo/run_1",
            },
            {
                "run_id": "run_2",
                "run_directory": "/srv/da/oh_god_no",
            },
        ]

        with pytest.raises(
            ValueError,
            match="Unexpected scratch directory",
        ):
            task(run_directories)



    def test_remove_run_directories(self, monkeypatch, tmp_path):
        """Remove each validated scratch directory."""
        task = DAG.get_task("remove_run_directories").python_callable

        scratch_base = tmp_path / "scratch"
        scratch_base.mkdir()

        run_1 = scratch_base / "run_1"
        run_2 = scratch_base / "run_2"
        run_1.mkdir()
        run_2.mkdir()

        monkeypatch.setitem(
            task.__globals__,
            "get_current_context",
            lambda: {"params": {"dry_run": False}},
        )

        variable = Mock()
        variable.get.return_value = str(scratch_base)

        monkeypatch.setitem(
            task.__globals__,
            "Variable",
            variable,
        )

        remove_run_directory = Mock()
        monkeypatch.setitem(
            task.__globals__,
            "remove_run_directory",
            remove_run_directory,
        )

        run_directories = [
            {
                "run_id": "run_1",
                "run_directory": str(run_1),
            },
            {
                "run_id": "run_2",
                "run_directory": str(run_2),
            },
        ]

        task(run_directories)

        assert remove_run_directory.call_count == 2
        remove_run_directory.assert_any_call(str(run_1))
        remove_run_directory.assert_any_call(str(run_2))


    def test_validate_cleanup_directory_rejects_outside_directory(self, tmp_path):
        """Reject directories outside the configured scratch base."""
        task = DAG.get_task("validate_run_directories").python_callable
        validate_cleanup_directory = task.__globals__["validate_cleanup_directory"]

        scratch_base = tmp_path / "scratch"
        scratch_base.mkdir()

        outside_base = tmp_path / "outside"
        outside_base.mkdir()

        run_id = "run_123"
        outside_directory = outside_base / run_id
        outside_directory.mkdir()

        with pytest.raises(ValueError, match="Unexpected scratch directory"):
            validate_cleanup_directory(outside_directory, scratch_base, run_id)

        # The directory must remain untouched.
        assert outside_directory.is_dir()



    def test_remove_run_directories_dry_run(self, monkeypatch, tmp_path):
        """Do not remove scratch directories when dry-run mode is enabled."""
        task = DAG.get_task("remove_run_directories").python_callable

        scratch_base = tmp_path / "scratch"
        scratch_base.mkdir()

        run_directory = scratch_base / "run_1"
        run_directory.mkdir()

        monkeypatch.setitem(
            task.__globals__,
            "get_current_context",
            lambda: {"params": {"dry_run": True}},
        )

        variable = Mock()
        variable.get.return_value = str(scratch_base)

        monkeypatch.setitem(
            task.__globals__,
            "Variable",
            variable,
        )

        remove_run_directory = Mock()
        monkeypatch.setitem(
            task.__globals__,
            "remove_run_directory",
            remove_run_directory,
        )

        run_directories = [
            {
                "run_id": "run_1",
                "run_directory": str(run_directory),
            },
        ]

        task(run_directories)

        remove_run_directory.assert_not_called()
        assert run_directory.exists()

    def test_find_run_directories_rejects_invalid_xcom(self, monkeypatch):
        """Reject unexpected XCom values instead of treating them as paths."""
        task = DAG.get_task("find_run_directories").python_callable

        ti = Mock()
        ti.xcom_pull.return_value = {"path": "/srv/ocr/quiabo/run_1"}

        monkeypatch.setitem(
            task.__globals__,
            "get_current_context",
            lambda: {"ti": ti},
        )

        with pytest.raises(
            ValueError,
            match="Unexpected scratch directory XCom value",
        ):
            task(["run_1"])

    def test_remove_run_directories_rejects_symlink(self, monkeypatch, tmp_path):
        """Reject a directory replaced by a symlink before deletion."""
        task = DAG.get_task("remove_run_directories").python_callable

        scratch_base = tmp_path / "scratch"
        scratch_base.mkdir()

        important_files = tmp_path / "important_files"
        important_files.mkdir()
        (important_files / "keep_me.txt").write_text("Do not delete!")

        run_directory = scratch_base / "run_1"
        run_directory.mkdir()

        # Simulate the directory being replaced after initial validation.
        run_directory.rmdir()
        run_directory.symlink_to(important_files, target_is_directory=True)

        monkeypatch.setitem(
            task.__globals__,
            "get_current_context",
            lambda: {"params": {"dry_run": False}},
        )

        variable = Mock()
        variable.get.return_value = str(scratch_base)

        monkeypatch.setitem(
            task.__globals__,
            "Variable",
            variable,
        )

        remove_run_directory = Mock()
        monkeypatch.setitem(
            task.__globals__,
            "remove_run_directory",
            remove_run_directory,
        )

        run_directories = [
            {
                "run_id": "run_1",
                "run_directory": str(run_directory),
            },
        ]

        with pytest.raises(
            ValueError,
            match="Unsafe scratch directory",
        ):
            task(run_directories)

        remove_run_directory.assert_not_called()
        assert run_directory.is_symlink()
        assert (important_files / "keep_me.txt").exists()
