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

    def test_find_cleanup_runs(self):
        """Return PDF creation runs eligible for cleanup."""
        task = DAG.get_task("calculate_cutoff").python_callable
        find_cleanup_runs = task.__globals__["find_cleanup_runs"]

        cutoff = datetime(2026, 9, 14, tzinfo=timezone.utc)
        runs = [
            SimpleNamespace(run_id="run_1"),
            SimpleNamespace(run_id="run_2"),
        ]

        session = Mock()
        session.query.return_value.filter.return_value.all.return_value = runs

        result = find_cleanup_runs(cutoff, session=session)

        assert result == runs
        session.query.assert_called_once_with(
            task.__globals__["DagRun"]
        )

    def test_find_eligible_runs(self, monkeypatch):
        """Return the IDs of completed runs eligible for cleanup."""
        task = DAG.get_task("find_eligible_runs").python_callable

        runs = [
            SimpleNamespace(run_id="run_1"),
            SimpleNamespace(run_id="run_2"),
        ]
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

    def test_get_run_directory(self, monkeypatch):
        """Retrieve the recorded scratch directory for a PDF creation run."""
        task = DAG.get_task("calculate_cutoff").python_callable
        get_run_directory = task.__globals__["get_run_directory"]

        xcom = Mock()
        xcom.get_one.return_value = "/srv/ocr/quiabo/test_run"

        monkeypatch.setitem(
            get_run_directory.__globals__,
            "XCom",
            xcom,
        )

        result = get_run_directory("test_run")

        assert result == "/srv/ocr/quiabo/test_run"
        xcom.get_one.assert_called_once_with(
            key="return_value",
            dag_id="pdf_creation",
            task_id="prepare_run_directory",
            run_id="test_run",
        )

    def test_find_run_directories(self, monkeypatch):
        """Retrieve recorded scratch directories for eligible runs."""
        task = DAG.get_task("find_run_directories").python_callable

        get_run_directory = Mock(
            side_effect=[
                "/srv/ocr/quiabo/run_1",
                "/srv/ocr/quiabo/run_2",
            ]
        )

        monkeypatch.setitem(
            task.__globals__,
            "get_run_directory",
            get_run_directory,
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

    def test_find_run_directories_skips_missing_directory(self, monkeypatch):
        """Skip runs that do not have a recorded scratch directory."""
        task = DAG.get_task("find_run_directories").python_callable

        get_run_directory = Mock(
            side_effect=[
                "/srv/ocr/quiabo/run_1",
                None,
                "/srv/ocr/quiabo/run_3",
            ]
        )

        monkeypatch.setitem(
            task.__globals__,
            "get_run_directory",
            get_run_directory,
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

    def test_safe_run_directory(self):
        """Allow a run directory underneath the configured scratch base."""
        task = DAG.get_task("calculate_cutoff").python_callable
        safe_run_directory = task.__globals__["safe_run_directory"]

        result = safe_run_directory(
            "/srv/ocr/quiabo/test_run",
            "/srv/ocr/quiabo",
            "test_run",
        )

        assert result is True

    def test_safe_run_directory_rejects_scratch_base(self):
        """DO NOT, EVER, allow the scratch base itself to be removed."""
        task = DAG.get_task("calculate_cutoff").python_callable
        safe_run_directory = task.__globals__["safe_run_directory"]

        result = safe_run_directory(
            "/srv/ocr/quiabo",
            "/srv/ocr/quiabo",
            "test_run",
        )

        assert result is False

    def test_safe_run_directory_rejects_path_outside_scratch_base(self):
        """NEVER allow directories outside the scratch base to be removed."""
        task = DAG.get_task("calculate_cutoff").python_callable
        safe_run_directory = task.__globals__["safe_run_directory"]

        result = safe_run_directory(
            "/srv/da/important_stuff",
            "/srv/ocr/quiabo",
            "test_run",
        )

        assert result is False

    def test_safe_run_directory_rejects_path_traversal(self):
        """Do not allow some sneaky path that escapes the scratch base."""
        task = DAG.get_task("calculate_cutoff").python_callable
        safe_run_directory = task.__globals__["safe_run_directory"]

        result = safe_run_directory(
            "/srv/ocr/quiabo/test_run/../../../da/important_stuff",
            "/srv/ocr/quiabo",
            "test_run",
        )

        assert result is False

    def test_safe_run_directory_rejects_wrong_run_id(self):
        """Do not allow a directory that does not match the run ID."""
        task = DAG.get_task("calculate_cutoff").python_callable
        safe_run_directory = task.__globals__["safe_run_directory"]

        result = safe_run_directory(
            "/srv/ocr/quiabo/some_other_run",
            "/srv/ocr/quiabo",
            "test_run",
        )

        assert result is False

    def test_safe_run_directory_rejects_symlink(self, tmp_path):
        """Do not allow a run directory that is a symlink outside the scratch base."""
        task = DAG.get_task("calculate_cutoff").python_callable
        safe_run_directory = task.__globals__["safe_run_directory"]

        scratch_base = tmp_path / "scratch"
        scratch_base.mkdir()

        important_stuff = tmp_path / "important_stuff"
        important_stuff.mkdir()

        run_directory = scratch_base / "test_run"
        run_directory.symlink_to(important_stuff)

        result = safe_run_directory(
            str(run_directory),
            str(scratch_base),
            "test_run",
        )

        assert result is False        

    def test_validate_run_directories(self, monkeypatch):
        """Return recorded scratch directories that are safe to remove."""
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
                "run_directory": "/srv/ocr/quiabo/run_2",
            },
        ]

        result = task(run_directories)

        assert result == run_directories
        variable.get.assert_called_once_with("pdf_creation_scratch_base")

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
            match="Unsafe scratch directory",
        ):
            task(run_directories)

    def test_remove_run_directory(self, tmp_path):
        """Remove a scratch directory and its contents."""
        task = DAG.get_task("calculate_cutoff").python_callable
        remove_run_directory = task.__globals__["remove_run_directory"]

        run_directory = tmp_path / "test_run"
        run_directory.mkdir()

        test_file = run_directory / "page.tif"
        test_file.write_text("test data")

        remove_run_directory(str(run_directory))

        assert not run_directory.exists()

    def test_remove_run_directories(self, monkeypatch):
        """Remove each validated scratch directory."""
        task = DAG.get_task("remove_run_directories").python_callable

        remove_run_directory = Mock()
        monkeypatch.setitem(
            task.__globals__,
            "remove_run_directory",
            remove_run_directory,
        )

        run_directories = [
            {
                "run_id": "run_1",
                "run_directory": "/srv/ocr/quiabo/run_1",
            },
            {
                "run_id": "run_2",
                "run_directory": "/srv/ocr/quiabo/run_2",
            },
        ]

        task(run_directories)

        assert remove_run_directory.call_count == 2

    def test_remove_run_directory_skips_missing_directory(self, tmp_path, caplog):
        """Skip a scratch directory that has already been removed."""
        task = DAG.get_task("calculate_cutoff").python_callable
        remove_run_directory = task.__globals__["remove_run_directory"]

        run_directory = tmp_path / "already_gone"

        remove_run_directory(str(run_directory))

        assert "Scratch directory already removed" in caplog.text
