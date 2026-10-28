"""PyTest cases for the mokelumne.util.pdf_creation_cleanup_utils module."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock

import logging
import pytest

from mokelumne.util import pdf_creation_cleanup_utils


class TestPdfCreationCleanupUtils:
    def test_safe_run_directory(self, tmp_path):
        """Verify a valid scratch directory is safe to remove."""
        scratch_base = tmp_path / "scratch"
        scratch_base.mkdir()

        run_directory = scratch_base / "run_1"
        run_directory.mkdir()

        assert pdf_creation_cleanup_utils.safe_run_directory(
            str(run_directory),
            str(scratch_base),
            "run_1",
        )


    def test_safe_run_directory_rejects_scratch_base(self):
        """DO NOT, EVER, allow the scratch base itself to be removed."""

        result = pdf_creation_cleanup_utils.safe_run_directory(
            "/srv/ocr/quiabo",
            "/srv/ocr/quiabo",
            "test_run",
        )

        assert result is False


    def test_safe_run_directory_rejects_path_outside_scratch_base(self):
        """NEVER allow directories outside the scratch base to be removed."""
        result = pdf_creation_cleanup_utils.safe_run_directory(
            "/srv/da/important_stuff",
            "/srv/ocr/quiabo",
            "test_run",
        )

        assert result is False

    def test_safe_run_directory_rejects_path_traversal(self):
        """Do not allow some sneaky path that escapes the scratch base."""

        result = pdf_creation_cleanup_utils.safe_run_directory(
            "/srv/ocr/quiabo/test_run/../../../da/important_stuff",
            "/srv/ocr/quiabo",
            "test_run",
        )

        assert result is False

    def test_safe_run_directory_rejects_wrong_run_id(self):
        """Do not allow a directory that does not match the run ID."""

        result = pdf_creation_cleanup_utils.safe_run_directory(
            "/srv/ocr/quiabo/some_other_run",
            "/srv/ocr/quiabo",
            "test_run",
        )

        assert result is False

    def test_safe_run_directory_rejects_symlink(self, tmp_path):
        """Reject a scratch directory that is actually a symlink."""

        scratch_base = tmp_path / "scratch"
        scratch_base.mkdir()

        outside_directory = tmp_path / "important_files"
        outside_directory.mkdir()

        run_id = "run_123"
        run_directory = scratch_base / run_id

        # Make the apparent run directory point outside scratch.
        run_directory.symlink_to(outside_directory, target_is_directory=True)

        assert not pdf_creation_cleanup_utils.safe_run_directory(
            str(run_directory),
            str(scratch_base),
            run_id,
        )

    def test_find_cleanup_runs(self, monkeypatch):
        """Return completed PDF creation runs within the cleanup window."""
        base_url = "http://airflow:8080"

        session = Mock()
        session.__enter__ = Mock(return_value=session)
        session.__exit__ = Mock(return_value=False)

        session.get.return_value.json.return_value = {
            "dag_runs": [
                {
                    "dag_run_id": "old_success",
                    "state": "success",
                    "end_date": "2026-09-01T12:00:00Z",
                },
                {
                    "dag_run_id": "old_failed",
                    "state": "failed",
                    "end_date": "2026-09-02T12:00:00Z",
                },
                {
                    "dag_run_id": "recent_success",
                    "state": "success",
                    "end_date": "2026-10-01T12:00:00Z",
                },
                {
                    "dag_run_id": "still_running",
                    "state": "running",
                    "end_date": None,
                },
                {
                    "dag_run_id": "too_old",
                    "state": "success",
                    "end_date": "2026-07-01T12:00:00Z",
                },
            ],
            "next_cursor": None,
        }

        monkeypatch.setattr(
            pdf_creation_cleanup_utils,
            "get_api_session",
            Mock(return_value=(base_url, session)),
        )

        cutoff = datetime(2026, 9, 14, tzinfo=timezone.utc)
        lookback = datetime(2026, 8, 1, tzinfo=timezone.utc)

        result = pdf_creation_cleanup_utils.find_cleanup_runs(cutoff, lookback)

        assert result == ["old_success", "old_failed"]

        session.get.assert_called_once_with(
            f"{base_url}/api/v2/dags/pdf_creation/dagRuns",
            params={
                "limit": 100,
                "end_date_lte": cutoff.isoformat(),
                "end_date_gte": lookback.isoformat(),
            },
            timeout=30,
        )


    def test_find_cleanup_runs_pagination(self, monkeypatch):
        """Retrieve eligible PDF creation runs across multiple API pages."""
        base_url = "http://airflow:8080"

        session = Mock()
        session.__enter__ = Mock(return_value=session)
        session.__exit__ = Mock(return_value=False)

        session.get.return_value.json.side_effect = [
            {
                "dag_runs": [
                    {
                        "dag_run_id": "run_1",
                        "state": "success",
                        "end_date": "2026-09-01T12:00:00Z",
                    },
                ],
                "next_cursor": "page_2",
            },
            {
                "dag_runs": [
                    {
                        "dag_run_id": "run_2",
                        "state": "failed",
                        "end_date": "2026-09-02T12:00:00Z",
                    },
                ],
                "next_cursor": None,
            },
        ]

        monkeypatch.setattr(
            pdf_creation_cleanup_utils,
            "get_api_session",
            Mock(return_value=(base_url, session)),
        )

        cutoff = datetime(2026, 9, 14, tzinfo=timezone.utc)
        lookback = datetime(2026, 8, 1, tzinfo=timezone.utc)

        result = pdf_creation_cleanup_utils.find_cleanup_runs(cutoff, lookback)

        assert result == ["run_1", "run_2"]
        assert session.get.call_count == 2

        assert session.get.call_args_list[0].kwargs["params"] == {
            "limit": 100,
            "end_date_lte": cutoff.isoformat(),
            "end_date_gte": lookback.isoformat(),
        }

        assert session.get.call_args_list[1].kwargs["params"] == {
            "limit": 100,
            "end_date_lte": cutoff.isoformat(),
            "end_date_gte": lookback.isoformat(),
            "cursor": "page_2",
        }


    def test_remove_run_directory(self, tmp_path):
        """Remove a scratch directory and its contents."""
        run_directory = tmp_path / "test_run"
        run_directory.mkdir()

        test_file = run_directory / "page.tif"
        test_file.write_text("test data")

        pdf_creation_cleanup_utils.remove_run_directory(str(run_directory))

        assert not run_directory.exists()


    def test_remove_run_directory_skips_missing_directory(self, tmp_path, caplog):
        """Skip a scratch directory that has already been removed."""
        run_directory = tmp_path / "already_gone"

        with caplog.at_level(logging.INFO):
            pdf_creation_cleanup_utils.remove_run_directory(str(run_directory))

        assert "Scratch directory already removed" in caplog.text


    def test_get_api_session(self, monkeypatch):
        """Authenticate with the Airflow API and configure the bearer token."""
        from types import SimpleNamespace
        from airflow.sdk.bases.hook import BaseHook

        connection = SimpleNamespace(
            host="http://airflow",
            port=8080,
            login="test_user",
            password="test_password",
        )

        session = Mock()
        session.headers = {}
        session.post.return_value.json.return_value = {
            "access_token": "test_jwt_token",
        }

        monkeypatch.setattr(
            BaseHook,
            "get_connection",
            Mock(return_value=connection),
        )

        monkeypatch.setattr(
            pdf_creation_cleanup_utils.requests,
            "Session",
            Mock(return_value=session),
        )

        base_url, result_session = pdf_creation_cleanup_utils.get_api_session()

        assert base_url == "http://airflow:8080"
        assert result_session is session

        session.post.assert_called_once_with(
            "http://airflow:8080/auth/token",
            json={
                "username": "test_user",
                "password": "test_password",
            },
            timeout=10,
        )

        session.post.return_value.raise_for_status.assert_called_once()

        assert session.headers["Authorization"] == "Bearer test_jwt_token"

    def test_validate_cleanup_directory_rejects_symlink(self, tmp_path):
        """Reject symlinks during cleanup validation."""
        scratch_base = tmp_path / "scratch"
        scratch_base.mkdir()

        outside_directory = tmp_path / "important_files"
        outside_directory.mkdir()

        run_id = "run_123"
        run_directory = scratch_base / run_id
        run_directory.symlink_to(outside_directory, target_is_directory=True)

        with pytest.raises(ValueError, match="Unsafe scratch directory"):
            pdf_creation_cleanup_utils.validate_cleanup_directory(
                str(run_directory),
                str(scratch_base),
                run_id,
            )

        assert run_directory.is_symlink()

    @pytest.mark.parametrize(
        "run_id",
        ["..", ".", "../important_stuff", "nested/run"],
    )
    def test_expected_run_directory_rejects_invalid_run_id(self, tmp_path, run_id):
        """Reject run IDs that are not single directory names."""
        scratch_base = tmp_path / "scratch"
        scratch_base.mkdir()

        assert not pdf_creation_cleanup_utils.expected_run_directory(
            str(scratch_base / "test_run"),
            str(scratch_base),
            run_id,
        )
