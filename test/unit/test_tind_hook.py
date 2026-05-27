"""Pytest testcases for mokelumne.providers.tind.hooks.tind.TindHook."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from mokelumne.providers.tind.hooks.tind import TindHook

SAMPLE_FILE_DOWNLOAD_URL = (
    "https://example.edu/api/v1/record/123/files/image_file.jpg/download/?version=1"
)

def test_download_image_file_passes_meta_mtime_when_modified_present(monkeypatch):
    """Ensure download_image_file parses and forwards modified as UTC datetime."""
    hook = TindHook()

    metadata_mock = MagicMock(
        return_value={
            "url": SAMPLE_FILE_DOWNLOAD_URL,
            "modified": "2026-01-21 01:10:46",
        }
    )
    conn = MagicMock()
    conn.fetch_file.return_value = "/tmp/12345/image_file.jpg"
    record_dir_mock = MagicMock(return_value="/tmp/12345")

    # bypass cached conn
    hook.__dict__["conn"] = conn

    monkeypatch.setattr(hook, "get_first_file_metadata", metadata_mock)
    monkeypatch.setattr(
        "mokelumne.providers.tind.hooks.tind.record_dir",
        record_dir_mock,
    )

    result = hook.download_image_file("12345", "manual__2026-05-27")

    assert result is conn.fetch_file.return_value
    metadata_mock.assert_called_once_with("12345")
    record_dir_mock.assert_called_once_with("12345")
    conn.fetch_file.assert_called_once_with(
        SAMPLE_FILE_DOWNLOAD_URL,
        "/tmp/12345",
        meta_mtime=datetime(2026, 1, 21, 1, 10, 46, tzinfo=timezone.utc),
    )


def test_download_image_file_omits_meta_mtime_when_modified_missing(monkeypatch):
    """Ensure download_image_file does not pass meta_mtime when modified is absent."""
    hook = TindHook()

    metadata_mock = MagicMock(
        return_value={
            "url": SAMPLE_FILE_DOWNLOAD_URL,
        }
    )
    conn = MagicMock()
    conn.fetch_file.return_value = "/tmp/12345/image_file.jpg"
    record_dir_mock = MagicMock(return_value="/tmp/12345")

    # bypass cached conn
    hook.__dict__["conn"] = conn

    monkeypatch.setattr(hook, "get_first_file_metadata", metadata_mock)
    monkeypatch.setattr(
        "mokelumne.providers.tind.hooks.tind.record_dir",
        record_dir_mock,
    )

    hook.download_image_file("12345", "manual__2026-05-27")

    conn.fetch_file.assert_called_once_with(
        SAMPLE_FILE_DOWNLOAD_URL,
        "/tmp/12345",
    )
