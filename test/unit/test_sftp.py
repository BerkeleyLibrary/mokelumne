"""Unit tests for SFTP download helpers."""

from __future__ import annotations

import errno
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Callable, Iterator
from zoneinfo import ZoneInfo

import pytest

from mokelumne.util import sftp
from mokelumne.util.sftp import (
    DownloadStatus,
    download_sftp_file,
    gobi_filename_for,
    lbnl_filename_for,
    require_host_key_verification,
    validate_gobi_filename,
    validate_lbnl_filename,
    validate_lbnl_prefix,
)


class FakeSFTPSession:
    """Minimal in-memory stand-in for Paramiko's SFTP client."""

    def __init__(
        self,
        payload: bytes = b"record data",
        *,
        modified_at: datetime | None = None,
        stat_error: OSError | None = None,
        get_error: OSError | None = None,
        after_get: Callable[[Path], None] | None = None,
        reported_size: int | None = None,
    ) -> None:
        self.payload = payload
        self.modified_at = modified_at or datetime(2026, 9, 28, tzinfo=UTC)
        self.stat_error = stat_error
        self.get_error = get_error
        self.after_get = after_get
        self.reported_size = (
            len(payload) if reported_size is None else reported_size
        )
        self.stat_calls: list[str] = []
        self.get_calls: list[tuple[str, str]] = []

    def stat(self, remote_path: str) -> SimpleNamespace:
        """Return configured remote metadata."""

        self.stat_calls.append(remote_path)
        if self.stat_error is not None:
            raise self.stat_error
        return SimpleNamespace(
            st_size=self.reported_size,
            st_mtime=self.modified_at.timestamp(),
        )

    def get(self, remote_path: str, local_path: str) -> None:
        """Write the configured payload to the requested path."""

        self.get_calls.append((remote_path, local_path))
        path = Path(local_path)
        path.write_bytes(self.payload)
        if self.after_get is not None:
            self.after_get(path)
        if self.get_error is not None:
            raise self.get_error


class FakeSFTPHook:
    """Provide a managed fake SFTP session without opening a network socket."""

    def __init__(self, session: FakeSFTPSession) -> None:
        self.session = session
        self.connection_count = 0

    @contextmanager
    def get_managed_conn(self) -> Iterator[FakeSFTPSession]:
        """Yield the fake session."""

        self.connection_count += 1
        yield self.session


def test_gobi_filename_for_uses_month_and_day() -> None:
    assert (
        gobi_filename_for(datetime(2026, 2, 3, 5, tzinfo=UTC))
        == "ebook0203.ord"
    )


def test_gobi_filename_for_uses_pacific_calendar_date() -> None:
    pacific = datetime(2026, 9, 29, 0, 30, tzinfo=UTC).astimezone(
        ZoneInfo("America/Los_Angeles")
    )
    assert gobi_filename_for(pacific) == "ebook0928.ord"


@pytest.mark.parametrize("filename", ["ebook0229.ord", "ebook1231.ord"])
def test_validate_gobi_filename_accepts_calendar_dates(filename: str) -> None:
    assert validate_gobi_filename(filename) == filename


@pytest.mark.parametrize(
    "filename",
    [
        "../ebook0520.ord",
        "/gobiord/ebook0520.ord",
        "ebook0230.ord",
        "ebook1301.ord",
        "ebook0520.zip",
        "ebook\\0520.ord",
    ],
)
def test_validate_gobi_filename_rejects_unsafe_or_invalid_names(
    filename: str,
) -> None:
    with pytest.raises(ValueError):
        validate_gobi_filename(filename)


@pytest.mark.parametrize(
    ("scheduled_for", "expected"),
    [
        (datetime(2026, 9, 28, 1, tzinfo=UTC), "lbnl_people_20260928.zip"),
        (datetime(2026, 9, 29, 1, tzinfo=UTC), "lbnl_people_20260928.zip"),
        (datetime(2026, 10, 4, 23, tzinfo=UTC), "lbnl_people_20260928.zip"),
        (datetime(2027, 1, 1, 1, tzinfo=UTC), "lbnl_people_20261228.zip"),
    ],
)
def test_lbnl_filename_for_uses_most_recent_monday(
    scheduled_for: datetime, expected: str
) -> None:
    assert lbnl_filename_for(scheduled_for, "lbnl_people") == expected


def test_validate_lbnl_filename_accepts_expected_name() -> None:
    filename = "lbnl_people_20260228.zip"
    assert validate_lbnl_filename(filename, "lbnl_people") == filename


@pytest.mark.parametrize(
    "filename",
    [
        "../lbnl_people_20260228.zip",
        "other_20260228.zip",
        "lbnl_people_20260230.zip",
        "lbnl_people_20260228.txt",
    ],
)
def test_validate_lbnl_filename_rejects_unsafe_or_invalid_names(
    filename: str,
) -> None:
    with pytest.raises(ValueError):
        validate_lbnl_filename(filename, "lbnl_people")


@pytest.mark.parametrize("prefix", ["", "../lbnl", "lbnl.people", "lbnl people"])
def test_validate_lbnl_prefix_rejects_invalid_values(prefix: str) -> None:
    with pytest.raises(ValueError):
        validate_lbnl_prefix(prefix)


@pytest.mark.parametrize("value", [False, "false", "FALSE", 0, "0"])
def test_require_host_key_verification_accepts_explicit_false(
    monkeypatch: pytest.MonkeyPatch, value: object
) -> None:
    connection = SimpleNamespace(extra_dejson={"no_host_key_check": value})
    monkeypatch.setattr(sftp.BaseHook, "get_connection", lambda _conn_id: connection)

    require_host_key_verification("test_sftp")


@pytest.mark.parametrize("value", [None, True, "true", 1, "1"])
def test_require_host_key_verification_rejects_insecure_values(
    monkeypatch: pytest.MonkeyPatch, value: object
) -> None:
    connection = SimpleNamespace(extra_dejson={"no_host_key_check": value})
    monkeypatch.setattr(sftp.BaseHook, "get_connection", lambda _conn_id: connection)

    with pytest.raises(ValueError, match="no_host_key_check=false"):
        require_host_key_verification("test_sftp")


def test_download_sftp_file_publishes_complete_file(tmp_path: Path) -> None:
    session = FakeSFTPSession(payload=b"complete contents")

    result = download_sftp_file(
        FakeSFTPHook(session),
        "/remote/ebook0928.ord",
        tmp_path,
        "ebook0928.ord",
    )

    assert result.status is DownloadStatus.DOWNLOADED
    assert result.bytes_downloaded == len(b"complete contents")
    assert (tmp_path / "ebook0928.ord").read_bytes() == b"complete contents"
    assert list(tmp_path.glob("*.part")) == []
    assert list(tmp_path.glob(".*.part")) == []


def test_download_sftp_file_skips_missing_remote(tmp_path: Path) -> None:
    missing = FileNotFoundError(errno.ENOENT, "missing")
    session = FakeSFTPSession(stat_error=missing)

    result = download_sftp_file(
        FakeSFTPHook(session),
        "/remote/ebook0928.ord",
        tmp_path,
        "ebook0928.ord",
    )

    assert result.status is DownloadStatus.REMOTE_MISSING
    assert not (tmp_path / "ebook0928.ord").exists()


def test_download_sftp_file_skips_stale_remote(tmp_path: Path) -> None:
    session = FakeSFTPSession(
        modified_at=datetime(2026, 9, 1, tzinfo=UTC)
    )

    result = download_sftp_file(
        FakeSFTPHook(session),
        "/remote/ebook0928.ord",
        tmp_path,
        "ebook0928.ord",
        minimum_modified_at=datetime(2026, 9, 18, tzinfo=UTC),
    )

    assert result.status is DownloadStatus.REMOTE_STALE
    assert session.get_calls == []


def test_download_sftp_file_allows_old_remote_without_cutoff(tmp_path: Path) -> None:
    session = FakeSFTPSession(
        payload=b"historical",
        modified_at=datetime(2020, 1, 1, tzinfo=UTC),
    )

    result = download_sftp_file(
        FakeSFTPHook(session),
        "/remote/ebook0101.ord",
        tmp_path,
        "ebook0101.ord",
    )

    assert result.status is DownloadStatus.DOWNLOADED
    assert (tmp_path / "ebook0101.ord").read_bytes() == b"historical"


def test_download_sftp_file_skips_existing_local_file(tmp_path: Path) -> None:
    local_path = tmp_path / "ebook0928.ord"
    local_path.write_bytes(b"existing")
    hook = FakeSFTPHook(FakeSFTPSession())

    result = download_sftp_file(
        hook,
        "/remote/ebook0928.ord",
        tmp_path,
        "ebook0928.ord",
    )

    assert result.status is DownloadStatus.ALREADY_RETRIEVED
    assert local_path.read_bytes() == b"existing"
    assert hook.connection_count == 0


def test_download_sftp_file_recognizes_lbnl_old_marker(tmp_path: Path) -> None:
    marker = tmp_path / "lbnl_people_20260928.zip.processed.old"
    marker.write_bytes(b"processed")
    hook = FakeSFTPHook(FakeSFTPSession())

    result = download_sftp_file(
        hook,
        "lbnl_people_20260928.zip",
        tmp_path,
        "lbnl_people_20260928.zip",
        recognize_old_markers=True,
    )

    assert result.status is DownloadStatus.ALREADY_RETRIEVED
    assert marker.name in result.message
    assert hook.connection_count == 0


def test_download_sftp_file_rejects_size_mismatch_and_cleans_up(
    tmp_path: Path,
) -> None:
    session = FakeSFTPSession(payload=b"short", reported_size=20)

    with pytest.raises(RuntimeError, match="size mismatch"):
        download_sftp_file(
            FakeSFTPHook(session),
            "/remote/ebook0928.ord",
            tmp_path,
            "ebook0928.ord",
        )

    assert list(tmp_path.iterdir()) == []


def test_download_sftp_file_cleans_up_after_transfer_error(tmp_path: Path) -> None:
    session = FakeSFTPSession(get_error=OSError(errno.EIO, "transfer failed"))

    with pytest.raises(OSError, match="transfer failed"):
        download_sftp_file(
            FakeSFTPHook(session),
            "/remote/ebook0928.ord",
            tmp_path,
            "ebook0928.ord",
        )

    assert list(tmp_path.iterdir()) == []


def test_download_sftp_file_propagates_non_missing_stat_error(
    tmp_path: Path,
) -> None:
    session = FakeSFTPSession(stat_error=OSError(errno.EACCES, "denied"))

    with pytest.raises(OSError, match="denied"):
        download_sftp_file(
            FakeSFTPHook(session),
            "/remote/ebook0928.ord",
            tmp_path,
            "ebook0928.ord",
        )


def test_download_sftp_file_cleans_up_if_remote_disappears(
    tmp_path: Path,
) -> None:
    session = FakeSFTPSession(
        get_error=FileNotFoundError(errno.ENOENT, "disappeared")
    )

    result = download_sftp_file(
        FakeSFTPHook(session),
        "/remote/ebook0928.ord",
        tmp_path,
        "ebook0928.ord",
    )

    assert result.status is DownloadStatus.REMOTE_MISSING
    assert list(tmp_path.iterdir()) == []


def test_download_sftp_file_does_not_overwrite_publication_race(
    tmp_path: Path,
) -> None:
    final_path = tmp_path / "ebook0928.ord"

    def create_competing_file(_temporary_path: Path) -> None:
        final_path.write_bytes(b"winner")

    session = FakeSFTPSession(after_get=create_competing_file)
    result = download_sftp_file(
        FakeSFTPHook(session),
        "/remote/ebook0928.ord",
        tmp_path,
        "ebook0928.ord",
    )

    assert result.status is DownloadStatus.ALREADY_RETRIEVED
    assert final_path.read_bytes() == b"winner"
    assert sorted(path.name for path in tmp_path.iterdir()) == ["ebook0928.ord"]


def test_download_sftp_file_rejects_symlink_collision(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.write_bytes(b"unsafe")
    (tmp_path / "ebook0928.ord").symlink_to(target)

    with pytest.raises(ValueError, match="unsafe local path collision"):
        download_sftp_file(
            FakeSFTPHook(FakeSFTPSession()),
            "/remote/ebook0928.ord",
            tmp_path,
            "ebook0928.ord",
        )


def test_download_sftp_file_rejects_symlink_destination(tmp_path: Path) -> None:
    actual = tmp_path / "actual"
    actual.mkdir()
    destination = tmp_path / "destination"
    destination.symlink_to(actual, target_is_directory=True)

    with pytest.raises(ValueError, match="cannot be a symlink"):
        download_sftp_file(
            FakeSFTPHook(FakeSFTPSession()),
            "/remote/ebook0928.ord",
            destination,
            "ebook0928.ord",
        )
