"""Helpers for safely downloading files over SFTP."""

from __future__ import annotations

import errno
import logging
import os
import re
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Final
from uuid import uuid4

from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.sdk import BaseHook

logger = logging.getLogger(__name__)

GOBI_FILENAME_PATTERN: Final = re.compile(
    r"ebook(?P<month>[0-9]{2})(?P<day>[0-9]{2})[.]ord"
)
LBNL_PREFIX_PATTERN: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*")


class DownloadStatus(StrEnum):
    """Possible outcomes for one SFTP download attempt."""

    DOWNLOADED = "downloaded"
    REMOTE_MISSING = "remote_missing"
    REMOTE_STALE = "remote_stale"
    ALREADY_RETRIEVED = "already_retrieved"


@dataclass(frozen=True)
class DownloadResult:
    """Describe the outcome of an SFTP download attempt."""

    status: DownloadStatus
    remote_path: str
    local_path: str
    message: str
    bytes_downloaded: int | None = None

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation of the result."""

        return asdict(self)


def validate_gobi_filename(filename: str) -> str:
    """Validate and return a GOBI order filename."""

    _validate_basename(filename)
    match = GOBI_FILENAME_PATTERN.fullmatch(filename)
    if match is None:
        raise ValueError(f"Invalid GOBI filename: {filename!r}")
    _validate_month_day(int(match["month"]), int(match["day"]), filename)
    return filename


def gobi_filename_for(scheduled_for: datetime) -> str:
    """Return the GOBI filename for a scheduled local datetime."""

    return f"ebook{scheduled_for:%m%d}.ord"


def validate_lbnl_prefix(prefix: str) -> str:
    """Validate and return the configured LBNL filename prefix."""

    _validate_basename(prefix)
    if LBNL_PREFIX_PATTERN.fullmatch(prefix) is None:
        raise ValueError(f"Invalid LBNL filename prefix: {prefix!r}")
    return prefix


def validate_lbnl_filename(filename: str, prefix: str) -> str:
    """Validate and return an LBNL patron filename."""

    _validate_basename(filename)
    validated_prefix = validate_lbnl_prefix(prefix)
    match = re.fullmatch(
        rf"{re.escape(validated_prefix)}_(?P<file_date>[0-9]{{8}})[.]zip",
        filename,
    )
    if match is None:
        raise ValueError(f"Invalid LBNL filename: {filename!r}")
    try:
        date.fromisoformat(
            f"{match['file_date'][0:4]}-{match['file_date'][4:6]}-"
            f"{match['file_date'][6:8]}"
        )
    except ValueError as ex:
        raise ValueError(f"Invalid LBNL filename date: {filename!r}") from ex
    return filename


def lbnl_filename_for(scheduled_for: datetime, prefix: str) -> str:
    """Return the most recent Monday's LBNL patron filename."""

    validated_prefix = validate_lbnl_prefix(prefix)
    most_recent_monday = scheduled_for.date() - timedelta(
        days=scheduled_for.weekday()
    )
    return f"{validated_prefix}_{most_recent_monday:%Y%m%d}.zip"


def require_host_key_verification(conn_id: str) -> None:
    """Require an SFTP connection to explicitly enable host-key checking."""

    connection = BaseHook.get_connection(conn_id)
    no_host_key_check = connection.extra_dejson.get("no_host_key_check")
    if not _is_explicit_false(no_host_key_check):
        raise ValueError(
            f"SFTP connection {conn_id!r} must set no_host_key_check=false"
        )


# The transaction keeps all preflight, transfer, verification, and publication
# branches together so a retry cannot expose a partially downloaded file.
# pylint: disable=too-many-arguments,too-many-locals,too-many-return-statements,too-many-branches
def download_sftp_file(
    hook: SFTPHook,
    remote_path: str,
    destination: Path | str,
    filename: str,
    *,
    minimum_modified_at: datetime | None = None,
    recognize_old_markers: bool = False,
) -> DownloadResult:
    """Download one remote file and publish it without exposing partial data."""

    _validate_basename(filename)
    destination_path = _require_destination(destination)
    local_path = destination_path / filename

    existing = _existing_regular_file(local_path)
    if existing is not None:
        return _already_retrieved(remote_path, local_path, existing)

    if recognize_old_markers:
        marker = _find_old_marker(destination_path, filename)
        if marker is not None:
            return _already_retrieved(remote_path, local_path, marker)

    temporary_path = destination_path / f".{filename}.{uuid4().hex}.part"
    attributes = None
    try:
        with hook.get_managed_conn() as connection:
            try:
                attributes = connection.stat(remote_path)
            except OSError as ex:
                if _is_missing_path_error(ex):
                    return DownloadResult(
                        status=DownloadStatus.REMOTE_MISSING,
                        remote_path=remote_path,
                        local_path=str(local_path),
                        message=f"Remote file does not exist: {remote_path}",
                    )
                raise

            remote_size = getattr(attributes, "st_size", None)
            if not isinstance(remote_size, int) or remote_size < 0:
                raise RuntimeError(f"Remote file has no valid size: {remote_path}")

            if minimum_modified_at is not None:
                remote_mtime = _remote_modified_at(attributes, remote_path)
                cutoff = _as_utc(minimum_modified_at)
                if remote_mtime < cutoff:
                    return DownloadResult(
                        status=DownloadStatus.REMOTE_STALE,
                        remote_path=remote_path,
                        local_path=str(local_path),
                        message=(
                            f"Remote file modification time {remote_mtime.isoformat()} "
                            f"is before cutoff {cutoff.isoformat()}"
                        ),
                    )

            try:
                connection.get(remote_path, str(temporary_path))
            except OSError as ex:
                if _is_missing_path_error(ex):
                    return DownloadResult(
                        status=DownloadStatus.REMOTE_MISSING,
                        remote_path=remote_path,
                        local_path=str(local_path),
                        message=f"Remote file disappeared before download: {remote_path}",
                    )
                raise

        downloaded_size = _downloaded_file_size(temporary_path)
        remote_size = getattr(attributes, "st_size")
        if downloaded_size != remote_size:
            raise RuntimeError(
                f"Downloaded size mismatch for {remote_path}: "
                f"expected {remote_size}, got {downloaded_size}"
            )

        try:
            os.link(temporary_path, local_path, follow_symlinks=False)
        except FileExistsError:
            existing = _existing_regular_file(local_path)
            if existing is None:
                raise
            return _already_retrieved(remote_path, local_path, existing)

        logger.info(
            "Downloaded %s to %s (%s bytes)",
            remote_path,
            local_path,
            downloaded_size,
        )
        return DownloadResult(
            status=DownloadStatus.DOWNLOADED,
            remote_path=remote_path,
            local_path=str(local_path),
            message=f"Downloaded {remote_path} to {local_path}",
            bytes_downloaded=downloaded_size,
        )
    finally:
        temporary_path.unlink(missing_ok=True)


# pylint: enable=too-many-arguments,too-many-locals,too-many-return-statements,too-many-branches


def _validate_basename(filename: str) -> None:
    """Reject empty, traversal, absolute, or multi-component filenames."""

    if (
        not filename
        or filename in {".", ".."}
        or "/" in filename
        or "\\" in filename
        or Path(filename).is_absolute()
    ):
        raise ValueError(f"Filename must be a single basename: {filename!r}")


def _validate_month_day(month: int, day: int, filename: str) -> None:
    """Validate a month and day using a leap year."""

    try:
        date(2000, month, day)
    except ValueError as ex:
        raise ValueError(f"Invalid GOBI filename date: {filename!r}") from ex


def _require_destination(destination: Path | str) -> Path:
    """Return a resolved, existing, non-symlink destination directory."""

    path = Path(destination)
    if path.is_symlink():
        raise ValueError(f"Destination directory cannot be a symlink: {path}")
    if not path.exists():
        raise FileNotFoundError(f"Destination directory does not exist: {path}")
    if not path.is_dir():
        raise NotADirectoryError(f"Destination path is not a directory: {path}")
    return path.resolve(strict=True)


def _existing_regular_file(path: Path) -> Path | None:
    """Return an existing regular file, or reject an unsafe collision."""

    if not os.path.lexists(path):
        return None
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"Refusing unsafe local path collision: {path}")
    return path


def _find_old_marker(destination: Path, filename: str) -> Path | None:
    """Return the first safe LBNL processed marker, if any."""

    for marker in sorted(destination.glob(f"{filename}*.old")):
        existing = _existing_regular_file(marker)
        if existing is not None:
            return existing
    return None


def _already_retrieved(
    remote_path: str, local_path: Path, existing_path: Path
) -> DownloadResult:
    """Build an already-retrieved outcome."""

    return DownloadResult(
        status=DownloadStatus.ALREADY_RETRIEVED,
        remote_path=remote_path,
        local_path=str(local_path),
        message=f"File was already retrieved or processed: {existing_path}",
    )


def _remote_modified_at(attributes: object, remote_path: str) -> datetime:
    """Return an aware UTC remote modification time."""

    modified_at = getattr(attributes, "st_mtime", None)
    if not isinstance(modified_at, int | float):
        raise RuntimeError(
            f"Remote file has no valid modification time: {remote_path}"
        )
    return datetime.fromtimestamp(modified_at, tz=UTC)


def _downloaded_file_size(path: Path) -> int:
    """Return a downloaded regular file's size."""

    if path.is_symlink() or not path.is_file():
        raise RuntimeError(f"SFTP download did not produce a regular file: {path}")
    return path.stat().st_size


def _as_utc(value: datetime) -> datetime:
    """Normalize an aware datetime to UTC."""

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("minimum_modified_at must be timezone-aware")
    return value.astimezone(UTC)


def _is_missing_path_error(ex: OSError) -> bool:
    """Return whether an SFTP error reports a missing path."""

    return ex.errno == errno.ENOENT or (
        bool(ex.args) and isinstance(ex.args[0], int) and ex.args[0] == errno.ENOENT
    )


def _is_explicit_false(value: object) -> bool:
    """Return whether an Airflow connection extra explicitly means false."""

    if value is False or value == 0:
        return True
    return isinstance(value, str) and value.strip().lower() in {"false", "0"}
