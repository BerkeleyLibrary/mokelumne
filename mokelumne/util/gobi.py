"""Helpers for processing GOBI MARC order files."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from shutil import move
from typing import Final
from uuid import uuid4

from pymarc import MARCReader, MARCWriter, Record

logger = logging.getLogger(__name__)

FALLBACK_PROVIDER: Final = "ZZZ"
GOBI_PROVIDERS: Final = frozenset(
    {
        "BLO",
        "BRL",
        "CUP",
        "DEG",
        "EBS",
        "ESD",
        "IGI",
        "JST",
        "KOR",
        "PQE",
        "SAG",
        "TAF",
        "UPS",
        "WIL",
        "WSC",
        FALLBACK_PROVIDER,
    }
)


def require_directory(directory: Path | str, label: str) -> Path:
    """Return a directory path or raise a descriptive exception."""

    path = Path(directory)
    if not path.exists():
        raise FileNotFoundError(f"{label} directory does not exist: {path}")
    if not path.is_dir():
        raise NotADirectoryError(f"{label} path is not a directory: {path}")
    return path


def find_order_files(input_directory: Path | str) -> list[str]:
    """Return regular ``.ord`` files in deterministic filename order."""

    input_path = require_directory(input_directory, "Input")
    return [str(path) for path in sorted(input_path.glob("*.ord")) if path.is_file()]


def get_provider_code(record: Record) -> str:
    """Return the supported provider code from the first 961$d, or ``ZZZ``."""

    provider_field = record.get("961")
    provider_value = provider_field.get("d") if provider_field is not None else None
    provider = provider_value[:3] if provider_value else None

    return provider if provider in GOBI_PROVIDERS else FALLBACK_PROVIDER


def build_output_path(
    input_file: Path | str,
    provider: str,
    output_directory: Path | str,
    year: int,
) -> Path:
    """Build the provider filename used by the original GOBI processor."""

    input_name = Path(input_file).name
    output_name = input_name.replace("ebook", f"ebook{provider}{year}")
    return Path(output_directory) / output_name


class _ProviderOutputStager:  # pylint: disable=too-many-instance-attributes
    """Stage the provider outputs for one order file."""

    def __init__(self, input_file: Path, output_directory: Path, year: int) -> None:
        self.input_file = input_file
        self.output_directory = output_directory
        self.year = year
        self.writers: dict[str, MARCWriter] = {}
        self.temporary_paths: dict[str, Path] = {}
        self.output_paths: dict[str, Path] = {}
        self.skipped_providers: set[str] = set()
        self.records_written = 0
        self.records_skipped = 0

    def write(self, record: Record) -> None:
        """Write a record to its provider's staging file when needed."""

        provider = get_provider_code(record)
        if provider in self.skipped_providers:
            self.records_skipped += 1
            return

        writer = self.writers.get(provider) or self.open_writer(provider)
        if writer is not None:
            writer.write(record)
            self.records_written += 1
        else:
            self.records_skipped += 1

    def open_writer(self, provider: str) -> MARCWriter | None:
        """Open a new staging writer unless the final output already exists."""

        final_path = build_output_path(
            self.input_file,
            provider,
            self.output_directory,
            self.year,
        )
        self.output_paths[provider] = final_path

        if final_path.exists():
            logger.info(
                "Skipping provider %s because output already exists: %s",
                provider,
                final_path,
            )
            self.skipped_providers.add(provider)
            return None

        temporary_path = final_path.with_name(f".{final_path.name}.{uuid4().hex}.tmp")
        writer = MARCWriter(temporary_path.open("xb"))
        self.temporary_paths[provider] = temporary_path
        self.writers[provider] = writer
        return writer

    def close(self) -> None:
        """Close every open MARC writer."""

        for writer in self.writers.values():
            writer.close()
        self.writers.clear()

    def publish(self) -> None:
        """Rename completed staging files to their final provider paths."""

        for provider, temporary_path in self.temporary_paths.items():
            final_path = self.output_paths[provider]
            if final_path.exists():
                logger.info(
                    "Discarding staged provider %s output because %s now exists",
                    provider,
                    final_path,
                )
                self.skipped_providers.add(provider)
                temporary_path.unlink()
                continue
            temporary_path.rename(final_path)

    def cleanup(self) -> None:
        """Close writers and remove any staging files that remain after failure."""

        for writer in self.writers.values():
            writer.close()
        for temporary_path in self.temporary_paths.values():
            temporary_path.unlink(missing_ok=True)


def process_order_file(
    input_file: Path | str,
    output_directory: Path | str,
    processed_directory: Path | str,
    *,
    year: int | None = None,
) -> dict[str, object]:
    """Split one GOBI order file by provider and archive the source file.

    Provider outputs are first written beside their destination as hidden temporary
    files. They are renamed into place only after every MARC record parses and
    writes successfully. Existing provider outputs are left unchanged, matching
    the behavior of the original processor and making a retry safe after output
    publication but before source archival.
    """

    input_path = Path(input_file)
    if not input_path.is_file():
        raise FileNotFoundError(f"Order file does not exist: {input_path}")
    if input_path.suffix != ".ord":
        raise ValueError(f"Order file must have a .ord extension: {input_path}")

    output_path = require_directory(output_directory, "Output")
    processed_path = require_directory(processed_directory, "Processed")
    archive_path = processed_path / input_path.name
    if archive_path.exists():
        raise FileExistsError(f"Processed order file already exists: {archive_path}")

    output_year = year if year is not None else date.today().year
    stager = _ProviderOutputStager(input_path, output_path, output_year)
    records_read = 0

    try:
        with input_path.open("rb") as input_handle:
            reader = MARCReader(input_handle, to_unicode=True, utf8_handling="strict")

            for record in reader:
                if record is None:
                    message = f"Invalid MARC record in {input_path}"
                    if reader.current_exception is not None:
                        message = f"{message}: {reader.current_exception}"
                    raise ValueError(message) from reader.current_exception

                records_read += 1
                stager.write(record)

        stager.close()
        stager.publish()
        move(input_path, archive_path)
    except Exception:
        stager.cleanup()
        raise

    logger.info(
        "Processed %s: read=%s written=%s skipped=%s outputs=%s",
        input_path,
        records_read,
        stager.records_written,
        stager.records_skipped,
        len(stager.output_paths),
    )

    return {
        "input_file": str(input_path),
        "archive_file": str(archive_path),
        "records_read": records_read,
        "records_written": stager.records_written,
        "records_skipped": stager.records_skipped,
        "output_files": {
            provider: str(path)
            for provider, path in sorted(stager.output_paths.items())
        },
        "skipped_providers": sorted(stager.skipped_providers),
    }
