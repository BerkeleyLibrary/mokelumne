"""Unit tests for GOBI MARC processing helpers."""

from pathlib import Path

import pytest

from pymarc import Field, Indicators, MARCReader, MARCWriter, Record, Subfield

from mokelumne.util.gobi import (
    build_output_path,
    build_staging_directory,
    find_order_files,
    get_provider_code,
    process_order_file,
)


def make_record(provider: str | None, control_number: str) -> Record:
    """Build a minimal serializable MARC record for a GOBI provider."""

    record = Record(force_utf8=True)
    record.add_field(Field(tag="001", data=control_number))
    if provider is not None:
        record.add_field(
            Field(
                tag="961",
                indicators=Indicators(" ", " "),
                subfields=[Subfield(code="d", value=provider)],
            )
        )
    return record


def write_records(path: Path, records: list[Record]) -> None:
    """Write records to a binary MARC fixture."""

    writer = MARCWriter(path.open("wb"))
    for record in records:
        writer.write(record)
    writer.close()


def read_control_numbers(path: Path) -> list[str]:
    """Read record control numbers from a binary MARC file."""

    with path.open("rb") as handle:
        return [record["001"].data for record in MARCReader(handle) if record is not None]


class TestGetProviderCode:
    def test_returns_first_three_characters_for_supported_provider(self):
        assert get_provider_code(make_record("DEG additional data", "1")) == "DEG"

    @pytest.mark.parametrize("provider", [None, "", "OMO additional data"])
    def test_returns_fallback_for_missing_or_unsupported_provider(self, provider):
        assert get_provider_code(make_record(provider, "1")) == "ZZZ"


def test_build_output_path_preserves_original_filename_convention(tmp_path):
    output = build_output_path(
        "ebook0223.ord",
        "EBS",
        tmp_path,
        2026,
    )

    assert output == tmp_path / "ebookEBS20260223.ord"


def test_build_staging_directory_uses_run_dir_beside_output(tmp_path):
    output = tmp_path / "gobi_processed"
    output.mkdir()

    staging = build_staging_directory(
        "incoming/ebook0223.ord",
        output,
        "process_gobi_orders",
        "scheduled__2026-08-13T00:00:00+00:00",
    )

    assert staging == (
        tmp_path
        / ".airflow"
        / "process_gobi_orders"
        / "scheduled__2026-08-13T00:00:00+00:00"
        / "ebook0223.ord"
    )
    assert staging.is_dir()


def test_find_order_files_returns_only_sorted_regular_ord_files(tmp_path):
    (tmp_path / "b.ord").touch()
    (tmp_path / "a.ord").touch()
    (tmp_path / "notes.txt").touch()
    (tmp_path / "directory.ord").mkdir()

    assert find_order_files(tmp_path) == [
        str(tmp_path / "a.ord"),
        str(tmp_path / "b.ord"),
    ]


def test_find_order_files_requires_an_existing_directory(tmp_path):
    with pytest.raises(FileNotFoundError, match="Input directory does not exist"):
        find_order_files(tmp_path / "missing")


def test_process_order_file_splits_records_and_archives_source(tmp_path):
    incoming = tmp_path / "incoming"
    output = tmp_path / "output"
    processed = tmp_path / "processed"
    staging = tmp_path / "staging" / "ebook0223.ord"
    incoming.mkdir()
    output.mkdir()
    processed.mkdir()
    staging.mkdir(parents=True)
    order_file = incoming / "ebook0223.ord"
    write_records(
        order_file,
        [
            make_record("DEG data", "1"),
            make_record("EBS data", "2"),
            make_record("DEG more data", "3"),
            make_record("unsupported", "4"),
            make_record(None, "5"),
        ],
    )

    result = process_order_file(order_file, output, processed, staging, year=2026)

    assert not order_file.exists()
    assert (processed / order_file.name).is_file()
    assert read_control_numbers(output / "ebookDEG20260223.ord") == ["1", "3"]
    assert read_control_numbers(output / "ebookEBS20260223.ord") == ["2"]
    assert read_control_numbers(output / "ebookZZZ20260223.ord") == ["4", "5"]
    assert result["records_read"] == 5
    assert result["records_written"] == 5
    assert result["records_skipped"] == 0
    assert result["staging_directory"] == str(staging)
    assert list(staging.iterdir()) == []


def test_process_order_file_does_not_replace_existing_provider_output(tmp_path):
    incoming = tmp_path / "incoming"
    output = tmp_path / "output"
    processed = tmp_path / "processed"
    staging = tmp_path / "staging" / "ebook0223.ord"
    incoming.mkdir()
    output.mkdir()
    processed.mkdir()
    staging.mkdir(parents=True)
    order_file = incoming / "ebook0223.ord"
    existing_output = output / "ebookDEG20260223.ord"
    write_records(order_file, [make_record("DEG data", "new")])
    write_records(existing_output, [make_record("DEG data", "existing")])

    result = process_order_file(order_file, output, processed, staging, year=2026)

    assert read_control_numbers(existing_output) == ["existing"]
    assert (processed / order_file.name).is_file()
    assert result["records_written"] == 0
    assert result["records_skipped"] == 1
    assert result["skipped_providers"] == ["DEG"]
    assert list(staging.iterdir()) == []


def test_process_order_file_keeps_source_and_removes_staging_on_invalid_marc(tmp_path):
    incoming = tmp_path / "incoming"
    output = tmp_path / "output"
    processed = tmp_path / "processed"
    staging = tmp_path / "staging" / "ebook0223.ord"
    incoming.mkdir()
    output.mkdir()
    processed.mkdir()
    staging.mkdir(parents=True)
    order_file = incoming / "ebook0223.ord"
    write_records(order_file, [make_record("DEG data", "1")])
    with order_file.open("ab") as handle:
        handle.write(b"not a MARC record")

    with pytest.raises(ValueError, match="Invalid MARC record"):
        process_order_file(order_file, output, processed, staging, year=2026)

    assert order_file.is_file()
    assert list(output.iterdir()) == []
    assert list(processed.iterdir()) == []
    assert list(staging.iterdir()) == []
