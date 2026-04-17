"""Provides a helper class, TindCsvWriter, to write filtered TIND records to CSV."""

import csv
from collections.abc import Callable
from pathlib import Path

from pymarc import Record


def _get_subfield(record: Record, field_tag: str, subfield_code: str) -> str:
    """Return the first matching subfield value, or empty string if none found.

    :param record: A pymarc Record to search.
    :param field_tag: MARC field tag (e.g. '035', '336').
    :param subfield_code: Subfield code within the field (e.g. 'a', 'b').
    :returns: The first matching subfield value, or '' if no match.
    """
    return next((v for f in record.get_fields(field_tag)
                 for v in f.get_subfields(subfield_code)), '')


def _get_856_urls(record: Record) -> list[str]:
    """Return URLs from 856 fields with indicator1='4' and indicator2=' '.

    :param record: A pymarc Record to search.
    :returns: List of URL strings from subfield 'u' of matching 856 fields.
    """
    return [
        v
        for f in record.get_fields('856')
        if f.indicator1 == '4' and f.indicator2 == ' '
        for v in f.get_subfields('u')
    ]


def _record_link(record_id: str) -> str:
    """Return a TIND record URL, or a placeholder if no ID is provided.

    :param record_id: The TIND record ID (e.g. '001234').
    :returns: A URL to the record on digicoll, or 'No Record Link' if record_id is falsy.
    """
    return (
        "No Record Link"
        if not record_id
        else f"https://digicoll.lib.berkeley.edu/record/{record_id}?ln=en"
    )


class TindCsvWriter:
    """
    Utility class to write filtered TIND records to CSV files.
    """

    def __init__(self, download_dir: str | Path,
                 record_filter: Callable[[Record], bool]) -> None:
        """Initialize the writer.

        :param download_dir: Directory where to_process.csv and skipped.csv will be written.
        :param record_filter: Callable that accepts a pymarc Record and returns True if the
            record should be written to to_process.csv, or False for skipped.csv.
        """
        self._record_filter = record_filter
        download_path = Path(download_dir)
        self.csv_p = download_path / "to_process.csv"
        self.csv_s = download_path / "skipped.csv"
        self.fp = self.fs = None
        self.writer_p = self.writer_s = None
        self.count_p = 0
        self.count_s = 0

    def __enter__(self):
        self.fp = open(self.csv_p, 'w', newline='', encoding='utf-8')
        self.fs = open(self.csv_s, 'w', newline='', encoding='utf-8')
        self.writer_p = csv.writer(self.fp)
        self.writer_s = csv.writer(self.fs)

        self.writer_p.writerow(['Record ID', '035__a', 'Collection name', 'Status',
                                'Link to record', 'Image Url'])
        self.writer_s.writerow(['Record ID', '035__a', 'Collection name', 'Status',
                                'Link to record'])
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        if self.fp:
            self.fp.close()
        if self.fs:
            self.fs.close()
        return False

    def process_tind_record(self, record):
        """
        Parse a TIND record and write it to the proper output CSV.
        """
        record_id = record['001'].data.strip() if record['001'] else ''

        record_link = _record_link(record_id)
        f035 = _get_subfield(record, '035', 'a')
        f982 = _get_subfield(record, '982', 'b')

        should_process = self._record_filter(record)
        status = 'to_process' if should_process else 'skipped'
        row = [record_id, f035, f982, status, record_link]
        if should_process:
            f856 = _get_856_urls(record)
            if not f856:
                raise ValueError(
                    f"Record {record_id} accepted by filter but has no 856 URLs"
                )
            row.append(f856[0])

        self._write_row(row, should_process)

    def _write_row(self, row, should_process):
        if should_process:
            self.writer_p.writerow(row)
            self.count_p += 1
        else:
            self.writer_s.writerow(row)
            self.count_s += 1



def is_single_image_record(record: Record) -> bool:
    """Return True if the record has exactly one 856 URL and resource type 'Image'."""
    f856 = _get_856_urls(record)
    f336 = _get_subfield(record, '336', 'a')
    return len(f856) == 1 and f336 == 'Image'
