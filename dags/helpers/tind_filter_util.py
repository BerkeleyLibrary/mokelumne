import csv
from pathlib import Path


class TindCsvWriter:
    '''
    Utility class to write filtered TIND records to CSV files.
    '''

    def __init__(self, download_dir):
        self.csv_p = Path(f"{download_dir}/to_process.csv")
        self.csv_s = Path(f"{download_dir}/skipped.csv")
        self.fp = self.fs = None
        self.writer_p = self.writer_s = None
        self.count_p = 0
        self.count_s = 0

    '''
    Add context manager
    '''
    def __enter__(self):
        self.fp = open(self.csv_p, 'w', newline='', encoding='utf-8')
        self.fs = open(self.csv_s, 'w', newline='', encoding='utf-8')
        self.writer_p = csv.writer(self.fp)
        self.writer_s = csv.writer(self.fs)

        header = ['Record ID', '035__a', 'Collection name', 'Link to record', 'Image Url(s)']
        self.writer_p.writerow(header)
        self.writer_s.writerow(header)
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        if self.fp:
            self.fp.close()
        if self.fs:
            self.fs.close()
        return False

    def write_row(self, row, should_process):
        if should_process:
            self.writer_p.writerow(row)
            self.count_p += 1
        else:
            self.writer_s.writerow(row)
            self.count_s += 1


def _get_subfield(record, field_tag, subfield_code):
    return next((v for f in record.get_fields(field_tag) for v in f.get_subfields(subfield_code)), '')


def _get_856_urls(record):
    return [
        v
        for f in record.get_fields('856')
        if f.indicator1 == '4' and f.indicator2 == ' '
        for v in f.get_subfields('u')
    ]


def _record_link(record_id):
    return (
        "No Record Link"
        if not record_id
        else f"https://digicoll.lib.berkeley.edu/record/{record_id}?ln=en"
    )


def process_tind_record(record, csv_writer):
    '''
    Parse a TIND record and write it to the proper output CSV.
    '''
    record_id = record['001'].data.strip() if record['001'] else ''

    record_link = _record_link(record_id)
    f035 = _get_subfield(record, '035', 'a')
    f982 = _get_subfield(record, '982', 'b')
    f336 = _get_subfield(record, '336', 'a')
    f856 = _get_856_urls(record)

    row = [record_id, f035, f982, record_link, '|'.join(f856)]
    should_process = len(f856) == 1 and f336 == 'Image'
    csv_writer.write_row(row, should_process)