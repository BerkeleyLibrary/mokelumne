import csv
from pathlib import Path
from pymarc.marcxml import XmlHandler

class TindXmlHandler(XmlHandler):
    def __init__(self, download_dir, **kwargs):
        super().__init__(**kwargs)
        self.csv_p = Path(f"{download_dir}/to_process.csv")
        self.csv_s = Path(f"{download_dir}/skipped.csv")
        self.fp = open(self.csv_p, 'w', newline='', encoding='utf-8')
        self.fs = open(self.csv_s, 'w', newline='', encoding='utf-8')
        self.writer_p = csv.writer(self.fp)
        self.writer_s = csv.writer(self.fs)

        header = ['Record ID', '035__a', 'Collection name', 'Link to record' , '8564_u']
        self.writer_p.writerow(header)
        self.writer_s.writerow(header)

    def _get_subfield(self, record, field_tag, subfield_code):
        return next((v for f in record.get_fields(field_tag) for v in f.get_subfields(subfield_code)), '')

    def _get_856_urls(self, record):
        return [
            v
            for f in record.get_fields('856')
            if f.indicator1 == '4' and f.indicator2 == ' '
            for v in f.get_subfields('u')
        ]

    def _record_link(self, record_id):
        return (
            "No Record Link"
            if not record_id
            else f"https://digicoll.lib.berkeley.edu/record/{record_id}?ln=en"
        )

    def process_record(self, record):
        record_id = record['001'].data.strip() if record['001'] else ''

        link = self._record_link(record_id)
        f035 = self._get_subfield(record, '035', 'a')
        f982 = self._get_subfield(record, '982', 'b')
        f336 = self._get_subfield(record, '336', 'a')
        f856 = self._get_856_urls(record)

        row = [record_id, f035, f982, link, '|'.join(f856)]

        if len(f856) == 1 and f336 == 'Image':
            self.writer_p.writerow(row)
        else:
            self.writer_s.writerow(row)