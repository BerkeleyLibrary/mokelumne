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

        header = ['001', '035__a', '982__a', '8564_u']
        self.writer_p.writerow(header)
        self.writer_s.writerow(header)

    def process_record(self, record):
        record_id = record['001'].data.strip() if record['001'] else ''

        f035 = next((v for f in record.get_fields('035') for v in f.get_subfields('a')), '')
        f982 = next((v for f in record.get_fields('982') for v in f.get_subfields('b')), '')
        f336 = next((v for f in record.get_fields('336') for v in f.get_subfields('a')), '')
        f856 = [
            v
            for f in record.get_fields('856')
            if f.indicator1 == '4' and f.indicator2 == ' '
            for v in f.get_subfields('u')
        ]

        row = [record_id, f035, f982, '|'.join(f856)]

        if len(f856) == 1 and f336 == 'Image':
            self.writer_p.writerow(row)
        else:
            self.writer_s.writerow(row)