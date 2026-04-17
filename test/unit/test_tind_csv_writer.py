"""
PyTest cases for the TindCsvWriter module
"""

import pytest
from pymarc import Record, Field, Subfield

from test.conftest import read_csv
from mokelumne.util.tind_csv_writer import TindCsvWriter, is_single_image_record


def _make_record(record_id='12345', f035='OCN123', f982='Test Collection',
                 f336='Image', f856_urls=('http://example.com/img.jpg',)):
    """Build a minimal pymarc Record for testing."""
    r = Record()
    if record_id is not None:
        r.add_field(Field(tag='001', data=record_id))
    if f035:
        r.add_field(Field(tag='035', indicators=[' ', ' '],
                          subfields=[Subfield(code='a', value=f035)]))
    if f982:
        r.add_field(Field(tag='982', indicators=[' ', ' '],
                          subfields=[Subfield(code='b', value=f982)]))
    if f336:
        r.add_field(Field(tag='336', indicators=[' ', ' '],
                          subfields=[Subfield(code='a', value=f336)]))
    for url in (f856_urls or ()):
        r.add_field(Field(tag='856', indicators=['4', ' '],
                          subfields=[Subfield(code='u', value=url)]))
    return r


class TestIsSingleImageRecord:
    """Tests the is_single_image_record filter"""

    def test_single_image_url(self):
        """Image record with one 856 image URL is selected"""
        record = _make_record(f336='Image', f856_urls=('http://img.jpg',))
        assert is_single_image_record(record) is True

    def test_no_urls(self):
        """Image record with no 856 image URLs is rejected"""
        record = _make_record(f336='Image', f856_urls=())
        assert is_single_image_record(record) is False

    def test_multiple_urls(self):
        """Image record with multiple 856 image URLs is rejected"""
        record = _make_record(f336='Image',
                              f856_urls=('http://a.jpg', 'http://b.jpg'))
        assert is_single_image_record(record) is False

    def test_non_image_type(self):
        """Non-image (text) record is rejected"""
        record = _make_record(f336='Text', f856_urls=('http://img.jpg',))
        assert is_single_image_record(record) is False

    def test_missing_336(self):
        """Unknown record type is rejected"""
        record = _make_record(f336=None, f856_urls=('http://img.jpg',))
        assert is_single_image_record(record) is False

    def test_856_with_wrong_indicators_not_counted(self):
        """'Valid' image record with wrong indicators is rejected"""
        record = Record()
        record.add_field(Field(tag='001', data='99'))
        record.add_field(Field(tag='336', indicators=[' ', ' '],
                               subfields=[Subfield(code='a', value='Image')]))
        record.add_field(Field(tag='856', indicators=['0', ' '],
                               subfields=[Subfield(code='u', value='http://wrong.jpg')]))
        assert is_single_image_record(record) is False


class TestProcessTindRecord:
    """
    Tests TindCsvWriter.process_tind_record

    Especially focused on ensuring that filtered records end up
    in the correct CSV (to_process or skipped).
    """

    @staticmethod
    def _accept_all(_record):
        return True

    @staticmethod
    def _reject_all(_record):
        return False

    def test_accepted_record_written_to_process(self, tmp_path):
        """Writes any record when given a select-all filter"""
        record = _make_record()
        with TindCsvWriter(tmp_path, self._accept_all) as w:
            w.process_tind_record(record)

        rows = list(read_csv(tmp_path / 'to_process.csv'))
        assert len(rows) == 1
        assert rows[0]['Record ID'] == '12345'
        assert rows[0]['Status'] == 'to_process'
        assert rows[0]['Image Url'] == 'http://example.com/img.jpg'

    def test_rejected_record_written_to_skipped(self, tmp_path):
        """Writes no records when given a reject-all filter"""
        record = _make_record()
        with TindCsvWriter(tmp_path, self._reject_all) as w:
            w.process_tind_record(record)

        skipped = read_csv(tmp_path / 'skipped.csv')
        rows = list(skipped)
        assert len(rows) == 1
        assert rows[0]['Record ID'] == '12345'
        assert rows[0]['Status'] == 'skipped'
        assert len(skipped.fieldnames) == 5  # no image URL column

    def test_counts_tracked(self, tmp_path):
        """Keeps track of how many records were processed / skipped"""
        accepted = _make_record(record_id='1', f336='Image')
        rejected = _make_record(record_id='2', f336='Text')

        with TindCsvWriter(tmp_path, is_single_image_record) as w:
            w.process_tind_record(accepted)
            w.process_tind_record(rejected)
            assert w.count_p == 1
            assert w.count_s == 1

    def test_record_link_present(self, tmp_path):
        """Includes the record link in to_process.csv"""
        record = _make_record(record_id='99')
        with TindCsvWriter(tmp_path, self._accept_all) as w:
            w.process_tind_record(record)

        rows = list(read_csv(tmp_path / 'to_process.csv'))
        assert rows[0]['Link to record'] == 'https://digicoll.lib.berkeley.edu/record/99?ln=en'

    def test_empty_record_id(self, tmp_path):
        """Skips and notes records missing the record ID/Link"""
        record = _make_record(record_id='   ')
        with TindCsvWriter(tmp_path, self._reject_all) as w:
            w.process_tind_record(record)

        rows = list(read_csv(tmp_path / 'skipped.csv'))
        assert rows[0]['Record ID'] == ''
        assert rows[0]['Link to record'] == 'No Record Link'

    def test_csv_headers(self, tmp_path):
        """Writes correct skipped/to_process CSV headers"""
        with TindCsvWriter(tmp_path, self._accept_all) as w:
            pass  # no records — just check headers

        assert (read_csv(tmp_path / 'to_process.csv')).fieldnames == [
            'Record ID', '035__a', 'Collection name', 'Status',
            'Link to record', 'Image Url']
        
        assert (read_csv(tmp_path / 'skipped.csv')).fieldnames == [
            'Record ID', '035__a', 'Collection name', 'Status',
            'Link to record']

    def test_accepted_without_856_raises(self, tmp_path):
        """Raises if the filter selects a record without 856 image URLs"""
        record = _make_record(f856_urls=())
        with TindCsvWriter(tmp_path, self._accept_all) as w:
            with pytest.raises(ValueError, match="no 856 URLs"):
                w.process_tind_record(record)

    def test_end_to_end_with_real_filter(self, tmp_path):
        """Integration test using is_single_image_record as the filter."""
        good = _make_record(record_id='1', f336='Image',
                            f856_urls=('http://img.jpg',))
        no_url = _make_record(record_id='2', f336='Image', f856_urls=())
        two_urls = _make_record(record_id='3', f336='Image',
                                f856_urls=('http://a.jpg', 'http://b.jpg'))
        wrong_type = _make_record(record_id='4', f336='Text',
                                  f856_urls=('http://img.jpg',))

        with TindCsvWriter(tmp_path, is_single_image_record) as w:
            for rec in (good, no_url, two_urls, wrong_type):
                w.process_tind_record(rec)

        processed_rows = list(read_csv(tmp_path / 'to_process.csv'))
        skipped_rows = list(read_csv(tmp_path / 'skipped.csv'))

        assert len(processed_rows) == 1
        assert processed_rows[0]['Record ID'] == '1'
        assert processed_rows[0]['Image Url'] == 'http://img.jpg'

        assert len(skipped_rows) == 3
        skipped_ids = {row['Record ID'] for row in skipped_rows}
        assert skipped_ids == {'2', '3', '4'}
