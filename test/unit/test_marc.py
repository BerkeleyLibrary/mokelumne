"""Unit tests for MARC utility helpers."""

from io import StringIO
from pathlib import Path

import pytest
from airflow.sdk.exceptions import AirflowException
from pymarc.marcxml import parse_xml_to_array

from mokelumne.util import marc
from mokelumne.util.marc import (
    _extract_language_codes,
    derive_tesseract_codes_from_marc,
    _language_codes_from_record,
    extract_url_names,
    list_values_from_marc_xml,
)


def _record_xml(*fields: str) -> str:
    """Build a MARCXML record from field XML fragments."""
    return "\n".join(
        [
            '<record xmlns="http://www.loc.gov/MARC21/slim">',
            "<leader>00000nam a2200000 i 4500</leader>",
            *fields,
            "</record>",
        ]
    )


def test_extract_language_codes_reads_controlfield_008():
    """Extract the fixed-field language code from a single Alma MARCXML record."""
    controlfield = (
        '<controlfield tag="008">'
        "230101s2023    cau|||||||||||000 0|eng|d"
        "</controlfield>"
    )

    assert _extract_language_codes(_record_xml(controlfield)) == ["eng"]


def test_extract_language_codes_reads_041_subfields_once():
    """Extract unique language codes from all 041 language subfields."""
    controlfield = (
        '<controlfield tag="008">'
        "230101s2023    cau|||||||||||000 0|eng|d"
        "</controlfield>"
    )
    datafield = """
    <datafield tag="041" ind1="1" ind2=" ">
      <subfield code="a">engfre</subfield>
      <subfield code="b">ger</subfield>
      <subfield code="z">ignored</subfield>
      <subfield code="e">eng</subfield>
    </datafield>
    """

    assert _extract_language_codes(_record_xml(controlfield, datafield)) == [
        "eng",
        "fre",
        "ger",
    ]


def test_extract_language_codes_rejects_multiple_records():
    """Reject collection XML because AlmaHook returns one MARCXML record."""
    collection_xml = "\n".join(
        [
            '<collection xmlns="http://www.loc.gov/MARC21/slim">',
            _record_xml(),
            _record_xml(),
            "</collection>",
        ]
    )

    with pytest.raises(AirflowException, match="contains 2 records; expected 1"):
        _extract_language_codes(collection_xml)


def test_extract_language_codes_rejects_malformed_xml():
    """Reject XML that cannot be parsed at all."""
    with pytest.raises(AirflowException, match="Could not parse Alma MARCXML record"):
        _extract_language_codes("<record><unclosed>")


def test_extract_language_codes_returns_empty_list_when_no_language_data():
    """Return an empty list when the record has no 008, 041, 546, or 500 data."""
    assert not _extract_language_codes(_record_xml())


def test_extract_language_codes_ignores_008_shorter_than_38_chars():
    """Ignore an 008 field too short to contain the language code position."""
    controlfield = '<controlfield tag="008">230101s2023</controlfield>'

    assert not _extract_language_codes(_record_xml(controlfield))


def test_extract_language_codes_ignores_non_alpha_008_language_code():
    """Ignore an 008 language code that is not three lowercase letters."""
    controlfield = (
        '<controlfield tag="008">'
        "230101s2023    cau|||||||||||000 0|1||d"
        "</controlfield>"
    )

    assert not _extract_language_codes(_record_xml(controlfield))


def test_extract_language_codes_falls_back_to_546_when_008_is_mul():
    """Parse a free-text 546 note for a language when 008 reports 'mul'."""
    controlfield = (
        '<controlfield tag="008">'
        "230101s2023    cau|||||||||||000 0|mul|d"
        "</controlfield>"
    )
    datafield = """
    <datafield tag="546" ind1=" " ind2=" ">
      <subfield code="a">Text in French and Spanish.</subfield>
    </datafield>
    """

    assert _extract_language_codes(_record_xml(controlfield, datafield)) == [
        "mul",
        "fre",
        "spa",
    ]


def test_extract_language_codes_falls_back_to_500_when_no_546():
    """Parse a 500 note for a leading or contextual language phrase when 546 is absent."""
    datafield = """
    <datafield tag="500" ind1=" " ind2=" ">
      <subfield code="a">Captions in Spanish.</subfield>
    </datafield>
    """

    assert _extract_language_codes(_record_xml(datafield)) == ["spa"]


def test_extract_language_codes_500_matches_leading_language_phrase():
    """Match a 500 note that begins directly with the language name."""
    datafield = """
    <datafield tag="500" ind1=" " ind2=" ">
      <subfield code="a">French subtitles included.</subfield>
    </datafield>
    """

    assert _extract_language_codes(_record_xml(datafield)) == ["fre"]


def test_extract_language_codes_500_when_546_present_but_empty():
    """Fall back to 500 notes when a 546 field exists but has no useful text."""
    datafield_546 = '<datafield tag="546" ind1=" " ind2=" "></datafield>'
    datafield_500 = """
    <datafield tag="500" ind1=" " ind2=" ">
      <subfield code="a">In French.</subfield>
    </datafield>
    """

    assert _extract_language_codes(_record_xml(datafield_546, datafield_500)) == ["fre"]


def test_extract_language_codes_no_match_returns_empty_when_546_and_500_unhelpful():
    """Return an empty list when neither 546 nor 500 notes match any known language."""
    datafield = """
    <datafield tag="500" ind1=" " ind2=" ">
      <subfield code="a">No language information here.</subfield>
    </datafield>
    """

    assert not _extract_language_codes(_record_xml(datafield))


def test_language_codes_from_record_deduplicates_across_008_and_041():
    """Do not duplicate a language code already found in 008 when it recurs in 041."""
    controlfield = (
        '<controlfield tag="008">'
        "230101s2023    cau|||||||||||000 0|eng|d"
        "</controlfield>"
    )
    datafield = """
    <datafield tag="041" ind1="1" ind2=" ">
      <subfield code="a">eng</subfield>
    </datafield>
    """

    assert _language_codes_from_record(
        parse_xml_to_array(StringIO(_record_xml(controlfield, datafield)))[0]
    ) == ["eng"]


def test_derive_tesseract_codes_from_marc_maps_and_joins_unique_codes():
    """Map MARC language codes to Tesseract codes and join them with '+'."""
    controlfield = (
        '<controlfield tag="008">'
        "230101s2023    cau|||||||||||000 0|ara|d"
        "</controlfield>"
    )
    datafield = """
    <datafield tag="041" ind1="1" ind2=" ">
      <subfield code="a">arefre</subfield>
    </datafield>
    """

    assert (
        derive_tesseract_codes_from_marc(_record_xml(controlfield, datafield))
        == "ara+fra"
    )


def test_derive_tesseract_codes_from_marc_for_chinese_lang():
    """Correctly returns tesseract script codes for Chinese language records."""
    controlfield = (
        '<controlfield tag="008">'
        "230101s2023    cau|||||||||||000 0|chi|d"
        "</controlfield>"
    )
    datafield = """
    <datafield tag="041" ind1="1" ind2=" ">
      <subfield code="a">zho</subfield>
    </datafield>
    """

    assert (
        derive_tesseract_codes_from_marc(_record_xml(controlfield, datafield))
        == "script/HanT+script/HanS"
    )


def test_derive_tesseract_codes_from_marc_returns_empty_string_when_no_codes():
    """Return an empty string when no language codes can be derived."""
    assert derive_tesseract_codes_from_marc(_record_xml()) == ""



class TestExtractUrlNames:
    """Tests for extract_url_names."""

    def test_extracts_file_names_from_urls(self):
        """Extracts file names from paths, ignoring empty paths"""
        urls = [
            "https://example.org/path/to/file_one.jpg",
            "",
            "https://example.org/another/path/file_two.pdf",
        ]

        assert extract_url_names(urls) == [
            "file_one.jpg",
            "file_two.pdf",
        ]


class _FakeField:
    def __init__(self, indicator1: str, indicator2: str, values: dict[str, str]):
        self.indicator1 = indicator1
        self.indicator2 = indicator2
        self._values = values

    def __getitem__(self, key: str) -> str:
        return self._values[key]

    def get(self, key: str, default=None):
        """return value from key"""
        return self._values.get(key, default)


class _FakeRecord:
    def __init__(self, fields_by_tag: dict[str, list[_FakeField]]):
        self._fields_by_tag = fields_by_tag

    def get_fields(self, field_num: str) -> list[_FakeField]:
        """return all fields by tag"""
        return self._fields_by_tag.get(field_num, [])


class TestListValuesFromMarcXml:
    """Tests for list_values_from_marc_xml."""

    def test_collects_matching_subfield_values(self, tmp_path: Path, monkeypatch):
        """collects only matching subfield values"""
        xml_file = tmp_path / "tind_bulk.xml"
        xml_file.write_text("<xml />", encoding="utf-8")

        records = [
            _FakeRecord(
                {
                    "856": [
                        _FakeField("4", " ", {"u": "https://example.org/file_one.jpg"}),
                        _FakeField("0", " ", {"u": "https://example.org/ignored.jpg"}),
                    ]
                }
            ),
            _FakeRecord(
                {
                    "856": [
                        _FakeField("4", " ", {"u": "https://example.org/file_two.pdf"}),
                    ],
                    "245": [
                        _FakeField("1", "0", {"a": "ignored title"}),
                    ],
                }
            ),
        ]

        def fake_map_xml(callback, xml_path):
            """fake map"""
            assert xml_path == str(xml_file)
            for record in records:
                callback(record)

        monkeypatch.setattr(marc, "map_xml", fake_map_xml)

        result = list_values_from_marc_xml(
            xml_file_path=xml_file,
            field_num="856",
            ind1="4",
            ind2=" ",
            sub="u",
        )

        assert result == [
            "https://example.org/file_one.jpg",
            "https://example.org/file_two.pdf",
        ]

    def test_raises_when_xml_file_is_missing(self, tmp_path: Path):
        """Raises an error when xml file is missing"""
        missing_file = tmp_path / "missing.xml"

        with pytest.raises(FileNotFoundError, match="MARCXML file does not exist"):
            list_values_from_marc_xml(
                xml_file_path=missing_file,
                field_num="856",
                ind1="4",
                ind2=" ",
                sub="u",
            )
