"""PyTest cases for the mokelumne.util.parse_marc_xml module."""

from pathlib import Path

import pytest

from mokelumne.util import parse_marc_xml


class TestExtractUrlNames:
    """Tests for extract_url_names."""

    def test_extracts_file_names_from_urls(self):
        urls = [
            "https://example.org/path/to/file_one.jpg",
            "",
            "https://example.org/another/path/file_two.pdf",
        ]

        assert parse_marc_xml.extract_url_names(urls) == [
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
        return self._values.get(key, default)


class _FakeRecord:
    def __init__(self, fields_by_tag: dict[str, list[_FakeField]]):
        self._fields_by_tag = fields_by_tag

    def get_fields(self, field_num: str) -> list[_FakeField]:
        return self._fields_by_tag.get(field_num, [])


class TestListValuesFromMarcXml:
    """Tests for list_values_from_marc_xml."""

    def test_collects_matching_subfield_values(self, tmp_path: Path, monkeypatch):
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
            assert xml_path == str(xml_file)
            for record in records:
                callback(record)

        monkeypatch.setattr(parse_marc_xml, "map_xml", fake_map_xml)

        result = parse_marc_xml.list_values_from_marc_xml(
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
        missing_file = tmp_path / "missing.xml"

        with pytest.raises(FileNotFoundError, match="MARCXML file does not exist"):
            parse_marc_xml.list_values_from_marc_xml(
                xml_file_path=missing_file,
                field_num="856",
                ind1="4",
                ind2=" ",
                sub="u",
            )
