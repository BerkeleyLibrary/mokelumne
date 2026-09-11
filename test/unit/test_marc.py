"""Unit tests for MARC utility helpers."""

import pytest
from airflow.sdk.exceptions import AirflowException

from mokelumne.util.marc import _extract_language_codes


def _record_xml(*fields: str) -> str:
    """Build a MARCXML record from field XML fragments."""
    return "\n".join(
        [
            '<record xmlns="http://www.loc.gov/MARC21/slim">',
            '<leader>00000nam a2200000 i 4500</leader>',
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
