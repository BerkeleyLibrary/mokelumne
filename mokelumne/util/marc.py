"""Utilities for working with MARC records."""

import re
from collections.abc import Iterable
from io import StringIO
from pathlib import Path
from xml.sax import SAXException
from yarl import URL

from airflow.sdk.exceptions import AirflowException
from pymarc import Record
from pymarc.marcxml import parse_xml_to_array, map_xml

from mokelumne.util.marc_tesseract_mapper import (
    marc_lang_regex,
    lang_to_marc_map,
    get_tesseract_code,
    tesseract_code_list_to_string,
)

_MMSID_RE = re.compile(r"^\d{18}$")
_LANG_CODE_RE = re.compile(r"[a-z]{3}")

# 041 subfields that carry language codes
_LANG_SUBFIELDS = ("a", "b", "e", "f", "g")


def _extract_language_codes(record_xml: str) -> list[str]:
    """Parse an Alma SRU response MARCXML record and return MARC language codes.

    :param record_xml: Raw MARCXML record string.
    :returns: Unique three-letter MARC language codes.
    :rtype: list[str]
    :raises AirflowException: If the XML cannot be parsed or does not contain
        exactly one record.
    """
    try:
        records = parse_xml_to_array(StringIO(record_xml), strict=True)
    except SAXException as exc:
        raise AirflowException(f"Could not parse Alma MARCXML record: {exc}") from exc

    if len(records) != 1:
        raise AirflowException(
            f"Alma MARCXML response contains {len(records)} records; expected 1"
        )

    return _language_codes_from_record(records[0])


def _language_codes_from_notes(
    record: Record, lang_regex: str, lang_map: dict[str, str]
) -> list[str]:
    """Extract language codes from 546 and 500 note fields in the MARC record."""
    codes = []
    fields_546 = record.get_fields("546")
    first_546a = fields_546[0].get("a") if fields_546 else None
    if first_546a:
        for match in re.finditer(lang_regex, first_546a, flags=re.IGNORECASE):
            lang = match.group().lower()
            if marc_code := lang_map.get(lang):
                codes.append(marc_code)
    else:
        # if no 546a, check if 500a starts with a language or phrases like
        # "In [language]", "Captions in [language]", or "Text in [language]"
        context_regex = rf"(?:In|Captions in|Text in)\s+({lang_regex})|^({lang_regex})"
        fields_500 = record.get_fields("500")
        for field_500 in fields_500:
            subfield_a = field_500.get("a")
            if subfield_a:
                for match in re.finditer(
                    context_regex, subfield_a, flags=re.IGNORECASE
                ):
                    lang = (match.group(1) or match.group(2)).lower()
                    if marc_code := lang_map.get(lang):
                        codes.append(marc_code)
    return codes


def _language_codes_from_record(record: Record) -> list[str]:
    """Return unique MARC language codes from *record*.

    :param record: pyMARC record to inspect.
    :returns: Unique three-letter MARC language codes.
    :rtype: list[str]
    """

    codes: list[str] = []

    fields_008 = record.get_fields("008")
    if fields_008 and fields_008[0].data and len(fields_008[0].data) >= 38:
        lang = fields_008[0].data[35:38]
        if re.fullmatch(r"[a-z]{3}", lang):
            codes.append(lang)

    for field_041 in record.get_fields("041"):
        combined = "".join(field_041.get_subfields(*_LANG_SUBFIELDS))
        for m in _LANG_CODE_RE.finditer(combined):
            lang = m.group()
            if lang not in codes:
                codes.append(lang)

    # if codes is empty or contains a 'mul' value, check the 546a
    if not codes or "mul" in codes:
        lang_regex = marc_lang_regex()
        lang_map = lang_to_marc_map()
        codes = list(
            dict.fromkeys(
                codes + _language_codes_from_notes(record, lang_regex, lang_map)
            )
        )
    return codes


def derive_tesseract_codes_from_marc(record_xml: str) -> str:
    """Derive Tesseract language codes from MARCXML record.

    :param record_xml: Raw MARCXML record string.
    :returns: Tesseract language codes as a string suitable for Tesseract CLI.
    :rtype: str
    """
    marc_codes = _extract_language_codes(record_xml)
    tesseract_codes = [
        code for marc_code in marc_codes if (code := get_tesseract_code(marc_code))
    ]
    return tesseract_code_list_to_string(tesseract_codes)


def extract_url_names(url_list: Iterable[str]) -> list[str]:
    """Converts a list of URLs into a list of their file basenames."""
    return [URL(url).name for url in url_list if url]


def list_values_from_marc_xml(
    xml_file_path: str | Path,
    field_num: str,
    ind1: str,
    ind2: str,
    sub: str,
) -> list[str]:
    """Parses a MARCXML file and returns a flat list of matching field values."""
    xml_path = Path(xml_file_path)

    if not xml_path.exists():
        raise FileNotFoundError(f"MARCXML file does not exist: {xml_path}")

    matching_values: list[str] = []

    def process_record(record) -> None:
        fields = record.get_fields(field_num)

        matching_values.extend(
            subfield
            for field in fields
            if field.indicator1 == ind1
            and field.indicator2 == ind2
            and (subfield := field.get(sub))
        )

    map_xml(process_record, str(xml_path))

    return matching_values
