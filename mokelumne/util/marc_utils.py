from airflow.sdk.exceptions import AirflowException

_MMSID_RE = re.compile(r"^\d{18}$")
_LANG_CODE_RE = re.compile(r"[a-z]{3}")

_SRW_NS = "http://www.loc.gov/zing/srw/"
_MARC_NS = "http://www.loc.gov/MARC21/slim"
# 041 subfields that carry language codes
_LANG_SUBFIELDS = ("a", "b", "e", "f", "g")

def _extract_language_codes(sru_xml: str) -> list[str]:
    """Parse an Alma SRU marcxml response and return MARC language codes.

    :param sru_xml: Raw SRU response XML string.
    :returns: Unique three-letter MARC language codes.
    :rtype: list[str]
    :raises AirflowException: If the XML cannot be parsed or contains
        more than one record.
    """
    try:
        root = ET.fromstring(sru_xml)
    except ET.ParseError as exc:
        raise AirflowException(f"Could not parse Alma SRU response: {exc}") from exc

    num_el = root.find(f"{{{_SRW_NS}}}numberOfRecords")
    if num_el is None or not num_el.text:
        raise AirflowException("Alma SRU response missing numberOfRecords")
    num = int(num_el.text.strip())
    if num == 0:
        return []
    if num > 1:
        raise AirflowException(f"Alma SRU returned {num} records for MMSID; expected 1")

    marc_record = root.find(f".//{{{_MARC_NS}}}record")
    if marc_record is None:
        raise AirflowException("Alma SRU response contains no MARC record element")

    codes: list[str] = []

    cf008 = marc_record.find(f"{{{_MARC_NS}}}controlfield[@tag='008']")
    if cf008 is not None and cf008.text and len(cf008.text) >= 38:
        lang = cf008.text[35:38]
        if re.fullmatch(r"[a-z]{3}", lang):
            codes.append(lang)

    for f041 in marc_record.findall(f"{{{_MARC_NS}}}datafield[@tag='041']"):
        combined = ""
        for sf_code in _LANG_SUBFIELDS:
            for subfield in f041.findall(f"{{{_MARC_NS}}}subfield[@code='{sf_code}']"):
                combined += subfield.text or ""
        for m in _LANG_CODE_RE.finditer(combined):
            lang = m.group()
            if lang not in codes:
                codes.append(lang)

    return codes