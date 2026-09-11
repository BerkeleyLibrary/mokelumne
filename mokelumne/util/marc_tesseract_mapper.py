"""
Utility functions for working with the marc_tesseract_map.json file.
"""

import json
from functools import cache
from pathlib import Path
from typing import Dict

_MAP_PATH = Path(__file__).parent / "marc_tesseract_map.json"


@cache
def load_marc_tesseract_map() -> Dict[str, Dict[str, str]]:
    """
    Load the MARC-to-Tesseract language code mapping.

    :returns: mapping of MARC language codes to their Tesseract
        code and language-detection regex.
    :rtype: Dict[str, Dict[str, str]]
    """
    with _MAP_PATH.open(encoding="utf-8") as f:
        return json.load(f)


def get_tesseract_code(marc_code: str) -> str | None:
    """
    Get the Tesseract language code for a given MARC language code.

    :param marc_code: Three-letter MARC language code.
    :returns: Corresponding Tesseract language code, or None if not found.
    :rtype: str | None
    """
    return load_marc_tesseract_map().get(marc_code, {}).get("tesseract_code")


@cache
def lang_to_marc_map() -> Dict[str, str]:
    """
    Create a mapping of languages to MARC codes.
    Note: Will return a single marc code for each language, even if multiple
    marc codes share the same language. The split handles a pipe-delimited
    string like "persian|farsi". As with the previous perl script, the last
    language match will be the one that is returned in the mapping.

    :returns: Mapping of languages to MARC language codes.
    :rtype: Dict[str, str]
    """
    result: Dict[str,str] = {}
    for marc_code, entry in load_marc_tesseract_map().items():
        for lang in entry["lang_regex"].split("|"):
            result[lang] = marc_code
    return result


@cache
def marc_lang_regex() -> str:
    """
    Create a regex pattern that matches any of the languages in the MARC-to-Tesseract mapping.

    :returns: Regex pattern string.
    :rtype: str
    """
    return "|".join(entry["lang_regex"] for entry in load_marc_tesseract_map().values())

def tesseract_code_list_to_string(tesseract_codes: list[str]) -> str:
    """
    Convert a list of Tesseract language codes into a Tesseract language string.

    Entries may themselves be comma-separated (e.g. "deu_frak, deu"); each sub-code is split out and
    deduplicated (preserving first-seen order) before joining, matching the previous Perl behavior.

    :param tesseract_codes: List of Tesseract language codes, each possibly comma-separated.
    :returns: String of unique language codes separated by '+'.
    :rtype: str
    """
    tess_codes: dict[str, None] = {}
    for code in tesseract_codes:
        for sub_code in code.split(","):
            sub_code = sub_code.strip()
            if sub_code:
                tess_codes.setdefault(sub_code, None)
    return "+".join(tess_codes)
