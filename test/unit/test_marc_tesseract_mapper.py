"""Unit tests for the MARC-to-Tesseract mapping helpers."""

import re

from mokelumne.util.marc_tesseract_mapper import (
    get_tesseract_code,
    lang_to_marc_map,
    marc_lang_regex,
    tesseract_code_list_to_string,
)


def test_get_tesseract_code_returns_known_code():
    """Return the tesseract code for a known MARC language code."""
    assert get_tesseract_code("ara") == "ara"


def test_get_tesseract_code_returns_none_for_unknown_code():
    """Return None for a MARC code not present in the mapping."""
    assert get_tesseract_code("zzz") is None


def test_lang_to_marc_map_splits_pipe_delimited_regex():
    """Split a pipe-delimited lang_regex into separate map entries."""
    mapping = lang_to_marc_map()

    assert mapping["arabic"] == "jrb"  # last MARC code sharing this language wins


def test_marc_lang_regex_is_valid_and_matches_known_language():
    """Compile the combined regex and match a known language name."""
    pattern = marc_lang_regex()

    assert re.search(pattern, "Arabic", flags=re.IGNORECASE)


def test_tesseract_code_list_to_string_empty_list():
    """Return an empty string for an empty code list."""
    assert tesseract_code_list_to_string([]) == ""


def test_tesseract_code_list_to_string_splits_comma_separated_entry():
    """Split a comma-separated tesseract code entry into individual codes."""
    assert tesseract_code_list_to_string(["deu_frak, deu"]) == "deu_frak+deu"


def test_tesseract_code_list_to_string_deduplicates_preserving_order():
    """Deduplicate repeated codes across entries, keeping first-seen order."""
    assert tesseract_code_list_to_string(["deu", "deu_frak, deu"]) == "deu+deu_frak"
