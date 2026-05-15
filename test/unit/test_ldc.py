# pylint: disable=line-too-long

"""Pytest testcases for mokelumne.util.ldc"""
import importlib.resources
import json
import pytest

from bs4 import BeautifulSoup

from mokelumne.util.ldc import (
    filter_corpora, get_csrf_token, get_latest_invoice_date, scrape_corpus_metadata,
)
from .. import fixtures


class TestLDC:
    """Test class for mokelumne.util.ldc."""
    with (
        importlib.resources.path(fixtures, "ldc-treebank-3.json") as test_json,
        open(test_json, encoding="utf-8") as fh
    ):
        duplicate_invoice_data = json.loads(fh.read())
        single_item = [duplicate_invoice_data[0]]

    @pytest.mark.parametrize(
        "markup,param_name,expected", [
        pytest.param(
            '<form><input name="authenticity_token" value="foo"/></form>',
            "authenticity_token",
            {"authenticity_token": "foo"},
            id="with_default_param_name"
        ),
        pytest.param(
            '<form><input name="something_else" value="bar"/><input name="authenticity_token" value="baz"/></form>',
            "something_else",
            {"something_else": "bar"},
            id="with_param_name"
        ),
        pytest.param(
            '<form><input name="csrf-token" value="quux"/></form',
            "doesnt_exist",
            {},
            id="without_matching_tag"
        )
    ])
    def test_get_csrf_token(self, markup, param_name, expected) -> None:
        """Ensure we can gather the CSRF token from the LDC login form."""
        assert get_csrf_token(markup=markup, param_name=param_name) == expected


    @pytest.mark.parametrize(
        "corpora,corpus_id,expected", [
        pytest.param(
            [{ "catalog_id": "LDC99T42", "corpus_name": "Treebank-3", "download_link": "/download/4c0512a1451377eb2790d557fc76a690fa11693ad846df02f3ee59d12788", "invoice_date": "2025-01-01", "file": "treebank_3_LDC99T42", "filesize": "51.6 MB", "checksum": "98c74f99f6ca17dc88efb4077fcd9539" }],
            "LDC99T42",
            "2025-01-01",
            id="with_single_item_list"
        ),
        pytest.param(duplicate_invoice_data, "LDC99T42", "2020-08-22", id="with_dupes"),
        pytest.param([], "bogus", None, id="with_empty_corpora_list")
        ]
    )
    def test_get_latest_invoice_date(
        self, corpora, corpus_id, expected
    ) -> None:
        """Ensure latest invoice date is fetched for a single corpus."""
        assert get_latest_invoice_date(corpora=corpora, corpus_id=corpus_id) == expected


    @pytest.mark.parametrize(
        "tag,expected", [
            pytest.param(BeautifulSoup("""
                <tr class="odd">
                <td class="">LDC2026S04</td>
                <td>CALLHOME Spanish Second Edition</td>
                <td class="">2026-03-16</td>
                <td class="download-counter-cell">
                    <span class='download-counter-counter'>(2)</span> <a class='button download-counter-button' href='/download/6223e1ba26b43ce2787aa7303fd0329c1955d225fa30f8688e85abb019e8' title='Download Corpus'><span class='glyphicon glyphicon-download-alt'></span></a>
                </td>
                <td>
                    CALLHOME_Spanish_Second_Edition.zip<br/>
                        File Size: 1.46 GB
                        MD5 Checksum: d57395eacde73a80ca6e2abcd7ddde52
                </td>
                </tr>""", "html.parser"),
                {
                    "catalog_id": "LDC2026S04",
                    "corpus_name": "CALLHOME Spanish Second Edition",
                    "invoice_date": "2026-03-16",
                    "download_link": "/download/6223e1ba26b43ce2787aa7303fd0329c1955d225fa30f8688e85abb019e8",
                    "file": "CALLHOME_Spanish_Second_Edition.zip",
                    "filesize": "1.46 GB",
                    "checksum": "d57395eacde73a80ca6e2abcd7ddde52"
                },
                id="with_single_row"
            ),
            pytest.param(
                BeautifulSoup("<tr></tr>", "html.parser"),
                {},
                id="with_empty_row"
            ),
            pytest.param(
                BeautifulSoup("<tr><td>boop</td></tr>", "html.parser"),
                {},
                id="with_malformed_row"
            ),
            pytest.param(
                BeautifulSoup("""
                <tr class="odd">
                <td class="">LDC2026S04</td>
                <td>CALLHOME Spanish Second Edition</td>
                <td class="">2026-03-16</td>
                <td class="download-counter-cell">
                    <span class='download-counter-counter'>(2)</span> <a class='button download-counter-button' fake='/download/blah' title='Download Corpus'><span class='glyphicon glyphicon-download-alt'></span></a>
                </td>""", "html.parser"),
                {},
                id="with_malformed_download_link"
            )
        ]
    )
    def test_scrape_corpus_metadata(self, tag, expected) -> None:
        """Ensure we can parse the LDC organization downloads page into a
        JSON-based structure."""
        assert scrape_corpus_metadata(tag) == expected


    @pytest.mark.parametrize(
        "corpora,corpus_id,filename_regex,expected", [
        pytest.param(
            single_item,
            "LDC99T42",
            None,
            single_item,
            id="with_single_matching_item"
        ),
        pytest.param(
            duplicate_invoice_data,
            "LDC99T42",
            ".*treebank.*",
            single_item,
            id="with_regex_filter"
        ),
        pytest.param(
            duplicate_invoice_data,
            "LDC99T42",
            ".*nonexistent.*",
            [],
            id="with_no_matches"
        ),
        pytest.param(
            [],
            "bogus",
            None,
            [],
            id="with_empty_corpora_list"
        )
        ]
    )
    def test_filter_corpora(
        self, corpora, corpus_id, filename_regex, expected
    ) -> None:
        """Ensure corpora are filtered correctly by corpus_id and filename regex."""
        assert filter_corpora(corpora=corpora, corpus_id=corpus_id, filename_regex=filename_regex) == expected

