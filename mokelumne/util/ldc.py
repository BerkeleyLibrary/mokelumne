"""Helper functions for the LDC fetcher."""

import re

from datetime import date
from typing import Optional

from bs4 import BeautifulSoup, Tag
from bs4._typing import _IncomingMarkup

def get_csrf_token(
        markup: _IncomingMarkup, param_name: str="authenticity_token"
    ) -> dict[str, str]:
    """
    Given LDC login page HTML, extract the CSRF authenticity token and
    its parameter name.

    :param  markup: the incoming HTML markup to parse.
    :type markup: bs4._typing._IncomingMarkup
    :param param_name: the parameter name to look up and return
    :type param_name: str
    :returns: The parameter name and its value.
    :rtype: dict[str, str]
    """
    soup = BeautifulSoup(markup=markup, features="html.parser")
    tag = soup.find(name="input", attrs={"name": param_name})
    if tag and tag.get("value"):
        return {param_name: tag["value"]}  # pyright: ignore[reportReturnType]
    return {}


def scrape_corpus_metadata(tag: Tag) -> dict[str, str]:
    """
    Parse the HTML of a given table row to return structed metadata.

    :param tag: a `<tr>` tag preparsed by BeautifulSoup.
    :type tag: bs4.element.Tag
    :returns: Structured metadata for a corpus.
    :rtype: dict[str, str]
    """
    cells = tag.find_all("td")
    if len(cells) > 0:
        try:
            catalog_id = cells[0].get_text(strip=True)
            corpus_name = cells[1].get_text(strip=True)

            # note: there is a unique download link for each distinct 
            # invoice date that is associated woith a file
            invoice_date = cells[2].get_text(strip=True)
            download_link = cells[3].a["href"]   # pyright: ignore[reportOptionalSubscript]

            # the file-level metadata is not broken up into distinct cells, so
            # we have to parse it more. the "file" metadata is also not 
            # the true filename as returned by LDC. 
            techmd = cells[4].get_text(strip=True, separator="\n").splitlines()
            file, filesize, checksum = [
                re.sub(r"^\s*(File Size|MD5 Checksum): ", "", t) for t in techmd
            ]
            return {
                "catalog_id": catalog_id,
                "corpus_name": corpus_name,
                "download_link": download_link,  # pyright: ignore[reportReturnType]
                "invoice_date": invoice_date,
                "file": file,
                "filesize": filesize,
                "checksum": checksum
            }
        except (IndexError, KeyError):
            return {}
    return {}


def get_latest_invoice_date(
    corpora: list[dict[str, str]] = [], corpus_id: str = ""
) -> Optional[str]:
    """
    Given a list of corpora and a corpus ID, fetch the latest invoice date for 
    that corpus.

    :param corpora: Corpora metadata parsed from the LDC downloads page.
    :type corpora: list[dict[str, str]]
    :param corpus_id: LDC Catalog ID for the desired corpus.
    :type corpus_id: str
    :returns: Latest invoice date or `None` if no invoice dates for corpus_id.
    :rtype: Optional[str]
    """
    corpus_invoices = [
        (c["catalog_id"], c["invoice_date"]) 
        for c in corpora if c["catalog_id"] == corpus_id
    ]

    if corpus_invoices:
        _, latest = max(corpus_invoices, key=lambda d: date.fromisoformat(d[1]))
        return latest
    
    return None


def filter_corpora(
    corpora: list[dict[str, str]],
    corpus_id: str,
    filename_regex: str | None = None,
) -> list[dict[str, str]]:
    """Return the latest matching corpora entries from parsed metadata."""
    latest = get_latest_invoice_date(corpora=corpora, corpus_id=corpus_id)
    if not latest:
        return []

    fnregex = re.compile(filename_regex or ".*")
    return [
        c for c in corpora
        if (
            c["catalog_id"] == corpus_id
            and c["invoice_date"] == latest
            and re.search(fnregex, c.get("file", ""))
        )
    ]
