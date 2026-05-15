"""
Fetches files for a given corpus from the Linguistic Data Consortium catalog.
"""

# pyright: reportTypedDictNotRequiredAccess=false

from __future__ import annotations
import hashlib
import json
import logging
import re

from mmap import mmap, ACCESS_READ

from airflow.sdk import Param, chain, dag, get_current_context, task
from bs4 import BeautifulSoup

from mokelumne.providers.ldc.hooks.ldc import LDCHook
from mokelumne.util.ldc import filter_corpora, scrape_corpus_metadata
from mokelumne.util.storage import run_dir

logger = logging.getLogger(__name__)

@dag(
    description="Fetches files for a given corpus from the Linguistic Data Consortium catalog",
    schedule=None,
    catchup=False,
    params={
        "ldc_corpus": Param(
            default="",
            title="LDC Catalog ID",
            description="The catalog ID for the desired LDC corpus",
            type="string",
            ),
        "filename_regex": Param(
            default="",
            title="Filename regular expression",
            description="""Regular expression to match file metadata in the
LDC catalog. Note that this is not necessarily the same as the downloaded
filename reported by LDC's webserver.""",
            type=["string", "null"],
            format="regex",
            ),
    },
    tags=["ldsp"],
)
def fetch_ldc_corpus_files():
    """
    Fetch a corpus from the `Linguistic Data Consortium catalog`_.  LDC
    does not provide an API, so we have to screenscrape into an authorized
    session to fetch the list of available datasets.

    This is effectively a reimplementation of `ldcdl`_ by Jonathan May and
    Alex Hedges.

    .. _Linguistic Data Consortium catalog: https://catalog.ldc.upenn.edu/
    .. _ldcdl: https://github.com/jonmay/ldcdl
    """

    hook = LDCHook()

    @task
    def get_available_ldc_corpora() -> str:
        """
        Fetch the page listing the corpora available for download from the LDC
        catalog. This is an HTML page cached locally for further parsing.

        :returns: Path to the HTML file fetched from LDC.
        :rtype: str
        """
        ctx = get_current_context()
        dest_dir = run_dir(ctx["run_id"])
        corpora_html_path = dest_dir / "corpora.html"
        
        response = hook.get_corpora_response()
        with open(corpora_html_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        return str(corpora_html_path)


    @task
    def parse_corpora_metadata(corpora_file) -> list[dict[str, str]]:
        """
        Parse the HTML of the catalog to create a structured representation for
        further use. There is no API for LDC, so we are forced to screenscrape.

        :param corpora_file: Location of the fetched downloads page.
        :returns: Parsed LDC metadata as a list of dicts.
        :rtype: list[dict[str, str]]
        """
        ctx = get_current_context()
        corpora_json = run_dir(ctx["run_id"]) / "corpora.json"

        with open(corpora_file) as page:
            corpora_html = page.read()

        data = BeautifulSoup(corpora_html, "html.parser")
        rows = data.select("#user-corpora-download-table > tbody > tr")
        corpora = [scrape_corpus_metadata(row) for row in rows]

        with open(corpora_json, "w") as corpora_out:
            corpora_out.write(json.dumps(corpora))

        return corpora


    @task.short_circuit
    def corpus_is_available(corpora_list) -> list[dict[str, str]]:
        """
        Check to see if the requested corpus is listed in the set of corpora
        available for download. Used to shortcircuit the ``fetch_ldc_corpus_files()``
        task if the dataset is not available.

        Since the LDC catalog provides multiple duplicate downloads based on
        invoice date, this also filters the downloads available to
        those with the latest invoice date.

        :param corpora_list: Parsed LDC metadata list.
        :returns: A list of LDC downloads for fetching or an empty list.
        :rtype: list[dict[str, str]]
        """
        ctx = get_current_context()
        id_ = ctx["params"].get("ldc_corpus", "")
        filename_regex = ctx["params"].get("filename_regex")

        return filter_corpora(corpora=corpora_list, corpus_id=id_, filename_regex=filename_regex)


    @task
    def download_corpus_file(filedict) -> str:
        """
        Download a corpus from the LDC catalog using the authenticated hook and
        verify that the MD5 checksum reported by LDC matches the downloaded file.

        :param filedict: A dict representing the file metadata
        :returns: The location of the downloaded dataset file.
        :rtype: str
        """
        ctx = get_current_context()
        dest_dir = run_dir(ctx["run_id"])
        resp = hook.get_corpus_file(filedict["download_link"])

        match = re.match(
            r'^attachment; filename="(.*)"$', resp.headers.get("Content-Disposition", "")
        )

        if match:
            dest = dest_dir / match.group(1)
        else:
            logger.warning("No Content-Disposition header; falling back to catalog filename")
            dest = dest_dir / filedict["filename"]

        with open(dest, "wb") as out:
            for chunk in resp.iter_content(chunk_size=(8*1024)):
                if chunk:
                    out.write(chunk)

        with (
            open(dest, "rb") as f,
            mmap(f.fileno(), 0, access=ACCESS_READ) as f
        ):
            dl_checksum = hashlib.md5(f).hexdigest()

        if dl_checksum != filedict["checksum"]:
            logger.warning(
                "Downloaded file's checksum %s does not match LDC checksum %s" % (
                    dl_checksum, filedict["checksum"]
                )
            )
        return str(dest)


    corpora_file = get_available_ldc_corpora()
    available_corpora = parse_corpora_metadata(corpora_file)
    files_to_download = corpus_is_available(available_corpora)
    chain(
        files_to_download,
        download_corpus_file.expand(filedict=files_to_download)
    )


fetch_ldc_corpus_files()  # pyright: ignore[reportUnusedExpression]
