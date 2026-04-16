# pyright: reportTypedDictNotRequiredAccess=false

from __future__ import annotations
import logging
import re

import mechanize
import requests

from requests.cookies import RequestsCookieJar, cookiejar_from_dict

from airflow.sdk import Connection, Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException
from bs4 import BeautifulSoup as bs

from mokelumne.util.storage import run_dir

logger = logging.getLogger(__name__)

@dag(
    schedule=None,
    catchup=False,
    params={
        "ldc_corpus": Param("", description="LDC Catalog ID", type="string")
    },
)
def ldc_fetcher():

    def authed_browser() -> mechanize.Browser:
        conn = Connection.get("ldc")
        br = mechanize.Browser()
        br.set_debug_redirects(True)
        br.set_debug_http(True)
        br.set_handle_robots(False)
        br.open(f"{conn.host}/login")
        br.select_form(nr=0)
        br["spree_user[login]"] = conn.login
        br["spree_user[password]"] = conn.login
        br.submit()
        return br

    @task
    def get_available_ldc_corpora() -> dict[str, dict[str, str]]:
        conn = Connection.get("ldc")
        datasets_url = f"{conn.host}/organization/downloads"
        br = authed_browser()
        corpora_html = br.open(datasets_url)
        page = bs(corpora_html.read(), 'html.parser')  # pyright: ignore[reportOptionalMemberAccess]
        corpora_table = page.find(id_='user-corpora-download-table')
        logger.debug(page.get_text())
        corpora_rows = corpora_table.tbody.find_all("tr")

        corpora = {}
        for c in corpora_rows:
            data = c.find_all('td')
            cid = data[0].get_text(strip=True)
            corpora[cid] = {}
            corpora[cid]["name"] = data[1].get_text(strip=True)
            corpora[cid]["invoice_date"] = data[2].get_text(strip=True)
            corpora[cid]["download_link"] = data[3].a.href.get_text(strip=True)
            
            techmd = data[4].get_text(strip=True, separator="\n").splitlines()
            corpora[cid]["filename"] = techmd[0]
            corpora[cid]["filesize"], corpora[cid]["checksum"] = [
                re.sub(r"^(File Size|MD5 Checksum): ", "", t) for t in techmd[1]
            ]
        
        return corpora

    @task
    def fetch_ldc_corpus(available_corpora):
        br = authed_browser()
        context = get_current_context()
        conn = Connection.get("ldc")
        corpus = context["params"].get("ldc_corpus")
        if corpus in available_corpora:
            dl_uri = f"{conn.host}/{available_corpora[corpus].get("download_link")}"
            logger.info(
                f"TODO: fetch {dl_uri} for corpus {corpus}"
            )
        else:
            raise AirflowFailException(
                f"LDC corpus {corpus} not found in available corpora: ",
                repr(available_corpora)
            )
    
    available_corpora = get_available_ldc_corpora()
    fetch_ldc_corpus(available_corpora)

    
    # @task
    # def fetch_ldc_dataset():
    #     context = get_current_context()
    #     run_id = context["run_id"]
    #     ldc_dataset = context["params"]["ldc_dataset"]
    
    # fetch_ldc_dataset # type: ignore

ldc_fetcher()  # pyright: ignore[reportUnusedExpression]
