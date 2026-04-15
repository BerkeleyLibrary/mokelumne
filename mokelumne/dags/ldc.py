# pyright: reportTypedDictNotRequiredAccess=false

from __future__ import annotations
import logging
import re

import requests

from airflow.sdk import BaseHook, Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException
from bs4 import BeautifulSoup as bs

from mokelumne.util.storage import run_dir

logger = logging.getLogger(__name__)

@dag(
    schedule=None,
    catchup=False,
    params={"ldc_corpus": Param("", type="string")},
)
def ldc_fetcher():

    @task
    def get_ldc_tokens() -> dict[str, str]:
        conn = BaseHook.get_connection("ldc")
        login_url = f"{conn.schema}://{conn.host}/login"
        credentials = {
            "spree_user[login]": conn.login, 
            "spree_user[password]": conn.password
        }
        r = requests.post(login_url, data=credentials)
        return r.cookies.get_dict()


    @task
    def get_available_ldc_corpora(tokens) -> dict[str, dict[str, str]]:
        corpora = {}
        conn = BaseHook.get_connection("ldc")
        datasets_url = f"{conn.schema}://{conn.host}/organization/downloads"
        r = requests.get(datasets_url, cookies=tokens)
        page = bs(r.text, 'html.parser')
        corpora_table = page.find(id='user-corpora-download-table')
        corpora_rows = corpora_table.tbody.find_all("tr")

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
    def fetch_ldc_corpus(tokens, available_corpora):
        context = get_current_context()
        conn = BaseHook.get_connection("ldc")
        corpus = context["params"].get("ldc_corpus", "")
        if corpus in available_corpora:
            logger.info(
                f"TODO: fetch {conn.schema}://{conn.host}/{corpus.get("download_link")}"
            )
        else:
            raise AirflowFailException(
                f"Requested LDC corpus {corpus} not found in available corpora"
            )
    
    tokens = get_ldc_tokens()
    available_corpora = get_available_ldc_corpora(tokens)
    fetch_ldc_corpus(tokens, available_corpora)

    
    # @task
    # def fetch_ldc_dataset():
    #     context = get_current_context()
    #     run_id = context["run_id"]
    #     ldc_dataset = context["params"]["ldc_dataset"]
    
    # fetch_ldc_dataset # type: ignore

ldc_fetcher()  # pyright: ignore[reportUnusedExpression]
