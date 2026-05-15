"""Provides a hook for authenticating and fetching files from LDC."""

from __future__ import annotations

import logging
from functools import cached_property
from urllib.parse import urljoin

import requests
from airflow.sdk.exceptions import AirflowException
from airflow.sdk import BaseHook

from mokelumne.util.ldc import get_csrf_token

logger = logging.getLogger(__name__)


class LDCHook(BaseHook):
    """Interact with the LDC catalog using an authenticated requests session."""

    conn_type = "ldc"
    conn_name_attr = "conn_id"
    default_conn_name = "ldc_default"
    hook_name = "LDC"

    def __init__(self, conn_id: str = "ldc_default") -> None:
        super().__init__()
        self.conn_id = conn_id

    def get_conn(self) -> requests.Session:
        """Return an authenticated requests session for the LDC catalog."""
        return self._get_session()

    @cached_property
    def conn(self) -> requests.Session:
        """Return a cached authenticated requests session."""
        return self._get_session()

    def _get_session(self) -> requests.Session:
        connection = self.get_connection(self.conn_id)
        if not connection.host:
            raise AirflowException("LDC connection host is not configured")

        login_url = urljoin(connection.host, "login")
        session = requests.Session()

        response = session.get(login_url)
        if response.status_code != 200:
            raise AirflowException(
                f"LDC login page request failed: {response.status_code}"
            )

        form_data = get_csrf_token(response.text)
        if not form_data:
            raise AirflowException(
                "Unable to extract CSRF token from LDC login page"
            )

        form_data["spree_user[login]"] = connection.login or ""
        form_data["spree_user[password]"] = connection.password or ""
        form_data["utf8"] = "✓"

        login_request = requests.Request("POST", url=login_url, data=form_data)
        prepped = session.prepare_request(login_request)
        login_response = session.send(prepped)
        if login_response.status_code not in (200, 302):
            raise AirflowException(
                f"LDC authentication failed: {login_response.status_code}"
            )

        return session

    def refresh_session(self) -> None:
        """Clear the cached session so it is recreated on next access."""
        if "conn" in self.__dict__:
            del self.__dict__["conn"]

    def test_connection(self) -> tuple[bool, str]:
        try:
            self.get_conn()
            return True, "Connection successful"
        except Exception as exc:
            return False, str(exc)

    def get_corpora_response(self) -> requests.Response:
        """Fetch the LDC corpora downloads page response."""
        connection = self.get_connection(self.conn_id)
        session = self.get_conn()
        datasets_url = urljoin(connection.host, "organization/downloads")   # pyright: ignore[reportArgumentType]

        response = session.get(datasets_url, stream=True)
        if response.status_code == 401:
            logger.warning("LDC corpora fetch received 401, refreshing session")
            self.refresh_session()
            session = self.get_conn()
            response = session.get(datasets_url, stream=True)

        if response.status_code != 200:
            raise AirflowException(
                f"Failed to fetch LDC corpora page: {response.status_code}"
            )

        return response

    def get_corpus_file(self, download_link: str) -> requests.Response:
        """Fetch a corpus download response for the given download link."""
        if not download_link:
            raise AirflowException("Download link is missing")

        connection = self.get_connection(self.conn_id)
        session = self.get_conn()
        dl_uri = urljoin(connection.host, download_link)  # pyright: ignore[reportArgumentType]
        response = session.get(dl_uri, stream=True)
        if response.status_code == 401:
            logger.warning("LDC download request received 401, refreshing session")
            self.refresh_session()
            session = self.get_conn()
            response = session.get(dl_uri, stream=True)

        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise AirflowException(f"LDC download failed: {exc}") from exc

        return response
