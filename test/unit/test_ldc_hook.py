"""Pytest testcases for `mokelumne.providers.ldc.hooks.ldc.LDCHook`."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests
from airflow.sdk.exceptions import AirflowException

from mokelumne.providers.ldc.hooks.ldc import LDCHook


class DummyConnection:
    """Mock Airflow connection."""
    host = "https://catalog.ldc.upenn.edu"
    login = "user"
    password = "pass"


def make_login_page_response(csrf_value: str = "abc") -> MagicMock:
    """Generate a mocked response that contains the login page form element."""
    response = MagicMock(spec=requests.Response)
    response.status_code = 200
    response.text = f"<input name='authenticity_token' value='{csrf_value}'/>"
    return response


def make_download_response(status_code: int = 200, headers: dict[str, str] | 
None = None) -> MagicMock:
    """Create a mock download response."""
    response = MagicMock(spec=requests.Response)
    response.status_code = status_code
    response.headers = headers or {}
    response.iter_content.return_value = [b"chunk"]
    response.raise_for_status = MagicMock()
    return response


def test_get_conn_authenticates_and_returns_session(monkeypatch):
    """Ensure an authenticated connection returns as session."""
    session = MagicMock(spec=requests.Session)
    login_page = make_login_page_response()
    login_response = MagicMock(spec=requests.Response)
    login_response.status_code = 302

    session.get.side_effect = [login_page]
    session.prepare_request.return_value = MagicMock()
    session.send.return_value = login_response

    monkeypatch.setattr(
        "mokelumne.providers.ldc.hooks.ldc.requests.Session",
        MagicMock(return_value=session),
    )

    hook = LDCHook()
    hook.get_connection = MagicMock(return_value=DummyConnection())

    conn = hook.get_conn()

    assert conn is session
    session.get.assert_called_once_with("https://catalog.ldc.upenn.edu/login")
    session.send.assert_called_once()


def test_refresh_session_recreates_session(monkeypatch):
    """Ensure an expired session gets recreated transparently."""
    session_one = MagicMock(spec=requests.Session)
    session_two = MagicMock(spec=requests.Session)

    login_page_one = make_login_page_response()
    login_page_two = make_login_page_response("xyz")
    login_response = MagicMock(spec=requests.Response)
    login_response.status_code = 302

    session_one.get.side_effect = [login_page_one]
    session_one.prepare_request.return_value = MagicMock()
    session_one.send.return_value = login_response

    session_two.get.side_effect = [login_page_two]
    session_two.prepare_request.return_value = MagicMock()
    session_two.send.return_value = login_response

    mock_session_cls = MagicMock(side_effect=[session_one, session_two])
    monkeypatch.setattr(
        "mokelumne.providers.ldc.hooks.ldc.requests.Session",
        mock_session_cls,
    )

    hook = LDCHook()
    hook.get_connection = MagicMock(return_value=DummyConnection())

    assert hook.get_conn() is session_one
    hook.refresh_session()
    assert hook.get_conn() is session_two
    assert mock_session_cls.call_count == 2


def test_get_corpora_response_returns_response(monkeypatch):
    """Ensure the organization downloads page is fetched."""
    session = MagicMock(spec=requests.Session)
    login_page = make_login_page_response()
    corpora_response = MagicMock(spec=requests.Response)
    corpora_response.status_code = 200

    session.get.side_effect = [login_page, corpora_response]
    session.prepare_request.return_value = MagicMock()
    session.send.return_value = MagicMock(status_code=302)

    monkeypatch.setattr(
        "mokelumne.providers.ldc.hooks.ldc.requests.Session",
        MagicMock(return_value=session),
    )

    hook = LDCHook()
    hook.get_connection = MagicMock(return_value=DummyConnection())

    result = hook.get_corpora_response()

    assert result is corpora_response
    session.get.assert_any_call("https://catalog.ldc.upenn.edu/login")
    session.get.assert_any_call("https://catalog.ldc.upenn.edu/organization/downloads", stream=True)


def test_get_corpus_file_returns_response(monkeypatch):
    """Ensure that the file download is created appropriately."""
    session = MagicMock(spec=requests.Session)
    login_page = make_login_page_response()
    download_response = make_download_response(200, {"Content-Disposition": "attachment; filename=example.zip"})

    session.get.side_effect = [login_page, download_response]
    session.prepare_request.return_value = MagicMock()
    session.send.return_value = MagicMock(status_code=302)

    monkeypatch.setattr(
        "mokelumne.providers.ldc.hooks.ldc.requests.Session",
        MagicMock(return_value=session),
    )

    hook = LDCHook()
    hook.get_connection = MagicMock(return_value=DummyConnection())
    result = hook.get_corpus_file("/download/123")

    assert result is download_response
    assert session.get.call_args_list[0].args[0] == "https://catalog.ldc.upenn.edu/login"
    assert session.get.call_args_list[1].args[0] == "https://catalog.ldc.upenn.edu/download/123"


def test_get_corpus_file_refreshes_on_401(monkeypatch):
    """Ensure an attempted download refreshes the connection if expired."""
    session_one = MagicMock(spec=requests.Session)
    session_two = MagicMock(spec=requests.Session)

    login_page_one = make_login_page_response()
    download_401 = make_download_response(401)
    download_401.raise_for_status.side_effect = requests.HTTPError("401")

    login_page_two = make_login_page_response()
    download_200 = make_download_response(200)

    session_one.get.side_effect = [login_page_one, download_401]
    session_one.prepare_request.return_value = MagicMock()
    session_one.send.return_value = MagicMock(status_code=302)

    session_two.get.side_effect = [login_page_two, download_200]
    session_two.prepare_request.return_value = MagicMock()
    session_two.send.return_value = MagicMock(status_code=302)

    mock_session_cls = MagicMock(side_effect=[session_one, session_two])
    monkeypatch.setattr(
        "mokelumne.providers.ldc.hooks.ldc.requests.Session",
        mock_session_cls,
    )

    hook = LDCHook()
    hook.get_connection = MagicMock(return_value=DummyConnection())

    result = hook.get_corpus_file("/download/123")

    assert result is download_200
    assert mock_session_cls.call_count == 2


def test_get_corpus_file_raises_on_missing_link():
    """If the download URL is missing, ensure an exception is raised."""
    hook = LDCHook()
    with pytest.raises(AirflowException, match="Download link is missing"):
        hook.get_corpus_file("")


def test_test_connection_success(monkeypatch):
    """Ensure the test_connection returns a success when appropriate."""
    session = MagicMock(spec=requests.Session)
    login_page = make_login_page_response()

    session.get.return_value = login_page
    session.prepare_request.return_value = MagicMock()
    session.send.return_value = MagicMock(status_code=302)

    monkeypatch.setattr(
        "mokelumne.providers.ldc.hooks.ldc.requests.Session",
        MagicMock(return_value=session),
    )

    hook = LDCHook()
    hook.get_connection = MagicMock(return_value=DummyConnection())

    success, message = hook.test_connection()
    assert success is True
    assert message == "Connection successful"


def test_test_connection_failure(monkeypatch):
    """Ensure the test_connection returns a failure when appropriate."""
    monkeypatch.setattr(
        "mokelumne.providers.ldc.hooks.ldc.requests.Session",
        MagicMock(side_effect=requests.RequestException("Network error")),
    )

    hook = LDCHook()
    hook.get_connection = MagicMock(return_value=DummyConnection())

    success, message = hook.test_connection()
    assert success is False
    assert "Network error" in message
