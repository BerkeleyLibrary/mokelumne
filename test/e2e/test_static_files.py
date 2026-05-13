"""
test_static_files.py

Thin e2e suite for the static_files plugin. Confirms that the plugin is
discovered by Airflow at startup and that its routes are gated by the real
auth manager. Route-level behavior (listings, MIME handling, traversal,
embed mode) is covered exhaustively in test/unit/test_static_files.py.
"""

import shutil
import uuid

from collections.abc import Generator
from pathlib import Path

import pytest

from playwright.sync_api import Page

# Repo root, two levels up from this file. Host-side ./files is bind-mounted
# into /opt/airflow/files in the airflow-apiserver container, which is what
# STATIC_FILES_ROOT points at.
REPO_ROOT = Path(__file__).resolve().parents[2]
FILES_ROOT = REPO_ROOT / "files"

# URL prefix the plugin is registered under in Airflow (see
# STATIC_FILES_URL_PREFIX in plugins/static_files/config.py).
PLUGIN_URL_PREFIX = "/files"


@pytest.fixture
def files_fixture_dir() -> Generator[str, None, None]:
    """Create a unique subdir under ./storage/ on the host, yield its
    URL-relative name, and clean up afterwards."""
    name = f"test-{uuid.uuid4().hex[:8]}"
    fixture_dir = FILES_ROOT / name
    fixture_dir.mkdir(parents=True)
    (fixture_dir / "hello.txt").write_text("hello world\n")
    yield name
    shutil.rmtree(fixture_dir)


def test_anonymous_request_is_unauthorized(page: Page) -> None:
    response = page.request.get(f"{PLUGIN_URL_PREFIX}/")
    assert response.status == 401


def test_user_can_fetch_file(
    page_as_testuser: Page, files_fixture_dir: str
) -> None:
    response = page_as_testuser.request.get(
        f"{PLUGIN_URL_PREFIX}/{files_fixture_dir}/hello.txt"
    )
    assert response.status == 200
    assert response.text() == "hello world\n"
