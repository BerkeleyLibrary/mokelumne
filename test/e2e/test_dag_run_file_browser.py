"""
test_dag_run_file_browser.py

Playwright-driven e2e tests for the DAG Run File Browser plugin.

Access control is enforced by Airflow's FAB auth manager via
``requires_access_dag`` on the plugin's FastAPI app. These tests seed a
run directory directly on the filesystem and assert the resulting file
routes behave as expected for anonymous, User-role, and Admin-role callers.
"""

import uuid

from pathlib import Path
from urllib.parse import quote

import pytest

from playwright.sync_api import Page, expect


DAG_ID = "summarise_job"
RUN_ID = "e2e-test-run"

# ./public on the host is bind-mounted to /opt/airflow/public inside the
# airflow containers. Seeded files written here are immediately visible to
# the plugin's FastAPI route.
PUBLIC_HOST_DIR = Path(__file__).resolve().parents[2] / "public"

SEEDED_CSV_FILENAME = "seed.csv"
SEEDED_CSV_TEXT = "col1,col2\nalpha,1\nbeta,2\n"


@pytest.fixture(scope="module")
def seeded_run() -> dict[str, str]:
    """Scaffold ./public/<DAG_ID>/<RUN_ID>/ with a known index.html and CSV.

    The index marker is a fresh UUID per test run so a stale file left behind
    by a previous run (or a developer) can't pass an assertion it shouldn't.
    """
    # Regenerated every run so leftover files on disk can't satisfy the assertion.
    marker = f"mokelumne-seeded-{uuid.uuid4()}"
    index_html = f"""
<!doctype html>
<html>
    <head>
        <title>Seeded Index</title>
    </head>
    <body>
        <h1>{marker}</h1>
        <a href="{SEEDED_CSV_FILENAME}">{SEEDED_CSV_FILENAME}</a>
    </body>
</html>
"""

    run_dir = PUBLIC_HOST_DIR / DAG_ID / RUN_ID
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "index.html").write_text(index_html, encoding="utf-8")
    (run_dir / SEEDED_CSV_FILENAME).write_text(SEEDED_CSV_TEXT, encoding="utf-8")

    quoted = quote(RUN_ID, safe="")
    return {
        "run_id": RUN_ID,
        "marker": marker,
        "index_url": f"/public/{DAG_ID}/{quoted}/",
        "csv_url": f"/public/{DAG_ID}/{quoted}/{SEEDED_CSV_FILENAME}",
    }


def test_unauthenticated_user_cannot_view_files(
    page: Page,
    seeded_run: dict[str, str]
) -> None:
    """A fresh browser (no login) must get a 401 from the plugin route."""
    response = page.goto(seeded_run["index_url"])
    assert response is not None
    assert response.status == 401


def test_testuser_can_view_files(
    page_as_testuser: Page,
    seeded_run: dict[str, str]
) -> None:
    page_as_testuser.goto(seeded_run["index_url"])
    expect(
        page_as_testuser.get_by_role("heading", name=seeded_run["marker"])
    ).to_be_visible()


def test_testadmin_can_view_files(
    page_as_testadmin: Page,
    seeded_run: dict[str, str]
) -> None:
    page_as_testadmin.goto(seeded_run["index_url"])
    expect(
        page_as_testadmin.get_by_role("heading", name=seeded_run["marker"])
    ).to_be_visible()
