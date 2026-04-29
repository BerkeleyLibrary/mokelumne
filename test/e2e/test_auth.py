"""
test_auth.py

Playwright-driven e2e tests for the OIDC login flow.
"""

import os
import pytest

from playwright.sync_api import Page, expect, BrowserContext

from .conftest import (
    airflow_login_url, airflow_url, keycloak_logout_url, logout
)

# Matches the main div of the homepage. Absent from the login page.
HOMEPAGE_MARKER = '[data-testid="main-content"]'

# Matches the Admin tab in the left navbar.
ADMIN_MARKER = 'button[aria-label="Admin"]'



def test_airflow_requires_login(page: Page) -> None:
    page.goto("/")
    expect(page).to_have_url(airflow_login_url())
    expect(page.locator("#btn-signin-keycloak")).to_be_visible()


def test_public_user_cannot_view_homepage(page_as_testpublic: Page) -> None:
    page_as_testpublic.goto('/')
    expect(page_as_testpublic).to_have_url(airflow_login_url())
    expect(page_as_testpublic.locator(HOMEPAGE_MARKER)).not_to_be_visible()


def test_regular_user_can_view_homepage(page_as_testuser: Page) -> None:
    page_as_testuser.goto('/')
    expect(page_as_testuser).to_have_url(airflow_url("/"))
    expect(page_as_testuser.locator(HOMEPAGE_MARKER)).to_be_visible()


def test_regular_user_cannot_view_admin_tab(page_as_testuser: Page) -> None:
    page_as_testuser.goto('/')
    expect(page_as_testuser).to_have_url(airflow_url("/"))
    expect(page_as_testuser.locator(ADMIN_MARKER)).not_to_be_visible()


def test_admin_user_can_view_admin_tab(page_as_testadmin: Page) -> None:
    page_as_testadmin.goto('/')
    expect(page_as_testadmin).to_have_url(airflow_url("/"))
    expect(page_as_testadmin.locator(ADMIN_MARKER)).to_be_visible()


@pytest.mark.skipif((os.getenv("CI") == "true"),
                    reason="Test times out in Github Actions")
def test_logout_redirects(page_as_testuser: Page, context: BrowserContext) -> None:
    page_as_testuser.goto('/')
    logout(page_as_testuser)
    expect(page_as_testuser).to_have_url(keycloak_logout_url())
    page_as_testuser.get_by_text("Logout").click()
    page_as_testuser.wait_for_url(airflow_login_url())
    expect(page_as_testuser).to_have_url(airflow_login_url())
    assert [c.get("name") != "_token" for c in context.cookies()]
