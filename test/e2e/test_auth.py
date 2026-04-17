"""
test_auth.py

Playwright-driven e2e tests for the OIDC login flow.
"""

import pytest

from playwright.sync_api import Page, expect

from .conftest import airflow_login_url, airflow_url


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
