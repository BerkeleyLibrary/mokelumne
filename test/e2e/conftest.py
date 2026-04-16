"""
conftest.py

Fixtures and helpers for e2e tests.
"""

import os
import re

from collections.abc import Generator

import pytest

from playwright.sync_api import Browser, BrowserContext, Page, Playwright


# Defaults match a host-browser developer's URLs. Chromium inside the
# Playwright sidecar reaches these via localhost TCP forwarders installed
# by playwright/entrypoint.sh, so the OAuth flow hits the same code paths
# as a human driving the UI from their workstation. Host devs running
# pytest from their shell only need to override PYTEST_PLAYWRIGHT_WS_ENDPOINT.
AIRFLOW_BASE_URL = os.environ.get("PYTEST_AIRFLOW_BASE_URL", "http://localhost:8080")
KEYCLOAK_BASE_URL = os.environ.get("PYTEST_KEYCLOAK_BASE_URL", "http://localhost:8180")
PLAYWRIGHT_WS_ENDPOINT = os.environ.get("PYTEST_PLAYWRIGHT_WS_ENDPOINT", "ws://playwright:3000/ws")


def airflow_url(path: str = ".*") -> re.Pattern[str]:
    """
    Generates AirFlow URL matchers for tests

    Use this when you want to assert that you're on AirFlow. Use `path`
    to narrow down the expected path.

    Ex.
       page.wait_for_url(airflow_url('/security/users.*'))
    """
    return re.compile(f"^{re.escape(AIRFLOW_BASE_URL)}{path}")


def airflow_login_url() -> re.Pattern[str]:
    """
    Shorthand for matching AirFlow's login page
    """
    return airflow_url("/auth/login.*")


def keycloak_url(path: str = ".*") -> re.Pattern[str]:
    """
    Generates KeyCloak URL matchers for tests

    Use this when you want to assert that you're on KeyCloak.
    """
    return re.compile(f"^{re.escape(KEYCLOAK_BASE_URL)}{path}")


def keycloak_login_url() -> re.Pattern[str]:
    """
    Shorthand for matching KeyCloak's mocked CalNet login page
    """
    return keycloak_url("/realms/berkeley-local/protocol/openid-connect/auth.*")


def login(page: Page, username: str, password: str) -> None:
    """Walk a fresh page through the Airflow -> Keycloak OIDC login flow.

    Leaves the page on whatever Airflow returns after the post-login redirect —
    the caller is responsible for asserting the authorized/denied outcome.
    """
    page.goto("/")
    page.wait_for_url(airflow_login_url())
    page.locator("#btn-signin-keycloak").click()
    page.wait_for_url(keycloak_login_url())
    page.locator("#username").fill(username)
    page.locator("#password").fill(password)
    page.locator("#kc-login").click()
    page.wait_for_url(airflow_url())


@pytest.fixture
def context(context) -> Generator[BrowserContext, None, None]:
    """
    Shorten pytest-playwright's default timeouts to 5s
    """
    context.set_default_timeout(5000)
    context.set_default_navigation_timeout(5000)
    yield context


@pytest.fixture
def page_as_testpublic(page: Page) -> Page:
    login(page, 'testpublic', 'testpublic')
    return page


@pytest.fixture
def page_as_testuser(page: Page) -> Page:
    login(page, 'testuser', 'testuser')
    return page


@pytest.fixture
def page_as_testadmin(page: Page) -> Page:
    login(page, 'testadmin', 'testadmin')
    return page


@pytest.fixture(scope="session")
def browser(playwright: Playwright) -> Generator[Browser, None, None]:
    browser = playwright.chromium.connect(PLAYWRIGHT_WS_ENDPOINT)
    yield browser
    browser.close()


@pytest.fixture(scope="session")
def browser_context_args(browser_context_args: dict) -> dict:
    return {**browser_context_args, "base_url": AIRFLOW_BASE_URL}
