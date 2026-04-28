from urllib.parse import urljoin

from airflow.api_fastapi.app import AUTH_MANAGER_FASTAPI_APP_PREFIX
from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager

class OIDCAuthManager(FabAuthManager):
    """
    Provides an auth manager implementation for OIDC. This is only necessary to
    monkeypatch in the get_url_logout() method below.
    """
    def get_url_logout(self) -> str | None:
        """Return the logout page url."""
        logout_url = urljoin(self.apiserver_endpoint,
                             f"{AUTH_MANAGER_FASTAPI_APP_PREFIX}/start-logout")
        return logout_url
