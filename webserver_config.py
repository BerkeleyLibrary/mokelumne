import os
from urllib.parse import urljoin
from airflow.configuration import conf
from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager
from flask_appbuilder.security.manager import AUTH_OAUTH
from oidc_security_manager import OIDCSecurityManager

SECURITY_MANAGER_CLASS = OIDCSecurityManager

AUTH_TYPE = AUTH_OAUTH
AUTH_ROLES_SYNC_AT_LOGIN = True
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = os.getenv("AUTH_USER_REGISTRATION_ROLE", "Public")

OIDC_NAME = os.getenv("OIDC_NAME")
OIDC_CLIENT_ID = os.getenv("OIDC_CLIENT_ID")
OIDC_CLIENT_SECRET = os.getenv("OIDC_CLIENT_SECRET")
OIDC_WELL_KNOWN = os.getenv("OIDC_WELL_KNOWN")
OIDC_ADMIN_GROUP = os.getenv("OIDC_ADMIN_GROUP")
OIDC_USER_GROUP = os.getenv("OIDC_USER_GROUP")

AUTH_ROLES_MAPPING = {
    OIDC_ADMIN_GROUP: ["Admin"],
    OIDC_USER_GROUP: ["User"],
}

OAUTH_PROVIDERS = [
    {
        "name": OIDC_NAME,
        "icon": "fa-key",
        "token_key": "access_token",
        "remote_app": {
            "client_id": OIDC_CLIENT_ID,
            "client_secret": OIDC_CLIENT_SECRET,
            "server_metadata_url": OIDC_WELL_KNOWN,
            "client_kwargs": {
                "scope": "openid profile berkeley_edu_groups"
            },
        },
    }
]

BASE_URL = conf.get("api", "base_url", fallback="/")

class OIDCAuthManager(FabAuthManager):
    def get_url_logout(self) -> str | None:
        return urljoin(BASE_URL, "auth/start_logout")
