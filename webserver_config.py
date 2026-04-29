import os
from urllib.parse import urljoin

from airflow.api_fastapi.app import AUTH_MANAGER_FASTAPI_APP_PREFIX
from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager
from flask_appbuilder.security.manager import AUTH_OAUTH

from mokelumne.oidc.auth_manager import OIDCAuthManager
from mokelumne.oidc.security_manager import OIDCSecurityManager

# despite this being an Airflow configuration option, we are intentionally not
# pulling in airflow.configuration.conf here as this module gets loaded before
# the configuration might be fully initialized
AIRFLOW__API__BASE_URL= os.getenv(
    "AIRFLOW__API__BASE_URL", "http://localhost:8080"
)

SECURITY_MANAGER_CLASS = OIDCSecurityManager

# There seems to be no other way to do this than monkeypatching this method.
FabAuthManager.get_url_logout = OIDCAuthManager.get_url_logout   # pyright: ignore[reportAttributeAccessIssue]

AUTH_TYPE = AUTH_OAUTH
AUTH_ROLES_SYNC_AT_LOGIN = True
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = os.getenv("AUTH_USER_REGISTRATION_ROLE", "Public")

OIDC_NAME = os.getenv("OIDC_NAME")
OIDC_CLIENT_ID = os.getenv("OIDC_CLIENT_ID")
OIDC_CLIENT_SECRET = os.getenv("OIDC_CLIENT_SECRET")
OIDC_END_SESSION_ENDPOINT = os.getenv("OIDC_END_SESSION_ENDPOINT")
OIDC_WELL_KNOWN = os.getenv("OIDC_WELL_KNOWN")
OIDC_ADMIN_GROUP = os.getenv("OIDC_ADMIN_GROUP")
OIDC_USER_GROUP = os.getenv("OIDC_USER_GROUP")

AUTH_ROLES_MAPPING = {
    OIDC_ADMIN_GROUP: ["Admin"],
    OIDC_USER_GROUP: ["User"],
}

LOGIN_URL = urljoin(
    AIRFLOW__API__BASE_URL, f"{AUTH_MANAGER_FASTAPI_APP_PREFIX}/login"
)

LOGOUT_REDIRECT_URL = (
    f"{OIDC_END_SESSION_ENDPOINT}?post_logout_redirect_uri={LOGIN_URL}"
    f"&client_id={OIDC_CLIENT_ID}"
)

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
