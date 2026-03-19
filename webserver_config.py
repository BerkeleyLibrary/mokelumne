import os
import requests
from flask_appbuilder.security.manager import AUTH_OAUTH
from oidc_security_manager import OIDCSecurityManager

AUTH_TYPE = AUTH_OAUTH
AUTH_USER_REGISTRATION = True
AUTH_ROLES_SYNC_AT_LOGIN = True

OIDC_ADMIN_GROUP = os.getenv("OIDC_ADMIN_GROUP")
OIDC_USER_GROUP = os.getenv("OIDC_USER_GROUP")
OIDC_WELL_KNOWN = os.getenv("OIDC_WELL_KNOWN")
OIDC_NAME = os.getenv("OIDC_NAME")
OIDC_CLIENT_ID = os.getenv("OIDC_CLIENT_ID")
OIDC_CLIENT_SECRET = os.getenv("OIDC_CLIENT_SECRET")

AUTH_ROLES_MAPPING = {
    OIDC_ADMIN_GROUP: ["Admin"],
    OIDC_USER_GROUP: ["User"],
}

if not os.getenv('CI'):
    oidc = requests.get(OIDC_WELL_KNOWN).json()

    OAUTH_PROVIDERS = [
        {
            # keycloak or CAS depending on environment
            "name": OIDC_NAME,
            "icon": "fa-key",
            "token_key": "access_token",
            "remote_app": {
                "client_id": OIDC_CLIENT_ID,
                "client_secret": OIDC_CLIENT_SECRET,
                "jwks_uri": oidc["jwks_uri"],
                "client_kwargs": {
                    "scope": "openid profile berkeley_edu_groups"
                },
                "access_token_url": oidc["token_endpoint"],
                "authorize_url": oidc["authorization_endpoint"],
                "request_token_url": None,
            },
        }
    ]

    SECURITY_MANAGER_CLASS = OIDCSecurityManager

else:
    OAUTH_PROVIDERS = []
