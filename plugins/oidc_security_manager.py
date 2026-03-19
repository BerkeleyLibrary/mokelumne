import logging
import os

import jwt
import requests

from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride
from jwt import PyJWKClient

log = logging.getLogger(__name__)

class OIDCSecurityManager(FabAirflowSecurityManagerOverride):
    """Provides a fab Security Manager implementation for Keycloak auth."""
    def __init__(self, appbuilder):
        super().__init__(appbuilder)
        well_known = requests.get(os.getenv("OIDC_WELL_KNOWN")).json()
        self.signing_algos = well_known["id_token_signing_alg_values_supported"]
        self._jwks_client = PyJWKClient(well_known.get("jwks_uri"))

    # TODO depending on CAS reponse we may want to spin this out into an abstract class...
    def get_oauth_user_info(self, provider, response):
        if provider != os.getenv("OIDC_NAME"):
            return {}

        token = response["access_token"]
        try:
            signing_key = self._jwks_client.get_signing_key_from_jwt(token)
            me = jwt.decode(token, signing_key, algorithms=self.signing_algos)
        except jwt.exceptions.PyJWTError as e:
            log.exception("Failed to decode JWT")
            return {}

        # TODO needs CAS reponse to have groups in the top level
        # currently is under attributes?
        groups = [g.lstrip("/") for g in me.get("groups", [])]

        userinfo = {
            # TODO see what CAS returns and have this match?
            "username": me.get("preferred_username"),
            "role_keys": groups,
        }

        return userinfo
