import os
from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride

class OIDCSecurityManager(FabAirflowSecurityManagerOverride):
    """
    Provides a Security Manager implementation for CalNet/Keycloak.
    """

    def get_oauth_user_info(self, provider, response):
        """
        Extracts userinfo from Keycloak/Calnet OIDC response

        FAB's upstream OAuth manager validates the JWT, so contrary
        to the examples in Airflow's documentation we don't need
        to do that here.
        """
        if provider != os.getenv("OIDC_NAME"):
            return {}

        userinfo = response["userinfo"]
        return {
            "username": userinfo["preferred_username"],
            "role_keys": userinfo["groups"],
        }
