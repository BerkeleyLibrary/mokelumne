import logging
import os
from flask import abort, flash
from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride

log = logging.getLogger(__name__)

class OIDCSecurityManager(FabAirflowSecurityManagerOverride):
    """
    Provides a Security Manager implementation for CalNet/Keycloak.
    """

    # Extra permissions granted to default roles on top of Airflow's built-in
    # ROLE_CONFIGS. Re-applied every time sync_roles() runs (on api-server
    # startup and `airflow sync-perm`), so these survive DB rebuilds.
    #
    # Plugins:can_read lets non-Op users load /api/v2/plugins, which the React
    # UI calls to discover external_views (e.g. the DAG-Run "Files" tab).
    EXTRA_ROLE_PERMS: dict[str, list[tuple[str, str]]] = {
        "Viewer": [("can_read", "Plugins")],
        "User": [("can_read", "Plugins")],
    }

    def sync_roles(self) -> None:
        super().sync_roles()
        for role_name, perms in self.EXTRA_ROLE_PERMS.items():
            role = self.find_role(role_name)
            if role is None:
                continue
            for action_name, resource_name in perms:
                self.add_permission_to_role(
                    role, self.create_permission(action_name, resource_name)
                )

    def get_oauth_user_info(self, provider, response):
        """
        Extracts userinfo from Keycloak/Calnet OIDC response

        FAB's upstream OAuth manager validates the JWT, so contrary
        to the examples in Airflow's documentation we don't need
        to do that here.

        Aborts with 403 if the user has no groups matching
        AUTH_ROLES_MAPPING, rather than creating a DB user with
        the default "Public" role.
        """
        if provider != os.getenv("OIDC_NAME"):
            return {}

        userinfo = response["userinfo"]
        username = userinfo["preferred_username"]
        user_groups = set(userinfo["groups"])

        # Access AUTH_ROLES_MAPPING from the loaded config at runtime
        # to avoid a circular import with webserver_config.py.
        roles_mapping = self.appbuilder.app.config.get("AUTH_ROLES_MAPPING", {})
        mapped_groups = set(roles_mapping.keys())
        matched_groups = user_groups & mapped_groups

        log.debug(
            "OIDC login: user=%s, groups=%s, mapped_groups=%s, matched=%s",
            username, user_groups, mapped_groups, matched_groups,
        )

        if not matched_groups:
            log.warning(
                "OIDC login denied: user=%s has no groups matching AUTH_ROLES_MAPPING", username
            )
            flash("Access denied. Your account does not belong to an authorized group.", "danger")
            abort(403)

        return {
            "username": username,
            "role_keys": userinfo["groups"],
        }
