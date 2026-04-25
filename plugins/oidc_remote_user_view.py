import logging
import os
from typing import cast
from urllib.parse import quote, urljoin

from flask import make_response, redirect, request, session
from flask_appbuilder import expose
from flask_login import logout_user
from airflow.api_fastapi.app import (
    AUTH_MANAGER_FASTAPI_APP_PREFIX, get_auth_manager, get_cookie_path
)
from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
from airflow.configuration import conf
from flask_appbuilder.security.views import AuthOAuthView


BASE_URL = conf.get("api", "base_url", fallback="/")
OIDC_NAME = os.getenv("OIDC_NAME")
OIDC_CLIENT_ID = os.getenv("OIDC_CLIENT_ID")

log = logging.getLogger(__name__)

class OIDCRemoteUserView(AuthOAuthView):

    @expose("/start_logout")
    def logout(self):
        oauth_remote = self.appbuilder.sm.oauth_remotes[OIDC_NAME]  # pyright: ignore[reportOptionalSubscript]
        end_session_endpoint = oauth_remote.server_metadata.get("end_session_endpoint")
        base_url = conf.get("api", "base_url", fallback="/")
        post_logout_redirect_uri = urljoin(
            base_url, f"{AUTH_MANAGER_FASTAPI_APP_PREFIX}/logout_callback"
        )
        logout_url = (
             f"{end_session_endpoint}?post_logout_redirect_uri={post_logout_redirect_uri}&client_id={OIDC_CLIENT_ID}"
        )        
        return redirect(logout_url)


    @expose("/logout_callback")
    def logout_callback(self):
        login_url = get_auth_manager().get_url_login()
        token_key = self.appbuilder.sm.get_oauth_token_key_name(OIDC_NAME)
        secure = request.base_url.startswith("https") or bool(conf.get("api", "ssl_cert", fallback=""))
        cookie_path = get_cookie_path()
        logout_user()
        session.clear()
        response = make_response(redirect(login_url))
        response.delete_cookie(
            key=COOKIE_NAME_JWT_TOKEN,
            path=cookie_path,
            secure=secure,
            httponly=True,
        )
        response.delete_cookie(
            key=token_key,
            path=cookie_path,
            secure=secure,
            httponly=True,
        )
        return response
