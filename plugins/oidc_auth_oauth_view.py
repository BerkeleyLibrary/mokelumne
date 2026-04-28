"""Custom view for the OIDC provider to handle ending the OIDC-authed session"""
from flask import make_response, redirect, request, session
from flask_appbuilder import expose
from flask_login import logout_user
from airflow.api_fastapi.app import get_cookie_path
from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
from airflow.configuration import conf
from airflow.providers.fab.auth_manager.views.auth_oauth import CustomAuthOAuthView


class OIDCAuthOAuthView(CustomAuthOAuthView):
    """
    Provides a custom OIDC logout view which allows us to inject logic to
    redirect to the OIDC provider's end session endpoint.
    """


    @expose("/start-logout")
    def logout(self):
        """
        Start the logout process, which clears the user's session and
        unsets their session token.
        """
        logout_user()
        session.clear()
        redir_url = self.appbuilder.app.config.get(
            "LOGOUT_REDIRECT_URL", self.appbuilder.get_url_for_login
        )
        secure = request.base_url.startswith("https") or bool(conf.get("api", "ssl_cert", fallback=""))
        cookie_path = get_cookie_path()
        response = make_response(redirect(redir_url, 307))
        response.delete_cookie(
            key=COOKIE_NAME_JWT_TOKEN,
            path=cookie_path,
            secure=secure,
            httponly=True,
        )
        return response
