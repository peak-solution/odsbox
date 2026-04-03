"""
Unified ConI factory for ASAM ODS connections with multiple auth flows.

This module provides ConIFactory, a factory class that simplifies authentication
and connection creation for ASAM ODS servers. It supports four authentication flows:

* **basic** — Username/password authentication
* **m2m** — OAuth2 client-credentials (machine-to-machine)
* **oidc** — Interactive browser-based OIDC login with automatic endpoint discovery

Quick Start Examples::

    # Basic authentication
    con = ConIFactory.basic(
        url="https://server:8443/api",
        username="user",
        password="pass"
    )
    with con:
        # Use the connection
        pass

    # M2M authentication
    con = ConIFactory.m2m(
        url="https://my.asam.server.com:8443/api",
        token_endpoint="https://auth/oauth2/token",
        client_id="client-id",
        client_secret="client-secret"
    )
    with con:
        # Use the connection
        pass

    # OIDC with automatic WebFinger discovery
    con = ConIFactory.oidc(
        url="https://server:8015/api",
        client_id="client-id",
        webfinger_path_prefix="/ods"  # Optional
    )
    with con:
        # Browser opens for login, then connection is ready
        pass
"""

from __future__ import annotations

import os
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Generator
from urllib.parse import urlparse

import requests
from oauthlib.oauth2 import BackendApplicationClient
from requests.models import PreparedRequest
from requests_oauthlib import OAuth2Session

from .con_i import ConI


@contextmanager
def _temp_env(**kwargs: Any) -> Generator[None, None, None]:
    """Context manager that temporarily sets environment variables."""
    old_values = {}
    for key, value in kwargs.items():
        old_values[key] = os.environ.get(key)
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    try:
        yield
    finally:
        for key, old_value in old_values.items():
            if old_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old_value


class _AuthCodeHTTPServer(HTTPServer):
    """HTTPServer subclass with an auth_code attribute for OIDC callback."""

    def __init__(self, redirect_host: str, redirect_port: int) -> None:
        super().__init__((redirect_host, redirect_port), self._CallbackHandler)
        self.auth_code: str | None = None

    class _CallbackHandler(BaseHTTPRequestHandler):
        server: _AuthCodeHTTPServer

        def do_GET(self) -> None:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Login successful! You can close this window.")
            if "code" in self.path:
                self.server.auth_code = self.path

        def log_message(self, format: str, *args: Any) -> None:
            pass


def _discover_endpoints(ods_base_url: str, webfinger_path_prefix: str = "", *, verify: bool = True) -> tuple[str, str]:
    """
    Discover OIDC authorization and token endpoints via ASAM ODS WebFinger.

    :param str ods_base_url: Base URL of the ODS server (e.g. ``https://host:port/api``).
    :param str webfinger_path_prefix: Path prefix for WebFinger endpoint (default: empty string).
        This is used to support servers that host WebFinger at a different path, than specified
        by the ASAM ODS standard. e.g. use ``/ods`` for ``<ods_base_url>/ods/.well-known/webfinger``.
    :param bool verify: Whether to verify TLS certificates for discovery requests (default: True).
    :raises ValueError: If discovery fails at any step.
    :return tuple[str, str]: Tuple of (authorization_endpoint, token_endpoint).
    """
    web_finger_url = PreparedRequest()
    web_finger_url.prepare_url(
        f"{ods_base_url}{webfinger_path_prefix}/.well-known/webfinger",
        {"rel": "http://openid.net/specs/connect/1.0/issuer"},
    )
    url = web_finger_url.url
    if url is None:
        raise ValueError("Failed to prepare WebFinger URL")
    response = requests.get(url, verify=verify)
    if response.status_code != 200:
        raise ValueError(f"WebFinger request failed: {response.status_code}")

    issuer = None
    for link in response.json().get("links", []):
        if link.get("rel") == "http://openid.net/specs/connect/1.0/issuer":
            issuer = link.get("href")
            break
    if not issuer:
        raise ValueError("OIDC issuer not found in WebFinger response")

    openid_config_response = requests.get(f"{issuer}/.well-known/openid-configuration", verify=verify)
    if openid_config_response.status_code != 200:
        raise ValueError(f"OIDC config request failed: {openid_config_response.status_code}")

    openid_config = openid_config_response.json()
    authorization_endpoint = openid_config.get("authorization_endpoint")
    token_endpoint = openid_config.get("token_endpoint")
    if not authorization_endpoint or not token_endpoint:
        raise ValueError("Missing endpoints in OIDC configuration")

    return authorization_endpoint, token_endpoint


class ConIFactory:
    """
    Factory for creating authenticated ASAM ODS ``ConI`` connections.

    Supports four authentication flows:

    * **basic** — username / password
    * **m2m** — OAuth2 client-credentials (machine-to-machine)
    * **oidc** — interactive browser-based OIDC login
      (with optional WebFinger discovery)

    Each factory method returns a ready-to-use ``ConI`` instance.
    """

    discover_endpoints = staticmethod(_discover_endpoints)

    # ------------------------------------------------------------------
    # Basic auth
    # ------------------------------------------------------------------
    @staticmethod
    def basic(
        url: str,
        username: str,
        password: str,
        *,
        verify_certificate: bool = True,
        **kwargs: Any,
    ) -> ConI:
        """
        Create a ConI with basic username/password authentication.

        Use this method for direct username/password authentication to an ODS server.

        :param str url: ODS server base URL (e.g., ``https://server.com:8443/api``).
        :param str username: Login username.
        :param str password: Login password.
        :param bool verify_certificate: Whether to verify the server TLS certificate (default: True).
            Set to False for development with self-signed certificates.
        :param kwargs: Additional keyword arguments passed to ConI (e.g., timeout, headers).
        :return: An opened ``ConI`` connection instance ready for use.

        Example::

            from odsbox.con_i_factory import ConIFactory

            con = ConIFactory.basic(
                url="https://server:8443/api",
                username="user",
                password="password"
            )
            with con:
                # Use the connection
                pass

        Example - For development with self-signed certificates::

            con = ConIFactory.basic(
                url="https://server:8443/api",
                username="user",
                password="password",
                verify_certificate=False
            )
        """
        return ConI(
            url=url,
            auth=(username, password),
            verify_certificate=verify_certificate,
            **kwargs,
        )

    # ------------------------------------------------------------------
    # M2M (client credentials) via OAuth2Session
    # ------------------------------------------------------------------
    @staticmethod
    def m2m(
        url: str,
        token_endpoint: str,
        client_id: str,
        client_secret: str,
        *,
        scope: list[str] | None = None,
        verify_certificate: bool = True,
        **kwargs: Any,
    ) -> ConI:
        """
        Create a ConI with OAuth2 client-credentials (M2M) authentication.

        Use this method for service-to-service communication where a client
        authenticates directly with the OAuth2 token endpoint using a client ID
        and secret, without user interaction.

        :param str url: ODS server base URL (e.g., ``https://server.com:8013/api``).
        :param str token_endpoint: OAuth2 token endpoint URL (e.g., ``https://auth.com/oauth2/token``).
        :param str client_id: OAuth2 client ID.
        :param str client_secret: OAuth2 client secret. Should be retrieved from secure storage.
        :param list[str] | None scope: OAuth2 scopes as a list. Defaults to ``["machine2machine"]``.
        :param bool verify_certificate: Whether to verify the server TLS certificate (default: True).
            Set to False for development with self-signed certificates.
        :param kwargs: Additional keyword arguments passed to ConI (e.g., timeout, headers).
        :return: An opened ``ConI`` connection instance ready for use.

        Example::

            from odsbox.con_i_factory import ConIFactory
            from keyring import get_password

            token_endpoint = "https://example.com/auth/realms/myrealm/protocol/openid-connect/token"
            client_id = "f0a8cec0-e980-48c4-9898-8a11f40da518"
            client_secret = get_password(token_endpoint, client_id)

            con = ConIFactory.m2m(
                url="https://my.asam.server.com:8443/api",
                token_endpoint=token_endpoint,
                client_id=client_id,
                client_secret=client_secret
            )
            with con:
                # Use the connection
                pass

        Example - With custom scopes::

            con = ConIFactory.m2m(
                url="https://my.asam.server.com:8443/api",
                token_endpoint=token_endpoint,
                client_id=client_id,
                client_secret=client_secret,
                scope=["api", "custom_scope"]
            )
        """

        effective_scope = scope or ["machine2machine"]
        client = BackendApplicationClient(client_id=client_id, scope=effective_scope)
        oauth = OAuth2Session(client=client)
        oauth.verify = verify_certificate
        oauth.fetch_token(
            token_url=token_endpoint,
            client_id=client_id,
            client_secret=client_secret,
        )
        return ConI(url=url, custom_session=oauth, **kwargs)

    # ------------------------------------------------------------------
    # OIDC (interactive browser login)
    # ------------------------------------------------------------------
    @staticmethod
    def oidc(
        url: str,
        client_id: str,
        redirect_uri: str,
        *,
        redirect_url_allow_insecure: bool = False,
        client_secret: str | None = None,
        scope: list[str] | None = None,
        authorization_endpoint: str | None = None,
        token_endpoint: str | None = None,
        login_timeout: int = 60,
        verify_certificate: bool = True,
        webfinger_path_prefix: str = "",
        **kwargs: Any,
    ) -> ConI:
        """
        Create a ConI with interactive OIDC browser login.

        Use this method for user-facing applications where users authenticate
        through their browser. The OIDC endpoints are automatically discovered
        via WebFinger if not explicitly provided.

        If ``authorization_endpoint`` and ``token_endpoint`` are not provided,
        they are discovered automatically via the ASAM ODS WebFinger protocol.

        :param str url: ODS server base URL (e.g., ``https://server.com:8015/api``).
        :param str client_id: OAuth2 client ID.
        :param str redirect_uri: Local redirect URI for the OIDC callback
            (e.g., ``http://127.0.0.1:1234``).
        :param bool redirect_url_allow_insecure: Allow HTTP (insecure) redirect URIs for
            local development (default: False). Set to True for development
            with local ``localhost`` redirects.
        :param str | None client_secret: OAuth2 client secret (optional).
        :param list[str] | None scope: OAuth2 scopes as a list. Defaults to ``["openid", "profile"]``.
        :param str | None authorization_endpoint: OIDC authorization endpoint. If not provided,
            automatically discovered via WebFinger.
        :param str | None token_endpoint: OIDC token endpoint. If not provided, automatically
            discovered via WebFinger.
        :param int login_timeout: Seconds to wait for the user to complete login (default: 60).
        :param bool verify_certificate: Whether to verify the server TLS certificate
            (default: True). Set to False for development with self-signed certificates.
        :param str webfinger_path_prefix: Path prefix for WebFinger endpoint. Use if the
            server hosts WebFinger at a non-standard path (e.g., ``/ods`` for
            ``<url>/ods/.well-known/webfinger``).
        :param kwargs: Additional keyword arguments passed to ConI (e.g., timeout, headers).
        :raises ValueError: On discovery failure, invalid redirect URI, or login timeout.
        :return: An opened ``ConI`` connection instance ready for use.

        Example - With automatic endpoint discovery::

            from odsbox.con_i_factory import ConIFactory

            con = ConIFactory.oidc(
                url="https://server:8015/api",
                client_id="f243866c-76b7-4e51-a16c-1d6bfe8f0c93",
                redirect_uri="http://127.0.0.1:1234",
                redirect_url_allow_insecure=True,
                webfinger_path_prefix="/ods"  # Optional
            )
            with con:
                # Browser opens for login, then connection is ready
                pass

        Example - With explicit endpoints::

            con = ConIFactory.oidc(
                url="https://server:8015/api",
                client_id="client-id",
                redirect_uri="http://127.0.0.1:1234",
                redirect_url_allow_insecure=True,
                authorization_endpoint="https://auth/authorize",
                token_endpoint="https://auth/token"
            )
            with con:
                pass

        Example - For local development with insecure redirects::

            con = ConIFactory.oidc(
                url="https://server:8015/api",
                client_id="client-id",
                redirect_uri="http://127.0.0.1:1234",
                redirect_url_allow_insecure=True,
                webfinger_path_prefix="/ods"
            )
            with con:
                pass
        """
        import webbrowser

        with _temp_env(OAUTHLIB_INSECURE_TRANSPORT="1" if redirect_url_allow_insecure else None):
            scope = scope or ["openid", "profile"]

            # Discover endpoints via WebFinger if not supplied directly
            if not authorization_endpoint or not token_endpoint:
                authorization_endpoint, token_endpoint = _discover_endpoints(
                    url, webfinger_path_prefix, verify=verify_certificate
                )

            oauth = OAuth2Session(
                client_id=client_id,
                redirect_uri=redirect_uri,
                scope=scope,
                auto_refresh_url=token_endpoint,
                auto_refresh_kwargs={
                    "client_id": client_id,
                    "client_secret": client_secret,
                },
            )

            authorization_url, _state = oauth.authorization_url(authorization_endpoint)
            webbrowser.open(authorization_url)

            # Parse redirect URI for local callback server
            parsed = urlparse(redirect_uri)
            if not parsed.hostname or not parsed.port:
                raise ValueError("Invalid redirect URI, missing host or port")

            server = _AuthCodeHTTPServer(parsed.hostname, parsed.port)
            server_thread = threading.Thread(target=server.serve_forever, daemon=True)
            server_thread.start()

            start_time = time.time()
            while server.auth_code is None and (time.time() - start_time) < login_timeout:
                time.sleep(0.1)
            server.shutdown()

            if not server.auth_code:
                raise ValueError("Login timed out")

            oauth.verify = verify_certificate
            oauth.fetch_token(
                token_url=token_endpoint,
                authorization_response=server.auth_code,
                client_secret=client_secret,
            )
            return ConI(url=url, custom_session=oauth, **kwargs)
