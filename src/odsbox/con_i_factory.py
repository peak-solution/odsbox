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

import logging
import os
import threading
import time
from collections.abc import Generator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import ParseResult, parse_qs, urlparse, urlunparse

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


class _AuthCodeHTTPServer(ThreadingHTTPServer):
    """HTTPServer subclass with an auth_code attribute for OIDC callback."""

    daemon_threads = True

    def __init__(self, redirect_host: str, redirect_port: int) -> None:
        super().__init__((redirect_host, redirect_port), self._CallbackHandler)
        self.auth_code: str | None = None
        self.auth_error: str | None = None

    class _CallbackHandler(BaseHTTPRequestHandler):
        server: _AuthCodeHTTPServer
        logger = logging.getLogger(__name__)

        def do_GET(self) -> None:
            query_params = parse_qs(urlparse(self.path).query)

            if "code" in query_params:
                self.server.auth_code = self.path
                self._reply(200, "Login successful! You can close this window.")
                return

            # RFC 6749 4.1.2.1: "error" is the required field on failure; the rest are optional details.
            if "error" in query_params:
                error_code = query_params["error"][0]
                description = query_params.get("error_description", [""])[0]
                error = f"{error_code}: {description}" if description else error_code
                self.logger.error("OAuth error: %s", error)
                self.server.auth_error = error
                self._reply(400, f"Login failed! You can close this window.\nError: {error}")
                return

            if not query_params:
                # Stray browser request (e.g. /favicon.ico), not the OAuth redirect - ignore it.
                self._reply(404, "Not found.")
                return

            # Redirect carried query params but neither 'code' nor 'error' - treat as a failed login.
            self.logger.error("Unexpected OAuth callback without 'code' or 'error': %s", self.path)
            self.server.auth_error = "authorization response contained neither 'code' nor 'error'"
            self._reply(400, "Login failed! You can close this window.")

        def _reply(self, status: int, message: str) -> None:
            body = message.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: Any) -> None:
            pass


def _parse_and_validate_url(url_value: str, *, field_name: str) -> ParseResult:
    """Parse and validate a URL used in OIDC discovery flow."""
    normalized_url = url_value.strip()
    parsed_url = urlparse(normalized_url)
    if not parsed_url.scheme or not parsed_url.netloc:
        raise ValueError(f"Invalid {field_name}: {url_value!r}")
    if parsed_url.query or parsed_url.fragment:
        raise ValueError(f"{field_name} must not include query or fragment")
    return parsed_url


def _build_openid_configuration_url(issuer: str) -> str:
    """Validate issuer and build OpenID configuration URL safely."""
    parsed_issuer = _parse_and_validate_url(issuer, field_name="OIDC issuer URL")

    issuer_path = parsed_issuer.path.rstrip("/")
    openid_config_path = (
        f"{issuer_path}/.well-known/openid-configuration" if issuer_path else "/.well-known/openid-configuration"
    )

    return urlunparse(
        (
            parsed_issuer.scheme,
            parsed_issuer.netloc,
            openid_config_path,
            "",
            "",
            "",
        )
    )


def _build_webfinger_url(ods_base_url: str, webfinger_path_prefix: str) -> str:
    """Validate ODS base URL and build WebFinger URL safely."""
    parsed_base_url = _parse_and_validate_url(ods_base_url, field_name="ODS base URL")

    base_path = parsed_base_url.path.rstrip("/")
    normalized_prefix = webfinger_path_prefix.strip()
    if "?" in normalized_prefix or "#" in normalized_prefix:
        raise ValueError("WebFinger path prefix must not include query or fragment")
    if normalized_prefix and not normalized_prefix.startswith("/"):
        normalized_prefix = f"/{normalized_prefix}"
    prefix_path = normalized_prefix.rstrip("/")

    webfinger_path = f"{base_path}{prefix_path}/.well-known/webfinger"
    return urlunparse(
        (
            parsed_base_url.scheme,
            parsed_base_url.netloc,
            webfinger_path,
            "",
            "",
            "",
        )
    )


def _discover_endpoints(ods_base_url: str, webfinger_path_prefix: str = "", *, verify: bool = True) -> tuple[str, str]:
    """
    Discover OIDC authorization and token endpoints via ASAM ODS WebFinger.

    Args:
        ods_base_url: Base URL of the ODS server (e.g. ``https://host:port/api``).
        webfinger_path_prefix: Path prefix for WebFinger endpoint (default: empty string).
            This is used to support servers that host WebFinger at a different path, than specified
            by the ASAM ODS standard. e.g. use ``/ods`` for ``<ods_base_url>/ods/.well-known/webfinger``.
        verify: Whether to verify TLS certificates for discovery requests (default: True).

    Returns:
        Tuple of (authorization_endpoint, token_endpoint).

    Raises:
        ValueError: If discovery fails at any step.
    """
    web_finger_url = PreparedRequest()
    web_finger_url.prepare_url(
        _build_webfinger_url(ods_base_url, webfinger_path_prefix),
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

    openid_configuration_url = _build_openid_configuration_url(issuer)
    openid_config_response = requests.get(openid_configuration_url, verify=verify)
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

        Args:
            url: ODS server base URL (e.g., ``https://server.com:8443/api``).
            username: Login username.
            password: Login password.
            verify_certificate: Whether to verify the server TLS certificate (default: True).
                Set to False for development with self-signed certificates.
            **kwargs: Additional keyword arguments passed to ConI (e.g., timeout, headers).

        Returns:
            An opened ``ConI`` connection instance ready for use.

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

        Args:
            url: ODS server base URL (e.g., ``https://server.com:8013/api``).
            token_endpoint: OAuth2 token endpoint URL (e.g., ``https://auth.com/oauth2/token``).
            client_id: OAuth2 client ID.
            client_secret: OAuth2 client secret. Should be retrieved from secure storage.
            scope: OAuth2 scopes as a list. Defaults to ``["machine2machine"]``.
            verify_certificate: Whether to verify the server TLS certificate (default: True).
                Set to False for development with self-signed certificates.
            **kwargs: Additional keyword arguments passed to ConI (e.g., timeout, headers).

        Returns:
            An opened ``ConI`` connection instance ready for use.

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

        Args:
            url: ODS server base URL (e.g., ``https://server.com:8015/api``).
            client_id: OAuth2 client ID.
            redirect_uri: Local redirect URI for the OIDC callback
                (e.g., ``http://127.0.0.1:1234``).
            redirect_url_allow_insecure: Allow HTTP (insecure) redirect URIs for
                local development (default: False). Set to True for development
                with local ``localhost`` redirects.
            client_secret: OAuth2 client secret (optional).
            scope: OAuth2 scopes as a list. Defaults to ``["openid", "profile"]``.
            authorization_endpoint: OIDC authorization endpoint. If not provided,
                automatically discovered via WebFinger.
            token_endpoint: OIDC token endpoint. If not provided, automatically
                discovered via WebFinger.
            login_timeout: Seconds to wait for the user to complete login (default: 60).
            verify_certificate: Whether to verify the server TLS certificate
                (default: True). Set to False for development with self-signed certificates.
            webfinger_path_prefix: Path prefix for WebFinger endpoint. Use if the
                server hosts WebFinger at a non-standard path (e.g., ``/ods`` for
                ``<url>/ods/.well-known/webfinger``).
            **kwargs: Additional keyword arguments passed to ConI (e.g., timeout, headers).

        Returns:
            An opened ``ConI`` connection instance ready for use.

        Raises:
            ValueError: On discovery failure, invalid redirect URI, or login timeout.

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

            # Parse redirect URI for local callback server
            parsed = urlparse(redirect_uri)
            if not parsed.hostname or not parsed.port:
                raise ValueError("Invalid redirect URI, missing host or port")

            server = _AuthCodeHTTPServer(parsed.hostname, parsed.port)
            server_thread = threading.Thread(target=server.serve_forever, daemon=True)
            server_thread.start()
            try:
                if not webbrowser.open(authorization_url):
                    raise ValueError("Failed to open web browser for authorization URL")

                start_time = time.time()
                while (
                    server.auth_code is None
                    and server.auth_error is None
                    and (time.time() - start_time) < login_timeout
                ):
                    time.sleep(0.1)
            finally:
                server.shutdown()
                server.server_close()

            if server.auth_error is not None:
                raise ValueError(f"OAuth login failed: {server.auth_error}")
            if server.auth_code is None:
                raise ValueError(f"OAuth login timed out after {login_timeout} seconds")

            oauth.verify = verify_certificate
            oauth.fetch_token(
                token_url=token_endpoint,
                authorization_response=server.auth_code,
                client_secret=client_secret,
                timeout=60,
            )
            return ConI(url=url, custom_session=oauth, **kwargs)
