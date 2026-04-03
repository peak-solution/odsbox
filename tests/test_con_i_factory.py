"""Tests for ConIFactory factory — all authentication flows."""

from __future__ import annotations

import os
import threading
import time
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from odsbox.con_i_factory import ConIFactory, _AuthCodeHTTPServer, _discover_endpoints

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeConI:
    """Lightweight stand-in for ``odsbox.ConI`` used in unit tests."""

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs

    def __enter__(self) -> _FakeConI:
        return self

    def __exit__(self, *args: Any) -> None:
        pass


def _patch_coni() -> Any:
    """Return a patch that replaces ``odsbox.ConI`` with ``_FakeConI``."""
    return patch("odsbox.con_i_factory.ConI", _FakeConI)


# ---------------------------------------------------------------------------
# Basic auth
# ---------------------------------------------------------------------------


class TestBasicAuth:
    def test_creates_coni_with_credentials(self) -> None:
        with _patch_coni():
            con = ConIFactory.basic("https://server/api", "user", "pass")

        assert isinstance(con, _FakeConI)
        assert con.kwargs["url"] == "https://server/api"
        assert con.kwargs["auth"] == ("user", "pass")
        assert con.kwargs["verify_certificate"] is True

    def test_verify_certificate_false(self) -> None:
        with _patch_coni():
            con = ConIFactory.basic("https://server/api", "u", "p", verify_certificate=False)

        assert con.kwargs["verify_certificate"] is False


# ---------------------------------------------------------------------------
# M2M (client credentials)
# ---------------------------------------------------------------------------


class TestM2mAuth:
    def test_creates_coni_with_oauth_session(self) -> None:
        fake_token = {"access_token": "tok123", "token_type": "Bearer"}

        with (
            _patch_coni(),
            patch("odsbox.con_i_factory.BackendApplicationClient"),
            patch("odsbox.con_i_factory.OAuth2Session") as mock_session_cls,
        ):
            mock_session = MagicMock()
            mock_session.fetch_token.return_value = fake_token
            mock_session_cls.return_value = mock_session

            con = ConIFactory.m2m(
                url="https://server/api",
                token_endpoint="https://auth/token",
                client_id="cid",
                client_secret="csecret",
            )

        assert isinstance(con, _FakeConI)
        assert con.kwargs["url"] == "https://server/api"
        assert con.kwargs["custom_session"] is mock_session
        mock_session.fetch_token.assert_called_once_with(
            token_url="https://auth/token",
            client_id="cid",
            client_secret="csecret",
        )

    def test_custom_scope(self) -> None:
        with (
            _patch_coni(),
            patch("odsbox.con_i_factory.BackendApplicationClient") as mock_client_cls,
            patch("odsbox.con_i_factory.OAuth2Session") as mock_session_cls,
        ):
            mock_session = MagicMock()
            mock_session_cls.return_value = mock_session

            ConIFactory.m2m(
                url="https://server/api",
                token_endpoint="https://auth/token",
                client_id="cid",
                client_secret="s",
                scope=["custom_scope"],
            )

        # Scope must be set on BackendApplicationClient, not OAuth2Session
        mock_client_cls.assert_called_once_with(client_id="cid", scope=["custom_scope"])


# ---------------------------------------------------------------------------
# OIDC (interactive browser login)
# ---------------------------------------------------------------------------


class TestOidcAuth:
    def test_oidc_with_direct_endpoints(self) -> None:
        """OIDC with explicitly provided endpoints — no WebFinger."""
        fake_token = {"access_token": "oidc_tok", "token_type": "Bearer"}

        with (
            _patch_coni(),
            patch("odsbox.con_i_factory.OAuth2Session") as mock_session_cls,
            patch("webbrowser.open") as mock_wb_open,
            patch("odsbox.con_i_factory._AuthCodeHTTPServer") as mock_server_cls,
        ):
            mock_session = MagicMock()
            mock_session.authorization_url.return_value = (
                "https://auth/login",
                "state",
            )
            mock_session.fetch_token.return_value = fake_token
            mock_session_cls.return_value = mock_session

            # Simulate the callback server receiving the auth code immediately
            mock_server = MagicMock()
            mock_server.auth_code = "/?code=abc123"
            mock_server_cls.return_value = mock_server

            con = ConIFactory.oidc(
                url="https://server/api",
                client_id="cid",
                redirect_uri="http://127.0.0.1:5678",
                authorization_endpoint="https://auth/authorize",
                token_endpoint="https://auth/token",
                login_timeout=1,
            )

        assert isinstance(con, _FakeConI)
        assert con.kwargs["custom_session"] is mock_session
        mock_wb_open.assert_called_once()
        mock_session.fetch_token.assert_called_once()

    def test_oidc_with_webfinger(self) -> None:
        """OIDC with WebFinger discovery (endpoints not provided)."""
        with (
            _patch_coni(),
            patch("odsbox.con_i_factory.OAuth2Session") as mock_session_cls,
            patch("webbrowser.open"),
            patch("odsbox.con_i_factory._AuthCodeHTTPServer") as mock_server_cls,
            patch("odsbox.con_i_factory._discover_endpoints") as mock_discover,
        ):
            mock_discover.return_value = (
                "https://auth/authorize",
                "https://auth/token",
            )

            mock_session = MagicMock()
            mock_session.authorization_url.return_value = ("https://auth/login", "st")
            mock_session_cls.return_value = mock_session

            mock_server = MagicMock()
            mock_server.auth_code = "/?code=xyz"
            mock_server_cls.return_value = mock_server

            con = ConIFactory.oidc(
                url="https://server/api",
                client_id="cid",
                redirect_uri="http://127.0.0.1:1234",
                redirect_url_allow_insecure=True,
                login_timeout=1,
            )

        mock_discover.assert_called_once_with("https://server/api", "", verify=True)
        assert isinstance(con, _FakeConI)

    def test_oidc_login_timeout_raises(self) -> None:
        """Timeout when the user does not complete login."""
        with (
            _patch_coni(),
            patch("odsbox.con_i_factory.OAuth2Session") as mock_session_cls,
            patch("webbrowser.open"),
            patch("odsbox.con_i_factory._AuthCodeHTTPServer") as mock_server_cls,
        ):
            mock_session = MagicMock()
            mock_session.authorization_url.return_value = ("https://auth/login", "st")
            mock_session_cls.return_value = mock_session

            mock_server = MagicMock()
            mock_server.auth_code = None  # never receives a code
            mock_server_cls.return_value = mock_server

            with pytest.raises(ValueError, match="Login timed out"):
                ConIFactory.oidc(
                    url="https://server/api",
                    client_id="cid",
                    redirect_uri="http://127.0.0.1:1234",
                    redirect_url_allow_insecure=True,
                    authorization_endpoint="https://auth/authorize",
                    token_endpoint="https://auth/token",
                    login_timeout=0,
                )

    def test_oidc_invalid_redirect_uri_raises(self) -> None:
        with (
            _patch_coni(),
            patch("odsbox.con_i_factory.OAuth2Session") as mock_session_cls,
            patch("webbrowser.open"),
        ):
            mock_session = MagicMock()
            mock_session.authorization_url.return_value = ("https://auth/login", "st")
            mock_session_cls.return_value = mock_session

            with pytest.raises(ValueError, match="Invalid redirect URI"):
                ConIFactory.oidc(
                    url="https://server/api",
                    client_id="cid",
                    redirect_uri="not-a-url",
                    authorization_endpoint="https://auth/authorize",
                    token_endpoint="https://auth/token",
                )

    def test_redirect_url_allow_insecure_false_default(self) -> None:
        """By default, insecure HTTP redirects are not allowed."""
        with (
            _patch_coni(),
            patch("odsbox.con_i_factory.OAuth2Session") as mock_session_cls,
            patch("webbrowser.open"),
            patch("odsbox.con_i_factory._AuthCodeHTTPServer") as mock_server_cls,
            patch.dict(os.environ, {}, clear=False) as mock_env,
        ):
            mock_session = MagicMock()
            mock_session.authorization_url.return_value = ("https://auth/login", "st")
            mock_session_cls.return_value = mock_session

            mock_server = MagicMock()
            mock_server.auth_code = "/?code=xyz"
            mock_server_cls.return_value = mock_server

            ConIFactory.oidc(
                url="https://server/api",
                client_id="cid",
                redirect_uri="http://127.0.0.1:1234",
                redirect_url_allow_insecure=False,
                authorization_endpoint="https://auth/authorize",
                token_endpoint="https://auth/token",
                login_timeout=1,
            )

        assert "OAUTHLIB_INSECURE_TRANSPORT" not in mock_env or mock_env["OAUTHLIB_INSECURE_TRANSPORT"] != "1"

    def test_redirect_url_allow_insecure_true(self) -> None:
        """When redirect_url_allow_insecure=True, OAUTHLIB_INSECURE_TRANSPORT is set during function execution."""
        # Clear the variable first to ensure it's not set
        original_value = os.environ.pop("OAUTHLIB_INSECURE_TRANSPORT", None)
        try:
            with (
                _patch_coni(),
                patch("odsbox.con_i_factory.OAuth2Session") as mock_session_cls,
                patch("webbrowser.open"),
                patch("odsbox.con_i_factory._AuthCodeHTTPServer") as mock_server_cls,
            ):
                mock_session = MagicMock()
                mock_session.authorization_url.return_value = (
                    "https://auth/login",
                    "st",
                )
                mock_session_cls.return_value = mock_session

                mock_server = MagicMock()
                mock_server.auth_code = "/?code=xyz"
                mock_server_cls.return_value = mock_server

                ConIFactory.oidc(
                    url="https://server/api",
                    client_id="cid",
                    redirect_uri="http://127.0.0.1:1234",
                    redirect_url_allow_insecure=True,
                    authorization_endpoint="https://auth/authorize",
                    token_endpoint="https://auth/token",
                    login_timeout=1,
                )

            # After function returns, the environment variable should be restored to its original value
            if original_value is None:
                assert "OAUTHLIB_INSECURE_TRANSPORT" not in os.environ
            else:
                assert os.environ["OAUTHLIB_INSECURE_TRANSPORT"] == original_value
        finally:
            # Restore original value
            if original_value is not None:
                os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = original_value
            else:
                os.environ.pop("OAUTHLIB_INSECURE_TRANSPORT", None)


# ---------------------------------------------------------------------------
# WebFinger discovery
# ---------------------------------------------------------------------------


class TestDiscoverEndpoints:
    def test_successful_discovery(self) -> None:
        webfinger_response = MagicMock()
        webfinger_response.status_code = 200
        webfinger_response.json.return_value = {
            "links": [
                {
                    "rel": "http://openid.net/specs/connect/1.0/issuer",
                    "href": "https://idp.example.com",
                }
            ]
        }

        oidc_config_response = MagicMock()
        oidc_config_response.status_code = 200
        oidc_config_response.json.return_value = {
            "authorization_endpoint": "https://idp.example.com/authorize",
            "token_endpoint": "https://idp.example.com/token",
        }

        with patch("odsbox.con_i_factory.requests.get") as mock_get:
            mock_get.side_effect = [webfinger_response, oidc_config_response]
            auth_ep, token_ep = _discover_endpoints("https://server/api")

        assert auth_ep == "https://idp.example.com/authorize"
        assert token_ep == "https://idp.example.com/token"

    def test_webfinger_failure_raises(self) -> None:
        resp = MagicMock()
        resp.status_code = 404

        with patch("odsbox.con_i_factory.requests.get", return_value=resp):
            with pytest.raises(ValueError, match="WebFinger request failed"):
                _discover_endpoints("https://server/api")

    def test_missing_issuer_raises(self) -> None:
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"links": []}

        with patch("odsbox.con_i_factory.requests.get", return_value=resp):
            with pytest.raises(ValueError, match="OIDC issuer not found"):
                _discover_endpoints("https://server/api")

    def test_missing_endpoints_raises(self) -> None:
        webfinger_response = MagicMock()
        webfinger_response.status_code = 200
        webfinger_response.json.return_value = {
            "links": [
                {
                    "rel": "http://openid.net/specs/connect/1.0/issuer",
                    "href": "https://idp.example.com",
                }
            ]
        }

        oidc_config_response = MagicMock()
        oidc_config_response.status_code = 200
        oidc_config_response.json.return_value = {}  # missing endpoints

        with patch("odsbox.con_i_factory.requests.get") as mock_get:
            mock_get.side_effect = [webfinger_response, oidc_config_response]
            with pytest.raises(ValueError, match="Missing endpoints"):
                _discover_endpoints("https://server/api")


# ---------------------------------------------------------------------------
# AuthCodeHTTPServer
# ---------------------------------------------------------------------------


class TestAuthCodeHTTPServer:
    def test_auth_code_initialized_to_none(self) -> None:
        """The custom server has auth_code attribute set to None on init."""
        server = _AuthCodeHTTPServer("127.0.0.1", 0)  # port 0 = OS picks free port
        try:
            assert server.auth_code is None
        finally:
            server.server_close()

    def test_callback_sets_auth_code(self) -> None:
        """Simulating a GET request with a code sets auth_code on the server."""
        server = _AuthCodeHTTPServer("127.0.0.1", 0)
        port = server.server_address[1]
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        try:
            import urllib.request

            urllib.request.urlopen(f"http://127.0.0.1:{port}/?code=test123")
            # Give the handler a moment to set the value
            time.sleep(0.2)
            assert server.auth_code == "/?code=test123"
        finally:
            server.shutdown()
            server.server_close()


# ---------------------------------------------------------------------------
# Integration-style smoke tests (still unit — no real servers)
# ---------------------------------------------------------------------------


class TestConIFactoryDiscoverEndpointsExposed:
    def test_discover_is_accessible(self) -> None:
        """``ConIFactory.discover_endpoints`` is the same function."""
        assert ConIFactory.discover_endpoints is _discover_endpoints
