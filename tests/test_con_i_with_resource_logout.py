"""Mock tests for ConI with resource logout behavior"""

from __future__ import annotations

import logging
from unittest import mock

import pytest
import requests

import odsbox.proto.ods_pb2 as ods
from odsbox.con_i import ConI


class TestConIWithResourceLogout:
    """Test class for ConI with resource context manager and logout behavior"""

    @pytest.fixture
    def mock_session(self):
        """Create a mock requests session"""
        session = mock.Mock(spec=requests.Session)
        session.auth = ("test_user", "test_pass")
        session.verify = True
        session.close = mock.Mock()
        return session

    @pytest.fixture
    def mock_post_response(self):
        """Create a mock response for the initial POST request"""
        response = mock.Mock(spec=requests.Response)
        response.status_code = 201
        response.headers = {"location": "http://test-server/api/sessions/test-session-id"}
        return response

    @pytest.fixture
    def mock_delete_response(self):
        """Create a mock response for the DELETE request during logout"""
        response = mock.Mock(spec=requests.Response)
        response.status_code = 200
        return response

    def test_successful_with_resource_logout(self, mock_session, mock_post_response, mock_delete_response):
        """Test successful context manager usage with proper logout"""
        # Arrange
        with mock.patch("requests.Session", return_value=mock_session):
            mock_session.post.return_value = mock_post_response
            mock_session.delete.return_value = mock_delete_response

            # Act & Assert
            with ConI(url="http://test-server/api", auth=("test_user", "test_pass"), load_model=False) as con_i:
                # Verify session was created
                assert con_i.con_i_url() == "http://test-server/api/sessions/test-session-id"

            # Verify logout was called
            mock_session.delete.assert_called_once_with(
                "http://test-server/api/sessions/test-session-id",
                timeout=60.0,
                headers={"Accept": "application/x-asamods+protobuf"},
                allow_redirects=False,
            )
            # Verify session was closed and cleaned up
            mock_session.close.assert_called_once()

    def test_exception_during_with_block_still_logs_out(self, mock_session, mock_post_response, mock_delete_response):
        """Test that logout still happens even if an exception occurs in the with block"""
        # Arrange
        with mock.patch("requests.Session", return_value=mock_session):
            mock_session.post.return_value = mock_post_response
            mock_session.delete.return_value = mock_delete_response

            # Act & Assert
            with pytest.raises(ValueError, match="Test exception"):
                with ConI(url="http://test-server/api", auth=("test_user", "test_pass"), load_model=False) as con_i:
                    # Verify session was created
                    assert con_i.con_i_url() == "http://test-server/api/sessions/test-session-id"
                    # Raise an exception to test cleanup
                    raise ValueError("Test exception")

            # Verify logout was still called despite the exception
            mock_session.delete.assert_called_once()
            mock_session.close.assert_called_once()

    def test_logout_failure_in_with_exit_is_handled(self, mock_session, mock_post_response, caplog):
        """Test that logout failures in __exit__ are handled gracefully"""
        # Arrange
        with mock.patch("requests.Session", return_value=mock_session):
            mock_session.post.return_value = mock_post_response
            # Make delete raise an exception
            mock_session.delete.side_effect = requests.HTTPError("Delete failed")

            # Act
            with caplog.at_level(logging.ERROR):
                with ConI(url="http://test-server/api", auth=("test_user", "test_pass"), load_model=False):
                    pass

            # Assert - exception should be logged but not re-raised
            assert "Exception during logout in close" in caplog.text
            mock_session.close.assert_called_once()

    def test_logout_failure_with_check_requests_response_error(self, mock_session, mock_post_response, caplog):
        """Test logout failure when check_requests_response raises HTTPError"""
        # Arrange
        mock_delete_response = mock.Mock(spec=requests.Response)
        mock_delete_response.status_code = 500
        mock_delete_response.headers = {}
        mock_delete_response.raise_for_status.side_effect = requests.HTTPError("Server error")

        with mock.patch("requests.Session", return_value=mock_session):
            mock_session.post.return_value = mock_post_response
            mock_session.delete.return_value = mock_delete_response

            # Act
            with caplog.at_level(logging.ERROR):
                with ConI(url="http://test-server/api", auth=("test_user", "test_pass"), load_model=False):
                    pass

            # Assert - HTTPError should be logged but not re-raised in __exit__
            assert "Exception during logout in close" in caplog.text
            mock_session.close.assert_called_once()

    def test_logout_with_protobuf_error_response(self, mock_session, mock_post_response, caplog):
        """Test logout with protobuf error response"""
        # Arrange
        error_info = ods.ErrorInfo()
        error_info.reason = "Session expired"

        mock_delete_response = mock.Mock(spec=requests.Response)
        mock_delete_response.status_code = 400
        mock_delete_response.headers = {"Content-Type": "application/x-asamods+protobuf"}
        mock_delete_response.content = error_info.SerializeToString()

        with mock.patch("requests.Session", return_value=mock_session):
            mock_session.post.return_value = mock_post_response
            mock_session.delete.return_value = mock_delete_response

            # Act
            with caplog.at_level(logging.ERROR):
                with ConI(url="http://test-server/api", auth=("test_user", "test_pass"), load_model=False):
                    pass

            # Assert - Error should be logged but not re-raised in __exit__
            assert "Exception during logout in close" in caplog.text
            mock_session.close.assert_called_once()

    def test_manual_close_before_with_exit(self, mock_session, mock_post_response, mock_delete_response):
        """Test that manual close before __exit__ doesn't cause issues"""
        # Arrange
        with mock.patch("requests.Session", return_value=mock_session):
            mock_session.post.return_value = mock_post_response
            mock_session.delete.return_value = mock_delete_response

            # Act
            with ConI(url="http://test-server/api", auth=("test_user", "test_pass"), load_model=False) as con_i:
                con_i.close()  # Manual close
                # Verify connection is closed
                with pytest.raises(ValueError, match="ConI already closed"):
                    con_i.con_i_url()

            # Assert - delete should only be called once (from manual close)
            mock_session.delete.assert_called_once()
            # Session close might be called twice, but that's ok
            assert mock_session.close.call_count >= 1

    def test_multiple_close_calls_are_safe(self, mock_session, mock_post_response, mock_delete_response):
        """Test that calling close multiple times is safe"""
        # Arrange
        with mock.patch("requests.Session", return_value=mock_session):
            mock_session.post.return_value = mock_post_response
            mock_session.delete.return_value = mock_delete_response

            con_i = ConI(url="http://test-server/api", auth=("test_user", "test_pass"), load_model=False)

            # Act - call close multiple times
            con_i.close()
            con_i.close()
            con_i.close()

            # Assert - delete should only be called once
            mock_session.delete.assert_called_once()

    def test_context_variables_in_constructor(self, mock_session, mock_post_response, mock_delete_response):
        """Test ConI creation with context variables"""
        # Arrange
        context_vars = {"key1": "value1", "key2": "value2"}

        with mock.patch("requests.Session", return_value=mock_session):
            mock_session.post.return_value = mock_post_response
            mock_session.delete.return_value = mock_delete_response

            # Act
            with ConI(
                url="http://test-server/api",
                auth=("test_user", "test_pass"),
                context_variables=context_vars,
                load_model=False,
            ):
                pass

            # Assert - POST should be called with serialized context variables
            mock_session.post.assert_called_once()
            call_args = mock_session.post.call_args
            assert call_args[0][0] == "http://test-server/api/ods"
            # Verify that data parameter contains serialized context variables
            assert "data" in call_args[1]

    def test_destructor_calls_close(self, mock_session, mock_post_response, mock_delete_response):
        """Test that __del__ calls close"""
        # Arrange
        with mock.patch("requests.Session", return_value=mock_session):
            mock_session.post.return_value = mock_post_response
            mock_session.delete.return_value = mock_delete_response

            # Act
            con_i = ConI(url="http://test-server/api", auth=("test_user", "test_pass"), load_model=False)
            con_i.__del__()

            # Assert
            mock_session.delete.assert_called_once()
            mock_session.close.assert_called_once()

    def test_logout_with_no_session(self):
        """Test logout when session is already None"""
        # Create ConI instance but set session to None to simulate already closed state
        con_i = ConI.__new__(ConI)
        # Set minimal required attributes to avoid AttributeError
        for attr in ["_ConI__session", "_ConI__con_i", "_ConI__security", "_ConI__bulk_reader", "_ConI__mc"]:
            setattr(con_i, attr, None)
        setattr(con_i, "_ConI__connection_timeout", 60.0)

        # This should not raise an exception
        con_i.logout()

    def test_con_i_url_after_close_raises_error(self, mock_session, mock_post_response, mock_delete_response):
        """Test that accessing con_i_url after close raises ValueError"""
        # Arrange
        with mock.patch("requests.Session", return_value=mock_session):
            mock_session.post.return_value = mock_post_response
            mock_session.delete.return_value = mock_delete_response

            con_i = ConI(url="http://test-server/api", auth=("test_user", "test_pass"), load_model=False)
            con_i.close()

            # Act & Assert
            with pytest.raises(ValueError, match="ConI already closed"):
                con_i.con_i_url()

    def test_nested_with_context_managers(self, mock_session, mock_post_response, mock_delete_response):
        """Test using ConI with nested context managers"""
        # Arrange
        with mock.patch("requests.Session", return_value=mock_session):
            mock_session.post.return_value = mock_post_response
            mock_session.delete.return_value = mock_delete_response

            # Act & Assert
            with ConI(url="http://test-server/api", auth=("test_user", "test_pass"), load_model=False) as con_i1:
                with ConI(url="http://test-server/api", auth=("test_user", "test_pass"), load_model=False) as con_i2:
                    # Both should have the same session URL in this mock
                    url1 = con_i1.con_i_url()
                    url2 = con_i2.con_i_url()
                    assert url1 == "http://test-server/api/sessions/test-session-id"
                    assert url2 == "http://test-server/api/sessions/test-session-id"

            # Both sessions should be properly closed
            assert mock_session.delete.call_count == 2
            assert mock_session.close.call_count == 2

    def test_custom_timeout_values_in_logout(self, mock_session, mock_post_response, mock_delete_response):
        """Test that custom timeout values are used in logout"""
        # Arrange
        custom_timeout = 30.0
        with mock.patch("requests.Session", return_value=mock_session):
            mock_session.post.return_value = mock_post_response
            mock_session.delete.return_value = mock_delete_response

            # Act
            with ConI(
                url="http://test-server/api",
                auth=("test_user", "test_pass"),
                connection_timeout=custom_timeout,
                load_model=False,
            ):
                pass

            # Assert - verify custom timeout was used in delete call
            mock_session.delete.assert_called_once_with(
                "http://test-server/api/sessions/test-session-id",
                timeout=custom_timeout,
                headers={"Accept": "application/x-asamods+protobuf"},
                allow_redirects=False,
            )

    def test_custom_allow_redirects_in_logout(self, mock_session, mock_post_response, mock_delete_response):
        """Test that custom allow_redirects setting is used in logout"""
        # Arrange
        with mock.patch("requests.Session", return_value=mock_session):
            mock_session.post.return_value = mock_post_response
            mock_session.delete.return_value = mock_delete_response

            # Act
            with ConI(
                url="http://test-server/api",
                auth=("test_user", "test_pass"),
                allow_redirects=True,  # Custom setting
                load_model=False,
            ):
                pass

            # Assert - verify custom allow_redirects was used in delete call
            mock_session.delete.assert_called_once_with(
                "http://test-server/api/sessions/test-session-id",
                timeout=60.0,
                headers={"Accept": "application/x-asamods+protobuf"},
                allow_redirects=True,  # Should be True
            )

    def test_logout_with_session_delete_network_error(self, mock_session, mock_post_response, caplog):
        """Test logout behavior when network error occurs during session delete"""
        # Arrange
        with mock.patch("requests.Session", return_value=mock_session):
            mock_session.post.return_value = mock_post_response
            mock_session.delete.side_effect = requests.ConnectionError("Network unreachable")

            # Act
            with caplog.at_level(logging.ERROR):
                with ConI(url="http://test-server/api", auth=("test_user", "test_pass"), load_model=False):
                    pass

            # Assert - error should be logged but not re-raised
            assert "Exception during logout in close" in caplog.text
            mock_session.close.assert_called_once()

    def test_context_variables_as_protobuf_object(self, mock_session, mock_post_response, mock_delete_response):
        """Test ConI creation with ContextVariables protobuf object"""
        # Arrange
        context_vars = ods.ContextVariables()
        context_vars.variables["test_key"].string_array.values.append("test_value")

        with mock.patch("requests.Session", return_value=mock_session):
            mock_session.post.return_value = mock_post_response
            mock_session.delete.return_value = mock_delete_response

            # Act
            with ConI(
                url="http://test-server/api",
                auth=("test_user", "test_pass"),
                context_variables=context_vars,
                load_model=False,
            ) as con_i:
                assert con_i.con_i_url() == "http://test-server/api/sessions/test-session-id"

            # Assert - logout should work normally
            mock_session.delete.assert_called_once()
            mock_session.close.assert_called_once()

    def test_enter_returns_self(self, mock_session, mock_post_response):
        """Test that __enter__ returns self"""
        # Arrange
        with mock.patch("requests.Session", return_value=mock_session):
            mock_session.post.return_value = mock_post_response

            con_i = ConI(url="http://test-server/api", auth=("test_user", "test_pass"), load_model=False)

            # Act & Assert
            assert con_i.__enter__() is con_i
