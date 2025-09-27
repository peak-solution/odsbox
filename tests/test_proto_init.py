"""Tests for odsbox.proto lazy loading functionality"""

from __future__ import annotations

import sys

import pytest


def test_proto_lazy_import_ods():
    """Test that ods module is lazily imported"""
    # Remove from cache if already imported
    if "odsbox.proto" in sys.modules:
        del sys.modules["odsbox.proto"]

    from odsbox.proto import ods

    # Should be the actual ods_pb2 module
    assert hasattr(ods, "Model")
    assert hasattr(ods, "DataMatrices")
    assert hasattr(ods, "SelectStatement")


def test_proto_lazy_import_ods_security():
    """Test that ods_security module is lazily imported"""
    from odsbox.proto import ods_security

    # Should be the actual ods_security_pb2 module
    assert hasattr(ods_security, "SecurityInfo")
    assert hasattr(ods_security, "SecurityEntry")


def test_proto_lazy_import_ods_notification():
    """Test that ods_notification module is lazily imported"""
    from odsbox.proto import ods_notification

    # Should be the actual ods_notification_pb2 module
    assert hasattr(ods_notification, "Notification")


def test_proto_lazy_import_multiple():
    """Test importing multiple modules at once"""
    from odsbox.proto import ods, ods_notification, ods_security

    assert hasattr(ods, "Model")
    assert hasattr(ods_security, "SecurityInfo")
    assert hasattr(ods_notification, "Notification")


def test_proto_getattr_invalid_module():
    """Test that invalid module names raise AttributeError"""
    import odsbox.proto as proto

    with pytest.raises(AttributeError) as exc_info:
        _ = proto.invalid_module

    assert "has no attribute 'invalid_module'" in str(exc_info.value)


def test_proto_dir_contains_expected_modules():
    """Test that __dir__ returns expected module names"""
    import odsbox.proto as proto

    dir_result = dir(proto)
    expected_modules = ["ods", "ods_security", "ods_notification", "ods_external_data", "ods_external_data_grpc"]

    for module in expected_modules:
        assert module in dir_result


def test_proto_all_contains_expected_modules():
    """Test that __all__ contains expected module names"""
    import odsbox.proto as proto

    expected_modules = ["ods", "ods_security", "ods_notification", "ods_external_data", "ods_external_data_grpc"]

    assert proto.__all__ == expected_modules


def test_proto_lazy_loading_behavior():
    """Test that modules are only loaded when accessed"""
    import importlib

    import odsbox.proto as proto

    # Force reload of the proto module to clear any caching
    importlib.reload(proto)

    # Remove from cache to ensure clean state
    modules_to_remove = [key for key in sys.modules.keys() if key.startswith("odsbox.proto.ods")]
    for mod in modules_to_remove:
        del sys.modules[mod]

    # Re-import after clearing cache
    if "odsbox.proto" in sys.modules:
        del sys.modules["odsbox.proto"]
    import odsbox.proto as proto

    # Verify module is not loaded yet
    assert "odsbox.proto.ods_pb2" not in sys.modules

    # Access ods - this should trigger the lazy import
    _ = proto.ods

    # Now odsbox.proto.ods_pb2 should be in sys.modules
    assert "odsbox.proto.ods_pb2" in sys.modules


def test_proto_external_data_import_error_handling():
    """Test graceful handling of missing grpcio dependency"""
    # Test this by actually trying to access the module
    # If grpcio is not installed, it should raise the expected error
    import odsbox.proto as proto

    try:
        _ = proto.ods_external_data
        # If we get here, grpcio is installed, which is fine
        # We can't easily test the error case without breaking the environment
        assert True
    except ImportError as e:
        # Expected if grpcio is not installed
        assert "requires grpcio" in str(e)
        assert "pip install odsbox[exd-data]" in str(e)


def test_proto_external_data_grpc_import_error_handling():
    """Test graceful handling of missing grpcio dependency for grpc module"""
    import odsbox.proto as proto

    try:
        _ = proto.ods_external_data_grpc
        # If we get here, grpcio is installed, which is fine
        assert True
    except ImportError as e:
        # Expected if grpcio is not installed
        assert "requires grpcio" in str(e)
        assert "pip install odsbox[exd-data]" in str(e)


def test_proto_import_from_star():
    """Test 'from odsbox.proto import *' functionality"""
    # This test verifies that __all__ works correctly
    # We can't easily test the actual star import in pytest, but we can verify __all__
    import odsbox.proto as proto

    # Verify that accessing attributes in __all__ works
    for attr_name in proto.__all__:
        if attr_name in ["ods_external_data", "ods_external_data_grpc"]:
            # These might fail due to missing grpcio, so we handle them separately
            try:
                attr = getattr(proto, attr_name)
                assert attr is not None
            except ImportError:
                # Expected if grpcio is not installed
                pass
        else:
            attr = getattr(proto, attr_name)
            assert attr is not None


def test_proto_module_caching():
    """Test that modules are cached after first import"""
    import odsbox.proto as proto

    # Import ods twice
    ods1 = proto.ods
    ods2 = proto.ods

    # Should be the same object (cached)
    assert ods1 is ods2
