"""Tests for odsbox.__init__ lazy loading functionality"""

from __future__ import annotations

import sys

import pytest


def test_odsbox_lazy_import_coni():
    """Test that ConI is lazily imported"""
    from odsbox import ConI

    # Should be the actual ConI class
    assert ConI.__name__ == "ConI"
    assert hasattr(ConI, "__enter__")  # Context manager
    assert hasattr(ConI, "__exit__")  # Context manager
    assert hasattr(ConI, "query_data")


def test_odsbox_getattr_invalid_attribute():
    """Test that invalid attribute names raise AttributeError"""
    import odsbox

    with pytest.raises(AttributeError) as exc_info:
        _ = odsbox.invalid_attribute

    assert "has no attribute 'invalid_attribute'" in str(exc_info.value)


def test_odsbox_dir_contains_expected_attributes():
    """Test that __dir__ returns expected attribute names"""
    import odsbox

    dir_result = dir(odsbox)
    expected_attributes = ["ConI", "__version__"]

    for attribute in expected_attributes:
        assert attribute in dir_result


def test_odsbox_all_contains_expected_attributes():
    """Test that __all__ contains expected attribute names"""
    import odsbox

    expected_attributes = ["ConI"]

    assert odsbox.__all__ == expected_attributes


def test_odsbox_version_attribute():
    """Test that __version__ is accessible"""
    import odsbox

    assert hasattr(odsbox, "__version__")
    assert isinstance(odsbox.__version__, str)
    version_parts = odsbox.__version__.split(".")
    assert len(version_parts) == 3
    for part in version_parts:
        assert part.isdigit()


def test_odsbox_lazy_loading_behavior():
    """Test that modules are only loaded when accessed"""
    import importlib

    import odsbox

    # Force reload to clear any caching
    importlib.reload(odsbox)

    # Remove relevant modules from cache
    modules_to_remove = [key for key in sys.modules.keys() if key.startswith("odsbox.con_i")]
    for mod in modules_to_remove:
        del sys.modules[mod]

    # Verify module is not loaded yet
    assert "odsbox.con_i" not in sys.modules

    # Access ConI - this should trigger the lazy import
    _ = odsbox.ConI

    # Now odsbox.con_i should be in sys.modules
    assert "odsbox.con_i" in sys.modules


def test_odsbox_module_caching():
    """Test that modules are cached after first import"""
    import odsbox

    # Import ConI twice
    coni1 = odsbox.ConI
    coni2 = odsbox.ConI

    # Should be the same class object (cached)
    assert coni1 is coni2


def test_odsbox_import_from_star():
    """Test 'from odsbox import *' functionality"""
    import odsbox

    # Verify that accessing attributes in __all__ works
    for attr_name in odsbox.__all__:
        attr = getattr(odsbox, attr_name)
        assert attr is not None


def test_odsbox_backwards_compatibility():
    """Test that old import style still works"""
    # Old style imports should still work
    # New style imports
    from odsbox import ConI
    from odsbox.con_i import ConI as ConI_old

    # Should be the same object
    assert ConI is ConI_old


def test_odsbox_readme_example():
    """Test that the README example works with the new API"""
    from odsbox import ConI

    # This should work without errors (just test the import and class access)
    assert ConI is not None
    assert hasattr(ConI, "__init__")

    # We can't test the actual connection without a server,
    # but we can verify the class has the expected interface
    assert hasattr(ConI, "query_data")
    assert hasattr(ConI, "__enter__")
    assert hasattr(ConI, "__exit__")
