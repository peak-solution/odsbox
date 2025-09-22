"""odsbox - Toolbox for accessing ASAM ODS servers using the HTTP API

This package provides convenient access to ASAM ODS servers with lazy loading
for better performance.

Example:
    from odsbox import ConI

    with ConI(url="http://localhost:8087/api", auth=("sa", "sa")) as con_i:
        units = con_i.query_data({"AoUnit": {}})
"""

from __future__ import annotations

__version__ = "1.0.12"


def __getattr__(name: str):
    """Lazy import for better performance"""
    if name == "ConI":
        from .con_i import ConI

        return ConI
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    """Return list of available attributes for tab completion"""
    return ["ConI", "__version__"]


# Define what gets imported with "from odsbox import *"
__all__ = ["ConI"]
