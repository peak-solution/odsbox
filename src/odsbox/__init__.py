"""odsbox - Toolbox for accessing ASAM ODS servers using the HTTP API

This package provides convenient access to ASAM ODS servers with lazy loading
for better performance.

Example::

    from odsbox import ConI

    with ConI(url="http://localhost:8087/api", auth=("sa", "sa")) as con_i:
        units = con_i.query_data({"AoUnit": {}})

"""

from __future__ import annotations

from typing import TYPE_CHECKING

__version__ = "1.2.0"

if TYPE_CHECKING:
    from .con_i import ConI
    from .con_i_factory import ConIFactory


def __getattr__(name: str):
    """Lazy import for better performance"""
    if name == "ConI":
        from .con_i import ConI

        return ConI
    elif name == "ConIFactory":
        try:
            from .con_i_factory import ConIFactory
        except ImportError as e:
            raise ImportError(
                "The 'oidc' and 'm2m' authentication methods require additional dependencies. "
                "Install them with:\n\n"
                "  pip install -e .[oidc]\n\n"
                "or\n\n"
                "  pip install requests-oauthlib\n"
            ) from e
        return ConIFactory
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    """Return list of available attributes for tab completion"""
    return ["ConI", "ConIFactory", "__version__"]


# Define what gets imported with "from odsbox import *"
__all__ = ["ConI", "ConIFactory", "__version__"]
