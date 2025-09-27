"""ASAM ODS Protocol Buffer interfaces

This module provides convenient access to ASAM ODS protobuf definitions
with lazy loading for better performance.

Example::

    from odsbox.proto import ods, ods_security

    # Create a model instance
    model = ods.Model()

"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from . import ods_notification_pb2 as ods_notification
    from . import ods_pb2 as ods
    from . import ods_security_pb2 as ods_security


def __getattr__(name: str):
    """Lazy import for better performance and optional dependencies"""
    if name == "ods":
        from . import ods_pb2 as ods

        return ods
    elif name == "ods_security":
        from . import ods_security_pb2 as ods_security

        return ods_security
    elif name == "ods_notification":
        from . import ods_notification_pb2 as ods_notification

        return ods_notification
    elif name == "ods_external_data":
        try:
            from . import ods_external_data_pb2 as ods_external_data

            return ods_external_data
        except ImportError as e:
            raise ImportError("ods_external_data requires grpcio. Install with: pip install odsbox[exd-data]") from e
    elif name == "ods_external_data_grpc":
        try:
            from . import ods_external_data_pb2_grpc as ods_external_data_grpc

            return ods_external_data_grpc
        except ImportError as e:
            raise ImportError(
                "ods_external_data_grpc requires grpcio. Install with: pip install odsbox[exd-data]"
            ) from e
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    """Return list of available attributes for tab completion"""
    return ["ods", "ods_security", "ods_notification", "ods_external_data", "ods_external_data_grpc"]


# Define what gets imported with "from odsbox.proto import *"
__all__ = ["ods", "ods_security", "ods_notification", "ods_external_data", "ods_external_data_grpc"]
