"""Helper class for ASAM ODS Security configuration"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .con_i import ConI
import odsbox.proto.ods_security_pb2 as ods_security

from enum import IntFlag


class Security:
    """
    This class offers the ASAM ODS server security API
    """

    def __init__(self, con_i: "ConI"):
        self.__con_i = con_i

    def security_read(self, security_read_request: ods_security.SecurityReadRequest) -> ods_security.SecurityInfo:
        """
        This method reads the security configuration from the ASAM ODS server.
        :param security_read_request: The security read request
        :raises requests.HTTPError: If fails.
        :return: The security information
        """
        response = self.__con_i.ods_post_request("security-read", security_read_request)
        security_info = ods_security.SecurityInfo()
        security_info.ParseFromString(response.content)
        return security_info

    def security_update(self, security_write_request: ods_security.SecurityWriteRequest) -> None:
        """
        This method updates the security configuration on the ASAM ODS server.
        :param security_write_request: The security write request
        :raises requests.HTTPError: If fails.
        """
        self.__con_i.ods_post_request("security-update", security_write_request)

    def initial_rights(self, security_write_request: ods_security.SecurityWriteRequest) -> None:
        """
        This method sets the initial rights for newly created instances.
        :param security_write_request: The security write request
        :raises requests.HTTPError: If fails.
        """
        self.__con_i.ods_post_request("initial-rights", security_write_request)

    class Level(IntFlag):
        """
        This class defines the security levels for the ASAM ODS server.
        The levels are defined as bit flags found in the security_level attribute
        of the entities.
        """

        ELEMENT = 1  # Bit 0
        INSTANCE = 2  # Bit 1
        ATTRIBUTE = 4  # Bit 2

    class Right(IntFlag):
        """
        This class defines the security rights for the ASAM ODS server.
        The rights are defined as bit flags found in the security api.
        In the API they are used as integer values.
        """

        READ = 1  # Bit 0
        UPDATE = 2  # Bit 1
        INSERT = 4  # Bit 2
        DELETE = 8  # Bit 3
        GRANT = 16  # Bit 4
