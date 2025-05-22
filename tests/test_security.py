from unittest import mock
from odsbox.security import Security

import odsbox.proto.ods_security_pb2 as ods_security


class DummyResponse:
    __slots__ = ("content",)

    def __init__(self, content):
        object.__setattr__(self, "content", content)

    def __setattr__(self, key, value):
        raise AttributeError(f"{self.__class__.__name__} is immutable")


def test_security_read_calls_ods_post_request_and_parses_response():
    con_i = mock.Mock()
    security_read_request = ods_security.SecurityReadRequest(
        data_object_type=ods_security.DataObjectTypeEnum.DOT_APPLICATION_ELEMENT
    )
    expected_security_info = ods_security.SecurityInfo(
        entries=[ods_security.SecurityEntry(rights=Security.Right.READ | Security.Right.UPDATE)]
    )
    serialized = expected_security_info.SerializeToString()
    con_i.ods_post_request.return_value = DummyResponse(serialized)

    security: Security = Security(con_i)
    result: ods_security.SecurityInfo = security.security_read(security_read_request)
    print(result)

    con_i.ods_post_request.assert_called_once_with("security-read", security_read_request)
    assert isinstance(result, ods_security.SecurityInfo)
    assert Security.Right.READ in Security.Right(result.entries[0].rights)


def test_security_update_calls_ods_post_request():
    con_i = mock.Mock()
    security_write_request = ods_security.SecurityWriteRequest()
    security = Security(con_i)

    security.security_update(security_write_request)

    con_i.ods_post_request.assert_called_once_with("security-update", security_write_request)


def test_initial_rights_calls_ods_post_request():
    con_i = mock.Mock()
    security_write_request = ods_security.SecurityWriteRequest()
    security = Security(con_i)

    security.initial_rights(security_write_request)

    con_i.ods_post_request.assert_called_once_with("initial-rights", security_write_request)
