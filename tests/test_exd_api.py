"""Check if import of ext data works"""

import asam_odsbox.proto.ods_external_data_pb2 as exd_api


def test_strcuture_creation():
    structure_request = exd_api.StructureRequest(handle=exd_api.Handle(uuid="abc-def"))
    assert structure_request is not None
