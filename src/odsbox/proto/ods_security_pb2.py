# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: odsbox/proto/ods_security.proto
# Protobuf Python Version: 5.26.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n$odsbox/proto/ods_security.proto\x12\x0cods.security"\xf0\x04\n\x13SecurityReadRequest\x12=\n\x15security_request_type\x18\x01 \x01(\x0e\x32\x1e.ods.security.SecurityTypeEnum\x12:\n\x10\x64\x61ta_object_type\x18\x02 \x01(\x0e\x32 .ods.security.DataObjectTypeEnum\x12]\n\x13\x61pplication_element\x18\x03 \x01(\x0b\x32>.ods.security.SecurityReadRequest.DataObjectApplicationElementH\x00\x12\x61\n\x15\x61pplication_attribute\x18\x04 \x01(\x0b\x32@.ods.security.SecurityReadRequest.DataObjectApplicationAttributeH\x00\x12H\n\x08instance\x18\x05 \x01(\x0b\x32\x34.ods.security.SecurityReadRequest.DataObjectInstanceH\x00\x12\x15\n\ruser_group_id\x18\x06 \x01(\x03\x1a+\n\x1c\x44\x61taObjectApplicationElement\x12\x0b\n\x03\x61id\x18\x01 \x01(\x03\x1a\x46\n\x1e\x44\x61taObjectApplicationAttribute\x12\x0b\n\x03\x61id\x18\x01 \x01(\x03\x12\x17\n\x0f\x61ttribute_names\x18\x02 \x03(\t\x1a\x33\n\x12\x44\x61taObjectInstance\x12\x0b\n\x03\x61id\x18\x01 \x01(\x03\x12\x10\n\x04iids\x18\x02 \x03(\x03\x42\x02\x10\x01\x42\x11\n\x0f\x44\x61taObjectOneOf"q\n\rSecurityEntry\x12\x0b\n\x03\x61id\x18\x01 \x01(\x03\x12\x0f\n\x07\x61\x61_name\x18\x02 \x01(\t\x12\x0b\n\x03iid\x18\x03 \x01(\x03\x12\x14\n\x0cusergroup_id\x18\x04 \x01(\x03\x12\x0e\n\x06rights\x18\x05 \x01(\x05\x12\x0f\n\x07ref_aid\x18\x06 \x01(\x03"\xaf\x01\n\x0cSecurityInfo\x12\x35\n\rsecurity_type\x18\x01 \x01(\x0e\x32\x1e.ods.security.SecurityTypeEnum\x12:\n\x10\x64\x61ta_object_type\x18\x02 \x01(\x0e\x32 .ods.security.DataObjectTypeEnum\x12,\n\x07\x65ntries\x18\x03 \x03(\x0b\x32\x1b.ods.security.SecurityEntry"\xd3\x02\n\x14SecurityWriteRequest\x12=\n\x15security_request_type\x18\x01 \x01(\x0e\x32\x1e.ods.security.SecurityTypeEnum\x12:\n\x10\x64\x61ta_object_type\x18\x02 \x01(\x0e\x32 .ods.security.DataObjectTypeEnum\x12,\n\x07\x65ntries\x18\x03 \x03(\x0b\x32\x1b.ods.security.SecurityEntry\x12N\n\x0bmodify_type\x18\x04 \x01(\x0e\x32\x39.ods.security.SecurityWriteRequest.SecurityModifyTypeEnum"B\n\x16SecurityModifyTypeEnum\x12\x0b\n\x07SST_ADD\x10\x00\x12\x0e\n\nSST_REMOVE\x10\x01\x12\x0b\n\x07SST_SET\x10\x02*7\n\x10SecurityTypeEnum\x12\n\n\x06ST_ACL\x10\x00\x12\x0b\n\x07ST_TPLI\x10\x01\x12\n\n\x06ST_EFF\x10\x02*b\n\x12\x44\x61taObjectTypeEnum\x12\x1b\n\x17\x44OT_APPLICATION_ELEMENT\x10\x00\x12\x1d\n\x19\x44OT_APPLICATION_ATTRIBUTE\x10\x01\x12\x10\n\x0c\x44OT_INSTANCE\x10\x02\x62\x06proto3'
)

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, "odsbox.proto.ods_security_pb2", _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    DESCRIPTOR._loaded_options = None
    _globals["_SECURITYREADREQUEST_DATAOBJECTINSTANCE"].fields_by_name["iids"]._loaded_options = None
    _globals["_SECURITYREADREQUEST_DATAOBJECTINSTANCE"].fields_by_name["iids"]._serialized_options = b"\020\001"
    _globals["_SECURITYTYPEENUM"]._serialized_start = 1316
    _globals["_SECURITYTYPEENUM"]._serialized_end = 1371
    _globals["_DATAOBJECTTYPEENUM"]._serialized_start = 1373
    _globals["_DATAOBJECTTYPEENUM"]._serialized_end = 1471
    _globals["_SECURITYREADREQUEST"]._serialized_start = 55
    _globals["_SECURITYREADREQUEST"]._serialized_end = 679
    _globals["_SECURITYREADREQUEST_DATAOBJECTAPPLICATIONELEMENT"]._serialized_start = 492
    _globals["_SECURITYREADREQUEST_DATAOBJECTAPPLICATIONELEMENT"]._serialized_end = 535
    _globals["_SECURITYREADREQUEST_DATAOBJECTAPPLICATIONATTRIBUTE"]._serialized_start = 537
    _globals["_SECURITYREADREQUEST_DATAOBJECTAPPLICATIONATTRIBUTE"]._serialized_end = 607
    _globals["_SECURITYREADREQUEST_DATAOBJECTINSTANCE"]._serialized_start = 609
    _globals["_SECURITYREADREQUEST_DATAOBJECTINSTANCE"]._serialized_end = 660
    _globals["_SECURITYENTRY"]._serialized_start = 681
    _globals["_SECURITYENTRY"]._serialized_end = 794
    _globals["_SECURITYINFO"]._serialized_start = 797
    _globals["_SECURITYINFO"]._serialized_end = 972
    _globals["_SECURITYWRITEREQUEST"]._serialized_start = 975
    _globals["_SECURITYWRITEREQUEST"]._serialized_end = 1314
    _globals["_SECURITYWRITEREQUEST_SECURITYMODIFYTYPEENUM"]._serialized_start = 1248
    _globals["_SECURITYWRITEREQUEST_SECURITYMODIFYTYPEENUM"]._serialized_end = 1314
# @@protoc_insertion_point(module_scope)
