# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: odsbox/proto/ods_external_data.proto
# Protobuf Python Version: 5.26.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from odsbox.proto import ods_pb2 as asam__odsbox_dot_proto_dot_ods__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n)odsbox/proto/ods_external_data.proto\x12\x11ods.external_data\x1a\x1b\x61sam_odsbox/proto/ods.proto"-\n\nIdentifier\x12\x0b\n\x03url\x18\x01 \x01(\t\x12\x12\n\nparameters\x18\x02 \x01(\t"\x16\n\x06Handle\x12\x0c\n\x04uuid\x18\x01 \x01(\t"\x07\n\x05\x45mpty"\x8c\x01\n\x10StructureRequest\x12)\n\x06handle\x18\x01 \x01(\x0b\x32\x19.ods.external_data.Handle\x12\x19\n\x11suppress_channels\x18\x02 \x01(\x08\x12\x1b\n\x13suppress_attributes\x18\x03 \x01(\x08\x12\x15\n\rchannel_names\x18\x04 \x03(\t"\x8a\x04\n\x0fStructureResult\x12\x31\n\nidentifier\x18\x01 \x01(\x0b\x32\x1d.ods.external_data.Identifier\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x38\n\x06groups\x18\x03 \x03(\x0b\x32(.ods.external_data.StructureResult.Group\x12)\n\nattributes\x18\x04 \x01(\x0b\x32\x15.ods.ContextVariables\x1a\x89\x01\n\x07\x43hannel\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x0c\n\x04name\x18\x02 \x01(\t\x12$\n\tdata_type\x18\x03 \x01(\x0e\x32\x11.ods.DataTypeEnum\x12\x13\n\x0bunit_string\x18\x04 \x01(\t\x12)\n\nattributes\x18\x05 \x01(\x0b\x32\x15.ods.ContextVariables\x1a\xc4\x01\n\x05Group\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x0c\n\x04name\x18\x02 \x01(\t\x12 \n\x18total_number_of_channels\x18\x03 \x01(\x03\x12\x16\n\x0enumber_of_rows\x18\x04 \x01(\x03\x12<\n\x08\x63hannels\x18\x05 \x03(\x0b\x32*.ods.external_data.StructureResult.Channel\x12)\n\nattributes\x18\x06 \x01(\x0b\x32\x15.ods.ContextVariables"\x7f\n\rValuesRequest\x12)\n\x06handle\x18\x01 \x01(\x0b\x32\x19.ods.external_data.Handle\x12\x10\n\x08group_id\x18\x02 \x01(\x03\x12\x13\n\x0b\x63hannel_ids\x18\x03 \x03(\x03\x12\r\n\x05start\x18\x04 \x01(\x03\x12\r\n\x05limit\x18\x05 \x01(\x03"\xcc\x01\n\x0cValuesResult\x12\n\n\x02id\x18\x01 \x01(\x03\x12?\n\x08\x63hannels\x18\x02 \x03(\x0b\x32-.ods.external_data.ValuesResult.ChannelValues\x1ao\n\rChannelValues\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x33\n\x06values\x18\x02 \x01(\x0b\x32#.ods.DataMatrix.Column.UnknownArray\x12\x1d\n\x05\x66lags\x18\x03 \x01(\x0b\x32\x0e.ods.LongArray"\x97\x01\n\x0fValuesExRequest\x12)\n\x06handle\x18\x01 \x01(\x0b\x32\x19.ods.external_data.Handle\x12\x10\n\x08group_id\x18\x02 \x01(\x03\x12\x15\n\rchannel_names\x18\x03 \x03(\t\x12\x12\n\nattributes\x18\x04 \x03(\t\x12\r\n\x05start\x18\x05 \x01(\x03\x12\r\n\x05limit\x18\x06 \x01(\x03"\xa3\x01\n\x0eValuesExResult\x12\x1f\n\x06values\x18\x01 \x01(\x0b\x32\x0f.ods.DataMatrix\x12@\n\x08unit_map\x18\x02 \x03(\x0b\x32..ods.external_data.ValuesExResult.UnitMapEntry\x1a.\n\x0cUnitMapEntry\x12\x0b\n\x03key\x18\x01 \x01(\x03\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x32\x9d\x03\n\x12\x45xternalDataReader\x12\x42\n\x04Open\x12\x1d.ods.external_data.Identifier\x1a\x19.ods.external_data.Handle"\x00\x12Y\n\x0cGetStructure\x12#.ods.external_data.StructureRequest\x1a".ods.external_data.StructureResult"\x00\x12P\n\tGetValues\x12 .ods.external_data.ValuesRequest\x1a\x1f.ods.external_data.ValuesResult"\x00\x12V\n\x0bGetValuesEx\x12".ods.external_data.ValuesExRequest\x1a!.ods.external_data.ValuesExResult"\x00\x12>\n\x05\x43lose\x12\x19.ods.external_data.Handle\x1a\x18.ods.external_data.Empty"\x00\x62\x06proto3'
)

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, "odsbox.proto.ods_external_data_pb2", _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    DESCRIPTOR._loaded_options = None
    _globals["_VALUESEXRESULT_UNITMAPENTRY"]._loaded_options = None
    _globals["_VALUESEXRESULT_UNITMAPENTRY"]._serialized_options = b"8\001"
    _globals["_IDENTIFIER"]._serialized_start = 93
    _globals["_IDENTIFIER"]._serialized_end = 138
    _globals["_HANDLE"]._serialized_start = 140
    _globals["_HANDLE"]._serialized_end = 162
    _globals["_EMPTY"]._serialized_start = 164
    _globals["_EMPTY"]._serialized_end = 171
    _globals["_STRUCTUREREQUEST"]._serialized_start = 174
    _globals["_STRUCTUREREQUEST"]._serialized_end = 314
    _globals["_STRUCTURERESULT"]._serialized_start = 317
    _globals["_STRUCTURERESULT"]._serialized_end = 839
    _globals["_STRUCTURERESULT_CHANNEL"]._serialized_start = 503
    _globals["_STRUCTURERESULT_CHANNEL"]._serialized_end = 640
    _globals["_STRUCTURERESULT_GROUP"]._serialized_start = 643
    _globals["_STRUCTURERESULT_GROUP"]._serialized_end = 839
    _globals["_VALUESREQUEST"]._serialized_start = 841
    _globals["_VALUESREQUEST"]._serialized_end = 968
    _globals["_VALUESRESULT"]._serialized_start = 971
    _globals["_VALUESRESULT"]._serialized_end = 1175
    _globals["_VALUESRESULT_CHANNELVALUES"]._serialized_start = 1064
    _globals["_VALUESRESULT_CHANNELVALUES"]._serialized_end = 1175
    _globals["_VALUESEXREQUEST"]._serialized_start = 1178
    _globals["_VALUESEXREQUEST"]._serialized_end = 1329
    _globals["_VALUESEXRESULT"]._serialized_start = 1332
    _globals["_VALUESEXRESULT"]._serialized_end = 1495
    _globals["_VALUESEXRESULT_UNITMAPENTRY"]._serialized_start = 1449
    _globals["_VALUESEXRESULT_UNITMAPENTRY"]._serialized_end = 1495
    _globals["_EXTERNALDATAREADER"]._serialized_start = 1498
    _globals["_EXTERNALDATAREADER"]._serialized_end = 1911
# @@protoc_insertion_point(module_scope)
