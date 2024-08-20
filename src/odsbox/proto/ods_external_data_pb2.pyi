from odsbox.proto import ods_pb2 as _ods_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Identifier(_message.Message):
    __slots__ = ("url", "parameters")
    URL_FIELD_NUMBER: _ClassVar[int]
    PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    url: str
    parameters: str
    def __init__(self, url: _Optional[str] = ..., parameters: _Optional[str] = ...) -> None: ...

class Handle(_message.Message):
    __slots__ = ("uuid",)
    UUID_FIELD_NUMBER: _ClassVar[int]
    uuid: str
    def __init__(self, uuid: _Optional[str] = ...) -> None: ...

class Empty(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class StructureRequest(_message.Message):
    __slots__ = ("handle", "suppress_channels", "suppress_attributes", "channel_names")
    HANDLE_FIELD_NUMBER: _ClassVar[int]
    SUPPRESS_CHANNELS_FIELD_NUMBER: _ClassVar[int]
    SUPPRESS_ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_NAMES_FIELD_NUMBER: _ClassVar[int]
    handle: Handle
    suppress_channels: bool
    suppress_attributes: bool
    channel_names: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, handle: _Optional[_Union[Handle, _Mapping]] = ..., suppress_channels: bool = ..., suppress_attributes: bool = ..., channel_names: _Optional[_Iterable[str]] = ...) -> None: ...

class StructureResult(_message.Message):
    __slots__ = ("identifier", "name", "groups", "attributes")
    class Channel(_message.Message):
        __slots__ = ("id", "name", "data_type", "unit_string", "attributes")
        ID_FIELD_NUMBER: _ClassVar[int]
        NAME_FIELD_NUMBER: _ClassVar[int]
        DATA_TYPE_FIELD_NUMBER: _ClassVar[int]
        UNIT_STRING_FIELD_NUMBER: _ClassVar[int]
        ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
        id: int
        name: str
        data_type: _ods_pb2.DataTypeEnum
        unit_string: str
        attributes: _ods_pb2.ContextVariables
        def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., data_type: _Optional[_Union[_ods_pb2.DataTypeEnum, str]] = ..., unit_string: _Optional[str] = ..., attributes: _Optional[_Union[_ods_pb2.ContextVariables, _Mapping]] = ...) -> None: ...
    class Group(_message.Message):
        __slots__ = ("id", "name", "total_number_of_channels", "number_of_rows", "channels", "attributes")
        ID_FIELD_NUMBER: _ClassVar[int]
        NAME_FIELD_NUMBER: _ClassVar[int]
        TOTAL_NUMBER_OF_CHANNELS_FIELD_NUMBER: _ClassVar[int]
        NUMBER_OF_ROWS_FIELD_NUMBER: _ClassVar[int]
        CHANNELS_FIELD_NUMBER: _ClassVar[int]
        ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
        id: int
        name: str
        total_number_of_channels: int
        number_of_rows: int
        channels: _containers.RepeatedCompositeFieldContainer[StructureResult.Channel]
        attributes: _ods_pb2.ContextVariables
        def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., total_number_of_channels: _Optional[int] = ..., number_of_rows: _Optional[int] = ..., channels: _Optional[_Iterable[_Union[StructureResult.Channel, _Mapping]]] = ..., attributes: _Optional[_Union[_ods_pb2.ContextVariables, _Mapping]] = ...) -> None: ...
    IDENTIFIER_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    GROUPS_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    identifier: Identifier
    name: str
    groups: _containers.RepeatedCompositeFieldContainer[StructureResult.Group]
    attributes: _ods_pb2.ContextVariables
    def __init__(self, identifier: _Optional[_Union[Identifier, _Mapping]] = ..., name: _Optional[str] = ..., groups: _Optional[_Iterable[_Union[StructureResult.Group, _Mapping]]] = ..., attributes: _Optional[_Union[_ods_pb2.ContextVariables, _Mapping]] = ...) -> None: ...

class ValuesRequest(_message.Message):
    __slots__ = ("handle", "group_id", "channel_ids", "start", "limit")
    HANDLE_FIELD_NUMBER: _ClassVar[int]
    GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_IDS_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    handle: Handle
    group_id: int
    channel_ids: _containers.RepeatedScalarFieldContainer[int]
    start: int
    limit: int
    def __init__(self, handle: _Optional[_Union[Handle, _Mapping]] = ..., group_id: _Optional[int] = ..., channel_ids: _Optional[_Iterable[int]] = ..., start: _Optional[int] = ..., limit: _Optional[int] = ...) -> None: ...

class ValuesResult(_message.Message):
    __slots__ = ("id", "channels")
    class ChannelValues(_message.Message):
        __slots__ = ("id", "values", "flags")
        ID_FIELD_NUMBER: _ClassVar[int]
        VALUES_FIELD_NUMBER: _ClassVar[int]
        FLAGS_FIELD_NUMBER: _ClassVar[int]
        id: int
        values: _ods_pb2.DataMatrix.Column.UnknownArray
        flags: _ods_pb2.LongArray
        def __init__(self, id: _Optional[int] = ..., values: _Optional[_Union[_ods_pb2.DataMatrix.Column.UnknownArray, _Mapping]] = ..., flags: _Optional[_Union[_ods_pb2.LongArray, _Mapping]] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    CHANNELS_FIELD_NUMBER: _ClassVar[int]
    id: int
    channels: _containers.RepeatedCompositeFieldContainer[ValuesResult.ChannelValues]
    def __init__(self, id: _Optional[int] = ..., channels: _Optional[_Iterable[_Union[ValuesResult.ChannelValues, _Mapping]]] = ...) -> None: ...

class ValuesExRequest(_message.Message):
    __slots__ = ("handle", "group_id", "channel_names", "attributes", "start", "limit")
    HANDLE_FIELD_NUMBER: _ClassVar[int]
    GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_NAMES_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    handle: Handle
    group_id: int
    channel_names: _containers.RepeatedScalarFieldContainer[str]
    attributes: _containers.RepeatedScalarFieldContainer[str]
    start: int
    limit: int
    def __init__(self, handle: _Optional[_Union[Handle, _Mapping]] = ..., group_id: _Optional[int] = ..., channel_names: _Optional[_Iterable[str]] = ..., attributes: _Optional[_Iterable[str]] = ..., start: _Optional[int] = ..., limit: _Optional[int] = ...) -> None: ...

class ValuesExResult(_message.Message):
    __slots__ = ("values", "unit_map")
    class UnitMapEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: str
        def __init__(self, key: _Optional[int] = ..., value: _Optional[str] = ...) -> None: ...
    VALUES_FIELD_NUMBER: _ClassVar[int]
    UNIT_MAP_FIELD_NUMBER: _ClassVar[int]
    values: _ods_pb2.DataMatrix
    unit_map: _containers.ScalarMap[int, str]
    def __init__(self, values: _Optional[_Union[_ods_pb2.DataMatrix, _Mapping]] = ..., unit_map: _Optional[_Mapping[int, str]] = ...) -> None: ...
