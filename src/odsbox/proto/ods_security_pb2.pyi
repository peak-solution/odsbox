from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SecurityTypeEnum(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ST_ACL: _ClassVar[SecurityTypeEnum]
    ST_TPLI: _ClassVar[SecurityTypeEnum]
    ST_EFF: _ClassVar[SecurityTypeEnum]

class DataObjectTypeEnum(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DOT_APPLICATION_ELEMENT: _ClassVar[DataObjectTypeEnum]
    DOT_APPLICATION_ATTRIBUTE: _ClassVar[DataObjectTypeEnum]
    DOT_INSTANCE: _ClassVar[DataObjectTypeEnum]
ST_ACL: SecurityTypeEnum
ST_TPLI: SecurityTypeEnum
ST_EFF: SecurityTypeEnum
DOT_APPLICATION_ELEMENT: DataObjectTypeEnum
DOT_APPLICATION_ATTRIBUTE: DataObjectTypeEnum
DOT_INSTANCE: DataObjectTypeEnum

class SecurityReadRequest(_message.Message):
    __slots__ = ("security_request_type", "data_object_type", "application_element", "application_attribute", "instance", "user_group_id")
    class DataObjectApplicationElement(_message.Message):
        __slots__ = ("aid",)
        AID_FIELD_NUMBER: _ClassVar[int]
        aid: int
        def __init__(self, aid: _Optional[int] = ...) -> None: ...
    class DataObjectApplicationAttribute(_message.Message):
        __slots__ = ("aid", "attribute_names")
        AID_FIELD_NUMBER: _ClassVar[int]
        ATTRIBUTE_NAMES_FIELD_NUMBER: _ClassVar[int]
        aid: int
        attribute_names: _containers.RepeatedScalarFieldContainer[str]
        def __init__(self, aid: _Optional[int] = ..., attribute_names: _Optional[_Iterable[str]] = ...) -> None: ...
    class DataObjectInstance(_message.Message):
        __slots__ = ("aid", "iids")
        AID_FIELD_NUMBER: _ClassVar[int]
        IIDS_FIELD_NUMBER: _ClassVar[int]
        aid: int
        iids: _containers.RepeatedScalarFieldContainer[int]
        def __init__(self, aid: _Optional[int] = ..., iids: _Optional[_Iterable[int]] = ...) -> None: ...
    SECURITY_REQUEST_TYPE_FIELD_NUMBER: _ClassVar[int]
    DATA_OBJECT_TYPE_FIELD_NUMBER: _ClassVar[int]
    APPLICATION_ELEMENT_FIELD_NUMBER: _ClassVar[int]
    APPLICATION_ATTRIBUTE_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_FIELD_NUMBER: _ClassVar[int]
    USER_GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    security_request_type: SecurityTypeEnum
    data_object_type: DataObjectTypeEnum
    application_element: SecurityReadRequest.DataObjectApplicationElement
    application_attribute: SecurityReadRequest.DataObjectApplicationAttribute
    instance: SecurityReadRequest.DataObjectInstance
    user_group_id: int
    def __init__(self, security_request_type: _Optional[_Union[SecurityTypeEnum, str]] = ..., data_object_type: _Optional[_Union[DataObjectTypeEnum, str]] = ..., application_element: _Optional[_Union[SecurityReadRequest.DataObjectApplicationElement, _Mapping]] = ..., application_attribute: _Optional[_Union[SecurityReadRequest.DataObjectApplicationAttribute, _Mapping]] = ..., instance: _Optional[_Union[SecurityReadRequest.DataObjectInstance, _Mapping]] = ..., user_group_id: _Optional[int] = ...) -> None: ...

class SecurityEntry(_message.Message):
    __slots__ = ("aid", "aa_name", "iid", "usergroup_id", "rights", "ref_aid")
    AID_FIELD_NUMBER: _ClassVar[int]
    AA_NAME_FIELD_NUMBER: _ClassVar[int]
    IID_FIELD_NUMBER: _ClassVar[int]
    USERGROUP_ID_FIELD_NUMBER: _ClassVar[int]
    RIGHTS_FIELD_NUMBER: _ClassVar[int]
    REF_AID_FIELD_NUMBER: _ClassVar[int]
    aid: int
    aa_name: str
    iid: int
    usergroup_id: int
    rights: int
    ref_aid: int
    def __init__(self, aid: _Optional[int] = ..., aa_name: _Optional[str] = ..., iid: _Optional[int] = ..., usergroup_id: _Optional[int] = ..., rights: _Optional[int] = ..., ref_aid: _Optional[int] = ...) -> None: ...

class SecurityInfo(_message.Message):
    __slots__ = ("security_type", "data_object_type", "entries")
    SECURITY_TYPE_FIELD_NUMBER: _ClassVar[int]
    DATA_OBJECT_TYPE_FIELD_NUMBER: _ClassVar[int]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    security_type: SecurityTypeEnum
    data_object_type: DataObjectTypeEnum
    entries: _containers.RepeatedCompositeFieldContainer[SecurityEntry]
    def __init__(self, security_type: _Optional[_Union[SecurityTypeEnum, str]] = ..., data_object_type: _Optional[_Union[DataObjectTypeEnum, str]] = ..., entries: _Optional[_Iterable[_Union[SecurityEntry, _Mapping]]] = ...) -> None: ...

class SecurityWriteRequest(_message.Message):
    __slots__ = ("security_request_type", "data_object_type", "entries", "modify_type")
    class SecurityModifyTypeEnum(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        SST_ADD: _ClassVar[SecurityWriteRequest.SecurityModifyTypeEnum]
        SST_REMOVE: _ClassVar[SecurityWriteRequest.SecurityModifyTypeEnum]
        SST_SET: _ClassVar[SecurityWriteRequest.SecurityModifyTypeEnum]
    SST_ADD: SecurityWriteRequest.SecurityModifyTypeEnum
    SST_REMOVE: SecurityWriteRequest.SecurityModifyTypeEnum
    SST_SET: SecurityWriteRequest.SecurityModifyTypeEnum
    SECURITY_REQUEST_TYPE_FIELD_NUMBER: _ClassVar[int]
    DATA_OBJECT_TYPE_FIELD_NUMBER: _ClassVar[int]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    MODIFY_TYPE_FIELD_NUMBER: _ClassVar[int]
    security_request_type: SecurityTypeEnum
    data_object_type: DataObjectTypeEnum
    entries: _containers.RepeatedCompositeFieldContainer[SecurityEntry]
    modify_type: SecurityWriteRequest.SecurityModifyTypeEnum
    def __init__(self, security_request_type: _Optional[_Union[SecurityTypeEnum, str]] = ..., data_object_type: _Optional[_Union[DataObjectTypeEnum, str]] = ..., entries: _Optional[_Iterable[_Union[SecurityEntry, _Mapping]]] = ..., modify_type: _Optional[_Union[SecurityWriteRequest.SecurityModifyTypeEnum, str]] = ...) -> None: ...
