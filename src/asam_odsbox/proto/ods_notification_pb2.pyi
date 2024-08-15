from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class NotificationTypeEnum(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    NT_ALL: _ClassVar[NotificationTypeEnum]
    NT_NEW: _ClassVar[NotificationTypeEnum]
    NT_MODIFY: _ClassVar[NotificationTypeEnum]
    NT_DELETE: _ClassVar[NotificationTypeEnum]
    NT_MODEL: _ClassVar[NotificationTypeEnum]
    NT_SECURITY: _ClassVar[NotificationTypeEnum]
NT_ALL: NotificationTypeEnum
NT_NEW: NotificationTypeEnum
NT_MODIFY: NotificationTypeEnum
NT_DELETE: NotificationTypeEnum
NT_MODEL: NotificationTypeEnum
NT_SECURITY: NotificationTypeEnum

class RegistrationRequest(_message.Message):
    __slots__ = ("mode", "entries")
    class NotificationModeEnum(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        NM_POOL: _ClassVar[RegistrationRequest.NotificationModeEnum]
        NM_PUSH: _ClassVar[RegistrationRequest.NotificationModeEnum]
    NM_POOL: RegistrationRequest.NotificationModeEnum
    NM_PUSH: RegistrationRequest.NotificationModeEnum
    class TypeElement(_message.Message):
        __slots__ = ("type", "aids")
        TYPE_FIELD_NUMBER: _ClassVar[int]
        AIDS_FIELD_NUMBER: _ClassVar[int]
        type: NotificationTypeEnum
        aids: _containers.RepeatedScalarFieldContainer[int]
        def __init__(self, type: _Optional[_Union[NotificationTypeEnum, str]] = ..., aids: _Optional[_Iterable[int]] = ...) -> None: ...
    MODE_FIELD_NUMBER: _ClassVar[int]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    mode: RegistrationRequest.NotificationModeEnum
    entries: _containers.RepeatedCompositeFieldContainer[RegistrationRequest.TypeElement]
    def __init__(self, mode: _Optional[_Union[RegistrationRequest.NotificationModeEnum, str]] = ..., entries: _Optional[_Iterable[_Union[RegistrationRequest.TypeElement, _Mapping]]] = ...) -> None: ...

class Notification(_message.Message):
    __slots__ = ("uuid", "type", "aid", "iids", "timestamp", "user_id")
    UUID_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    AID_FIELD_NUMBER: _ClassVar[int]
    IIDS_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    uuid: str
    type: NotificationTypeEnum
    aid: int
    iids: _containers.RepeatedScalarFieldContainer[int]
    timestamp: str
    user_id: int
    def __init__(self, uuid: _Optional[str] = ..., type: _Optional[_Union[NotificationTypeEnum, str]] = ..., aid: _Optional[int] = ..., iids: _Optional[_Iterable[int]] = ..., timestamp: _Optional[str] = ..., user_id: _Optional[int] = ...) -> None: ...

class NotificationPool(_message.Message):
    __slots__ = ("events",)
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    events: _containers.RepeatedCompositeFieldContainer[Notification]
    def __init__(self, events: _Optional[_Iterable[_Union[Notification, _Mapping]]] = ...) -> None: ...
