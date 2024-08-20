from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DataTypeEnum(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DT_UNKNOWN: _ClassVar[DataTypeEnum]
    DT_STRING: _ClassVar[DataTypeEnum]
    DT_SHORT: _ClassVar[DataTypeEnum]
    DT_FLOAT: _ClassVar[DataTypeEnum]
    DT_BOOLEAN: _ClassVar[DataTypeEnum]
    DT_BYTE: _ClassVar[DataTypeEnum]
    DT_LONG: _ClassVar[DataTypeEnum]
    DT_DOUBLE: _ClassVar[DataTypeEnum]
    DT_LONGLONG: _ClassVar[DataTypeEnum]
    DT_DATE: _ClassVar[DataTypeEnum]
    DT_BYTESTR: _ClassVar[DataTypeEnum]
    DT_BLOB: _ClassVar[DataTypeEnum]
    DT_COMPLEX: _ClassVar[DataTypeEnum]
    DT_DCOMPLEX: _ClassVar[DataTypeEnum]
    DS_STRING: _ClassVar[DataTypeEnum]
    DS_SHORT: _ClassVar[DataTypeEnum]
    DS_FLOAT: _ClassVar[DataTypeEnum]
    DS_BOOLEAN: _ClassVar[DataTypeEnum]
    DS_BYTE: _ClassVar[DataTypeEnum]
    DS_LONG: _ClassVar[DataTypeEnum]
    DS_DOUBLE: _ClassVar[DataTypeEnum]
    DS_LONGLONG: _ClassVar[DataTypeEnum]
    DS_COMPLEX: _ClassVar[DataTypeEnum]
    DS_DCOMPLEX: _ClassVar[DataTypeEnum]
    DS_DATE: _ClassVar[DataTypeEnum]
    DS_BYTESTR: _ClassVar[DataTypeEnum]
    DT_EXTERNALREFERENCE: _ClassVar[DataTypeEnum]
    DS_EXTERNALREFERENCE: _ClassVar[DataTypeEnum]
    DT_ENUM: _ClassVar[DataTypeEnum]
    DS_ENUM: _ClassVar[DataTypeEnum]

class AggregateEnum(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    AG_NONE: _ClassVar[AggregateEnum]
    AG_COUNT: _ClassVar[AggregateEnum]
    AG_DCOUNT: _ClassVar[AggregateEnum]
    AG_MIN: _ClassVar[AggregateEnum]
    AG_MAX: _ClassVar[AggregateEnum]
    AG_AVG: _ClassVar[AggregateEnum]
    AG_STDDEV: _ClassVar[AggregateEnum]
    AG_SUM: _ClassVar[AggregateEnum]
    AG_DISTINCT: _ClassVar[AggregateEnum]
    AG_VALUES_POINT: _ClassVar[AggregateEnum]
    AG_INSTANCE_ATTRIBUTE: _ClassVar[AggregateEnum]
DT_UNKNOWN: DataTypeEnum
DT_STRING: DataTypeEnum
DT_SHORT: DataTypeEnum
DT_FLOAT: DataTypeEnum
DT_BOOLEAN: DataTypeEnum
DT_BYTE: DataTypeEnum
DT_LONG: DataTypeEnum
DT_DOUBLE: DataTypeEnum
DT_LONGLONG: DataTypeEnum
DT_DATE: DataTypeEnum
DT_BYTESTR: DataTypeEnum
DT_BLOB: DataTypeEnum
DT_COMPLEX: DataTypeEnum
DT_DCOMPLEX: DataTypeEnum
DS_STRING: DataTypeEnum
DS_SHORT: DataTypeEnum
DS_FLOAT: DataTypeEnum
DS_BOOLEAN: DataTypeEnum
DS_BYTE: DataTypeEnum
DS_LONG: DataTypeEnum
DS_DOUBLE: DataTypeEnum
DS_LONGLONG: DataTypeEnum
DS_COMPLEX: DataTypeEnum
DS_DCOMPLEX: DataTypeEnum
DS_DATE: DataTypeEnum
DS_BYTESTR: DataTypeEnum
DT_EXTERNALREFERENCE: DataTypeEnum
DS_EXTERNALREFERENCE: DataTypeEnum
DT_ENUM: DataTypeEnum
DS_ENUM: DataTypeEnum
AG_NONE: AggregateEnum
AG_COUNT: AggregateEnum
AG_DCOUNT: AggregateEnum
AG_MIN: AggregateEnum
AG_MAX: AggregateEnum
AG_AVG: AggregateEnum
AG_STDDEV: AggregateEnum
AG_SUM: AggregateEnum
AG_DISTINCT: AggregateEnum
AG_VALUES_POINT: AggregateEnum
AG_INSTANCE_ATTRIBUTE: AggregateEnum

class StringArray(_message.Message):
    __slots__ = ("values",)
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, values: _Optional[_Iterable[str]] = ...) -> None: ...

class LongArray(_message.Message):
    __slots__ = ("values",)
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, values: _Optional[_Iterable[int]] = ...) -> None: ...

class FloatArray(_message.Message):
    __slots__ = ("values",)
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedScalarFieldContainer[float]
    def __init__(self, values: _Optional[_Iterable[float]] = ...) -> None: ...

class BooleanArray(_message.Message):
    __slots__ = ("values",)
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedScalarFieldContainer[bool]
    def __init__(self, values: _Optional[_Iterable[bool]] = ...) -> None: ...

class ByteArray(_message.Message):
    __slots__ = ("values",)
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: bytes
    def __init__(self, values: _Optional[bytes] = ...) -> None: ...

class DoubleArray(_message.Message):
    __slots__ = ("values",)
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedScalarFieldContainer[float]
    def __init__(self, values: _Optional[_Iterable[float]] = ...) -> None: ...

class LonglongArray(_message.Message):
    __slots__ = ("values",)
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, values: _Optional[_Iterable[int]] = ...) -> None: ...

class BytestrArray(_message.Message):
    __slots__ = ("values",)
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, values: _Optional[_Iterable[bytes]] = ...) -> None: ...

class DataMatrix(_message.Message):
    __slots__ = ("name", "base_name", "aid", "columns", "row_start", "values_start", "values_update_mode", "values_remove_length")
    class UpdateModeEnum(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        VU_APPEND: _ClassVar[DataMatrix.UpdateModeEnum]
        VU_UPDATE: _ClassVar[DataMatrix.UpdateModeEnum]
        VU_INSERT: _ClassVar[DataMatrix.UpdateModeEnum]
        VU_REMOVE: _ClassVar[DataMatrix.UpdateModeEnum]
    VU_APPEND: DataMatrix.UpdateModeEnum
    VU_UPDATE: DataMatrix.UpdateModeEnum
    VU_INSERT: DataMatrix.UpdateModeEnum
    VU_REMOVE: DataMatrix.UpdateModeEnum
    class Column(_message.Message):
        __slots__ = ("name", "base_name", "unit_id", "aggregate", "data_type", "is_null", "string_array", "long_array", "float_array", "boolean_array", "byte_array", "double_array", "longlong_array", "bytestr_array", "string_arrays", "long_arrays", "float_arrays", "boolean_arrays", "byte_arrays", "double_arrays", "longlong_arrays", "bytestr_arrays", "unknown_arrays")
        class UnknownArray(_message.Message):
            __slots__ = ("data_type", "unit_id", "string_array", "long_array", "float_array", "boolean_array", "byte_array", "double_array", "longlong_array", "bytestr_array")
            DATA_TYPE_FIELD_NUMBER: _ClassVar[int]
            UNIT_ID_FIELD_NUMBER: _ClassVar[int]
            STRING_ARRAY_FIELD_NUMBER: _ClassVar[int]
            LONG_ARRAY_FIELD_NUMBER: _ClassVar[int]
            FLOAT_ARRAY_FIELD_NUMBER: _ClassVar[int]
            BOOLEAN_ARRAY_FIELD_NUMBER: _ClassVar[int]
            BYTE_ARRAY_FIELD_NUMBER: _ClassVar[int]
            DOUBLE_ARRAY_FIELD_NUMBER: _ClassVar[int]
            LONGLONG_ARRAY_FIELD_NUMBER: _ClassVar[int]
            BYTESTR_ARRAY_FIELD_NUMBER: _ClassVar[int]
            data_type: DataTypeEnum
            unit_id: int
            string_array: StringArray
            long_array: LongArray
            float_array: FloatArray
            boolean_array: BooleanArray
            byte_array: ByteArray
            double_array: DoubleArray
            longlong_array: LonglongArray
            bytestr_array: BytestrArray
            def __init__(self, data_type: _Optional[_Union[DataTypeEnum, str]] = ..., unit_id: _Optional[int] = ..., string_array: _Optional[_Union[StringArray, _Mapping]] = ..., long_array: _Optional[_Union[LongArray, _Mapping]] = ..., float_array: _Optional[_Union[FloatArray, _Mapping]] = ..., boolean_array: _Optional[_Union[BooleanArray, _Mapping]] = ..., byte_array: _Optional[_Union[ByteArray, _Mapping]] = ..., double_array: _Optional[_Union[DoubleArray, _Mapping]] = ..., longlong_array: _Optional[_Union[LonglongArray, _Mapping]] = ..., bytestr_array: _Optional[_Union[BytestrArray, _Mapping]] = ...) -> None: ...
        class StringArrays(_message.Message):
            __slots__ = ("values",)
            VALUES_FIELD_NUMBER: _ClassVar[int]
            values: _containers.RepeatedCompositeFieldContainer[StringArray]
            def __init__(self, values: _Optional[_Iterable[_Union[StringArray, _Mapping]]] = ...) -> None: ...
        class LongArrays(_message.Message):
            __slots__ = ("values",)
            VALUES_FIELD_NUMBER: _ClassVar[int]
            values: _containers.RepeatedCompositeFieldContainer[LongArray]
            def __init__(self, values: _Optional[_Iterable[_Union[LongArray, _Mapping]]] = ...) -> None: ...
        class FloatArrays(_message.Message):
            __slots__ = ("values",)
            VALUES_FIELD_NUMBER: _ClassVar[int]
            values: _containers.RepeatedCompositeFieldContainer[FloatArray]
            def __init__(self, values: _Optional[_Iterable[_Union[FloatArray, _Mapping]]] = ...) -> None: ...
        class BooleanArrays(_message.Message):
            __slots__ = ("values",)
            VALUES_FIELD_NUMBER: _ClassVar[int]
            values: _containers.RepeatedCompositeFieldContainer[BooleanArray]
            def __init__(self, values: _Optional[_Iterable[_Union[BooleanArray, _Mapping]]] = ...) -> None: ...
        class ByteArrays(_message.Message):
            __slots__ = ("values",)
            VALUES_FIELD_NUMBER: _ClassVar[int]
            values: _containers.RepeatedCompositeFieldContainer[ByteArray]
            def __init__(self, values: _Optional[_Iterable[_Union[ByteArray, _Mapping]]] = ...) -> None: ...
        class DoubleArrays(_message.Message):
            __slots__ = ("values",)
            VALUES_FIELD_NUMBER: _ClassVar[int]
            values: _containers.RepeatedCompositeFieldContainer[DoubleArray]
            def __init__(self, values: _Optional[_Iterable[_Union[DoubleArray, _Mapping]]] = ...) -> None: ...
        class LonglongArrays(_message.Message):
            __slots__ = ("values",)
            VALUES_FIELD_NUMBER: _ClassVar[int]
            values: _containers.RepeatedCompositeFieldContainer[LonglongArray]
            def __init__(self, values: _Optional[_Iterable[_Union[LonglongArray, _Mapping]]] = ...) -> None: ...
        class BytestrArrays(_message.Message):
            __slots__ = ("values",)
            VALUES_FIELD_NUMBER: _ClassVar[int]
            values: _containers.RepeatedCompositeFieldContainer[BytestrArray]
            def __init__(self, values: _Optional[_Iterable[_Union[BytestrArray, _Mapping]]] = ...) -> None: ...
        class UnknownArrays(_message.Message):
            __slots__ = ("values",)
            VALUES_FIELD_NUMBER: _ClassVar[int]
            values: _containers.RepeatedCompositeFieldContainer[DataMatrix.Column.UnknownArray]
            def __init__(self, values: _Optional[_Iterable[_Union[DataMatrix.Column.UnknownArray, _Mapping]]] = ...) -> None: ...
        NAME_FIELD_NUMBER: _ClassVar[int]
        BASE_NAME_FIELD_NUMBER: _ClassVar[int]
        UNIT_ID_FIELD_NUMBER: _ClassVar[int]
        AGGREGATE_FIELD_NUMBER: _ClassVar[int]
        DATA_TYPE_FIELD_NUMBER: _ClassVar[int]
        IS_NULL_FIELD_NUMBER: _ClassVar[int]
        STRING_ARRAY_FIELD_NUMBER: _ClassVar[int]
        LONG_ARRAY_FIELD_NUMBER: _ClassVar[int]
        FLOAT_ARRAY_FIELD_NUMBER: _ClassVar[int]
        BOOLEAN_ARRAY_FIELD_NUMBER: _ClassVar[int]
        BYTE_ARRAY_FIELD_NUMBER: _ClassVar[int]
        DOUBLE_ARRAY_FIELD_NUMBER: _ClassVar[int]
        LONGLONG_ARRAY_FIELD_NUMBER: _ClassVar[int]
        BYTESTR_ARRAY_FIELD_NUMBER: _ClassVar[int]
        STRING_ARRAYS_FIELD_NUMBER: _ClassVar[int]
        LONG_ARRAYS_FIELD_NUMBER: _ClassVar[int]
        FLOAT_ARRAYS_FIELD_NUMBER: _ClassVar[int]
        BOOLEAN_ARRAYS_FIELD_NUMBER: _ClassVar[int]
        BYTE_ARRAYS_FIELD_NUMBER: _ClassVar[int]
        DOUBLE_ARRAYS_FIELD_NUMBER: _ClassVar[int]
        LONGLONG_ARRAYS_FIELD_NUMBER: _ClassVar[int]
        BYTESTR_ARRAYS_FIELD_NUMBER: _ClassVar[int]
        UNKNOWN_ARRAYS_FIELD_NUMBER: _ClassVar[int]
        name: str
        base_name: str
        unit_id: int
        aggregate: AggregateEnum
        data_type: DataTypeEnum
        is_null: _containers.RepeatedScalarFieldContainer[bool]
        string_array: StringArray
        long_array: LongArray
        float_array: FloatArray
        boolean_array: BooleanArray
        byte_array: ByteArray
        double_array: DoubleArray
        longlong_array: LonglongArray
        bytestr_array: BytestrArray
        string_arrays: DataMatrix.Column.StringArrays
        long_arrays: DataMatrix.Column.LongArrays
        float_arrays: DataMatrix.Column.FloatArrays
        boolean_arrays: DataMatrix.Column.BooleanArrays
        byte_arrays: DataMatrix.Column.ByteArrays
        double_arrays: DataMatrix.Column.DoubleArrays
        longlong_arrays: DataMatrix.Column.LonglongArrays
        bytestr_arrays: DataMatrix.Column.BytestrArrays
        unknown_arrays: DataMatrix.Column.UnknownArrays
        def __init__(self, name: _Optional[str] = ..., base_name: _Optional[str] = ..., unit_id: _Optional[int] = ..., aggregate: _Optional[_Union[AggregateEnum, str]] = ..., data_type: _Optional[_Union[DataTypeEnum, str]] = ..., is_null: _Optional[_Iterable[bool]] = ..., string_array: _Optional[_Union[StringArray, _Mapping]] = ..., long_array: _Optional[_Union[LongArray, _Mapping]] = ..., float_array: _Optional[_Union[FloatArray, _Mapping]] = ..., boolean_array: _Optional[_Union[BooleanArray, _Mapping]] = ..., byte_array: _Optional[_Union[ByteArray, _Mapping]] = ..., double_array: _Optional[_Union[DoubleArray, _Mapping]] = ..., longlong_array: _Optional[_Union[LonglongArray, _Mapping]] = ..., bytestr_array: _Optional[_Union[BytestrArray, _Mapping]] = ..., string_arrays: _Optional[_Union[DataMatrix.Column.StringArrays, _Mapping]] = ..., long_arrays: _Optional[_Union[DataMatrix.Column.LongArrays, _Mapping]] = ..., float_arrays: _Optional[_Union[DataMatrix.Column.FloatArrays, _Mapping]] = ..., boolean_arrays: _Optional[_Union[DataMatrix.Column.BooleanArrays, _Mapping]] = ..., byte_arrays: _Optional[_Union[DataMatrix.Column.ByteArrays, _Mapping]] = ..., double_arrays: _Optional[_Union[DataMatrix.Column.DoubleArrays, _Mapping]] = ..., longlong_arrays: _Optional[_Union[DataMatrix.Column.LonglongArrays, _Mapping]] = ..., bytestr_arrays: _Optional[_Union[DataMatrix.Column.BytestrArrays, _Mapping]] = ..., unknown_arrays: _Optional[_Union[DataMatrix.Column.UnknownArrays, _Mapping]] = ...) -> None: ...
    NAME_FIELD_NUMBER: _ClassVar[int]
    BASE_NAME_FIELD_NUMBER: _ClassVar[int]
    AID_FIELD_NUMBER: _ClassVar[int]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    ROW_START_FIELD_NUMBER: _ClassVar[int]
    VALUES_START_FIELD_NUMBER: _ClassVar[int]
    VALUES_UPDATE_MODE_FIELD_NUMBER: _ClassVar[int]
    VALUES_REMOVE_LENGTH_FIELD_NUMBER: _ClassVar[int]
    name: str
    base_name: str
    aid: int
    columns: _containers.RepeatedCompositeFieldContainer[DataMatrix.Column]
    row_start: int
    values_start: int
    values_update_mode: DataMatrix.UpdateModeEnum
    values_remove_length: int
    def __init__(self, name: _Optional[str] = ..., base_name: _Optional[str] = ..., aid: _Optional[int] = ..., columns: _Optional[_Iterable[_Union[DataMatrix.Column, _Mapping]]] = ..., row_start: _Optional[int] = ..., values_start: _Optional[int] = ..., values_update_mode: _Optional[_Union[DataMatrix.UpdateModeEnum, str]] = ..., values_remove_length: _Optional[int] = ...) -> None: ...

class DataMatrices(_message.Message):
    __slots__ = ("matrices", "partial_result")
    MATRICES_FIELD_NUMBER: _ClassVar[int]
    PARTIAL_RESULT_FIELD_NUMBER: _ClassVar[int]
    matrices: _containers.RepeatedCompositeFieldContainer[DataMatrix]
    partial_result: bool
    def __init__(self, matrices: _Optional[_Iterable[_Union[DataMatrix, _Mapping]]] = ..., partial_result: bool = ...) -> None: ...

class Model(_message.Message):
    __slots__ = ("enumerations", "entities")
    class RelationTypeEnum(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        RT_FATHER_CHILD: _ClassVar[Model.RelationTypeEnum]
        RT_INFO: _ClassVar[Model.RelationTypeEnum]
        RT_INHERITANCE: _ClassVar[Model.RelationTypeEnum]
    RT_FATHER_CHILD: Model.RelationTypeEnum
    RT_INFO: Model.RelationTypeEnum
    RT_INHERITANCE: Model.RelationTypeEnum
    class RelationshipEnum(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        RS_FATHER: _ClassVar[Model.RelationshipEnum]
        RS_CHILD: _ClassVar[Model.RelationshipEnum]
        RS_INFO_TO: _ClassVar[Model.RelationshipEnum]
        RS_INFO_FROM: _ClassVar[Model.RelationshipEnum]
        RS_INFO_REL: _ClassVar[Model.RelationshipEnum]
        RS_SUPERTYPE: _ClassVar[Model.RelationshipEnum]
        RS_SUBTYPE: _ClassVar[Model.RelationshipEnum]
        RS_ALL_REL: _ClassVar[Model.RelationshipEnum]
    RS_FATHER: Model.RelationshipEnum
    RS_CHILD: Model.RelationshipEnum
    RS_INFO_TO: Model.RelationshipEnum
    RS_INFO_FROM: Model.RelationshipEnum
    RS_INFO_REL: Model.RelationshipEnum
    RS_SUPERTYPE: Model.RelationshipEnum
    RS_SUBTYPE: Model.RelationshipEnum
    RS_ALL_REL: Model.RelationshipEnum
    class Enumeration(_message.Message):
        __slots__ = ("name", "items")
        class ItemsEntry(_message.Message):
            __slots__ = ("key", "value")
            KEY_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            key: str
            value: int
            def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
        NAME_FIELD_NUMBER: _ClassVar[int]
        ITEMS_FIELD_NUMBER: _ClassVar[int]
        name: str
        items: _containers.ScalarMap[str, int]
        def __init__(self, name: _Optional[str] = ..., items: _Optional[_Mapping[str, int]] = ...) -> None: ...
    class Attribute(_message.Message):
        __slots__ = ("name", "base_name", "data_type", "length", "obligatory", "unique", "autogenerated", "unit_id", "enumeration", "id")
        NAME_FIELD_NUMBER: _ClassVar[int]
        BASE_NAME_FIELD_NUMBER: _ClassVar[int]
        DATA_TYPE_FIELD_NUMBER: _ClassVar[int]
        LENGTH_FIELD_NUMBER: _ClassVar[int]
        OBLIGATORY_FIELD_NUMBER: _ClassVar[int]
        UNIQUE_FIELD_NUMBER: _ClassVar[int]
        AUTOGENERATED_FIELD_NUMBER: _ClassVar[int]
        UNIT_ID_FIELD_NUMBER: _ClassVar[int]
        ENUMERATION_FIELD_NUMBER: _ClassVar[int]
        ID_FIELD_NUMBER: _ClassVar[int]
        name: str
        base_name: str
        data_type: DataTypeEnum
        length: int
        obligatory: bool
        unique: bool
        autogenerated: bool
        unit_id: int
        enumeration: str
        id: int
        def __init__(self, name: _Optional[str] = ..., base_name: _Optional[str] = ..., data_type: _Optional[_Union[DataTypeEnum, str]] = ..., length: _Optional[int] = ..., obligatory: bool = ..., unique: bool = ..., autogenerated: bool = ..., unit_id: _Optional[int] = ..., enumeration: _Optional[str] = ..., id: _Optional[int] = ...) -> None: ...
    class Relation(_message.Message):
        __slots__ = ("name", "base_name", "inverse_name", "inverse_base_name", "entity_name", "entity_base_name", "entity_aid", "virtual_reference", "acl_reference", "range_min", "range_max", "inverse_range_min", "inverse_range_max", "relation_type", "relationship")
        NAME_FIELD_NUMBER: _ClassVar[int]
        BASE_NAME_FIELD_NUMBER: _ClassVar[int]
        INVERSE_NAME_FIELD_NUMBER: _ClassVar[int]
        INVERSE_BASE_NAME_FIELD_NUMBER: _ClassVar[int]
        ENTITY_NAME_FIELD_NUMBER: _ClassVar[int]
        ENTITY_BASE_NAME_FIELD_NUMBER: _ClassVar[int]
        ENTITY_AID_FIELD_NUMBER: _ClassVar[int]
        VIRTUAL_REFERENCE_FIELD_NUMBER: _ClassVar[int]
        ACL_REFERENCE_FIELD_NUMBER: _ClassVar[int]
        RANGE_MIN_FIELD_NUMBER: _ClassVar[int]
        RANGE_MAX_FIELD_NUMBER: _ClassVar[int]
        INVERSE_RANGE_MIN_FIELD_NUMBER: _ClassVar[int]
        INVERSE_RANGE_MAX_FIELD_NUMBER: _ClassVar[int]
        RELATION_TYPE_FIELD_NUMBER: _ClassVar[int]
        RELATIONSHIP_FIELD_NUMBER: _ClassVar[int]
        name: str
        base_name: str
        inverse_name: str
        inverse_base_name: str
        entity_name: str
        entity_base_name: str
        entity_aid: int
        virtual_reference: bool
        acl_reference: bool
        range_min: int
        range_max: int
        inverse_range_min: int
        inverse_range_max: int
        relation_type: Model.RelationTypeEnum
        relationship: Model.RelationshipEnum
        def __init__(self, name: _Optional[str] = ..., base_name: _Optional[str] = ..., inverse_name: _Optional[str] = ..., inverse_base_name: _Optional[str] = ..., entity_name: _Optional[str] = ..., entity_base_name: _Optional[str] = ..., entity_aid: _Optional[int] = ..., virtual_reference: bool = ..., acl_reference: bool = ..., range_min: _Optional[int] = ..., range_max: _Optional[int] = ..., inverse_range_min: _Optional[int] = ..., inverse_range_max: _Optional[int] = ..., relation_type: _Optional[_Union[Model.RelationTypeEnum, str]] = ..., relationship: _Optional[_Union[Model.RelationshipEnum, str]] = ...) -> None: ...
    class Entity(_message.Message):
        __slots__ = ("name", "base_name", "aid", "security_level", "attributes", "relations")
        class AttributesEntry(_message.Message):
            __slots__ = ("key", "value")
            KEY_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            key: str
            value: Model.Attribute
            def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[Model.Attribute, _Mapping]] = ...) -> None: ...
        class RelationsEntry(_message.Message):
            __slots__ = ("key", "value")
            KEY_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            key: str
            value: Model.Relation
            def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[Model.Relation, _Mapping]] = ...) -> None: ...
        NAME_FIELD_NUMBER: _ClassVar[int]
        BASE_NAME_FIELD_NUMBER: _ClassVar[int]
        AID_FIELD_NUMBER: _ClassVar[int]
        SECURITY_LEVEL_FIELD_NUMBER: _ClassVar[int]
        ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
        RELATIONS_FIELD_NUMBER: _ClassVar[int]
        name: str
        base_name: str
        aid: int
        security_level: int
        attributes: _containers.MessageMap[str, Model.Attribute]
        relations: _containers.MessageMap[str, Model.Relation]
        def __init__(self, name: _Optional[str] = ..., base_name: _Optional[str] = ..., aid: _Optional[int] = ..., security_level: _Optional[int] = ..., attributes: _Optional[_Mapping[str, Model.Attribute]] = ..., relations: _Optional[_Mapping[str, Model.Relation]] = ...) -> None: ...
    class EnumerationsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: Model.Enumeration
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[Model.Enumeration, _Mapping]] = ...) -> None: ...
    class EntitiesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: Model.Entity
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[Model.Entity, _Mapping]] = ...) -> None: ...
    ENUMERATIONS_FIELD_NUMBER: _ClassVar[int]
    ENTITIES_FIELD_NUMBER: _ClassVar[int]
    enumerations: _containers.MessageMap[str, Model.Enumeration]
    entities: _containers.MessageMap[str, Model.Entity]
    def __init__(self, enumerations: _Optional[_Mapping[str, Model.Enumeration]] = ..., entities: _Optional[_Mapping[str, Model.Entity]] = ...) -> None: ...

class SelectStatement(_message.Message):
    __slots__ = ("columns", "where", "joins", "order_by", "group_by", "row_limit", "row_start", "values_limit", "values_start")
    class ConditionItem(_message.Message):
        __slots__ = ("conjunction", "condition")
        class ConjuctionEnum(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            CO_AND: _ClassVar[SelectStatement.ConditionItem.ConjuctionEnum]
            CO_OR: _ClassVar[SelectStatement.ConditionItem.ConjuctionEnum]
            CO_NOT: _ClassVar[SelectStatement.ConditionItem.ConjuctionEnum]
            CO_OPEN: _ClassVar[SelectStatement.ConditionItem.ConjuctionEnum]
            CO_CLOSE: _ClassVar[SelectStatement.ConditionItem.ConjuctionEnum]
        CO_AND: SelectStatement.ConditionItem.ConjuctionEnum
        CO_OR: SelectStatement.ConditionItem.ConjuctionEnum
        CO_NOT: SelectStatement.ConditionItem.ConjuctionEnum
        CO_OPEN: SelectStatement.ConditionItem.ConjuctionEnum
        CO_CLOSE: SelectStatement.ConditionItem.ConjuctionEnum
        class Condition(_message.Message):
            __slots__ = ("aid", "attribute", "unit_id", "operator", "string_array", "long_array", "float_array", "boolean_array", "byte_array", "double_array", "longlong_array", "nested_statement")
            class OperatorEnum(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
                __slots__ = ()
                OP_EQ: _ClassVar[SelectStatement.ConditionItem.Condition.OperatorEnum]
                OP_NEQ: _ClassVar[SelectStatement.ConditionItem.Condition.OperatorEnum]
                OP_LT: _ClassVar[SelectStatement.ConditionItem.Condition.OperatorEnum]
                OP_GT: _ClassVar[SelectStatement.ConditionItem.Condition.OperatorEnum]
                OP_LTE: _ClassVar[SelectStatement.ConditionItem.Condition.OperatorEnum]
                OP_GTE: _ClassVar[SelectStatement.ConditionItem.Condition.OperatorEnum]
                OP_INSET: _ClassVar[SelectStatement.ConditionItem.Condition.OperatorEnum]
                OP_NOTINSET: _ClassVar[SelectStatement.ConditionItem.Condition.OperatorEnum]
                OP_LIKE: _ClassVar[SelectStatement.ConditionItem.Condition.OperatorEnum]
                OP_CI_EQ: _ClassVar[SelectStatement.ConditionItem.Condition.OperatorEnum]
                OP_CI_NEQ: _ClassVar[SelectStatement.ConditionItem.Condition.OperatorEnum]
                OP_CI_LT: _ClassVar[SelectStatement.ConditionItem.Condition.OperatorEnum]
                OP_CI_GT: _ClassVar[SelectStatement.ConditionItem.Condition.OperatorEnum]
                OP_CI_LTE: _ClassVar[SelectStatement.ConditionItem.Condition.OperatorEnum]
                OP_CI_GTE: _ClassVar[SelectStatement.ConditionItem.Condition.OperatorEnum]
                OP_CI_INSET: _ClassVar[SelectStatement.ConditionItem.Condition.OperatorEnum]
                OP_CI_NOTINSET: _ClassVar[SelectStatement.ConditionItem.Condition.OperatorEnum]
                OP_CI_LIKE: _ClassVar[SelectStatement.ConditionItem.Condition.OperatorEnum]
                OP_IS_NULL: _ClassVar[SelectStatement.ConditionItem.Condition.OperatorEnum]
                OP_IS_NOT_NULL: _ClassVar[SelectStatement.ConditionItem.Condition.OperatorEnum]
                OP_NOTLIKE: _ClassVar[SelectStatement.ConditionItem.Condition.OperatorEnum]
                OP_CI_NOTLIKE: _ClassVar[SelectStatement.ConditionItem.Condition.OperatorEnum]
                OP_BETWEEN: _ClassVar[SelectStatement.ConditionItem.Condition.OperatorEnum]
            OP_EQ: SelectStatement.ConditionItem.Condition.OperatorEnum
            OP_NEQ: SelectStatement.ConditionItem.Condition.OperatorEnum
            OP_LT: SelectStatement.ConditionItem.Condition.OperatorEnum
            OP_GT: SelectStatement.ConditionItem.Condition.OperatorEnum
            OP_LTE: SelectStatement.ConditionItem.Condition.OperatorEnum
            OP_GTE: SelectStatement.ConditionItem.Condition.OperatorEnum
            OP_INSET: SelectStatement.ConditionItem.Condition.OperatorEnum
            OP_NOTINSET: SelectStatement.ConditionItem.Condition.OperatorEnum
            OP_LIKE: SelectStatement.ConditionItem.Condition.OperatorEnum
            OP_CI_EQ: SelectStatement.ConditionItem.Condition.OperatorEnum
            OP_CI_NEQ: SelectStatement.ConditionItem.Condition.OperatorEnum
            OP_CI_LT: SelectStatement.ConditionItem.Condition.OperatorEnum
            OP_CI_GT: SelectStatement.ConditionItem.Condition.OperatorEnum
            OP_CI_LTE: SelectStatement.ConditionItem.Condition.OperatorEnum
            OP_CI_GTE: SelectStatement.ConditionItem.Condition.OperatorEnum
            OP_CI_INSET: SelectStatement.ConditionItem.Condition.OperatorEnum
            OP_CI_NOTINSET: SelectStatement.ConditionItem.Condition.OperatorEnum
            OP_CI_LIKE: SelectStatement.ConditionItem.Condition.OperatorEnum
            OP_IS_NULL: SelectStatement.ConditionItem.Condition.OperatorEnum
            OP_IS_NOT_NULL: SelectStatement.ConditionItem.Condition.OperatorEnum
            OP_NOTLIKE: SelectStatement.ConditionItem.Condition.OperatorEnum
            OP_CI_NOTLIKE: SelectStatement.ConditionItem.Condition.OperatorEnum
            OP_BETWEEN: SelectStatement.ConditionItem.Condition.OperatorEnum
            AID_FIELD_NUMBER: _ClassVar[int]
            ATTRIBUTE_FIELD_NUMBER: _ClassVar[int]
            UNIT_ID_FIELD_NUMBER: _ClassVar[int]
            OPERATOR_FIELD_NUMBER: _ClassVar[int]
            STRING_ARRAY_FIELD_NUMBER: _ClassVar[int]
            LONG_ARRAY_FIELD_NUMBER: _ClassVar[int]
            FLOAT_ARRAY_FIELD_NUMBER: _ClassVar[int]
            BOOLEAN_ARRAY_FIELD_NUMBER: _ClassVar[int]
            BYTE_ARRAY_FIELD_NUMBER: _ClassVar[int]
            DOUBLE_ARRAY_FIELD_NUMBER: _ClassVar[int]
            LONGLONG_ARRAY_FIELD_NUMBER: _ClassVar[int]
            NESTED_STATEMENT_FIELD_NUMBER: _ClassVar[int]
            aid: int
            attribute: str
            unit_id: int
            operator: SelectStatement.ConditionItem.Condition.OperatorEnum
            string_array: StringArray
            long_array: LongArray
            float_array: FloatArray
            boolean_array: BooleanArray
            byte_array: ByteArray
            double_array: DoubleArray
            longlong_array: LonglongArray
            nested_statement: SelectStatement
            def __init__(self, aid: _Optional[int] = ..., attribute: _Optional[str] = ..., unit_id: _Optional[int] = ..., operator: _Optional[_Union[SelectStatement.ConditionItem.Condition.OperatorEnum, str]] = ..., string_array: _Optional[_Union[StringArray, _Mapping]] = ..., long_array: _Optional[_Union[LongArray, _Mapping]] = ..., float_array: _Optional[_Union[FloatArray, _Mapping]] = ..., boolean_array: _Optional[_Union[BooleanArray, _Mapping]] = ..., byte_array: _Optional[_Union[ByteArray, _Mapping]] = ..., double_array: _Optional[_Union[DoubleArray, _Mapping]] = ..., longlong_array: _Optional[_Union[LonglongArray, _Mapping]] = ..., nested_statement: _Optional[_Union[SelectStatement, _Mapping]] = ...) -> None: ...
        CONJUNCTION_FIELD_NUMBER: _ClassVar[int]
        CONDITION_FIELD_NUMBER: _ClassVar[int]
        conjunction: SelectStatement.ConditionItem.ConjuctionEnum
        condition: SelectStatement.ConditionItem.Condition
        def __init__(self, conjunction: _Optional[_Union[SelectStatement.ConditionItem.ConjuctionEnum, str]] = ..., condition: _Optional[_Union[SelectStatement.ConditionItem.Condition, _Mapping]] = ...) -> None: ...
    class OrderByItem(_message.Message):
        __slots__ = ("aid", "attribute", "order")
        class OrderEnum(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            OD_ASCENDING: _ClassVar[SelectStatement.OrderByItem.OrderEnum]
            OD_DESCENDING: _ClassVar[SelectStatement.OrderByItem.OrderEnum]
        OD_ASCENDING: SelectStatement.OrderByItem.OrderEnum
        OD_DESCENDING: SelectStatement.OrderByItem.OrderEnum
        AID_FIELD_NUMBER: _ClassVar[int]
        ATTRIBUTE_FIELD_NUMBER: _ClassVar[int]
        ORDER_FIELD_NUMBER: _ClassVar[int]
        aid: int
        attribute: str
        order: SelectStatement.OrderByItem.OrderEnum
        def __init__(self, aid: _Optional[int] = ..., attribute: _Optional[str] = ..., order: _Optional[_Union[SelectStatement.OrderByItem.OrderEnum, str]] = ...) -> None: ...
    class GroupByItem(_message.Message):
        __slots__ = ("aid", "attribute")
        AID_FIELD_NUMBER: _ClassVar[int]
        ATTRIBUTE_FIELD_NUMBER: _ClassVar[int]
        aid: int
        attribute: str
        def __init__(self, aid: _Optional[int] = ..., attribute: _Optional[str] = ...) -> None: ...
    class AttributeItem(_message.Message):
        __slots__ = ("aid", "attribute", "aggregate", "unit_id")
        AID_FIELD_NUMBER: _ClassVar[int]
        ATTRIBUTE_FIELD_NUMBER: _ClassVar[int]
        AGGREGATE_FIELD_NUMBER: _ClassVar[int]
        UNIT_ID_FIELD_NUMBER: _ClassVar[int]
        aid: int
        attribute: str
        aggregate: AggregateEnum
        unit_id: int
        def __init__(self, aid: _Optional[int] = ..., attribute: _Optional[str] = ..., aggregate: _Optional[_Union[AggregateEnum, str]] = ..., unit_id: _Optional[int] = ...) -> None: ...
    class JoinItem(_message.Message):
        __slots__ = ("aid_from", "aid_to", "relation", "join_type")
        class JoinTypeEnum(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            JT_DEFAULT: _ClassVar[SelectStatement.JoinItem.JoinTypeEnum]
            JT_OUTER: _ClassVar[SelectStatement.JoinItem.JoinTypeEnum]
        JT_DEFAULT: SelectStatement.JoinItem.JoinTypeEnum
        JT_OUTER: SelectStatement.JoinItem.JoinTypeEnum
        AID_FROM_FIELD_NUMBER: _ClassVar[int]
        AID_TO_FIELD_NUMBER: _ClassVar[int]
        RELATION_FIELD_NUMBER: _ClassVar[int]
        JOIN_TYPE_FIELD_NUMBER: _ClassVar[int]
        aid_from: int
        aid_to: int
        relation: str
        join_type: SelectStatement.JoinItem.JoinTypeEnum
        def __init__(self, aid_from: _Optional[int] = ..., aid_to: _Optional[int] = ..., relation: _Optional[str] = ..., join_type: _Optional[_Union[SelectStatement.JoinItem.JoinTypeEnum, str]] = ...) -> None: ...
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    WHERE_FIELD_NUMBER: _ClassVar[int]
    JOINS_FIELD_NUMBER: _ClassVar[int]
    ORDER_BY_FIELD_NUMBER: _ClassVar[int]
    GROUP_BY_FIELD_NUMBER: _ClassVar[int]
    ROW_LIMIT_FIELD_NUMBER: _ClassVar[int]
    ROW_START_FIELD_NUMBER: _ClassVar[int]
    VALUES_LIMIT_FIELD_NUMBER: _ClassVar[int]
    VALUES_START_FIELD_NUMBER: _ClassVar[int]
    columns: _containers.RepeatedCompositeFieldContainer[SelectStatement.AttributeItem]
    where: _containers.RepeatedCompositeFieldContainer[SelectStatement.ConditionItem]
    joins: _containers.RepeatedCompositeFieldContainer[SelectStatement.JoinItem]
    order_by: _containers.RepeatedCompositeFieldContainer[SelectStatement.OrderByItem]
    group_by: _containers.RepeatedCompositeFieldContainer[SelectStatement.GroupByItem]
    row_limit: int
    row_start: int
    values_limit: int
    values_start: int
    def __init__(self, columns: _Optional[_Iterable[_Union[SelectStatement.AttributeItem, _Mapping]]] = ..., where: _Optional[_Iterable[_Union[SelectStatement.ConditionItem, _Mapping]]] = ..., joins: _Optional[_Iterable[_Union[SelectStatement.JoinItem, _Mapping]]] = ..., order_by: _Optional[_Iterable[_Union[SelectStatement.OrderByItem, _Mapping]]] = ..., group_by: _Optional[_Iterable[_Union[SelectStatement.GroupByItem, _Mapping]]] = ..., row_limit: _Optional[int] = ..., row_start: _Optional[int] = ..., values_limit: _Optional[int] = ..., values_start: _Optional[int] = ...) -> None: ...

class ValueMatrixRequestStruct(_message.Message):
    __slots__ = ("mode", "aid", "iid", "attributes", "columns", "values_limit", "values_start", "exd_group_ids")
    class ModeEnum(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        MO_CALCULATED: _ClassVar[ValueMatrixRequestStruct.ModeEnum]
        MO_STORAGE: _ClassVar[ValueMatrixRequestStruct.ModeEnum]
        MO_EXD_VIRTUAL_DATA: _ClassVar[ValueMatrixRequestStruct.ModeEnum]
    MO_CALCULATED: ValueMatrixRequestStruct.ModeEnum
    MO_STORAGE: ValueMatrixRequestStruct.ModeEnum
    MO_EXD_VIRTUAL_DATA: ValueMatrixRequestStruct.ModeEnum
    class ColumnItem(_message.Message):
        __slots__ = ("name", "unit_id")
        NAME_FIELD_NUMBER: _ClassVar[int]
        UNIT_ID_FIELD_NUMBER: _ClassVar[int]
        name: str
        unit_id: int
        def __init__(self, name: _Optional[str] = ..., unit_id: _Optional[int] = ...) -> None: ...
    MODE_FIELD_NUMBER: _ClassVar[int]
    AID_FIELD_NUMBER: _ClassVar[int]
    IID_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    VALUES_LIMIT_FIELD_NUMBER: _ClassVar[int]
    VALUES_START_FIELD_NUMBER: _ClassVar[int]
    EXD_GROUP_IDS_FIELD_NUMBER: _ClassVar[int]
    mode: ValueMatrixRequestStruct.ModeEnum
    aid: int
    iid: int
    attributes: _containers.RepeatedScalarFieldContainer[str]
    columns: _containers.RepeatedCompositeFieldContainer[ValueMatrixRequestStruct.ColumnItem]
    values_limit: int
    values_start: int
    exd_group_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, mode: _Optional[_Union[ValueMatrixRequestStruct.ModeEnum, str]] = ..., aid: _Optional[int] = ..., iid: _Optional[int] = ..., attributes: _Optional[_Iterable[str]] = ..., columns: _Optional[_Iterable[_Union[ValueMatrixRequestStruct.ColumnItem, _Mapping]]] = ..., values_limit: _Optional[int] = ..., values_start: _Optional[int] = ..., exd_group_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class ContextVariables(_message.Message):
    __slots__ = ("variables",)
    class ContextVariableValue(_message.Message):
        __slots__ = ("string_array", "long_array", "float_array", "boolean_array", "byte_array", "double_array", "longlong_array")
        STRING_ARRAY_FIELD_NUMBER: _ClassVar[int]
        LONG_ARRAY_FIELD_NUMBER: _ClassVar[int]
        FLOAT_ARRAY_FIELD_NUMBER: _ClassVar[int]
        BOOLEAN_ARRAY_FIELD_NUMBER: _ClassVar[int]
        BYTE_ARRAY_FIELD_NUMBER: _ClassVar[int]
        DOUBLE_ARRAY_FIELD_NUMBER: _ClassVar[int]
        LONGLONG_ARRAY_FIELD_NUMBER: _ClassVar[int]
        string_array: StringArray
        long_array: LongArray
        float_array: FloatArray
        boolean_array: BooleanArray
        byte_array: ByteArray
        double_array: DoubleArray
        longlong_array: LonglongArray
        def __init__(self, string_array: _Optional[_Union[StringArray, _Mapping]] = ..., long_array: _Optional[_Union[LongArray, _Mapping]] = ..., float_array: _Optional[_Union[FloatArray, _Mapping]] = ..., boolean_array: _Optional[_Union[BooleanArray, _Mapping]] = ..., byte_array: _Optional[_Union[ByteArray, _Mapping]] = ..., double_array: _Optional[_Union[DoubleArray, _Mapping]] = ..., longlong_array: _Optional[_Union[LonglongArray, _Mapping]] = ...) -> None: ...
    class VariablesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: ContextVariables.ContextVariableValue
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[ContextVariables.ContextVariableValue, _Mapping]] = ...) -> None: ...
    VARIABLES_FIELD_NUMBER: _ClassVar[int]
    variables: _containers.MessageMap[str, ContextVariables.ContextVariableValue]
    def __init__(self, variables: _Optional[_Mapping[str, ContextVariables.ContextVariableValue]] = ...) -> None: ...

class AsamPath(_message.Message):
    __slots__ = ("path",)
    PATH_FIELD_NUMBER: _ClassVar[int]
    path: str
    def __init__(self, path: _Optional[str] = ...) -> None: ...

class Instance(_message.Message):
    __slots__ = ("aid", "iid")
    AID_FIELD_NUMBER: _ClassVar[int]
    IID_FIELD_NUMBER: _ClassVar[int]
    aid: int
    iid: int
    def __init__(self, aid: _Optional[int] = ..., iid: _Optional[int] = ...) -> None: ...

class ErrorInfo(_message.Message):
    __slots__ = ("err_code", "minor_code", "reason")
    class ErrorCodeEnum(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        AO_UNKNOWN_ERROR: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_ACCESS_DENIED: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_BAD_OPERATION: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_BAD_PARAMETER: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_CONNECT_FAILED: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_CONNECT_REFUSED: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_CONNECTION_LOST: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_DUPLICATE_BASE_ATTRIBUTE: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_DUPLICATE_NAME: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_DUPLICATE_VALUE: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_HAS_INSTANCES: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_HAS_REFERENCES: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_IMPLEMENTATION_PROBLEM: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_INCOMPATIBLE_UNITS: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_INVALID_ASAM_PATH: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_INVALID_ATTRIBUTE_TYPE: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_INVALID_BASE_ELEMENT: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_INVALID_BASETYPE: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_INVALID_BUILDUP_FUNCTION: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_INVALID_COLUMN: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_INVALID_COUNT: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_INVALID_DATATYPE: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_INVALID_ELEMENT: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_INVALID_LENGTH: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_INVALID_ORDINALNUMBER: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_INVALID_RELATION: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_INVALID_RELATION_RANGE: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_INVALID_RELATION_TYPE: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_INVALID_RELATIONSHIP: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_INVALID_SET_TYPE: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_INVALID_SMATLINK: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_INVALID_SUBMATRIX: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_IS_BASE_ATTRIBUTE: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_IS_BASE_RELATION: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_IS_MEASUREMENT_MATRIX: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_MATH_ERROR: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_MISSING_APPLICATION_ELEMENT: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_MISSING_ATTRIBUTE: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_MISSING_RELATION: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_MISSING_VALUE: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_NO_MEMORY: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_NO_PATH_TO_ELEMENT: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_NOT_FOUND: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_NOT_IMPLEMENTED: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_NOT_UNIQUE: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_OPEN_MODE_NOT_SUPPORTED: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_SESSION_LIMIT_REACHED: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_SESSION_NOT_ACTIVE: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_TRANSACTION_ALREADY_ACTIVE: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_TRANSACTION_NOT_ACTIVE: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_HAS_BASE_RELATION: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_HAS_BASE_ATTRIBUTE: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_UNKNOWN_UNIT: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_NO_SCALING_COLUMN: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_QUERY_TYPE_INVALID: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_QUERY_INVALID: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_QUERY_PROCESSING_ERROR: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_QUERY_TIMEOUT_EXCEEDED: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_QUERY_INCOMPLETE: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_QUERY_INVALID_RESULTTYPE: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_INVALID_VALUEMATRIX_STRUCTURE: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_FILE_LOCKED: _ClassVar[ErrorInfo.ErrorCodeEnum]
        AO_SYSTEM_PROBLEM: _ClassVar[ErrorInfo.ErrorCodeEnum]
    AO_UNKNOWN_ERROR: ErrorInfo.ErrorCodeEnum
    AO_ACCESS_DENIED: ErrorInfo.ErrorCodeEnum
    AO_BAD_OPERATION: ErrorInfo.ErrorCodeEnum
    AO_BAD_PARAMETER: ErrorInfo.ErrorCodeEnum
    AO_CONNECT_FAILED: ErrorInfo.ErrorCodeEnum
    AO_CONNECT_REFUSED: ErrorInfo.ErrorCodeEnum
    AO_CONNECTION_LOST: ErrorInfo.ErrorCodeEnum
    AO_DUPLICATE_BASE_ATTRIBUTE: ErrorInfo.ErrorCodeEnum
    AO_DUPLICATE_NAME: ErrorInfo.ErrorCodeEnum
    AO_DUPLICATE_VALUE: ErrorInfo.ErrorCodeEnum
    AO_HAS_INSTANCES: ErrorInfo.ErrorCodeEnum
    AO_HAS_REFERENCES: ErrorInfo.ErrorCodeEnum
    AO_IMPLEMENTATION_PROBLEM: ErrorInfo.ErrorCodeEnum
    AO_INCOMPATIBLE_UNITS: ErrorInfo.ErrorCodeEnum
    AO_INVALID_ASAM_PATH: ErrorInfo.ErrorCodeEnum
    AO_INVALID_ATTRIBUTE_TYPE: ErrorInfo.ErrorCodeEnum
    AO_INVALID_BASE_ELEMENT: ErrorInfo.ErrorCodeEnum
    AO_INVALID_BASETYPE: ErrorInfo.ErrorCodeEnum
    AO_INVALID_BUILDUP_FUNCTION: ErrorInfo.ErrorCodeEnum
    AO_INVALID_COLUMN: ErrorInfo.ErrorCodeEnum
    AO_INVALID_COUNT: ErrorInfo.ErrorCodeEnum
    AO_INVALID_DATATYPE: ErrorInfo.ErrorCodeEnum
    AO_INVALID_ELEMENT: ErrorInfo.ErrorCodeEnum
    AO_INVALID_LENGTH: ErrorInfo.ErrorCodeEnum
    AO_INVALID_ORDINALNUMBER: ErrorInfo.ErrorCodeEnum
    AO_INVALID_RELATION: ErrorInfo.ErrorCodeEnum
    AO_INVALID_RELATION_RANGE: ErrorInfo.ErrorCodeEnum
    AO_INVALID_RELATION_TYPE: ErrorInfo.ErrorCodeEnum
    AO_INVALID_RELATIONSHIP: ErrorInfo.ErrorCodeEnum
    AO_INVALID_SET_TYPE: ErrorInfo.ErrorCodeEnum
    AO_INVALID_SMATLINK: ErrorInfo.ErrorCodeEnum
    AO_INVALID_SUBMATRIX: ErrorInfo.ErrorCodeEnum
    AO_IS_BASE_ATTRIBUTE: ErrorInfo.ErrorCodeEnum
    AO_IS_BASE_RELATION: ErrorInfo.ErrorCodeEnum
    AO_IS_MEASUREMENT_MATRIX: ErrorInfo.ErrorCodeEnum
    AO_MATH_ERROR: ErrorInfo.ErrorCodeEnum
    AO_MISSING_APPLICATION_ELEMENT: ErrorInfo.ErrorCodeEnum
    AO_MISSING_ATTRIBUTE: ErrorInfo.ErrorCodeEnum
    AO_MISSING_RELATION: ErrorInfo.ErrorCodeEnum
    AO_MISSING_VALUE: ErrorInfo.ErrorCodeEnum
    AO_NO_MEMORY: ErrorInfo.ErrorCodeEnum
    AO_NO_PATH_TO_ELEMENT: ErrorInfo.ErrorCodeEnum
    AO_NOT_FOUND: ErrorInfo.ErrorCodeEnum
    AO_NOT_IMPLEMENTED: ErrorInfo.ErrorCodeEnum
    AO_NOT_UNIQUE: ErrorInfo.ErrorCodeEnum
    AO_OPEN_MODE_NOT_SUPPORTED: ErrorInfo.ErrorCodeEnum
    AO_SESSION_LIMIT_REACHED: ErrorInfo.ErrorCodeEnum
    AO_SESSION_NOT_ACTIVE: ErrorInfo.ErrorCodeEnum
    AO_TRANSACTION_ALREADY_ACTIVE: ErrorInfo.ErrorCodeEnum
    AO_TRANSACTION_NOT_ACTIVE: ErrorInfo.ErrorCodeEnum
    AO_HAS_BASE_RELATION: ErrorInfo.ErrorCodeEnum
    AO_HAS_BASE_ATTRIBUTE: ErrorInfo.ErrorCodeEnum
    AO_UNKNOWN_UNIT: ErrorInfo.ErrorCodeEnum
    AO_NO_SCALING_COLUMN: ErrorInfo.ErrorCodeEnum
    AO_QUERY_TYPE_INVALID: ErrorInfo.ErrorCodeEnum
    AO_QUERY_INVALID: ErrorInfo.ErrorCodeEnum
    AO_QUERY_PROCESSING_ERROR: ErrorInfo.ErrorCodeEnum
    AO_QUERY_TIMEOUT_EXCEEDED: ErrorInfo.ErrorCodeEnum
    AO_QUERY_INCOMPLETE: ErrorInfo.ErrorCodeEnum
    AO_QUERY_INVALID_RESULTTYPE: ErrorInfo.ErrorCodeEnum
    AO_INVALID_VALUEMATRIX_STRUCTURE: ErrorInfo.ErrorCodeEnum
    AO_FILE_LOCKED: ErrorInfo.ErrorCodeEnum
    AO_SYSTEM_PROBLEM: ErrorInfo.ErrorCodeEnum
    ERR_CODE_FIELD_NUMBER: _ClassVar[int]
    MINOR_CODE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    err_code: ErrorInfo.ErrorCodeEnum
    minor_code: int
    reason: str
    def __init__(self, err_code: _Optional[_Union[ErrorInfo.ErrorCodeEnum, str]] = ..., minor_code: _Optional[int] = ..., reason: _Optional[str] = ...) -> None: ...

class ContextVariablesFilter(_message.Message):
    __slots__ = ("pattern",)
    PATTERN_FIELD_NUMBER: _ClassVar[int]
    pattern: str
    def __init__(self, pattern: _Optional[str] = ...) -> None: ...

class FileIdentifier(_message.Message):
    __slots__ = ("aid", "iid", "attribute", "index")
    AID_FIELD_NUMBER: _ClassVar[int]
    IID_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTE_FIELD_NUMBER: _ClassVar[int]
    INDEX_FIELD_NUMBER: _ClassVar[int]
    aid: int
    iid: int
    attribute: str
    index: int
    def __init__(self, aid: _Optional[int] = ..., iid: _Optional[int] = ..., attribute: _Optional[str] = ..., index: _Optional[int] = ...) -> None: ...

class FileControl(_message.Message):
    __slots__ = ("aid", "iid", "url", "action")
    class ActionEnum(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        AC_TAKE: _ClassVar[FileControl.ActionEnum]
        AC_REMOVE: _ClassVar[FileControl.ActionEnum]
    AC_TAKE: FileControl.ActionEnum
    AC_REMOVE: FileControl.ActionEnum
    AID_FIELD_NUMBER: _ClassVar[int]
    IID_FIELD_NUMBER: _ClassVar[int]
    URL_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    aid: int
    iid: int
    url: str
    action: FileControl.ActionEnum
    def __init__(self, aid: _Optional[int] = ..., iid: _Optional[int] = ..., url: _Optional[str] = ..., action: _Optional[_Union[FileControl.ActionEnum, str]] = ...) -> None: ...

class BaseModel(_message.Message):
    __slots__ = ("version", "enumerations", "entities")
    class DerivationEnum(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        DV_UNRESTRICTED: _ClassVar[BaseModel.DerivationEnum]
        DV_SINGLE: _ClassVar[BaseModel.DerivationEnum]
        DV_TREE_AT_INSTANCE: _ClassVar[BaseModel.DerivationEnum]
        DV_TREE_AT_MODEL: _ClassVar[BaseModel.DerivationEnum]
    DV_UNRESTRICTED: BaseModel.DerivationEnum
    DV_SINGLE: BaseModel.DerivationEnum
    DV_TREE_AT_INSTANCE: BaseModel.DerivationEnum
    DV_TREE_AT_MODEL: BaseModel.DerivationEnum
    class Attribute(_message.Message):
        __slots__ = ("name", "data_type", "mandatory", "obligatory", "autogenerated", "enumeration")
        NAME_FIELD_NUMBER: _ClassVar[int]
        DATA_TYPE_FIELD_NUMBER: _ClassVar[int]
        MANDATORY_FIELD_NUMBER: _ClassVar[int]
        OBLIGATORY_FIELD_NUMBER: _ClassVar[int]
        AUTOGENERATED_FIELD_NUMBER: _ClassVar[int]
        ENUMERATION_FIELD_NUMBER: _ClassVar[int]
        name: str
        data_type: DataTypeEnum
        mandatory: bool
        obligatory: bool
        autogenerated: bool
        enumeration: str
        def __init__(self, name: _Optional[str] = ..., data_type: _Optional[_Union[DataTypeEnum, str]] = ..., mandatory: bool = ..., obligatory: bool = ..., autogenerated: bool = ..., enumeration: _Optional[str] = ...) -> None: ...
    class Relation(_message.Message):
        __slots__ = ("name", "inverse_name", "entity_names", "range_min", "range_max", "relationship", "inverse_range_min", "inverse_range_max", "relation_type", "mandatory")
        NAME_FIELD_NUMBER: _ClassVar[int]
        INVERSE_NAME_FIELD_NUMBER: _ClassVar[int]
        ENTITY_NAMES_FIELD_NUMBER: _ClassVar[int]
        RANGE_MIN_FIELD_NUMBER: _ClassVar[int]
        RANGE_MAX_FIELD_NUMBER: _ClassVar[int]
        RELATIONSHIP_FIELD_NUMBER: _ClassVar[int]
        INVERSE_RANGE_MIN_FIELD_NUMBER: _ClassVar[int]
        INVERSE_RANGE_MAX_FIELD_NUMBER: _ClassVar[int]
        RELATION_TYPE_FIELD_NUMBER: _ClassVar[int]
        MANDATORY_FIELD_NUMBER: _ClassVar[int]
        name: str
        inverse_name: str
        entity_names: _containers.RepeatedScalarFieldContainer[str]
        range_min: int
        range_max: int
        relationship: Model.RelationshipEnum
        inverse_range_min: int
        inverse_range_max: int
        relation_type: Model.RelationTypeEnum
        mandatory: bool
        def __init__(self, name: _Optional[str] = ..., inverse_name: _Optional[str] = ..., entity_names: _Optional[_Iterable[str]] = ..., range_min: _Optional[int] = ..., range_max: _Optional[int] = ..., relationship: _Optional[_Union[Model.RelationshipEnum, str]] = ..., inverse_range_min: _Optional[int] = ..., inverse_range_max: _Optional[int] = ..., relation_type: _Optional[_Union[Model.RelationTypeEnum, str]] = ..., mandatory: bool = ...) -> None: ...
    class Entity(_message.Message):
        __slots__ = ("name", "bid", "derivation", "attributes", "relations")
        class AttributesEntry(_message.Message):
            __slots__ = ("key", "value")
            KEY_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            key: str
            value: BaseModel.Attribute
            def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[BaseModel.Attribute, _Mapping]] = ...) -> None: ...
        class RelationsEntry(_message.Message):
            __slots__ = ("key", "value")
            KEY_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            key: str
            value: BaseModel.Relation
            def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[BaseModel.Relation, _Mapping]] = ...) -> None: ...
        NAME_FIELD_NUMBER: _ClassVar[int]
        BID_FIELD_NUMBER: _ClassVar[int]
        DERIVATION_FIELD_NUMBER: _ClassVar[int]
        ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
        RELATIONS_FIELD_NUMBER: _ClassVar[int]
        name: str
        bid: int
        derivation: BaseModel.DerivationEnum
        attributes: _containers.MessageMap[str, BaseModel.Attribute]
        relations: _containers.MessageMap[str, BaseModel.Relation]
        def __init__(self, name: _Optional[str] = ..., bid: _Optional[int] = ..., derivation: _Optional[_Union[BaseModel.DerivationEnum, str]] = ..., attributes: _Optional[_Mapping[str, BaseModel.Attribute]] = ..., relations: _Optional[_Mapping[str, BaseModel.Relation]] = ...) -> None: ...
    class EnumerationsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: Model.Enumeration
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[Model.Enumeration, _Mapping]] = ...) -> None: ...
    class EntitiesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: BaseModel.Entity
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[BaseModel.Entity, _Mapping]] = ...) -> None: ...
    VERSION_FIELD_NUMBER: _ClassVar[int]
    ENUMERATIONS_FIELD_NUMBER: _ClassVar[int]
    ENTITIES_FIELD_NUMBER: _ClassVar[int]
    version: str
    enumerations: _containers.MessageMap[str, Model.Enumeration]
    entities: _containers.MessageMap[str, BaseModel.Entity]
    def __init__(self, version: _Optional[str] = ..., enumerations: _Optional[_Mapping[str, Model.Enumeration]] = ..., entities: _Optional[_Mapping[str, BaseModel.Entity]] = ...) -> None: ...

class NtoMRelationIdentifier(_message.Message):
    __slots__ = ("aid", "iid", "relation_name", "related_aid")
    AID_FIELD_NUMBER: _ClassVar[int]
    IID_FIELD_NUMBER: _ClassVar[int]
    RELATION_NAME_FIELD_NUMBER: _ClassVar[int]
    RELATED_AID_FIELD_NUMBER: _ClassVar[int]
    aid: int
    iid: int
    relation_name: str
    related_aid: int
    def __init__(self, aid: _Optional[int] = ..., iid: _Optional[int] = ..., relation_name: _Optional[str] = ..., related_aid: _Optional[int] = ...) -> None: ...

class NtoMRelatedInstances(_message.Message):
    __slots__ = ("identifier", "related_iids")
    IDENTIFIER_FIELD_NUMBER: _ClassVar[int]
    RELATED_IIDS_FIELD_NUMBER: _ClassVar[int]
    identifier: NtoMRelationIdentifier
    related_iids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, identifier: _Optional[_Union[NtoMRelationIdentifier, _Mapping]] = ..., related_iids: _Optional[_Iterable[int]] = ...) -> None: ...

class NtoMWriteRelatedInstances(_message.Message):
    __slots__ = ("change_mode", "relation_info")
    class WriteTypeEnum(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        WT_ADD: _ClassVar[NtoMWriteRelatedInstances.WriteTypeEnum]
        WT_REMOVE: _ClassVar[NtoMWriteRelatedInstances.WriteTypeEnum]
        WT_SET: _ClassVar[NtoMWriteRelatedInstances.WriteTypeEnum]
    WT_ADD: NtoMWriteRelatedInstances.WriteTypeEnum
    WT_REMOVE: NtoMWriteRelatedInstances.WriteTypeEnum
    WT_SET: NtoMWriteRelatedInstances.WriteTypeEnum
    CHANGE_MODE_FIELD_NUMBER: _ClassVar[int]
    RELATION_INFO_FIELD_NUMBER: _ClassVar[int]
    change_mode: NtoMWriteRelatedInstances.WriteTypeEnum
    relation_info: NtoMRelatedInstances
    def __init__(self, change_mode: _Optional[_Union[NtoMWriteRelatedInstances.WriteTypeEnum, str]] = ..., relation_info: _Optional[_Union[NtoMRelatedInstances, _Mapping]] = ...) -> None: ...

class CopyRequest(_message.Message):
    __slots__ = ("mode", "aid", "iid", "new_name", "new_version")
    class CopyModeEnum(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        CM_SHALLOW: _ClassVar[CopyRequest.CopyModeEnum]
        CM_DEEP: _ClassVar[CopyRequest.CopyModeEnum]
    CM_SHALLOW: CopyRequest.CopyModeEnum
    CM_DEEP: CopyRequest.CopyModeEnum
    MODE_FIELD_NUMBER: _ClassVar[int]
    AID_FIELD_NUMBER: _ClassVar[int]
    IID_FIELD_NUMBER: _ClassVar[int]
    NEW_NAME_FIELD_NUMBER: _ClassVar[int]
    NEW_VERSION_FIELD_NUMBER: _ClassVar[int]
    mode: CopyRequest.CopyModeEnum
    aid: int
    iid: int
    new_name: str
    new_version: str
    def __init__(self, mode: _Optional[_Union[CopyRequest.CopyModeEnum, str]] = ..., aid: _Optional[int] = ..., iid: _Optional[int] = ..., new_name: _Optional[str] = ..., new_version: _Optional[str] = ...) -> None: ...

class PasswordUpdate(_message.Message):
    __slots__ = ("user_id", "old_password", "new_password")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    OLD_PASSWORD_FIELD_NUMBER: _ClassVar[int]
    NEW_PASSWORD_FIELD_NUMBER: _ClassVar[int]
    user_id: int
    old_password: str
    new_password: str
    def __init__(self, user_id: _Optional[int] = ..., old_password: _Optional[str] = ..., new_password: _Optional[str] = ...) -> None: ...
