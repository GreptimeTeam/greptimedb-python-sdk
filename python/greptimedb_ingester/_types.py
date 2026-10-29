"""Public column schema shared by both backends."""

from dataclasses import dataclass
from enum import IntEnum


from ._errors import GreptimeError


def _schema_error(message):
    return GreptimeError(message)


class _NamedEnum(IntEnum):
    def __repr__(self):
        return f"{type(self).__name__}.{self.name}"


class SemanticType(_NamedEnum):
    TAG = 0
    FIELD = 1
    TIMESTAMP = 2


class ColumnDataType(_NamedEnum):
    BOOLEAN = 0
    INT8 = 1
    INT16 = 2
    INT32 = 3
    INT64 = 4
    UINT8 = 5
    UINT16 = 6
    UINT32 = 7
    UINT64 = 8
    FLOAT32 = 9
    FLOAT64 = 10
    BINARY = 11
    STRING = 12
    DATE = 13
    TIMESTAMP_SECOND = 15
    TIMESTAMP_MILLISECOND = 16
    TIMESTAMP_MICROSECOND = 17
    TIMESTAMP_NANOSECOND = 18
    TIME_SECOND = 19
    TIME_MILLISECOND = 20
    TIME_MICROSECOND = 21
    TIME_NANOSECOND = 22
    DECIMAL128 = 30
    JSON = 31


@dataclass(frozen=True)
class Column:
    name: str
    data_type: ColumnDataType
    semantic_type: SemanticType
    precision: int | None = None
    scale: int | None = None

    def __post_init__(self):
        if not isinstance(self.name, str) or not self.name or "\x00" in self.name:
            raise _schema_error("column name must be a nonempty string without NUL")
        if not isinstance(self.data_type, ColumnDataType):
            raise _schema_error("data_type must be ColumnDataType")
        if not isinstance(self.semantic_type, SemanticType):
            raise _schema_error("semantic_type must be SemanticType")
        if self.data_type == ColumnDataType.DECIMAL128:
            if self.precision is None or self.scale is None:
                raise _schema_error("decimal128 columns require precision and scale")
            if type(self.precision) is not int or not 1 <= self.precision <= 38:
                raise _schema_error("decimal128 precision must be between 1 and 38")
            if type(self.scale) is not int or not 0 <= self.scale <= self.precision:
                raise _schema_error("decimal128 scale must be between 0 and precision")
        elif self.precision is not None or self.scale is not None:
            raise _schema_error(
                "precision and scale are only valid for decimal128 columns"
            )
