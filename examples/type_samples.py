"""One value for every column type.

A table has a single time index. The other timestamp types are fields.
"""

from datetime import date, datetime, timezone
from decimal import Decimal

from greptimedb_ingester import Column, ColumnDataType, SemanticType

WHEN = datetime(2009, 2, 13, 23, 31, 30, tzinfo=timezone.utc)

FIELD = SemanticType.FIELD
TS = SemanticType.TIMESTAMP

ALL_COLUMNS = [
    Column("ok", ColumnDataType.BOOLEAN, FIELD),
    Column("i8", ColumnDataType.INT8, FIELD),
    Column("i16", ColumnDataType.INT16, FIELD),
    Column("i32", ColumnDataType.INT32, FIELD),
    Column("i64", ColumnDataType.INT64, FIELD),
    Column("u8", ColumnDataType.UINT8, FIELD),
    Column("u16", ColumnDataType.UINT16, FIELD),
    Column("u32", ColumnDataType.UINT32, FIELD),
    Column("u64", ColumnDataType.UINT64, FIELD),
    Column("f32", ColumnDataType.FLOAT32, FIELD),
    Column("f64", ColumnDataType.FLOAT64, FIELD),
    Column("payload", ColumnDataType.BINARY, FIELD),
    Column("host", ColumnDataType.STRING, FIELD),
    Column("day", ColumnDataType.DATE, FIELD),
    Column("ts_s", ColumnDataType.TIMESTAMP_SECOND, FIELD),
    Column("ts_ms", ColumnDataType.TIMESTAMP_MILLISECOND, TS),
    Column("ts_us", ColumnDataType.TIMESTAMP_MICROSECOND, FIELD),
    Column("ts_ns", ColumnDataType.TIMESTAMP_NANOSECOND, FIELD),
    Column("clock_s", ColumnDataType.TIME_SECOND, FIELD),
    Column("clock_ms", ColumnDataType.TIME_MILLISECOND, FIELD),
    Column("clock_us", ColumnDataType.TIME_MICROSECOND, FIELD),
    Column("clock_ns", ColumnDataType.TIME_NANOSECOND, FIELD),
    Column("price", ColumnDataType.DECIMAL128, FIELD, precision=10, scale=2),
    Column("attrs", ColumnDataType.JSON, FIELD),
]

ALL_ROW = [
    True,
    -8,
    -16,
    -32,
    -64,
    8,
    16,
    32,
    64,
    1.5,
    23.5,
    b"\xde\xad",
    "edge",
    date(2009, 2, 13),
    WHEN,
    WHEN,
    WHEN,
    WHEN,
    3661,
    3_661_000,
    3_661_000_000,
    3_661_000_000_000,
    Decimal("12.34"),
    {"a": 1},
]

INSERT_COLUMNS = ALL_COLUMNS
INSERT_ROW = ALL_ROW
BULK_COLUMNS = ALL_COLUMNS
# Caller-supplied JSONB bytes. bulk does not encode JSON text.
BULK_ROW = ALL_ROW[:-1] + [b"\x40\x00"]
