from datetime import datetime, time, timezone
from decimal import Decimal

import pytest

from greptimedb_ingester import (
    Client,
    Column,
    ColumnDataType,
    GreptimeError,
    SemanticType,
    WriteOptions,
)
from greptimedb_ingester._native import describe_row
from type_samples import ALL_COLUMNS, ALL_ROW, BULK_COLUMNS, BULK_ROW


def col(name, data_type, semantic=SemanticType.FIELD, precision=None, scale=None):
    return Column(name, data_type, semantic, precision=precision, scale=scale)


def test_integer_widths_and_ranges():
    columns = [
        col("i8", ColumnDataType.INT8),
        col("u8", ColumnDataType.UINT8),
        col("i64", ColumnDataType.INT64),
        col("u64", ColumnDataType.UINT64),
    ]
    assert describe_row(columns, [-128, 255, -1, 2**64 - 1]) == [
        "i8:-128",
        "u8:255",
        "i64:-1",
        "u64:18446744073709551615",
    ]
    with pytest.raises(GreptimeError, match="out of range") as exc:
        describe_row([col("i8", ColumnDataType.INT8)], [128])
    assert exc.value.retriable is False


def test_timestamps_accept_integers_and_datetimes():
    # 2009-02-13 23:31:30.123456 UTC
    aware = datetime(2009, 2, 13, 23, 31, 30, 123456, tzinfo=timezone.utc)
    naive = datetime(2009, 2, 13, 23, 31, 30, 123456)
    cases = [
        (ColumnDataType.TIMESTAMP_SECOND, "timestamp_second:1234567890"),
        (ColumnDataType.TIMESTAMP_MILLISECOND, "timestamp_millisecond:1234567890123"),
        (ColumnDataType.TIMESTAMP_MICROSECOND, "timestamp_microsecond:1234567890123456"),
        (ColumnDataType.TIMESTAMP_NANOSECOND, "timestamp_nanosecond:1234567890123456000"),
    ]
    for data_type, expected in cases:
        columns = [col("ts", data_type, SemanticType.TIMESTAMP)]
        assert describe_row(columns, [aware]) == [expected]
        assert describe_row(columns, [naive]) == [expected]
        assert describe_row(columns, [aware], bulk=True) == [expected]

    columns = [col("ts", ColumnDataType.TIMESTAMP_MILLISECOND, SemanticType.TIMESTAMP)]
    assert describe_row(columns, [1_234_567_890_123]) == ["timestamp_millisecond:1234567890123"]


def test_time_is_an_integer_offset_from_midnight():
    column = col("clock", ColumnDataType.TIME_MILLISECOND)
    assert describe_row([column], [3_661_000]) == ["time_millisecond:3661000"]
    with pytest.raises(GreptimeError, match="expected time_millisecond, got datetime"):
        describe_row([column], [datetime(2009, 2, 13, 23, 31, 30, tzinfo=timezone.utc)])
    with pytest.raises(GreptimeError, match="unsupported Python value time"):
        describe_row([column], [time(1, 1, 1)])


INSERT_ENCODED = {
    "ok": "bool:true",
    "i8": "i8:-8",
    "i16": "i16:-16",
    "i32": "i32:-32",
    "i64": "i64:-64",
    "u8": "u8:8",
    "u16": "u16:16",
    "u32": "u32:32",
    "u64": "u64:64",
    "f32": "f32:1.5",
    "f64": "f64:23.5",
    "payload": "binary:dead",
    "host": "string:edge",
    "day": "date:14288",
    "ts_s": "timestamp_second:1234567890",
    "ts_ms": "timestamp_millisecond:1234567890000",
    "ts_us": "timestamp_microsecond:1234567890000000",
    "ts_ns": "timestamp_nanosecond:1234567890000000000",
    "clock_s": "time_second:3661",
    "clock_ms": "time_millisecond:3661000",
    "clock_us": "time_microsecond:3661000000",
    "clock_ns": "time_nanosecond:3661000000000",
    "price": "decimal128:1234",
    "attrs": 'string:{"a": 1}',
}


def test_every_column_type_encodes():
    covered = {str(column.data_type) for column in ALL_COLUMNS}
    declared = {
        str(getattr(ColumnDataType, name))
        for name in dir(ColumnDataType)
        if name.isupper()
    }
    assert covered == declared
    assert [column.semantic_type for column in ALL_COLUMNS].count(SemanticType.TIMESTAMP) == 1
    assert describe_row(ALL_COLUMNS, ALL_ROW) == [
        INSERT_ENCODED[column.name] for column in ALL_COLUMNS
    ]


def test_bulk_sample_covers_every_type():
    assert [column.name for column in BULK_COLUMNS] == [column.name for column in ALL_COLUMNS]
    encoded = dict(INSERT_ENCODED)
    encoded["attrs"] = 'json:{"a": 1}'
    assert describe_row(BULK_COLUMNS, BULK_ROW, bulk=True) == [
        encoded[column.name] for column in BULK_COLUMNS
    ]


def test_null_timestamp_is_rejected():
    columns = [col("ts", ColumnDataType.TIMESTAMP_SECOND, SemanticType.TIMESTAMP)]
    with pytest.raises(GreptimeError, match="cannot be null") as exc:
        describe_row(columns, [None])
    assert exc.value.retriable is False
    with pytest.raises(GreptimeError, match="cannot be null"):
        describe_row(columns, [None], bulk=True)


def test_null_timestamp_field_is_allowed():
    column = col("seen_at", ColumnDataType.TIMESTAMP_MILLISECOND, SemanticType.FIELD)
    assert describe_row([column], [None]) == ["null"]
    assert describe_row([column], [None], bulk=True) == ["null"]


def test_decimal128_scale():
    column = col("price", ColumnDataType.DECIMAL128, precision=10, scale=2)
    assert describe_row([column], [Decimal("12.34")]) == ["decimal128:1234"]
    assert describe_row([column], [1234]) == ["decimal128:1234"]
    assert describe_row([column], [Decimal("-0.50")]) == ["decimal128:-50"]
    with pytest.raises(GreptimeError, match="fractional digits"):
        describe_row([column], [Decimal("12.345")])


def test_decimal_requires_precision_and_scale():
    with pytest.raises(GreptimeError, match="precision and scale"):
        col("price", ColumnDataType.DECIMAL128)


def test_binary_null_and_string():
    columns = [
        col("payload", ColumnDataType.BINARY),
        col("host", ColumnDataType.STRING),
        col("ok", ColumnDataType.BOOLEAN),
    ]
    assert describe_row(columns, [b"\xde\xad", "edge", True]) == [
        "binary:dead",
        "string:edge",
        "bool:true",
    ]
    assert describe_row(columns, [None, None, None]) == ["null", "null", "null"]


def test_json_accepts_objects_and_rejects_invalid_text():
    column = col("attrs", ColumnDataType.JSON)
    assert describe_row([column], ['{"a":1}']) == ['string:{"a":1}']
    assert describe_row([column], [{"a": 1}]) == ['string:{"a": 1}']
    assert describe_row([column], [None]) == ["null"]
    with pytest.raises(GreptimeError, match="JSON") as exc:
        describe_row([column], ["["])
    assert exc.value.retriable is False


def test_bulk_json():
    column = col("attrs", ColumnDataType.JSON)
    assert describe_row([column], ['{"a":1}'], bulk=True) == ['json:{"a":1}']


def test_named_rows_follow_column_order():
    columns = [
        col("device", ColumnDataType.STRING, SemanticType.TAG),
        col("ts", ColumnDataType.TIMESTAMP_MILLISECOND, SemanticType.TIMESTAMP),
        col("temperature", ColumnDataType.FLOAT64),
    ]
    described = describe_row(
        columns,
        {"temperature": 23.5, "device": "device_001", "ts": 10},
    )
    assert described == ["string:device_001", "timestamp_millisecond:10", "f64:23.5"]
    with pytest.raises(GreptimeError, match="missing column"):
        describe_row(columns, {"device": "device_001"})


def test_client_rejects_incomplete_options():
    with pytest.raises(GreptimeError, match="urls"):
        Client([])
    with pytest.raises(GreptimeError, match="username and password"):
        Client(["127.0.0.1:4001"], username="user")
    with pytest.raises(GreptimeError, match="tls_server_ca"):
        Client(["127.0.0.1:4001"], tls_server_ca="ca.pem")
    with pytest.raises(GreptimeError, match="gRPC compression"):
        Client(["127.0.0.1:4001"], send_compression="lz4")


def test_client_constructor_does_not_connect():
    client = Client(
        ["127.0.0.1:4001"],
        database="public",
        timeout_secs=1,
        connect_timeout_secs=0.5,
        send_compression="zstd",
        accept_compression="gzip",
    )
    assert client.database == "public"
    assert "public" in repr(client)


def test_write_options_defaults():
    options = WriteOptions()
    assert options.compression == "lz4"
    assert options.timeout_secs == 60
    assert options.parallelism == 4
    with pytest.raises(GreptimeError, match="parallelism"):
        WriteOptions(parallelism=0)
