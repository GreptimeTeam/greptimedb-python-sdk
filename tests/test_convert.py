from datetime import datetime, timezone
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


def test_timestamps_keep_the_column_unit():
    columns = [
        col("ts", ColumnDataType.TIMESTAMP_MILLISECOND, SemanticType.TIMESTAMP),
    ]
    assert describe_row(columns, [1_234_567_890_000]) == ["timestamp_millisecond:1234567890000"]
    aware = datetime(2024, 1, 1, tzinfo=timezone.utc)
    described = describe_row(columns, [aware])[0]
    assert described == f"timestamp_millisecond:{int(aware.timestamp() * 1000)}"


def test_null_timestamp_is_rejected():
    columns = [col("ts", ColumnDataType.TIMESTAMP_SECOND, SemanticType.TIMESTAMP)]
    with pytest.raises(GreptimeError, match="cannot be null") as exc:
        describe_row(columns, [None])
    assert exc.value.retriable is False


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


def test_json2_accepts_objects_and_rejects_arrays():
    column = col("attrs", ColumnDataType.JSON2)
    assert describe_row([column], ['{"a":1}']) == ["json2"]
    assert describe_row([column], [{"a": 1}]) == ["json2"]
    assert describe_row([column], [None]) == ["null"]
    with pytest.raises(GreptimeError, match="JSON") as exc:
        describe_row([column], ["[]"])
    assert exc.value.retriable is False


def test_bulk_json_and_rejects_json2():
    json_column = col("attrs", ColumnDataType.JSON)
    assert describe_row([json_column], ['{"a":1}'], bulk=True) == ['json:{"a":1}']
    with pytest.raises(GreptimeError, match="bulk writer"):
        describe_row([col("attrs", ColumnDataType.JSON2)], [None], bulk=True)


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


def test_interval_month_day_nano():
    column = col("span", ColumnDataType.INTERVAL_MONTH_DAY_NANO)
    assert describe_row([column], [(1, 2, 3)]) == ["interval_month_day_nano:1,2,3"]


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
