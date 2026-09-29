"""Write paths against a running GreptimeDB.

Skipped unless GREPTIMEDB_GRPC is set, so the default pytest run stays offline.
"""

import json
import os
import urllib.parse
import urllib.request
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from greptimedb_ingester import Client, Column, ColumnDataType, GreptimeError, SemanticType, WriteOptions

pytestmark = pytest.mark.integration

FIELD = SemanticType.FIELD
TS = SemanticType.TIMESTAMP
ORIGIN = datetime(2024, 6, 1, tzinfo=timezone.utc)
BATCH = 20_000

COLUMNS = [
    Column("host", ColumnDataType.STRING, SemanticType.TAG),
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
    Column("name", ColumnDataType.STRING, FIELD),
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


def bulk_row(i, host="edge"):
    values = row(i, host)
    values[-1] = b"\x40\x00"
    return values


def row(i, host="edge"):
    when = ORIGIN + timedelta(milliseconds=i)
    second = i % 86_400
    return [
        host,
        i % 2 == 0,
        i % 128 - 64,
        (i % 30_000) - 15_000,
        i,
        i,
        i % 256,
        i,
        i,
        i,
        float(i) + 0.5,
        float(i) + 0.25,
        bytes([i % 256, (i // 256) % 256]),
        f"row-{i}",
        date(2024, 1, 1) + timedelta(days=i % 28),
        when,
        when,
        when,
        when,
        second,
        second * 1_000,
        second * 1_000_000,
        second * 1_000_000_000,
        Decimal(i % 1000) / Decimal(100),
        {"i": i, "host": host},
    ]


def sql(statement):
    http = os.environ.get("GREPTIMEDB_HTTP", "http://127.0.0.1:4000")
    data = urllib.parse.urlencode({"sql": statement}).encode()
    request = urllib.request.Request(f"{http}/v1/sql?db=public", data=data, method="POST")
    with urllib.request.urlopen(request, timeout=10) as response:
        body = json.loads(response.read().decode())
    if body.get("error"):
        raise AssertionError(body["error"])
    return body["output"][0]["records"]["rows"]


@pytest.fixture(scope="module")
def client():
    endpoint = os.environ.get("GREPTIMEDB_GRPC")
    if not endpoint:
        pytest.skip("GREPTIMEDB_GRPC is not set")
    db = Client([endpoint], database="public")
    db.health_check()
    return db


def count(table):
    return sql(f'SELECT COUNT(*) FROM "{table}"')[0][0]


def test_insert_20k_creates_table_and_tags(client):
    table = f"py_it_insert_{uuid.uuid4().hex[:8]}"
    affected = client.insert(table, COLUMNS, (row(i) for i in range(BATCH)))
    assert affected == BATCH
    described = sql(f'DESC TABLE "{table}"')
    by_name = {item[0]: item for item in described}
    assert by_name["host"][5] == "TAG"
    assert by_name["host"][2] == "PRI"
    assert by_name["ts_ms"][5] == "TIMESTAMP"
    assert by_name["f64"][5] == "FIELD"
    assert count(table) == BATCH

    affected = client.insert(table, COLUMNS, [row(BATCH)])
    assert affected == 1
    assert count(table) == BATCH + 1


def test_insert_skip_wal_hint(client):
    table = f"py_it_hint_{uuid.uuid4().hex[:8]}"
    affected = client.insert(
        table,
        COLUMNS,
        [row(0)],
        hints=[("insert_skip_wal", "true")],
    )
    assert affected == 1
    assert count(table) == 1


def test_delete_by_tag_and_time_index(client):
    table = f"py_it_delete_{uuid.uuid4().hex[:8]}"
    client.insert(table, COLUMNS, [row(0, "a"), row(1, "b")])
    keys = [
        Column("host", ColumnDataType.STRING, SemanticType.TAG),
        Column("ts_ms", ColumnDataType.TIMESTAMP_MILLISECOND, SemanticType.TIMESTAMP),
    ]
    when = ORIGIN + timedelta(milliseconds=0)
    affected = client.delete(table, keys, [["a", when]])
    assert affected == 1
    assert sql(f'SELECT COUNT(*) FROM "{table}"') == [[1]]


def test_null_timestamp_field_inserts(client):
    table = f"py_it_null_{uuid.uuid4().hex[:8]}"
    values = row(0)
    ts_s = next(i for i, column in enumerate(COLUMNS) if column.name == "ts_s")
    values[ts_s] = None
    assert client.insert(table, COLUMNS, [values]) == 1


def test_null_time_index_is_rejected_locally():
    column = Column("ts_ms", ColumnDataType.TIMESTAMP_MILLISECOND, SemanticType.TIMESTAMP)
    client = Client(["127.0.0.1:4001"], database="public")
    with pytest.raises(GreptimeError, match="cannot be null"):
        client.insert("py_it_null_index", [column], [[None]])


def test_bulk_requires_existing_table(client):
    table = f"py_it_missing_{uuid.uuid4().hex[:8]}"
    writer = client.bulk_writer(table, COLUMNS)
    rows = writer.alloc_rows(1)
    rows.add_row(bulk_row(0))
    with pytest.raises(GreptimeError, match="not found"):
        writer.write(rows)
    with pytest.raises(GreptimeError):
        writer.finish()


def test_bulk_write_20k(client):
    table = f"py_it_bulk_write_{uuid.uuid4().hex[:8]}"
    assert client.insert(table, COLUMNS, [row(0)]) == 1
    with client.bulk_writer(
        table,
        COLUMNS,
        options=WriteOptions(compression="lz4", parallelism=4),
    ) as writer:
        rows = writer.alloc_rows(BATCH)
        assert rows.add_rows(bulk_row(i) for i in range(1, BATCH + 1)) == BATCH
        response = writer.write(rows)
        assert len(rows) == 0
    assert response.affected_rows == BATCH
    assert count(table) == BATCH + 1


def test_bulk_write_async_20k(client):
    table = f"py_it_bulk_async_{uuid.uuid4().hex[:8]}"
    assert client.insert(table, COLUMNS, [row(0)]) == 1
    with client.bulk_writer(
        table,
        COLUMNS,
        options=WriteOptions(compression="zstd", parallelism=4),
    ) as writer:
        rows = writer.alloc_rows(BATCH)
        rows.add_row(bulk_row(1))
        assert rows.add_rows(bulk_row(i) for i in range(2, BATCH + 1)) == BATCH - 1
        request_ids = writer.write_async(rows)
        assert request_ids
        waited = [writer.wait(request_id) for request_id in request_ids]
        flushed = writer.flush()
    assert sum(response.affected_rows for response in waited) == BATCH
    assert flushed == []
    assert count(table) == BATCH + 1


def test_finish_with_responses(client):
    table = f"py_it_finish_{uuid.uuid4().hex[:8]}"
    assert client.insert(table, COLUMNS, [row(0)]) == 1
    writer = client.bulk_writer(table, COLUMNS, options=WriteOptions(parallelism=2))
    rows = writer.alloc_rows(2)
    rows.add_rows([bulk_row(1), bulk_row(2)])
    request_ids = writer.write_async(rows)
    assert request_ids
    responses = writer.finish_with_responses()
    assert sum(response.affected_rows for response in responses) == 2
    with pytest.raises(GreptimeError, match="already finished"):
        writer.flush()
    assert count(table) == 3
