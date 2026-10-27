"""COPY insert integration tests; require GREPTIMEDB_POSTGRES."""

import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("psycopg2")
import psycopg2
from psycopg2 import sql
from greptimedb_ingester import (
    Column,
    ColumnDataType as D,
    GreptimeError,
    SemanticType as S,
)
from greptimedb_ingester.postgres import Client

pytestmark = pytest.mark.integration
BASE_MS = 1793066400000
INTEGER_TYPES = [
    (prefix + str(bits), bits, prefix == "UINT")
    for prefix in ["INT", "UINT"]
    for bits in [8, 16, 32, 64]
]
COLUMNS = [
    Column("host", D.STRING, S.TAG),
    Column("ts", D.TIMESTAMP_MILLISECOND, S.TIMESTAMP),
    Column("seq", D.INT64, S.FIELD),
    Column("ok", D.BOOLEAN, S.FIELD),
]
COLUMNS += [
    Column(name.lower(), getattr(D, name), S.FIELD) for name, _, _ in INTEGER_TYPES
]
COLUMNS += [
    Column("f32", D.FLOAT32, S.FIELD),
    Column("f64", D.FLOAT64, S.FIELD),
    Column("note", D.STRING, S.FIELD),
]
COLUMNS += [
    Column("time_" + unit, getattr(D, "TIMESTAMP_" + unit.upper()), S.FIELD)
    for unit in ["second", "millisecond", "microsecond", "nanosecond"]
]


def row(i):
    values = ["设备,一", BASE_MS + i, i, i % 2 == 0]
    for _, bits, unsigned in INTEGER_TYPES:
        low, high = (
            (0, 2**bits - 1) if unsigned else (-(2 ** (bits - 1)), 2 ** (bits - 1) - 1)
        )
        values.append(low if i % 2 else high)
    values += [i + 0.5, i + 0.25, '中文,"引号"\n换行']
    values += [
        -1,
        BASE_MS + i,
        (BASE_MS + i) * 1000 + 123,
        (BASE_MS + i) * 1000000 + 123456,
    ]
    if i % 17 == 0:
        values[3:] = [None] * (len(values) - 3)
    return values


@pytest.fixture
def query():
    endpoint = os.getenv("GREPTIMEDB_POSTGRES")
    if not endpoint:
        pytest.skip("GREPTIMEDB_POSTGRES is not set")
    uri = (
        endpoint
        if endpoint.startswith(("postgres://", "postgresql://"))
        else "postgresql://" + endpoint
    )
    conn = psycopg2.connect(uri, dbname="public", connect_timeout=5)
    conn.autocommit = True
    yield conn
    conn.close()


@pytest.fixture
def client(query):
    with Client([os.environ["GREPTIMEDB_POSTGRES"]], batch_size=5000) as instance:
        instance.health_check()
        yield instance


@pytest.fixture
def table(query):
    name = "py_copy_it_" + uuid.uuid4().hex
    yield name
    with query.cursor() as cur:
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(name)))


def records(query, statement):
    with query.cursor() as cur:
        cur.execute(statement)
        return cur.fetchall()


def count(query, table):
    return records(
        query, sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table))
    )[0][0]


def test_insert_20k_all_types_creates_table_tags_and_reads_every_row(
    client, query, table
):
    supported = {
        "BOOLEAN",
        "STRING",
        "INT8",
        "INT16",
        "INT32",
        "INT64",
        "UINT8",
        "UINT16",
        "UINT32",
        "UINT64",
        "FLOAT32",
        "FLOAT64",
        "TIMESTAMP_SECOND",
        "TIMESTAMP_MILLISECOND",
        "TIMESTAMP_MICROSECOND",
        "TIMESTAMP_NANOSECOND",
    }
    assert {repr(c.data_type).split(".")[-1] for c in COLUMNS} == supported
    size = 20003  # Four full batches and a nonempty tail.
    assert client.insert(table, COLUMNS, (row(i) for i in range(size))) == size
    described = records(query, sql.SQL("DESC TABLE {}").format(sql.Identifier(table)))
    by_name = {item[0]: item for item in described}
    assert by_name["host"][2] == "PRI" and by_name["host"][5] == "TAG"
    assert by_name["ts"][5] == "TIMESTAMP" and by_name["f64"][5] == "FIELD"
    expressions = [
        sql.SQL("CAST({} AS BIGINT) AS {}").format(
            sql.Identifier(c.name), sql.Identifier("epoch_" + c.name)
        )
        if repr(c.data_type).split(".")[-1].startswith("TIMESTAMP")
        else sql.Identifier(c.name)
        for c in COLUMNS
    ]
    actual = records(
        query,
        sql.SQL("SELECT {} FROM {} ORDER BY seq").format(
            sql.SQL(", ").join(expressions), sql.Identifier(table)
        ),
    )
    assert actual == [tuple(row(i)) for i in range(size)]
    named = dict(zip([c.name for c in COLUMNS], row(size)))
    assert client.insert(table, COLUMNS, [named]) == 1
    assert count(query, table) == size + 1


def test_existing_table_schema_is_preserved(client, query, table):
    with query.cursor() as cur:
        cur.execute(
            sql.SQL(
                "CREATE TABLE {} (host STRING, ts TIMESTAMP(3) TIME INDEX, reading DOUBLE, note STRING, PRIMARY KEY(host))"
            ).format(sql.Identifier(table))
        )
    before = records(query, sql.SQL("DESC TABLE {}").format(sql.Identifier(table)))
    columns = [COLUMNS[0], COLUMNS[1], Column("reading", D.FLOAT64, S.FIELD)]
    assert client.insert(table, columns, [["a", BASE_MS, 1.25]]) == 1
    assert (
        records(query, sql.SQL("DESC TABLE {}").format(sql.Identifier(table))) == before
    )
    assert records(
        query, sql.SQL("SELECT reading, note FROM {}").format(sql.Identifier(table))
    ) == [(1.25, None)]


def test_empty_insert_does_not_create_table(client, query, table):
    assert client.insert(table, COLUMNS, iter(())) == 0
    found = records(
        query,
        sql.SQL(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name={}"
        ).format(sql.Literal(table)),
    )
    assert found == [(0,)]


def test_quoted_identifiers_and_csv_values(client, query, table):
    columns = [
        Column("host id", D.STRING, S.TAG),
        Column("event time", D.TIMESTAMP_NANOSECOND, S.TIMESTAMP),
        Column("select", D.INT64, S.FIELD),
    ]
    values = [['中文,"引号"\n换行', -1, 1], ["other", 1793066400123456789, None]]
    assert client.insert(table, columns, values) == 2
    actual = records(
        query,
        sql.SQL(
            "SELECT {}, CAST({} AS BIGINT) AS epoch_ns, {} FROM {} ORDER BY {}"
        ).format(
            sql.Identifier(columns[0].name),
            sql.Identifier(columns[1].name),
            sql.Identifier(columns[2].name),
            sql.Identifier(table),
            sql.Identifier(columns[1].name),
        ),
    )
    assert actual == [tuple(v) for v in values]


@pytest.mark.parametrize(
    "name",
    [
        "TIMESTAMP_SECOND",
        "TIMESTAMP_MILLISECOND",
        "TIMESTAMP_MICROSECOND",
        "TIMESTAMP_NANOSECOND",
    ],
)
def test_each_time_index_unit_without_tags(client, query, table, name):
    columns = [
        Column("ts", getattr(D, name), S.TIMESTAMP),
        Column("value", D.INT64, S.FIELD),
    ]
    values = [[-1, 1], [0, 2], [1, 3]]
    assert client.insert(table, columns, values) == 3
    assert records(
        query,
        sql.SQL(
            'SELECT CAST(ts AS BIGINT) AS epoch_value, "value" FROM {} ORDER BY ts'
        ).format(sql.Identifier(table)),
    ) == [tuple(v) for v in values]


def test_aware_datetimes_and_nullable_timestamp_fields(client, query, table):
    columns = [
        COLUMNS[0],
        COLUMNS[1],
        Column("seen_at", D.TIMESTAMP_NANOSECOND, S.FIELD),
    ]
    when = datetime(2026, 10, 27, 10, tzinfo=timezone(timedelta(hours=8)))
    assert client.insert(table, columns, [["a", when, None]]) == 1
    assert records(
        query,
        sql.SQL("SELECT CAST(ts AS BIGINT) AS ts_ms, seen_at FROM {}").format(
            sql.Identifier(table)
        ),
    ) == [(BASE_MS, None)]


def test_later_validation_failure_leaves_acknowledged_prefix(query, table):
    with Client([os.environ["GREPTIMEDB_POSTGRES"]], batch_size=2) as client:
        with pytest.raises(GreptimeError) as exc:
            client.insert(
                table,
                COLUMNS[:3],
                [["a", BASE_MS, 0], ["a", BASE_MS + 1, 1], ["a", None, 2]],
            )
        assert exc.value.retriable is False
        assert count(query, table) == 2
        client.health_check()


def test_copy_server_error_is_reported_and_connection_recovers(client, query, table):
    columns = [COLUMNS[0], COLUMNS[1], Column("reading", D.INT64, S.FIELD)]
    assert client.insert(table, columns, [["a", BASE_MS, 1]]) == 1
    wrong = columns[:2] + [Column("reading", D.STRING, S.FIELD)]
    with pytest.raises(GreptimeError) as exc:
        client.insert(table, wrong, [["b", BASE_MS + 1, "not-an-integer"]])
    assert exc.value.retriable is False
    assert count(query, table) == 1
    client.health_check()
    assert client.insert(table, columns, [["b", BASE_MS + 1, 2]]) == 1
    assert count(query, table) == 2


def test_tag_time_keys_and_overwrite(client, query, table):
    columns = COLUMNS[:3]
    assert client.insert(table, columns, [["a", BASE_MS, 1], ["b", BASE_MS, 2]]) == 2
    assert client.insert(table, columns, [["a", BASE_MS, 3]]) == 1
    assert records(
        query,
        sql.SQL("SELECT host, seq FROM {} ORDER BY host").format(sql.Identifier(table)),
    ) == [("a", 3), ("b", 2)]
