"""Offline tests for the PostgreSQL COPY backend."""

import csv
import io
import os
import subprocess
import sys
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from enum import IntEnum, IntFlag
from unittest.mock import MagicMock

import pytest

pytest.importorskip("psycopg2")
from greptimedb_ingester import (
    Column,
    ColumnDataType as D,
    GreptimeError,
    SemanticType as S,
)
from greptimedb_ingester import postgres as pg

COLUMNS = [
    Column("host", D.STRING, S.TAG),
    Column("ts", D.TIMESTAMP_NANOSECOND, S.TIMESTAMP),
    Column("value", D.INT64, S.FIELD),
]


TABLE_SCHEMA = [
    ("host", "String", "PRI", "YES", "", "TAG"),
    ("ts", "TimestampNanosecond", "PRI", "NO", "", "TIMESTAMP"),
    ("value", "Int64", "", "YES", "", "FIELD"),
]


@pytest.fixture
def database(monkeypatch):
    connection = MagicMock(closed=0)
    cursor = connection.cursor.return_value.__enter__.return_value
    payloads = []
    cursor.fetchall.return_value = list(TABLE_SCHEMA)

    def copy(command, data):
        rows = list(csv.reader(io.StringIO(data.read())))
        payloads.append((command, rows))
        cursor.rowcount = len(rows)

    cursor.copy_expert.side_effect = copy
    monkeypatch.setattr(
        pg.psycopg2.extensions,
        "quote_ident",
        lambda text, _: '"' + text.replace('"', '""') + '"',
    )
    connect = MagicMock(return_value=connection)
    monkeypatch.setattr(pg.psycopg2, "connect", connect)
    return connection, cursor, connect, payloads


def test_constructor_is_lazy_and_connection_options_reach_driver(database):
    conn, cur, connect, _ = database
    with pg.Client(
        "postgresql://uri:old@localhost:4003/other?sslmode=require",
        database="public",
        username="user",
        password="secret",
        connect_timeout_secs=3,
    ) as client:
        connect.assert_not_called()
        assert client.database == "public"
        client.health_check()
        client.health_check()
        connect.assert_called_once_with(
            user="user",
            password="secret",
            host="localhost",
            port="4003",
            dbname="public",
            sslmode="require",
            connect_timeout=3,
        )
        assert conn.autocommit is True
        assert cur.execute.call_args.args == ("SELECT 1",)
    conn.close.assert_called_once()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"urls": []},
        {"urls": ["a", "b"]},
        {"urls": [None]},
        {"urls": [""]},
        {"urls": ["bad\x00host"]},
        {"batch_size": 0},
        {"batch_size": True},
        {"batch_size": 1.5},
        {"database": ""},
        {"connect_timeout_secs": -1},
        {"connect_timeout_secs": 0.5},
        {"password": 12},
    ],
)
def test_invalid_connection_options_are_local_errors(kwargs):
    with pytest.raises(GreptimeError) as exc:
        pg.Client(**({"urls": ["127.0.0.1:4003"]} | kwargs))
    assert exc.value.retriable is False


def test_empty_insert_never_connects(database):
    _, _, connect, _ = database
    with pg.Client(["localhost:4003"]) as client:
        assert client.insert("empty", COLUMNS, iter(())) == 0
    connect.assert_not_called()


def test_batches_dict_rows_csv_escaping_and_inputs_are_preserved(database):
    _, cur, _, payloads = database
    rows = [
        ['设备,"一"\n行', 1, 3],
        {"value": None, "host": "b", "ts": -1},
        ("c", 1000000001, 5),
    ]
    before = deepcopy(rows)
    with pg.Client(["localhost:4003"], batch_size=2) as client:
        assert client.insert('table"name', COLUMNS, (r for r in rows)) == 3
        assert client.insert('table"name', COLUMNS, [["d", 2, 6]]) == 1
    assert [len(rows) for _, rows in payloads] == [2, 1, 1]
    assert payloads[0][1][0] == [rows[0][0], "1970-01-01T00:00:00.000000001+00:00", "3"]
    assert payloads[0][1][1] == ["b", "1969-12-31T23:59:59.999999999+00:00", ""]
    assert '"table""name"' in payloads[0][0]
    assert (
        cur.execute.call_count == 2
    )  # One create-and-validate, then cached existing-table writes.
    assert "TIME INDEX" in cur.execute.call_args_list[0].args[0].as_string(None)
    assert rows == before


@pytest.mark.parametrize("batch_size", [1, 2, 3])
def test_bom_at_copy_batch_boundaries_preserves_values(database, batch_size):
    _, cur, _, _ = database
    actual = []

    def copy(command, data):
        # GreptimeDB consumes a UTF-8 BOM at the start of each CSV stream.
        text = data.read().encode("utf-8").decode("utf-8-sig")
        batch = list(csv.reader(io.StringIO(text)))
        actual.extend(batch)
        cur.rowcount = len(batch)

    cur.copy_expert.side_effect = copy
    hosts = [
        "\ufeffvalue",
        "\ufeff",
        "随意\ufeff",
        '\ufeff"quoted",\nline',
        None,
        "value",
        "\ufefftail",
    ]
    rows = [{"host": host, "ts": 0, "value": i} for i, host in enumerate(hosts)]
    before = deepcopy(rows)
    with pg.Client("localhost:4003", batch_size=batch_size) as client:
        assert client.insert("bom_values", COLUMNS, iter(rows)) == len(rows)
    assert [row[0] for row in actual] == [
        host if host is not None else "" for host in hosts
    ]
    assert [row[2] for row in actual] == [str(i) for i in range(len(rows))]
    assert rows == before


@pytest.mark.parametrize(
    "rows",
    [
        [["", 0, 1]],
        [["bad\x00tag", 0, 1]],
        [[123, 0, 1]],
        [["a", None, 1]],
        [["a", True, 1]],
        [["a", 1, True]],
        [["a", 1]],
        [{"host": "a", "ts": 1, "value": 1, "extra": 0}],
        [42],
    ],
)
def test_invalid_first_batch_is_rejected_without_connecting(database, rows):
    _, _, connect, _ = database
    before = deepcopy(rows)
    with pytest.raises(GreptimeError) as exc:
        pg.Client(["localhost:4003"]).insert("bad", COLUMNS, rows)
    assert exc.value.retriable is False
    connect.assert_not_called()
    assert rows == before


@pytest.mark.parametrize(
    "columns",
    [
        [],
        [COLUMNS[0]],
        [COLUMNS[1], COLUMNS[1]],
        [object()],
        [Column("ts", D.INT64, S.TIMESTAMP)],
        [Column("tag", D.INT64, S.TAG), COLUMNS[1]],
    ],
)
def test_invalid_schema_is_rejected_without_connecting(database, columns):
    _, _, connect, _ = database
    with pytest.raises(GreptimeError) as exc:
        pg.Client(["localhost:4003"]).insert("bad", columns, [])
    assert exc.value.retriable is False
    connect.assert_not_called()


@pytest.mark.parametrize(
    "name",
    [
        "BINARY",
        "DATE",
        "TIME_SECOND",
        "TIME_MILLISECOND",
        "TIME_MICROSECOND",
        "TIME_NANOSECOND",
        "DECIMAL128",
        "JSON",
    ],
)
def test_unsupported_copy_types_fail_explicitly(database, name):
    kwargs = {"precision": 10, "scale": 2} if name == "DECIMAL128" else {}
    columns = COLUMNS[:2] + [Column("field", getattr(D, name), S.FIELD, **kwargs)]
    with pytest.raises(GreptimeError, match="does not support") as exc:
        pg.Client(["localhost:4003"]).insert("bad", columns, [])
    assert exc.value.retriable is False
    database[2].assert_not_called()


def test_all_declared_types_have_an_explicit_supported_or_rejected_case():
    declared = {name for name in dir(D) if name.isupper()}
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
    rejected = {
        "BINARY",
        "DATE",
        "TIME_SECOND",
        "TIME_MILLISECOND",
        "TIME_MICROSECOND",
        "TIME_NANOSECOND",
        "DECIMAL128",
        "JSON",
    }
    assert declared == supported | rejected
    for name in supported:
        value = {
            "BOOLEAN": True,
            "STRING": "text",
            "FLOAT32": 1.25,
            "FLOAT64": 1.25,
        }.get(name, 1)
        cols = [COLUMNS[1], Column("field", getattr(D, name), S.FIELD)]
        normalized = pg._columns(cols)
        assert pg._row_encoder(normalized)([0, value])[-1] is not None
        assert pg._row_encoder(normalized)([0, None])[-1] is None


@pytest.mark.parametrize(
    "name,bits,unsigned",
    [
        (f"{prefix}{bits}", bits, prefix == "UINT")
        for prefix in ["INT", "UINT"]
        for bits in [8, 16, 32, 64]
    ],
)
def test_every_integer_width_checks_bounds(name, bits, unsigned):
    low, high = (
        (0, 2**bits - 1) if unsigned else (-(2 ** (bits - 1)), 2 ** (bits - 1) - 1)
    )
    cols = pg._columns(
        COLUMNS[:2] + [Column("value", getattr(D, name), S.FIELD)]
    )
    encode = pg._row_encoder(cols)
    for value in [low, high]:
        assert encode(["a", 0, value])[-1] == value
    for value in [low - 1, high + 1, True, 1.0]:
        with pytest.raises((ValueError, TypeError)):
            encode(["a", 0, value])


@pytest.mark.parametrize("dtype", [D.FLOAT32, D.FLOAT64])
def test_float_validation(dtype):
    encode = pg._row_encoder(
        pg._columns([COLUMNS[1], Column("f", dtype, S.FIELD)])
    )
    for value in [float("inf"), float("nan"), True, "1"]:
        with pytest.raises((TypeError, ValueError)):
            encode([0, value])
    if dtype == D.FLOAT32:
        with pytest.raises(ValueError):
            encode([0, 3.5e38])


@pytest.mark.parametrize(
    "precision,expected",
    [
        (0, "1969-12-31T23:59:59+00:00"),
        (3, "1969-12-31T23:59:59.999+00:00"),
        (6, "1969-12-31T23:59:59.999999+00:00"),
        (9, "1969-12-31T23:59:59.999999999+00:00"),
    ],
)
def test_timestamp_units_keep_negative_integer_precision(precision, expected):
    assert pg._timestamp(-1, precision) == expected


def test_datetime_offsets_precision_and_nanoseconds():
    aware = datetime(
        2026, 10, 27, 10, 0, 0, 123456, tzinfo=timezone(timedelta(hours=8))
    )
    assert pg._timestamp(aware, 9) == "2026-10-27T02:00:00.123456000+00:00"
    assert (
        pg._timestamp(1793066400123456789, 9) == "2026-10-27T02:00:00.123456789+00:00"
    )
    for value in [aware, aware.replace(tzinfo=None)]:
        with pytest.raises(ValueError):
            pg._timestamp(value, 3)


def test_transport_failure_keeps_acknowledged_prefix_and_does_not_retry(database):
    _, cur, _, _ = database
    success = cur.copy_expert.side_effect
    attempts = 0
    pulled = []

    def copy(command, data):
        nonlocal attempts
        attempts += 1
        if attempts == 2:
            raise pg.psycopg2.OperationalError("connection lost")
        success(command, data)

    def rows():
        for i in range(10):
            pulled.append(i)
            yield ["a", i, i]

    cur.copy_expert.side_effect = copy
    with pytest.raises(GreptimeError, match="after 2 acknowledged rows") as exc:
        pg.Client(["localhost:4003"], batch_size=2).insert("partial", COLUMNS, rows())
    assert exc.value.retriable is True
    assert exc.value.acknowledged_rows == 2
    assert attempts == 2 and pulled == [0, 1, 2, 3]


def test_validation_failure_in_later_batch_does_not_resend_prefix(database):
    client = pg.Client(["localhost:4003"], batch_size=1)
    with pytest.raises(GreptimeError, match="after 1 acknowledged rows") as exc:
        client.insert("partial", COLUMNS, [["a", 1, 1], ["a", None, 2]])
    assert exc.value.acknowledged_rows == 1
    assert isinstance(exc.value.__cause__, ValueError)
    assert len(database[3]) == 1


def test_server_error_and_ack_count_mismatch(database):
    _, cur, _, _ = database
    cur.copy_expert.side_effect = pg.psycopg2.DataError("invalid data")
    with pytest.raises(GreptimeError) as exc:
        pg.Client(["localhost:4003"]).insert("bad", COLUMNS, [["a", 0, 1]])
    assert exc.value.retriable is False
    cur.copy_expert.side_effect = None
    cur.rowcount = 0
    with pytest.raises(GreptimeError, match="acknowledged"):
        pg.Client(["localhost:4003"]).insert("bad", COLUMNS, [["a", 0, 1]])


def test_health_errors_and_close_lifecycle(database):
    conn, cur, connect, _ = database
    client = pg.Client(["localhost:4003"])
    cur.execute.side_effect = pg.psycopg2.OperationalError("unavailable")
    with pytest.raises(GreptimeError) as exc:
        client.health_check()
    assert exc.value.retriable is True
    client.close()
    client.close()
    for operation in [
        client.health_check,
        lambda: client.insert("x", COLUMNS, []),
        client.__enter__,
    ]:
        with pytest.raises(GreptimeError, match="closed"):
            operation()
    assert connect.call_count == 1
    assert conn.close.call_count >= 1


def test_context_exit_closes_and_preserves_user_exception(database):
    with pytest.raises(LookupError, match="user error"):
        with pg.Client(["localhost:4003"]) as client:
            client.health_check()
            raise LookupError("user error")
    database[0].close.assert_called_once()


def test_postgres_only_import_has_no_native_dependency():
    code = """
import sys
import greptimedb_ingester
import greptimedb_ingester.postgres
assert "greptimedb_ingester_grpc" not in sys.modules
assert not any(name.endswith("._native") for name in sys.modules)
"""
    subprocess.run([sys.executable, "-c", code], check=True, env=os.environ.copy())


@pytest.mark.parametrize(
    "batch_size,size,expected",
    [
        (1, 3, [1, 1, 1]),
        (3, 3, [3]),
        (3, 4, [3, 1]),
        (8, 3, [3]),
        (None, 10001, [10000, 1]),
    ],
)
def test_batch_boundaries_preserve_row_order_and_acknowledged_count(
    database, batch_size, size, expected
):
    _, cur, connect, payloads = database
    options = {} if batch_size is None else {"batch_size": batch_size}
    rows = (("a", i, i) for i in range(size))
    with pg.Client(["localhost:4003"], **options) as client:
        assert client.insert("batch_edges", COLUMNS, rows) == size
    assert [len(batch) for _, batch in payloads] == expected
    assert [int(row[2]) for _, batch in payloads for row in batch] == list(range(size))
    connect.assert_called_once()
    assert cur.execute.call_count == 2


@pytest.mark.parametrize("table", [None, "", 42, "bad\x00name"])
def test_invalid_table_does_not_consume_rows_or_connect(database, table):
    _, _, connect, _ = database
    consumed = []

    def rows():
        consumed.append(True)
        yield ["a", 0, 1]

    with pg.Client(["localhost:4003"]) as client:
        with pytest.raises(GreptimeError) as exc:
            client.insert(table, COLUMNS, rows())
    assert exc.value.retriable is False
    assert consumed == []
    connect.assert_not_called()


@pytest.mark.parametrize("operation", ["health_check", "insert"])
def test_initial_connection_failure_is_not_retried_and_next_call_can_connect(
    database, operation
):
    conn, _, connect, payloads = database
    failure = pg.psycopg2.OperationalError("connection refused")
    connect.side_effect = [failure, conn]
    with pg.Client(["localhost:4003"]) as client:
        with pytest.raises(GreptimeError, match="connection refused") as exc:
            if operation == "health_check":
                client.health_check()
            else:
                client.insert("connect_failure", COLUMNS, [["a", 0, 1]])
        assert exc.value.retriable is True
        assert exc.value.__cause__ is failure
        assert connect.call_count == 1
        assert payloads == []
        # A new user call can connect; the failed call itself is never retried.
        assert client.insert("connect_failure", COLUMNS, [["a", 0, 1]]) == 1
        assert connect.call_count == 2
        assert len(payloads) == 1


def test_failed_table_creation_is_repeated_only_on_a_new_call(database):
    _, cur, connect, payloads = database
    failure = pg.psycopg2.ProgrammingError("create permission denied")
    cur.execute.side_effect = [failure, None, None]
    with pg.Client(["localhost:4003"]) as client:
        with pytest.raises(GreptimeError, match="create permission denied") as exc:
            client.insert("create_failure", COLUMNS, [["a", 0, 1]])
        assert exc.value.retriable is False
        assert exc.value.__cause__ is failure
        assert cur.execute.call_count == 1
        cur.copy_expert.assert_not_called()
        # A failed CREATE must not mark the table as already initialized.
        assert client.insert("create_failure", COLUMNS, [["a", 0, 1]]) == 1
        assert cur.execute.call_count == 3
        assert len(payloads) == 1
    connect.assert_called_once()


@pytest.mark.parametrize("yielded,acknowledged", [(1, 0), (3, 2)])
def test_generator_error_keeps_completed_batches_and_client_is_reusable(
    database, yielded, acknowledged
):
    _, _, connect, payloads = database
    failure = LookupError("source reader failed")

    def rows():
        for i in range(yielded):
            yield ["a", i, i]
        raise failure

    with pg.Client(["localhost:4003"], batch_size=2) as client:
        with pytest.raises(
            GreptimeError, match=f"after {acknowledged} acknowledged rows"
        ) as exc:
            client.insert("source_failure", COLUMNS, rows())
        assert exc.value.__cause__ is failure
        assert exc.value.acknowledged_rows == acknowledged
        assert exc.value.retriable is False
        assert [int(row[2]) for _, batch in payloads for row in batch] == list(
            range(acknowledged)
        )
        if acknowledged == 0:
            connect.assert_not_called()
        # The incomplete batch is never sent, and the client remains usable.
        assert client.insert("source_failure", COLUMNS, [["a", 100, 100]]) == 1
        assert sum(len(batch) for _, batch in payloads) == acknowledged + 1
    connect.assert_called_once()


def test_different_tables_keep_their_own_schema_and_encoder(database):
    _, cur, connect, payloads = database
    text_columns = COLUMNS[:2] + [Column("value", D.STRING, S.FIELD)]
    cur.fetchall.side_effect = [
        TABLE_SCHEMA,
        TABLE_SCHEMA[:2] + [("value", "String", "", "YES", "", "FIELD")],
    ]
    with pg.Client(["localhost:4003"]) as client:
        assert client.insert("numbers", COLUMNS, [["a", 0, 1]]) == 1
        assert client.insert("strings", text_columns, [["a", 1, 'text,"value"']]) == 1
        assert client.insert("numbers", COLUMNS, [["a", 2, 3]]) == 1
    assert [batch[0][2] for _, batch in payloads] == ["1", 'text,"value"', "3"]
    assert [command.split()[1] for command, _ in payloads] == [
        '"numbers"',
        '"strings"',
        '"numbers"',
    ]
    statements = [call.args[0].as_string(None) for call in cur.execute.call_args_list]
    creates = [
        statement for statement in statements if statement.startswith("CREATE TABLE")
    ]
    assert len(creates) == 2
    assert '"numbers"' in creates[0] and '"value" BIGINT' in creates[0]
    assert '"strings"' in creates[1] and '"value" STRING' in creates[1]
    connect.assert_called_once()


def test_closing_unused_client_does_not_create_a_connection(database):
    conn, _, connect, _ = database
    client = pg.Client(["localhost:4003"])
    client.close()
    client.close()
    with pytest.raises(GreptimeError, match="closed"):
        client.health_check()
    with pytest.raises(GreptimeError, match="closed"):
        client.insert("closed", COLUMNS, [["a", 0, 1]])
    connect.assert_not_called()
    conn.close.assert_not_called()


@pytest.mark.parametrize(
    "endpoint,options,expected",
    [
        ("postgresql://localhost:4003/production", {}, "production"),
        ("postgresql://localhost:4003/production", {"database": None}, "production"),
        ("postgresql://localhost:4003/production", {"database": "public"}, "public"),
        ("localhost:4003", {}, "public"),
        ("postgresql://localhost:4003/", {}, "public"),
        ("localhost:4003", {"database": "production"}, "production"),
    ],
)
def test_database_uses_explicit_argument_then_uri_then_default(
    database, endpoint, options, expected
):
    _, _, connect, _ = database
    with pg.Client(endpoint, **options) as client:
        assert client.database == expected
        client.health_check()
    assert connect.call_args.kwargs["dbname"] == expected


def test_broken_copy_connection_reopens_on_next_call_without_resending(database):
    conn, cur, connect, payloads = database
    healthy = MagicMock(closed=0)
    healthy_cur = healthy.cursor.return_value.__enter__.return_value
    healthy_cur.rowcount = 1
    healthy_cur.fetchall.return_value = TABLE_SCHEMA
    recovered_payloads = []
    healthy_cur.copy_expert.side_effect = lambda command, data: (
        recovered_payloads.extend(csv.reader(io.StringIO(data.read())))
    )
    connect.side_effect = [conn, healthy]
    success = cur.copy_expert.side_effect
    pulled = []

    def copy(command, data):
        if payloads:
            conn.closed = 2
            raise pg.psycopg2.OperationalError("connection lost")
        success(command, data)

    def rows():
        for i in range(3):
            pulled.append(i)
            yield ["a", i, i]

    cur.copy_expert.side_effect = copy
    with pg.Client(["localhost:4003"], batch_size=1) as client:
        with pytest.raises(GreptimeError, match="after 1 acknowledged rows") as exc:
            client.insert("recover", COLUMNS, rows())
        assert exc.value.retriable is True
        assert connect.call_count == 1
        assert pulled == [0, 1]
        assert len(payloads) == 1
        assert client.insert("recover", COLUMNS, [["a", 10, 10]]) == 1
        assert connect.call_count == 2
        assert [row[2] for row in recovered_payloads] == ["10"]
        # A new connection must also recheck the table, which may have been lost
        # in a server restart. Neither the failed batch nor its tail is replayed.
        assert healthy_cur.execute.call_count == 2
        healthy_cur.fetchall.assert_called_once()
        assert "CREATE TABLE IF NOT EXISTS" in healthy_cur.execute.call_args_list[
            0
        ].args[0].as_string(None)
    conn.close.assert_called_once()
    healthy.close.assert_called_once()


def test_health_check_reconnects_after_an_established_connection_breaks(database):
    conn, cur, connect, _ = database
    healthy = MagicMock(closed=0)
    connect.side_effect = [conn, healthy]

    def disconnect(*args):
        conn.closed = 2
        raise pg.psycopg2.OperationalError("connection lost")

    with pg.Client(["localhost:4003"]) as client:
        client.health_check()
        cur.execute.side_effect = disconnect
        with pytest.raises(GreptimeError) as exc:
            client.health_check()
        assert exc.value.retriable is True
        assert connect.call_count == 1
        client.health_check()
        assert connect.call_count == 2
    healthy.cursor.return_value.__enter__.return_value.execute.assert_called_once_with(
        "SELECT 1"
    )


@pytest.mark.parametrize("enum_type", [IntEnum, IntFlag])
@pytest.mark.parametrize(
    "dtype", [D.INT8, D.INT16, D.INT32, D.INT64, D.UINT8, D.UINT16, D.UINT32, D.UINT64]
)
def test_integer_enum_encodes_as_builtin_int(dtype, enum_type):
    state = enum_type("State", {"OK": 1})
    columns = pg._columns([COLUMNS[1], Column("value", dtype, S.FIELD)])
    encoded = pg._row_encoder(columns)([0, state.OK])
    assert type(encoded[1]) is int
    assert encoded[1] == 1
    buffer = io.StringIO()
    csv.writer(buffer).writerow(encoded)
    assert list(csv.reader(io.StringIO(buffer.getvalue())))[0][1] == "1"


@pytest.mark.parametrize(
    "description",
    [
        TABLE_SCHEMA[1:],
        [("host", "String", "", "YES", "", "FIELD")] + TABLE_SCHEMA[1:],
        [
            TABLE_SCHEMA[0],
            ("ts", "TimestampMillisecond", "PRI", "NO", "", "TIMESTAMP"),
            TABLE_SCHEMA[2],
        ],
        [
            TABLE_SCHEMA[0],
            ("ts", "TimestampNanosecond", "", "YES", "", "FIELD"),
            TABLE_SCHEMA[2],
        ],
        TABLE_SCHEMA[:2] + [("value", "String", "", "YES", "", "FIELD")],
    ],
    ids=["missing-column", "tag", "precision", "time-index", "data-type"],
)
def test_schema_mismatch_never_copies_or_caches_and_can_be_rechecked(
    database, description
):
    _, cur, _, payloads = database
    cur.fetchall.return_value = description
    with pg.Client("localhost:4003") as client:
        for _ in range(2):
            with pytest.raises(GreptimeError, match="schema mismatch") as exc:
                client.insert("existing", COLUMNS, [["a", 0, 1]])
            assert exc.value.retriable is False
            assert "existing" not in client._schemas
        assert cur.fetchall.call_count == 2
        cur.copy_expert.assert_not_called()
        assert payloads == []
        cur.fetchall.return_value = TABLE_SCHEMA
        assert client.insert("existing", COLUMNS, [["a", 0, 1]]) == 1
        assert cur.fetchall.call_count == 3
        assert len(payloads) == 1


def test_schema_validation_matches_by_name_and_allows_extra_columns(database):
    _, cur, _, _ = database
    cur.fetchall.return_value = list(reversed(TABLE_SCHEMA)) + [
        ("extra", "String", "", "YES", "", "FIELD")
    ]
    with pg.Client("localhost:4003", batch_size=1) as client:
        assert client.insert("existing", COLUMNS, [["a", 0, 1], ["b", 1, 2]]) == 2
        assert client.insert("existing", COLUMNS, [["c", 2, 3]]) == 1
    # Metadata is checked only once for an unchanged schema and connection.
    cur.fetchall.assert_called_once()


def test_schema_query_failure_does_not_cache_and_next_call_rechecks(database):
    _, cur, _, _ = database
    failure = pg.psycopg2.ProgrammingError("metadata unavailable")
    cur.fetchall.side_effect = [failure, TABLE_SCHEMA]
    with pg.Client("localhost:4003") as client:
        with pytest.raises(GreptimeError, match="metadata unavailable"):
            client.insert("existing", COLUMNS, [["a", 0, 1]])
        assert "existing" not in client._schemas
        cur.copy_expert.assert_not_called()
        assert client.insert("existing", COLUMNS, [["a", 0, 1]]) == 1
    assert cur.fetchall.call_count == 2


def test_changed_request_schema_is_revalidated_before_copy(database):
    _, cur, _, payloads = database
    with pg.Client("localhost:4003") as client:
        assert client.insert("existing", COLUMNS, [["a", 0, 1]]) == 1
        wrong = COLUMNS[:2] + [Column("value", D.STRING, S.FIELD)]
        with pytest.raises(GreptimeError, match="schema mismatch"):
            client.insert("existing", wrong, [["b", 1, "002"]])
        assert "existing" not in client._schemas
        assert len(payloads) == 1
        assert client.insert("existing", COLUMNS, [["b", 1, 2]]) == 1
        assert cur.fetchall.call_count == 3


@pytest.mark.parametrize("row_kind", ["list", "dict"])
@pytest.mark.parametrize("batch_size", [1, 2, 5])
def test_generator_reusing_mutable_row_is_snapshotted_before_advance(
    database, row_kind, batch_size
):
    _, _, _, payloads = database
    shared = ["a", 0, 0] if row_kind == "list" else {"host": "a", "ts": 0, "value": 0}

    def rows():
        for i in range(7):
            if row_kind == "list":
                shared[1:] = [i, i]
            else:
                shared.update(ts=i, value=i)
            yield shared

    with pg.Client("localhost:4003", batch_size=batch_size) as client:
        assert client.insert("reused", COLUMNS, rows()) == 7
    actual = [row for _, batch in payloads for row in batch]
    assert [int(row[2]) for row in actual] == list(range(7))
    assert len({row[1] for row in actual}) == 7


@pytest.mark.parametrize(
    "failure",
    [
        ValueError("bad value"),
        TypeError("bad row"),
        OverflowError("too large"),
        LookupError("reader failed"),
    ],
)
@pytest.mark.parametrize("yielded,acknowledged", [(1, 0), (3, 2), (4, 4)])
def test_input_errors_report_only_fully_acknowledged_batches(
    database, failure, yielded, acknowledged
):
    def rows():
        for i in range(yielded):
            yield ["a", i, i]
        raise failure

    with pg.Client("localhost:4003", batch_size=2) as client:
        with pytest.raises(
            GreptimeError, match=f"after {acknowledged} acknowledged rows"
        ) as exc:
            client.insert("input_error", COLUMNS, rows())
        assert exc.value.acknowledged_rows == acknowledged
        assert exc.value.__cause__ is failure
        assert exc.value.retriable is False
    assert sum(len(batch) for _, batch in database[3]) == acknowledged


def test_invalid_iterator_reports_zero_acknowledged_rows(database):
    class BrokenSource:
        def __iter__(self):
            raise LookupError("cannot start")

    with pytest.raises(GreptimeError, match="after 0 acknowledged rows") as exc:
        pg.Client("localhost:4003").insert("broken", COLUMNS, BrokenSource())
    assert exc.value.acknowledged_rows == 0
    assert isinstance(exc.value.__cause__, LookupError)
    database[2].assert_not_called()


@pytest.mark.parametrize("failed_batch", [1, 2])
@pytest.mark.parametrize("reported_rows", [-1, 0, 1, 3])
def test_ack_count_mismatch_reports_prefix_and_uncertain_batch(
    database, failed_batch, reported_rows
):
    _, cur, _, _ = database
    successful_copy = cur.copy_expert.side_effect
    attempts = 0
    consumed = []

    def copy(command, data):
        nonlocal attempts
        attempts += 1
        successful_copy(command, data)
        if attempts == failed_batch:
            cur.rowcount = reported_rows

    def rows():
        for i in range(6):
            consumed.append(i)
            yield ["a", i, i]

    cur.copy_expert.side_effect = copy
    acknowledged = (failed_batch - 1) * 2
    with pg.Client("localhost:4003", batch_size=2) as client:
        with pytest.raises(GreptimeError) as exc:
            client.insert("ack_mismatch", COLUMNS, rows())
        assert exc.value.acknowledged_rows == acknowledged
        assert f"after {acknowledged} acknowledged rows" in str(exc.value)
        assert f"Expected 2 acknowledged rows, received {reported_rows}" in str(
            exc.value
        )
        assert "current batch outcome is uncertain" in str(exc.value)
        assert exc.value.retriable is False
        assert attempts == failed_batch
        assert consumed == list(range(failed_batch * 2))
        cur.copy_expert.side_effect = successful_copy
        assert client.insert("ack_mismatch", COLUMNS, [["a", 10, 10]]) == 1
