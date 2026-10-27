"""Offline tests for the PostgreSQL COPY backend."""

import csv
import io
import os
import subprocess
import sys
from copy import deepcopy
from datetime import datetime, timedelta, timezone
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


@pytest.fixture
def database(monkeypatch):
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    payloads = []

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
        cur.execute.call_count == 1
    )  # One create-if-absent, then existing-table writes.
    assert "TIME INDEX" in cur.execute.call_args.args[0].as_string(None)
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
        normalized = pg._normalize_columns(cols)
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
    cols = pg._normalize_columns(
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
        pg._normalize_columns([COLUMNS[1], Column("f", dtype, S.FIELD)])
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
    assert attempts == 2 and pulled == [0, 1, 2, 3]


def test_validation_failure_in_later_batch_does_not_resend_prefix(database):
    client = pg.Client(["localhost:4003"], batch_size=1)
    with pytest.raises(GreptimeError):
        client.insert("partial", COLUMNS, [["a", 1, 1], ["a", None, 2]])
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
    import importlib.util

    if importlib.util.find_spec("greptimedb_ingester_grpc") is not None:
        pytest.skip("No-native assertion is exercised by postgres-only CI")
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
    assert cur.execute.call_count == 1


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
    cur.execute.side_effect = [failure, None]
    with pg.Client(["localhost:4003"]) as client:
        with pytest.raises(GreptimeError, match="create permission denied") as exc:
            client.insert("create_failure", COLUMNS, [["a", 0, 1]])
        assert exc.value.retriable is False
        assert exc.value.__cause__ is failure
        assert cur.execute.call_count == 1
        cur.copy_expert.assert_not_called()
        # A failed CREATE must not mark the table as already initialized.
        assert client.insert("create_failure", COLUMNS, [["a", 0, 1]]) == 1
        assert cur.execute.call_count == 2
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
        with pytest.raises(LookupError) as exc:
            client.insert("source_failure", COLUMNS, rows())
        assert exc.value is failure
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
    creates = [call.args[0].as_string(None) for call in cur.execute.call_args_list]
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
