"""Input validation and conversion regressions, without a database."""

from datetime import datetime, timezone, tzinfo
from decimal import Decimal

import pytest

pytest.importorskip("greptimedb_ingester_grpc")
from greptimedb_ingester import Column, ColumnDataType as D, GreptimeError, SemanticType as S
from greptimedb_ingester.grpc import Client, WriteOptions
from greptimedb_ingester_grpc._native import describe_row


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf"), 1e30, -1.0])
@pytest.mark.parametrize("option", ["timeout_secs", "connect_timeout_secs", "write_timeout"])
def test_invalid_timeout_is_a_nonretriable_error(value, option):
    with pytest.raises(GreptimeError, match="timeout") as exc:
        if option == "write_timeout":
            WriteOptions(timeout_secs=value)
        else:
            Client(["127.0.0.1:4001"], **{option: value})
    assert exc.value.retriable is False


@pytest.mark.parametrize("changes", [
    {"precision": None}, {"scale": None}, {"precision": 256},
    {"precision": True}, {"scale": -1}, {"scale": 11},
    {"name": ""}, {"name": "bad\x00name"},
    {"data_type": S.FIELD}, {"semantic_type": D.INT8},
])
@pytest.mark.parametrize("bulk", [False, True])
def test_forged_schema_is_rejected_without_panicking(changes, bulk):
    column = Column("price", D.DECIMAL128, S.FIELD, precision=10, scale=2)
    for name, value in changes.items():
        object.__setattr__(column, name, value)
    with pytest.raises(GreptimeError) as exc:
        describe_row([column], [Decimal("1.00")], bulk=bulk)
    assert exc.value.retriable is False


@pytest.mark.parametrize("operation", ["insert", "delete", "bulk_writer"])
def test_forged_schema_is_rejected_at_every_client_entry(operation):
    column = Column("price", D.DECIMAL128, S.FIELD, precision=10, scale=2)
    object.__setattr__(column, "scale", None)
    client = Client(["127.0.0.1:1"], timeout_secs=0.01, connect_timeout_secs=0.01)
    with pytest.raises(GreptimeError, match="decimal") as exc:
        if operation == "bulk_writer":
            client.bulk_writer("t", [column])
        else:
            getattr(client, operation)("t", [column], [[1]])
    assert exc.value.retriable is False


@pytest.mark.parametrize("bulk", [False, True])
@pytest.mark.parametrize("dtype,expected", [
    (D.TIMESTAMP_SECOND, -1), (D.TIMESTAMP_MILLISECOND, -1),
    (D.TIMESTAMP_MICROSECOND, -1), (D.TIMESTAMP_NANOSECOND, -1000),
])
def test_pre_epoch_datetime_rounds_down(dtype, expected, bulk):
    column = Column("ts", dtype, S.TIMESTAMP)
    value = datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)
    assert describe_row([column], [value], bulk=bulk) == [f"{dtype.name.lower()}:{expected}"]


@pytest.mark.parametrize("bulk", [False, True])
@pytest.mark.parametrize("year", [1, 2500, 9999])
@pytest.mark.parametrize("dtype,multiplier", [
    (D.TIMESTAMP_SECOND, 1), (D.TIMESTAMP_MILLISECOND, 1000),
    (D.TIMESTAMP_MICROSECOND, 1000000),
])
def test_datetime_range_depends_on_column_unit(year, dtype, multiplier, bulk):
    value = datetime(year, 1, 1, tzinfo=timezone.utc)
    delta = value - datetime(1970, 1, 1, tzinfo=timezone.utc)
    expected = delta.days * 86400 * multiplier
    column = Column("ts", dtype, S.TIMESTAMP)
    assert describe_row([column], [value], bulk=bulk) == [f"{dtype.name.lower()}:{expected}"]


@pytest.mark.parametrize("bulk", [False, True])
def test_datetime_nanoseconds_still_reject_overflow(bulk):
    column = Column("ts", D.TIMESTAMP_NANOSECOND, S.TIMESTAMP)
    with pytest.raises(GreptimeError, match="out of range"):
        describe_row([column], [datetime(2500, 1, 1, tzinfo=timezone.utc)], bulk=bulk)


class NaiveTimezone(tzinfo):
    def utcoffset(self, dt):
        return None


@pytest.mark.parametrize("bulk", [False, True])
def test_datetime_with_no_offset_follows_naive_utc_rule(bulk):
    column = Column("ts", D.TIMESTAMP_SECOND, S.TIMESTAMP)
    value = datetime(1970, 1, 1, tzinfo=NaiveTimezone())
    assert describe_row([column], [value], bulk=bulk) == ["timestamp_second:0"]


@pytest.mark.parametrize("bulk", [False, True])
@pytest.mark.parametrize("value,expected", [
    (Decimal("12.340"), 1234), (Decimal("-12.34000"), -1234),
    (Decimal("0E+1000000"), 0), (Decimal("0E-1000000"), 0),
    (Decimal("0E+9999999999"), 0), (Decimal("0E-9999999999"), 0),
    (Decimal("1." + "0" * 100), 100),
    (Decimal("99.99"), 9999), (Decimal("-99.99"), -9999),
    (9999, 9999),
])
def test_decimal_exact_scale_and_zero(value, expected, bulk):
    column = Column("price", D.DECIMAL128, S.FIELD, precision=4, scale=2)
    assert describe_row([column], [value], bulk=bulk) == [f"decimal128:{expected}"]


@pytest.mark.parametrize("bulk", [False, True])
@pytest.mark.parametrize("value", [Decimal("100.00"), Decimal("-100.00"), 10000, -10000, 2**127 - 1])
def test_decimal_enforces_declared_precision(value, bulk):
    column = Column("price", D.DECIMAL128, S.FIELD, precision=4, scale=2)
    with pytest.raises(GreptimeError, match="precision") as exc:
        describe_row([column], [value], bulk=bulk)
    assert exc.value.retriable is False


@pytest.mark.parametrize("value", [{"x": object()}, {"x": float("nan")}, {"x": Decimal("1")}])
def test_json_serialization_errors_use_public_error(value):
    column = Column("json", D.JSON, S.FIELD)
    with pytest.raises(GreptimeError, match="JSON") as exc:
        describe_row([column], [value])
    assert exc.value.retriable is False


def test_circular_json_uses_public_error():
    value = []
    value.append(value)
    with pytest.raises(GreptimeError, match="JSON"):
        describe_row([Column("json", D.JSON, S.FIELD)], [value])


@pytest.mark.parametrize("columns,rows,message", [
    ([], [[1]], "delete requires at least one column"),
    ([Column("ts", D.TIMESTAMP_SECOND, S.TIMESTAMP)], [], "cannot delete an empty row batch"),
    ([Column("v", D.INT64, S.FIELD)], [[1]], "FIELD"),
])
def test_delete_rejects_invalid_input_before_connecting(columns, rows, message):
    client = Client(["127.0.0.1:1"], timeout_secs=0.01, connect_timeout_secs=0.01)
    with pytest.raises(GreptimeError, match=message) as exc:
        client.delete("t", columns, rows)
    assert exc.value.retriable is False


@pytest.mark.parametrize("value", [D.INT8, S.TAG])
def test_schema_enums_are_not_integer_cells(value):
    with pytest.raises(GreptimeError, match="unsupported Python value"):
        describe_row([Column("n", D.INT64, S.FIELD)], [value])


def test_error_fallback_retains_original_message_and_retriable(monkeypatch):
    import sys

    monkeypatch.setitem(sys.modules, "greptimedb_ingester._errors", None)
    with pytest.raises(RuntimeError, match="timeout_secs") as exc:
        WriteOptions(timeout_secs=float("nan"))
    assert exc.value.retriable is False
