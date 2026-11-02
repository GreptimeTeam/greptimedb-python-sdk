"""Both backends consume the one public schema."""

from types import SimpleNamespace
import pytest
import greptimedb_ingester as sdk


def test_postgres_uses_public_columns_directly():
    pytest.importorskip("psycopg2")
    from greptimedb_ingester.postgres import _columns, _row_encoder

    column = sdk.Column(
        "ts", sdk.ColumnDataType.TIMESTAMP_NANOSECOND, sdk.SemanticType.TIMESTAMP
    )
    checked = _columns([column])
    assert checked[0] is column
    assert _row_encoder(checked)([1]) == ["1970-01-01T00:00:00.000000001+00:00"]


def test_grpc_uses_public_schema_and_error():
    pytest.importorskip("greptimedb_ingester_grpc")
    from greptimedb_ingester import grpc
    from greptimedb_ingester_grpc import _native

    for name in ["Column", "ColumnDataType", "SemanticType", "GreptimeError"]:
        assert name not in grpc.__all__
        assert not hasattr(_native, name)
    column = sdk.Column(
        "ts", sdk.ColumnDataType.TIMESTAMP_NANOSECOND, sdk.SemanticType.TIMESTAMP
    )
    assert _native.describe_row([column], [1]) == ["timestamp_nanosecond:1"]
    with pytest.raises(sdk.GreptimeError) as exc:
        _native.describe_row([column], ["invalid"])
    assert exc.value.retriable is False


def test_grpc_rejects_duck_typed_columns():
    pytest.importorskip("greptimedb_ingester_grpc")
    from greptimedb_ingester_grpc._native import describe_row

    column = SimpleNamespace(
        name="ts",
        data_type=sdk.ColumnDataType.TIMESTAMP_NANOSECOND,
        semantic_type=sdk.SemanticType.TIMESTAMP,
        precision=None,
        scale=None,
    )
    with pytest.raises(sdk.GreptimeError, match="Column objects"):
        describe_row([column], [1])
