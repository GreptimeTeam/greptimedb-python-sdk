"""Schema validation has no dependency on the Rust package."""

from dataclasses import FrozenInstanceError
from types import SimpleNamespace
import pytest
from greptimedb_ingester._types import (
    Column,
    ColumnDataType as D,
    SemanticType as S,
    GreptimeError,
)


@pytest.mark.parametrize(
    "args,kwargs",
    [
        (("", D.STRING, S.TAG), {}),
        (("bad\x00name", D.STRING, S.TAG), {}),
        (("name", "STRING", S.TAG), {}),
        (("name", D.STRING, "TAG"), {}),
        (("price", D.DECIMAL128, S.FIELD), {}),
        (("price", D.DECIMAL128, S.FIELD), {"precision": 0, "scale": 0}),
        (("price", D.DECIMAL128, S.FIELD), {"precision": 39, "scale": 0}),
        (("price", D.DECIMAL128, S.FIELD), {"precision": 10, "scale": -1}),
        (("price", D.DECIMAL128, S.FIELD), {"precision": 10, "scale": 11}),
        (("price", D.FLOAT64, S.FIELD), {"precision": 10, "scale": 2}),
    ],
)
def test_invalid_column(args, kwargs):
    with pytest.raises(GreptimeError) as exc:
        Column(*args, **kwargs)
    assert exc.value.retriable is False


def test_column_is_immutable_and_decimal_metadata_is_preserved():
    column = Column("price", D.DECIMAL128, S.FIELD, precision=10, scale=2)
    assert column.precision == 10 and column.scale == 2
    with pytest.raises(FrozenInstanceError):
        column.name = "changed"


def test_postgres_requires_public_column_objects():
    pytest.importorskip("psycopg2")
    from greptimedb_ingester.postgres import _columns

    column = SimpleNamespace(
        name="ts", data_type=D.TIMESTAMP_NANOSECOND, semantic_type=S.TIMESTAMP
    )
    with pytest.raises(ValueError, match="Column objects"):
        _columns([column])
    public = Column("ts", D.TIMESTAMP_NANOSECOND, S.TIMESTAMP)
    assert _columns([public]) == (public,)


def test_schema_enums_have_names_and_are_not_integer_values():
    assert str(D.STRING) == "ColumnDataType.STRING"
    assert f"{D.STRING}" == "ColumnDataType.STRING"
    assert str(S.TAG) == "SemanticType.TAG"
    assert f"{S.TAG}" == "SemanticType.TAG"
    assert len({D.BOOLEAN, S.TAG, 0, False}) == 3
    assert not isinstance(D.STRING, int)
    assert D.STRING.value == 12
