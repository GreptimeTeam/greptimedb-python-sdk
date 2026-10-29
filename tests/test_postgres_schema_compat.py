"""Keep the COPY backend's schema names compatible with gRPC."""

import pytest

pytest.importorskip("greptimedb_ingester_grpc")
from greptimedb_ingester import Column, ColumnDataType, SemanticType
from greptimedb_ingester import _types as pg


def test_copy_schema_enum_names_and_values_match_native():
    for native, pure in [
        (ColumnDataType, pg.ColumnDataType),
        (SemanticType, pg.SemanticType),
    ]:
        assert {name for name in dir(native) if name.isupper()} == set(pure.__members__)
        for name, member in pure.__members__.items():
            assert getattr(native, name) == member.value
            assert repr(getattr(native, name)) == repr(member)


def test_copy_column_shape_matches_native():
    native = Column("ts", ColumnDataType.TIMESTAMP_NANOSECOND, SemanticType.TIMESTAMP)
    pure = pg.Column(
        "ts", pg.ColumnDataType.TIMESTAMP_NANOSECOND, pg.SemanticType.TIMESTAMP
    )
    for field in ["name", "precision", "scale"]:
        assert getattr(native, field) == getattr(pure, field)
    for field in ["data_type", "semantic_type"]:
        assert repr(getattr(native, field)) == repr(getattr(pure, field))


def test_installed_copy_client_accepts_native_columns():
    import pytest

    pytest.importorskip("psycopg2")
    from greptimedb_ingester.postgres import _normalize_columns, _row_encoder

    native = [Column("ts", ColumnDataType.TIMESTAMP_NANOSECOND, SemanticType.TIMESTAMP)]
    assert _row_encoder(_normalize_columns(native))([1]) == [
        "1970-01-01T00:00:00.000000001+00:00"
    ]
