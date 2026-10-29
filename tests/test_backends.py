"""The same public package supports base, grpc, postgres, and combined installs."""

from importlib import metadata, util
import subprocess
import sys
import pytest
from packaging.requirements import Requirement
import greptimedb_ingester as sdk


def test_extras_are_the_only_runtime_dependencies():
    dependencies = [
        Requirement(value) for value in metadata.requires("greptimedb-ingester") or []
    ]

    def selected(extra):
        return {
            req.name
            for req in dependencies
            if req.marker is None or req.marker.evaluate({"extra": extra})
        }

    assert selected("") == set()
    assert selected("grpc") == {"greptimedb-ingester-grpc"}
    assert selected("postgres") == {"psycopg2-binary"}
    native = next(req for req in dependencies if req.name == "greptimedb-ingester-grpc")
    assert sdk.__version__ in native.specifier


def test_public_schema_is_available_without_a_backend():
    column = sdk.Column("host", sdk.ColumnDataType.STRING, sdk.SemanticType.TAG)
    assert column.name == "host"
    assert repr(column.data_type) == "ColumnDataType.STRING"
    assert repr(column.semantic_type) == "SemanticType.TAG"


def test_root_has_no_backend_exports_or_legacy_native_module():
    for name in ["Client", "WriteOptions", "BulkWriter", "Rows", "BulkResponse"]:
        assert not hasattr(sdk, name)
    assert util.find_spec("greptimedb_ingester._native") is None


def test_importing_public_schema_loads_no_backend():
    subprocess.run(
        [
            sys.executable,
            "-c",
            """
import sys
import greptimedb_ingester
assert 'psycopg2' not in sys.modules
assert 'greptimedb_ingester.postgres' not in sys.modules
assert 'greptimedb_ingester.grpc' not in sys.modules
assert 'greptimedb_ingester_grpc' not in sys.modules
""",
        ],
        check=True,
    )


def test_postgres_errors_are_the_public_error_type():
    pytest.importorskip("psycopg2")
    from greptimedb_ingester.postgres import Client

    with pytest.raises(sdk.GreptimeError) as exc:
        Client(["localhost:4003"], batch_size=0)
    assert exc.value.retriable is False


def test_missing_grpc_extra_has_a_clear_error():
    if util.find_spec("greptimedb_ingester_grpc") is not None:
        pytest.skip("gRPC is installed")
    with pytest.raises(ImportError, match=r"\[grpc\]"):
        __import__("greptimedb_ingester.grpc")


def test_missing_postgres_extra_has_a_clear_error():
    if util.find_spec("psycopg2") is not None:
        pytest.skip("Postgres is installed")
    with pytest.raises(ImportError, match=r"\[postgres\]"):
        __import__("greptimedb_ingester.postgres")


def test_grpc_import_does_not_load_the_postgres_driver():
    if util.find_spec("greptimedb_ingester_grpc") is None:
        pytest.skip("gRPC is not installed")
    code = """
import sys
import greptimedb_ingester as sdk
from greptimedb_ingester.grpc import Client
assert not hasattr(sdk, "Client")
assert "psycopg2" not in sys.modules
assert "greptimedb_ingester.postgres" not in sys.modules
"""
    subprocess.run([sys.executable, "-c", code], check=True)
