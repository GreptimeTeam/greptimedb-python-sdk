"""Explicit gRPC backend, installed by greptimedb-ingester[grpc]."""

try:
    from greptimedb_ingester_grpc._native import (
        BulkResponse,
        BulkWriter,
        Client,
        Column,
        ColumnDataType,
        GreptimeError,
        Rows,
        SemanticType,
        WriteOptions,
    )
except ModuleNotFoundError as exc:
    raise ImportError("Install greptimedb-ingester[grpc] for gRPC support") from exc

__all__ = [
    "Client",
    "Column",
    "ColumnDataType",
    "SemanticType",
    "GreptimeError",
    "WriteOptions",
    "Rows",
    "BulkWriter",
    "BulkResponse",
]
