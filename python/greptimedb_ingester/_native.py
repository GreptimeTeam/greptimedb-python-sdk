"""Compatibility import for callers of the former native module."""

try:
    from greptimedb_ingester_grpc._native import *  # noqa: F403
except ModuleNotFoundError as exc:
    raise ImportError(
        "Install greptimedb-ingester[grpc] for the native extension"
    ) from exc
