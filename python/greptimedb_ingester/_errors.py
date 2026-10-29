"""One public error type shared by all installed backends."""

try:
    from greptimedb_ingester_grpc._native import GreptimeError
except ModuleNotFoundError as exc:
    if not exc.name.startswith("greptimedb_ingester_grpc"):
        raise

    class GreptimeError(Exception):
        def __init__(self, message, retriable=False):
            super().__init__(message)
            self.retriable = retriable
