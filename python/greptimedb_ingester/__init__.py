"""Pure Python facade. Choose optional backends with [grpc] and [postgres]."""

from importlib.util import find_spec

from ._errors import GreptimeError
from ._types import Column, ColumnDataType, SemanticType

__version__ = "0.1.2"
__all__ = [
    "Client",
    "Column",
    "ColumnDataType",
    "SemanticType",
    "GreptimeError",
    "__version__",
]

if find_spec("greptimedb_ingester_grpc") is not None:
    # Preserve the existing gRPC Client and native schema classes when installed.
    from .grpc import (
        BulkResponse,
        BulkWriter,
        Client,
        Column,
        ColumnDataType,
        Rows,
        SemanticType,
        WriteOptions,
    )

    __all__ += ["BulkResponse", "BulkWriter", "Rows", "WriteOptions"]
elif find_spec("psycopg2") is not None:
    from .postgres import Client
else:

    class Client:
        def __init__(self, *args, **kwargs):
            raise ImportError(
                "Install greptimedb-ingester[grpc] or greptimedb-ingester[postgres]"
            )
