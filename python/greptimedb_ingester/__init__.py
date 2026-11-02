"""Shared schema for the explicit gRPC and PostgreSQL backends."""

from ._errors import GreptimeError
from ._types import Column, ColumnDataType, SemanticType

__version__ = "0.1.2"
__all__ = ["Column", "ColumnDataType", "SemanticType", "GreptimeError", "__version__"]
