"""Synchronous PostgreSQL COPY client, without a Rust or gRPC dependency."""

import csv
import io
import math
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import lru_cache
from itertools import islice

try:
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extensions import parse_dsn
except ModuleNotFoundError as exc:
    raise ImportError("Install greptimedb-ingester[postgres] for COPY support") from exc


from ._errors import GreptimeError


def _error(message, retriable=False, *, acknowledged_rows=None):
    error = GreptimeError(message)
    error.retriable = retriable
    if acknowledged_rows is not None:
        error.acknowledged_rows = acknowledged_rows
    return error


class _SemanticType(Enum):
    TAG = "TAG"
    FIELD = "FIELD"
    TIMESTAMP = "TIMESTAMP"


class _DataType(Enum):
    BOOLEAN = "BOOLEAN"
    INT8 = "TINYINT"
    INT16 = "SMALLINT"
    INT32 = "INT"
    INT64 = "BIGINT"
    UINT8 = "TINYINT UNSIGNED"
    UINT16 = "SMALLINT UNSIGNED"
    UINT32 = "INT UNSIGNED"
    UINT64 = "BIGINT UNSIGNED"
    FLOAT32 = "FLOAT"
    FLOAT64 = "DOUBLE"
    STRING = "STRING"
    TIMESTAMP_SECOND = "TIMESTAMP(0)"
    TIMESTAMP_MILLISECOND = "TIMESTAMP(3)"
    TIMESTAMP_MICROSECOND = "TIMESTAMP(6)"
    TIMESTAMP_NANOSECOND = "TIMESTAMP(9)"


_UNITS = {
    _DataType.TIMESTAMP_SECOND: 0,
    _DataType.TIMESTAMP_MILLISECOND: 3,
    _DataType.TIMESTAMP_MICROSECOND: 6,
    _DataType.TIMESTAMP_NANOSECOND: 9,
}
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True)
class _Column:
    name: str
    data_type: _DataType
    semantic_type: _SemanticType

    def __post_init__(self):
        if not isinstance(self.name, str) or not self.name or "\x00" in self.name:
            raise ValueError("Column name must be a nonempty string without NUL")
        if not isinstance(self.data_type, _DataType):
            raise TypeError("Invalid column data type")
        if not isinstance(self.semantic_type, _SemanticType):
            raise TypeError("Invalid column semantic type")
        if (
            self.semantic_type == _SemanticType.TIMESTAMP
            and self.data_type not in _UNITS
        ):
            raise ValueError("Time index must have a timestamp data type")
        if (
            self.semantic_type == _SemanticType.TAG
            and self.data_type != _DataType.STRING
        ):
            raise ValueError("This client supports STRING tags")


def _normalize_columns(columns):
    result = []
    for column in columns:
        try:
            name = repr(column.data_type).split(".")[-1]
            column_name = column.name
            semantic_name = repr(column.semantic_type).split(".")[-1]
        except AttributeError as exc:
            raise TypeError("columns must contain Column objects") from exc
        try:
            dtype = _DataType[name]
        except KeyError:
            raise ValueError(f"PostgreSQL COPY does not support {name}") from None
        try:
            semantic = _SemanticType[semantic_name]
        except KeyError:
            raise ValueError(f"Invalid column semantic type: {semantic_name}") from None
        result.append(_Column(column_name, dtype, semantic))
    return _columns(result)


def _columns(columns):
    columns = tuple(columns)
    if not columns or any(not isinstance(c, _Column) for c in columns):
        raise ValueError("columns must contain Column objects")
    if len({c.name for c in columns}) != len(columns):
        raise ValueError("Duplicate column names")
    if sum(c.semantic_type == _SemanticType.TIMESTAMP for c in columns) != 1:
        raise ValueError("Exactly one time index is required")
    return columns


def _validate_schema(columns, description):
    actual = {row[0]: (row[1], row[5]) for row in description}
    for column in columns:
        found = actual.get(column.name)
        expected_type = column.data_type.name.replace("_", "")
        expected_semantic = column.semantic_type.value
        if found is None:
            raise _error(f"Table schema mismatch: missing column {column.name!r}")
        if found[0].upper() != expected_type or found[1] != expected_semantic:
            raise _error(
                f"Table schema mismatch for column {column.name!r}: "
                f"expected {column.data_type.name} {expected_semantic}, "
                f"found {found[0]} {found[1]}"
            )


def _table(table):
    if not isinstance(table, str) or not table or "\x00" in table:
        raise ValueError("table must be a nonempty string without NUL")
    # Treat as a single identifier. The database comes from Client(database=...).
    return sql.Identifier(table)


def _second_text(seconds):
    # Convert each timestamp directly; no input-data cache or locality assumptions.
    return (_EPOCH + timedelta(seconds=seconds)).isoformat(timespec="seconds")[:-6]


@lru_cache(maxsize=4)
def _timestamp_encoder(precision):
    unit = 10**precision
    divisor = 10 ** max(6 - precision, 0)
    multiplier = 10 ** max(precision - 6, 0)
    fraction_format = f"0{precision}d"

    def encode(value):
        if type(value) is int:
            units = value
        elif isinstance(value, datetime):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("datetime must include timezone information")
            delta = value.astimezone(timezone.utc) - _EPOCH
            micros = (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds
            if micros % divisor:
                raise ValueError("Timestamp precision would lose data")
            units = micros // divisor * multiplier
        elif isinstance(value, int) and not isinstance(value, bool):
            units = value
        else:
            raise TypeError(
                "Timestamp must be an integer in the column's unit or an aware datetime"
            )
        seconds, fraction = divmod(units, unit)
        text = _second_text(seconds)
        return (
            text
            + ("." + format(fraction, fraction_format) if precision else "")
            + "+00:00"
        )

    return encode


def _timestamp(value, precision):
    return _timestamp_encoder(precision)(value)


def _column_encoder(column):
    # Select types and calculate bounds once per schema, not for each field.
    dtype = column.data_type
    if dtype in _UNITS:
        timestamp = _timestamp_encoder(_UNITS[dtype])
        required = column.semantic_type == _SemanticType.TIMESTAMP

        def encode(value):
            if value is None:
                if required:
                    raise ValueError("Time index cannot be NULL")
                return None
            return timestamp(value)

        return encode
    if dtype == _DataType.STRING:

        def encode(value):
            if value is None:
                return None
            if not isinstance(value, str):
                raise TypeError(f"{column.name}: expected str")
            # Quoted empty strings also become NULL in this CSV path.
            if value == "" or "\x00" in value:
                raise ValueError(
                    f"{column.name}: empty strings/NUL are unsupported by this COPY path; use None for NULL"
                )
            return value

        return encode
    if dtype == _DataType.BOOLEAN:

        def encode(value):
            if value is None:
                return None
            if not isinstance(value, bool):
                raise TypeError(f"{column.name}: expected bool")
            return "true" if value else "false"

        return encode
    if dtype.name.startswith(("INT", "UINT")):
        unsigned = dtype.name.startswith("UINT")
        bits = int(dtype.name[4:] if unsigned else dtype.name[3:])
        lower, upper = (
            (0, 2**bits) if unsigned else (-(2 ** (bits - 1)), 2 ** (bits - 1))
        )
        name = dtype.name

        def encode(value):
            if value is None:
                return None
            if type(value) is not int and (
                not isinstance(value, int) or isinstance(value, bool)
            ):
                raise TypeError(f"{column.name}: expected int")
            if not lower <= value < upper:
                raise ValueError(f"{column.name}: integer outside {name} range")
            # Python 3.10 IntEnum/IntFlag stringify as names, not CSV numbers.
            return value if type(value) is int else int(value)

        return encode
    if dtype in (_DataType.FLOAT32, _DataType.FLOAT64):
        limit = 3.4028234663852886e38 if dtype == _DataType.FLOAT32 else math.inf

        def encode(value):
            if value is None:
                return None
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise TypeError(f"{column.name}: expected float")
            value = float(value)
            if not math.isfinite(value) or abs(value) > limit:
                raise ValueError(f"{column.name}: float outside supported range")
            return value

        return encode
    raise TypeError(f"Unsupported type: {dtype}")


@lru_cache(maxsize=128)
def _row_encoder(columns):
    converters = tuple(_column_encoder(c) for c in columns)
    names = tuple(c.name for c in columns)
    keys = frozenset(names)
    width = len(columns)

    def encode(row):
        # Avoid the Mapping ABC check for the common positional row path.
        if not isinstance(row, (list, tuple)):
            if not isinstance(row, Mapping):
                raise TypeError("Each row must be a list, tuple, or mapping")
            if set(row) != keys:
                raise ValueError("Row keys must exactly match the column names")
            row = [row[name] for name in names]
        if len(row) != width:
            raise ValueError("Row length must match column count")
        return [convert(value) for convert, value in zip(converters, row)]

    return encode


def _encode_row(columns, row):
    return _row_encoder(tuple(columns))(row)


class Client:
    """Lazy, single-connection writer. Calls are serialized, without retries."""

    def __init__(
        self,
        urls,
        database=None,
        username=None,
        password=None,
        *,
        batch_size=10000,
        connect_timeout_secs=5,
    ):
        if isinstance(urls, str):
            urls = [urls]
        if not isinstance(urls, (list, tuple)) or len(urls) != 1:
            raise _error(
                "Provide exactly one PostgreSQL endpoint (typically port 4003)"
            )
        if (
            isinstance(batch_size, bool)
            or not isinstance(batch_size, int)
            or batch_size <= 0
        ):
            raise _error("batch_size must be a positive integer")
        if database is not None and (
            not isinstance(database, str) or not database or "\x00" in database
        ):
            raise _error("database must be a nonempty string without NUL")
        if (
            isinstance(connect_timeout_secs, bool)
            or not isinstance(connect_timeout_secs, int)
            or connect_timeout_secs < 0
        ):
            raise _error("connect_timeout_secs must be a nonnegative integer")
        for name, value in (("username", username), ("password", password)):
            if value is not None and (not isinstance(value, str) or "\x00" in value):
                raise _error(f"{name} must be a string without NUL")
        endpoint = urls[0]
        if not isinstance(endpoint, str) or not endpoint or "\x00" in endpoint:
            raise _error("endpoint must be a nonempty string without NUL")
        if not endpoint.startswith(("postgres://", "postgresql://")):
            endpoint = "postgresql://" + endpoint
        try:
            self._params = parse_dsn(endpoint)
        except psycopg2.Error as exc:
            raise _error(str(exc)) from exc
        if database is None:
            database = self._params.get("dbname") or "public"
        self._params.update(dbname=database, connect_timeout=connect_timeout_secs)
        if username is not None:
            self._params["user"] = username
        if password is not None:
            self._params["password"] = password
        self.database = database
        self.batch_size = batch_size
        self._conn = None
        self._closed = False
        self._lock = threading.RLock()
        self._schemas = {}

    def _connection(self):
        if self._closed:
            raise _error("Client is closed")
        if self._conn is not None and self._conn.closed:
            # Reconnect only on a new call, never by replaying a failed batch.
            self._conn.close()
            self._conn = None
            self._schemas.clear()
        if self._conn is None:
            self._conn = psycopg2.connect(**self._params)
            self._conn.autocommit = True
        return self._conn

    def health_check(self):
        with self._lock:
            try:
                with self._connection().cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            except psycopg2.Error as exc:
                raise _error(
                    str(exc),
                    isinstance(
                        exc, (psycopg2.OperationalError, psycopg2.InterfaceError)
                    ),
                ) from exc

    def insert(self, table, columns, rows):
        """Create if absent and COPY batches; return acknowledged row count.

        Batches are independently acknowledged. Failure may leave partial writes;
        neither validation nor transport failures are retried automatically.
        """
        try:
            return self._insert(table, _normalize_columns(columns), rows)
        except (ValueError, TypeError, OverflowError) as exc:
            raise _error(
                f"Input failed after 0 acknowledged rows: {exc}", acknowledged_rows=0
            ) from exc

    def _insert(self, table, columns, rows):
        encode_row = _row_encoder(columns)
        identifier = _table(table)
        command = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv)").format(
            identifier, sql.SQL(", ").join(sql.Identifier(c.name) for c in columns)
        )
        copy_command = None
        try:
            iterator = iter(rows)
        except Exception as exc:
            raise _error(
                f"Input failed after 0 acknowledged rows: {exc}", acknowledged_rows=0
            ) from exc
        affected = 0
        with self._lock:
            if self._closed:
                raise _error("Client is closed")
            while True:
                try:
                    # Snapshot each encoded row before advancing a source that
                    # may reuse the same mutable list or dict for its next row.
                    batch = [
                        encode_row(row) for row in islice(iterator, self.batch_size)
                    ]
                except Exception as exc:
                    raise _error(
                        f"Input failed after {affected} acknowledged rows: {exc}",
                        acknowledged_rows=affected,
                    ) from exc
                if not batch:
                    return affected
                buffer = io.StringIO(newline="")
                writer = csv.writer(buffer)
                first_row = batch[0]
                # The CSV decoder strips a BOM at the start of each COPY stream.
                # Quote that row so a leading U+FEFF remains part of the value.
                if isinstance(first_row[0], str) and first_row[0].startswith("\ufeff"):
                    csv.writer(buffer, quoting=csv.QUOTE_ALL).writerow(first_row)
                else:
                    writer.writerow(first_row)
                for row in islice(batch, 1, None):
                    writer.writerow(row)
                buffer.seek(0)
                try:
                    conn = self._connection()
                    with conn.cursor() as cur:
                        if self._schemas.get(table) != columns:
                            self._schemas.pop(table, None)
                            definitions = [
                                sql.SQL("{} {}").format(
                                    sql.Identifier(c.name), sql.SQL(c.data_type.value)
                                )
                                for c in columns
                            ]
                            time_col = next(
                                c
                                for c in columns
                                if c.semantic_type == _SemanticType.TIMESTAMP
                            )
                            definitions.append(
                                sql.SQL("TIME INDEX ({})").format(
                                    sql.Identifier(time_col.name)
                                )
                            )
                            tags = [
                                sql.Identifier(c.name)
                                for c in columns
                                if c.semantic_type == _SemanticType.TAG
                            ]
                            if tags:
                                definitions.append(
                                    sql.SQL("PRIMARY KEY ({})").format(
                                        sql.SQL(", ").join(tags)
                                    )
                                )
                            cur.execute(
                                sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
                                    identifier, sql.SQL(", ").join(definitions)
                                )
                            )
                            cur.execute(sql.SQL("DESC TABLE {}").format(identifier))
                            _validate_schema(columns, cur.fetchall())
                            self._schemas[table] = columns
                        if copy_command is None:
                            copy_command = command.as_string(conn)
                        cur.copy_expert(copy_command, buffer)
                        if cur.rowcount != len(batch):
                            raise _error(
                                f"Expected {len(batch)} acknowledged rows, received {cur.rowcount}"
                            )
                except psycopg2.Error as exc:
                    raise _error(
                        f"COPY failed after {affected} acknowledged rows; current batch may be partial: {exc}",
                        isinstance(
                            exc, (psycopg2.OperationalError, psycopg2.InterfaceError)
                        ),
                        acknowledged_rows=affected,
                    ) from exc
                affected += len(batch)

    def close(self):
        with self._lock:
            if self._conn is not None:
                self._conn.close()
            self._closed = True

    def __enter__(self):
        if self._closed:
            raise _error("Client is closed")
        return self

    def __exit__(self, *_):
        self.close()


__all__ = ["Client"]
