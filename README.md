# greptimedb-ingester

Python bindings for the [GreptimeDB Rust ingester](https://github.com/GreptimeTeam/greptimedb-ingester-rust) 0.19.0. The package supports Python 3.10 and later. Dependencies are managed with [uv](https://docs.astral.sh/uv/).

The binding exposes the two write paths of the Rust client:

- `Client.insert` sends a row insert over gRPC. GreptimeDB can create the table from the request schema.
- `Client.bulk_writer` streams Arrow batches over Flight `DoPut`. The table must already exist unless `auto_create_table=True` and the server allows it.

Calls are synchronous. The binding runs the Rust client's async work on a Tokio runtime and releases the GIL while waiting. A bulk writer stays on one background thread because the underlying response stream is not `Send`.

## Install

From a checkout:

```bash
uv python pin 3.10
uv sync --group dev
uv run maturin develop
```

## Insert

Integer timestamps are already in the column's unit. `datetime` values are absolute instants; a naive `datetime` is treated as UTC. `Decimal` values are scaled by the column scale. An `int` passed to a decimal column is the scaled coefficient.

```python
from greptimedb_ingester import Client, Column, ColumnDataType, SemanticType

client = Client(["127.0.0.1:4001"], database="public", username="user", password="pass")

affected = client.insert(
    "sensor_data",
    columns=[
        Column("device_id", ColumnDataType.STRING, SemanticType.TAG),
        Column("ts", ColumnDataType.TIMESTAMP_MILLISECOND, SemanticType.TIMESTAMP),
        Column("temperature", ColumnDataType.FLOAT64, SemanticType.FIELD),
    ],
    rows=[
        ["device_001", 1_234_567_890_000, 23.5],
        {"device_id": "device_002", "ts": 1_234_567_890_001, "temperature": 24.0},
    ],
)
print(affected)
```

`hints` is an optional list of `(key, value)` pairs, matching `Database.insert_with_hints`.

`delete` takes the same columns and rows and sends them as key columns. `JSON2` columns are rejected there; delete by the key columns instead.

## Bulk write

```python
from greptimedb_ingester import Client, Column, ColumnDataType, SemanticType, WriteOptions

client = Client(["127.0.0.1:4001"], database="public")
columns = [
    Column("ts", ColumnDataType.TIMESTAMP_MILLISECOND, SemanticType.TIMESTAMP),
    Column("device_id", ColumnDataType.STRING, SemanticType.FIELD),
    Column("temperature", ColumnDataType.FLOAT64, SemanticType.FIELD),
]

with client.bulk_writer(
    "sensor_readings",
    columns,
    options=WriteOptions(compression="zstd", parallelism=8, timeout_secs=60),
    auto_create_table=False,
) as writer:
    rows = writer.alloc_rows(10_000)
    rows.add_row([1_234_567_890_000, "device_001", 23.5])
    request_ids = writer.write_async(rows)
    responses = writer.wait_all()
```

`write` submits the batch and waits. `write_async` returns one request id per time window. `finish` runs when the context manager exits.

## Errors

Failures raise `GreptimeError`. `error.retriable` is false for invalid input and true for most transport failures, following the Rust client's `Error::is_retriable`.

## Development

```bash
uv run pytest
```

The default tests cover value conversion and do not need a running GreptimeDB.
