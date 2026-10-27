# greptimedb-ingester

[中文](README.zh-CN.md)

Synchronous Python write client for GreptimeDB. Python 3.10+.

## Install

```bash
uv add "greptimedb-ingester[postgres]"       # PostgreSQL COPY
uv add "greptimedb-ingester[grpc]"           # gRPC
uv add "greptimedb-ingester[grpc,postgres]"  # Both
```

Or use `pip install` with the same requirements. In `requirements.txt`, omit quotes.
Choose at least one backend. `[postgres]` uses psycopg2 without the Rust SDK.

## PostgreSQL COPY

```python
from greptimedb_ingester import Column, ColumnDataType as D, SemanticType as S
from greptimedb_ingester.postgres import Client

columns = [
    Column("host", D.STRING, S.TAG),
    Column("ts", D.TIMESTAMP_MILLISECOND, S.TIMESTAMP),
    Column("reading", D.FLOAT64, S.FIELD),
]
with Client(["127.0.0.1:4003"], batch_size=5000) as client:
    affected = client.insert("sensor_data", columns, [
        ["edge", 1793066400000, 23.5],
    ])
print(affected)
```

`insert` creates missing tables and writes COPY batches (default: 10,000 rows).
Batches commit separately. The server must support `COPY FROM STDIN`.
See the [API reference](docs/api.md#postgresql-copy-后端) for supported types and timestamp rules.

## gRPC

```python
from greptimedb_ingester.grpc import Client

client = Client(["127.0.0.1:4001"], database="public")
# client.insert(table, columns, rows)
```

Supports `insert`, `delete`, and `bulk_writer`. See [usage](docs/usage.md) and [examples](examples).
With both backends installed, the root `Client` defaults to gRPC; use the explicit
imports above to select a backend.

## Development

```bash
uv sync --python 3.10 --locked --extra postgres
uv run --extra postgres pytest
```

For both backends, use `--all-extras` instead of `--extra postgres` (requires Rust 1.85+).
See [testing and release](docs/testing.md) for integration tests and publishing.
