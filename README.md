# greptimedb-ingester

[中文](README.zh-CN.md)

Python write client for GreptimeDB, binding [greptimedb-ingester](https://github.com/GreptimeTeam/greptimedb-ingester-rust) 0.19.0. Python 3.10 and later. Calls are synchronous.

- `Client.insert`: gRPC row writes. If the table does not exist, GreptimeDB creates it from the schema in the request.
- `Client.bulk_writer`: Arrow Flight bulk writes. The table must already exist.

The client connects to the gRPC port, `127.0.0.1:4001` by default.

See [docs/usage.md](docs/usage.md) for the full guide and [docs/api.md](docs/api.md) for the API reference.

## Install

pip:

```bash
pip install greptimedb-ingester
```

uv:

```bash
uv add greptimedb-ingester
```

`requirements.txt`:

```text
greptimedb-ingester
```

```bash
pip install -r requirements.txt
```

or:

```bash
uv pip install -r requirements.txt
```

The installer downloads the wheel for the local OS and CPU. One wheel covers Python 3.10, 3.11, 3.12, and later on that platform. You do not download a wheel yourself.

Supported architectures:

- Linux x86_64, glibc 2.28 or later
- Linux aarch64, glibc 2.28 or later
- Windows x64
- macOS arm64 (Apple Silicon)

Published wheels:

| Platform | wheel |
| --- | --- |
| Linux x86_64, glibc 2.28 or later | `greptimedb_ingester-0.1.0-cp310-abi3-manylinux_2_28_x86_64.whl` |
| Linux aarch64, glibc 2.28 or later | `greptimedb_ingester-0.1.0-cp310-abi3-manylinux_2_28_aarch64.whl` |
| Windows x64 | `greptimedb_ingester-0.1.0-cp310-abi3-win_amd64.whl` |
| macOS Apple Silicon | `greptimedb_ingester-0.1.0-cp310-abi3-macosx_11_0_arm64.whl` |

Building from source requires Rust 1.85 or later:

```bash
pip install maturin
maturin build --release
pip install dist/greptimedb_ingester-*.whl
```

## Write

```python
from datetime import datetime, timezone

from greptimedb_ingester import Client, Column, ColumnDataType, SemanticType

client = Client(["127.0.0.1:4001"], database="public")

affected = client.insert(
    "sensor_data",
    columns=[
        Column("device_id", ColumnDataType.STRING, SemanticType.TAG),
        Column("ts", ColumnDataType.TIMESTAMP_MILLISECOND, SemanticType.TIMESTAMP),
        Column("temperature", ColumnDataType.FLOAT64, SemanticType.FIELD),
    ],
    rows=[
        ["device_001", datetime(2009, 2, 13, 23, 31, 30, tzinfo=timezone.utc), 23.5],
        {"device_id": "device_002", "ts": 1_234_567_890_001, "temperature": 24.0},
    ],
)
print(affected)
```

If `sensor_data` does not exist, this `insert` creates it. `examples/insert_example.py` also writes into a table that already exists. Bulk writes are in `examples/bulk_example.py`; the table must already exist.

## Development

```bash
uv python pin 3.10
uv sync --group dev
uv run maturin develop
uv run pytest
```

`pytest` checks value conversion only. It does not connect to GreptimeDB.
