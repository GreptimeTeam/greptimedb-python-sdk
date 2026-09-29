# greptimedb-ingester

GreptimeDB 的 Python 写入客户端，绑定 [greptimedb-ingester](https://github.com/GreptimeTeam/greptimedb-ingester-rust) 0.19.0。支持 Python 3.10 及以上。调用是同步的。

- `Client.insert`：gRPC 行写入。表不存在时，按请求里的 schema 建表。
- `Client.bulk_writer`：Arrow Flight 批量写入。表必须已经存在。

连接的是 gRPC 端口，默认 `127.0.0.1:4001`。

完整用法见 [docs/usage.md](docs/usage.md)。接口说明见 [docs/api.md](docs/api.md)。

## 安装

从 [GitHub Release](https://github.com/GreptimeTeam/greptimedb-python-sdk/releases) 下载和本机系统匹配的 wheel，然后安装。一个 wheel 可以给该系统上的 Python 3.10、3.11、3.12……使用。

| 系统 | wheel |
| --- | --- |
| Linux x86_64，glibc 2.28 及以上 | `greptimedb_ingester-0.1.0-cp310-abi3-manylinux_2_28_x86_64.whl` |
| Linux aarch64，glibc 2.28 及以上 | `greptimedb_ingester-0.1.0-cp310-abi3-manylinux_2_28_aarch64.whl` |
| Windows x64 | `greptimedb_ingester-0.1.0-cp310-abi3-win_amd64.whl` |
| macOS Apple Silicon | `greptimedb_ingester-0.1.0-cp310-abi3-macosx_11_0_arm64.whl` |

下面用 Linux x86_64 的文件名作例子。换成上表里和本机系统匹配的那个文件即可。

pip：

```bash
pip install greptimedb_ingester-0.1.0-cp310-abi3-manylinux_2_28_x86_64.whl
```

uv：

```bash
uv add greptimedb_ingester-0.1.0-cp310-abi3-manylinux_2_28_x86_64.whl
```

已经有虚拟环境、只想装进当前环境时：

```bash
uv pip install greptimedb_ingester-0.1.0-cp310-abi3-manylinux_2_28_x86_64.whl
```

`requirements.txt` 里写 wheel 的路径：

```text
./greptimedb_ingester-0.1.0-cp310-abi3-manylinux_2_28_x86_64.whl
```

然后：

```bash
pip install -r requirements.txt
```

或：

```bash
uv pip install -r requirements.txt
```

从源码安装需要 Rust 1.85 及以上：

```bash
pip install maturin
maturin build --release
pip install dist/greptimedb_ingester-*.whl
```

## 写入

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

表不存在时，这次 `insert` 会建表。`examples/insert_example.py` 里还有写入已有表的例子。批量写入见 `examples/bulk_example.py`，要先有表。

## 开发

```bash
uv python pin 3.10
uv sync --group dev
uv run maturin develop
uv run pytest
```

`pytest` 只检查值转换，不连接 GreptimeDB。
