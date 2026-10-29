# greptimedb-ingester

[English](README.md)

GreptimeDB 同步 Python 写入客户端，支持 Python 3.10+。

## 安装

```bash
uv add "greptimedb-ingester[postgres]"       # PostgreSQL COPY
uv add "greptimedb-ingester[grpc]"           # gRPC
uv add "greptimedb-ingester[grpc,postgres]"  # 两种后端
```

也可用 `pip install` 安装相同依赖，写入 `requirements.txt` 时不加引号。
至少选择一种后端；`[postgres]` 使用 psycopg2，不依赖 Rust SDK。

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

`insert` 自动创建不存在的表并分批 COPY，默认每批 10,000 行。
各批独立提交。服务端须支持 `COPY FROM STDIN`。
完整用法见 [COPY 示例](examples/postgres_example.py)。

## gRPC

```python
from greptimedb_ingester.grpc import Client

client = Client(["127.0.0.1:4001"], database="public")
# client.insert(table, columns, rows)
```

支持 `insert`、`delete` 和 `bulk_writer`，详见 [使用指南](docs/usage.md) 和 [示例](examples)。
从对应后端导入 `Client`，公共 schema 类型从 `greptimedb_ingester` 导入。

## 开发

```bash
uv sync --python 3.10 --locked --extra postgres
uv run --extra postgres pytest
```

开发两种后端时，将 `--extra postgres` 换成 `--all-extras`，需要 Rust 1.85+。
集成测试与发布流程见 [CI 配置](.github/workflows/wheels.yml)。
