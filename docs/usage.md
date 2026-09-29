# greptimedb-ingester 使用说明

GreptimeDB 的 Python 写入客户端。支持 Python 3.10 及以上。调用是同步的。

两条写入路径：

- `Client.insert`：gRPC 行写入。服务端可以按请求里的 schema 自动建表。
- `Client.bulk_writer`：Arrow Flight 批量写入。不会建表，表必须已经存在。

gRPC 端口默认是 `127.0.0.1:4001`，不是 HTTP 的 `4000`。

## 安装

按自己的系统安装对应的 wheel。同一个操作系统和 CPU 上的 Python 3.10 及以上共用一个 wheel。

```bash
pip install greptimedb_ingester-0.1.0-cp310-abi3-manylinux_2_28_x86_64.whl
```

装完之后：

```python
import greptimedb_ingester
print(greptimedb_ingester.__version__)
```

从源码装时，机器上要有 Rust 1.85+：

```bash
pip install maturin
maturin build --release
pip install dist/greptimedb_ingester-*.whl
```

## 连接

```python
from greptimedb_ingester import Client

client = Client(
    ["127.0.0.1:4001"],
    database="public",
    username="user",          # 和 password 要么都写，要么都不写
    password="pass",
    timeout_secs=30,          # gRPC 请求超时，秒
    connect_timeout_secs=5,   # 建连超时，秒
    send_compression="zstd",  # 可选 "gzip" 或 "zstd"
    accept_compression="zstd",
    tls_server_ca="ca.pem",   # 三个 TLS 路径要么都写，要么都不写
    tls_client_cert="client.pem",
    tls_client_key="client.key",
)

client.health_check()
```

多个 `Client` 各自有一套 gRPC 连接。它们共用进程里的一个 Tokio runtime。`Client` 没有 `close`。进程 `fork` 之后，子进程里不要再使用 fork 之前已经调用过的客户端；先 fork，再在子进程里第一次调用。

## 行写入

整数时间戳按列的单位解释。`datetime` 是绝对时间，没有时区的 `datetime` 当成 UTC。`Decimal` 按列的 scale 缩放。传给 decimal 列的 `int` 是已经缩放好的系数。

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
print(affected)  # 写入行数
```

一行可以是 list、tuple，或按列名给的 dict。列顺序以 `columns` 为准。

`examples/insert_example.py` 写两种情况：表不存在时由 `insert` 建表，以及表已经存在时再 `insert`。时间索引只有 `ts_ms`，其余时间戳类型是普通字段。

`hints` 是可选的 `(key, value)` 列表，对应服务端的 `x-greptime-hints`。例如跳过 WAL：

```python
client.insert(
    "sensor_data",
    columns=columns,
    rows=rows,
    hints=[("insert_skip_wal", "true")],
)
```

`delete` 用同一套列和行，把它们当成 key 列删掉。

```python
client.delete("sensor_data", columns, [["device_001", 1_234_567_890_000, 23.5]])
```

## 批量写入

`bulk` 不会建表。先用 `insert` 把表建出来，再批量写入。

```python
from datetime import datetime, timezone

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
) as writer:
    rows = writer.alloc_rows(10_000)
    rows.add_rows([
        [datetime(2009, 2, 13, 23, 31, 30, tzinfo=timezone.utc), "device_001", 23.5],
        {"ts": 1_234_567_890_001, "device_id": "device_002", "temperature": 24.0},
    ])
    request_ids = writer.write_async(rows)
    responses = writer.wait_all()
    for response in responses:
        print(response.request_id, response.affected_rows)
```

`examples/bulk_example.py` 先 `insert` 建表，再批量写入。

`add_rows` 一次传入一批。`add_row` 一次一行。`write` 提交并等待这一批的 ack。`write_async` 先提交，返回每个时间窗口一个 request id，之后用 `wait(request_id)` 或 `wait_all()` 收结果。`with` 退出时会 `finish`。也可以手动调用 `writer.finish()`。

`WriteOptions` 默认压缩 `lz4`，并行度 `4`，超时 `60` 秒。压缩还可以是 `"none"` 或 `"zstd"`。

## 列类型

`Column(name, data_type, semantic_type)`。`semantic_type` 是 `SemanticType.TAG`、`SemanticType.TIMESTAMP` 或 `SemanticType.FIELD`。一张表需要恰好一个时间戳列。

`ColumnDataType`：

| 值 | Python 值 |
|---|---|
| `BOOLEAN` | `bool` |
| `INT8` `INT16` `INT32` `INT64` | `int` |
| `UINT8` `UINT16` `UINT32` `UINT64` | `int` |
| `FLOAT32` `FLOAT64` | `float` 或 `int` |
| `STRING` | `str` |
| `BINARY` | `bytes` |
| `DATE` | 距 Unix epoch 的天数，或 `datetime.date` |
| `TIMESTAMP_SECOND` `TIMESTAMP_MILLISECOND` `TIMESTAMP_MICROSECOND` `TIMESTAMP_NANOSECOND` | 该单位下的整数，或 `datetime.datetime` |
| `TIME_SECOND` `TIME_MILLISECOND` `TIME_MICROSECOND` `TIME_NANOSECOND` | 从午夜起、按列单位计数的 `int` |
| `DECIMAL128` | 构造时要给 `precision` 和 `scale`。`Decimal` 按 scale 缩放，`int` 是系数 |
| `JSON` | JSON 字符串，或 `dict` / `list` |

`TIMESTAMP` 是绝对时间。`datetime.datetime` 表示一个时刻，按列的单位换算成 Unix 纪元偏移；`int` 则已经是这个单位下的偏移。没有时区的 `datetime` 当成 UTC。`datetime` 只有微秒，写到纳秒列时末三位是 0。

`TIME` 是一天之内的时刻，存的是从午夜起的秒、毫秒、微秒或纳秒，不是绝对时间。所以只接受这个整数，不接受 `datetime` 或 `datetime.time`。

`None` 写成 SQL NULL。时间戳列不能是 NULL。

每种类型一个值。四种 `TIMESTAMP` 都能当时间戳列，一张表里只放其中一个。

```python
from datetime import date, datetime, timezone
from decimal import Decimal

from greptimedb_ingester import Column, ColumnDataType, SemanticType

when = datetime(2009, 2, 13, 23, 31, 30, tzinfo=timezone.utc)
examples = [
    (Column("ok", ColumnDataType.BOOLEAN, SemanticType.FIELD), True),
    (Column("i8", ColumnDataType.INT8, SemanticType.FIELD), -8),
    (Column("i16", ColumnDataType.INT16, SemanticType.FIELD), -16),
    (Column("i32", ColumnDataType.INT32, SemanticType.FIELD), -32),
    (Column("i64", ColumnDataType.INT64, SemanticType.FIELD), -64),
    (Column("u8", ColumnDataType.UINT8, SemanticType.FIELD), 8),
    (Column("u16", ColumnDataType.UINT16, SemanticType.FIELD), 16),
    (Column("u32", ColumnDataType.UINT32, SemanticType.FIELD), 32),
    (Column("u64", ColumnDataType.UINT64, SemanticType.FIELD), 64),
    (Column("f32", ColumnDataType.FLOAT32, SemanticType.FIELD), 1.5),
    (Column("f64", ColumnDataType.FLOAT64, SemanticType.FIELD), 23.5),
    (Column("payload", ColumnDataType.BINARY, SemanticType.FIELD), b"\xde\xad"),
    (Column("host", ColumnDataType.STRING, SemanticType.FIELD), "edge"),
    (Column("day", ColumnDataType.DATE, SemanticType.FIELD), date(2009, 2, 13)),
    (Column("ts_s", ColumnDataType.TIMESTAMP_SECOND, SemanticType.TIMESTAMP), when),
    (Column("ts_ms", ColumnDataType.TIMESTAMP_MILLISECOND, SemanticType.TIMESTAMP), when),
    (Column("ts_us", ColumnDataType.TIMESTAMP_MICROSECOND, SemanticType.TIMESTAMP), when),
    (Column("ts_ns", ColumnDataType.TIMESTAMP_NANOSECOND, SemanticType.TIMESTAMP), when),
    (Column("clock_s", ColumnDataType.TIME_SECOND, SemanticType.FIELD), 3661),
    (Column("clock_ms", ColumnDataType.TIME_MILLISECOND, SemanticType.FIELD), 3_661_000),
    (Column("clock_us", ColumnDataType.TIME_MICROSECOND, SemanticType.FIELD), 3_661_000_000),
    (Column("clock_ns", ColumnDataType.TIME_NANOSECOND, SemanticType.FIELD), 3_661_000_000_000),
    (Column("price", ColumnDataType.DECIMAL128, SemanticType.FIELD, precision=10, scale=2), Decimal("12.34")),
    (Column("attrs", ColumnDataType.JSON, SemanticType.FIELD), {"a": 1}),
]
```

## 错误

失败抛 `GreptimeError`。`error.retriable` 为 `False` 表示参数不合法，重试没用。为 `True` 表示多数传输失败，可以再试。

```python
from greptimedb_ingester import GreptimeError

try:
    client.insert("sensor_data", columns, rows)
except GreptimeError as err:
    if err.retriable:
        ...
    raise
```

## 示例

- `examples/insert_example.py`：行写入。表不存在时自动建表，以及写入已有表
- `examples/bulk_example.py`：批量写入已有表
- `examples/bench_write.py`：对本机 GreptimeDB 测写入速度

```bash
uv run python examples/insert_example.py
uv run python examples/bulk_example.py
uv run python examples/bench_write.py --seconds 8
```
