# API

`greptimedb_ingester` 的公开对象。值怎么对应列类型见 [usage.md](usage.md)。

```python
import greptimedb_ingester
greptimedb_ingester.__version__  # "0.1.0"
```

## Client

```text
Client(
    urls,
    database="public",
    username=None,
    password=None,
    timeout_secs=None,
    connect_timeout_secs=None,
    send_compression=None,
    accept_compression=None,
    tls_server_ca=None,
    tls_client_cert=None,
    tls_client_key=None,
)
```

构造时不连接 GreptimeDB。`urls` 是 gRPC 地址列表，例如 `["127.0.0.1:4001"]`，不能为空。

| 参数 | 说明 |
| --- | --- |
| `database` | 数据库名，默认 `"public"` |
| `username`、`password` | 一起写，或一起不写 |
| `timeout_secs` | 单次 gRPC 调用超时，秒。不写则用 Rust client 的默认值 |
| `connect_timeout_secs` | 建连超时，秒 |
| `send_compression`、`accept_compression` | `"gzip"` 或 `"zstd"` |
| `tls_server_ca`、`tls_client_cert`、`tls_client_key` | 三个路径一起写，或一起不写 |

`client.database` 返回数据库名。没有 `close`。

### health_check

```text
health_check() -> None
```

连上 gRPC 并做健康检查。失败抛 `GreptimeError`。

### insert

```text
insert(table, columns, rows, hints=None) -> int
```

行写入。表不存在时，GreptimeDB 按 `columns` 建表。返回写入行数。

`rows` 是多行。每一行是 list、tuple，或按列名给的 dict。列顺序以 `columns` 为准。

`hints` 是 `(key, value)` 列表，对应请求头 `x-greptime-hints`。例如 `[("insert_skip_wal", "true")]`。

### delete

```text
delete(table, columns, rows) -> int
```

按 `columns` 和 `rows` 删除。返回删除行数。行的写法和 `insert` 相同。

### bulk_writer

```text
bulk_writer(table, columns, options=None) -> BulkWriter
```

打开一个批量写入。表必须已经存在。`columns` 不能为空。`options` 默认是 `WriteOptions()`。

## BulkWriter

`with client.bulk_writer(...) as writer` 正常退出时调用 `finish()`。发生异常时也会 `finish()`，结束失败不会盖住原来的异常。

| 属性 | 说明 |
| --- | --- |
| `table_name` | 表名 |
| `columns` | 构造时传入的列 |

### alloc_rows

```text
alloc_rows(capacity) -> Rows
```

分配一个行缓冲。`capacity` 是预留行数，不是必须写满的行数。

### write

```text
write(rows) -> BulkResponse
```

提交 `rows` 并等待这一批的 ack。`rows` 在提交后被清空。空缓冲会失败。`rows` 必须由这个 writer 的 `alloc_rows` 创建。

### write_async

```text
write_async(rows) -> list[int]
```

提交 `rows`，不等待 ack。返回 request id 列表。一次提交若被按时间窗口拆开，会有多个 id。之后用 `wait` 或 `wait_all` 收结果。

### wait

```text
wait(request_id) -> BulkResponse
```

等待一个 request id。

### wait_all

```text
wait_all() -> list[BulkResponse]
```

等待当前还没返回的全部请求。

### flush

```text
flush() -> list[BulkResponse]
```

取回已经完成、还没被收走的响应。不等待仍在进行的请求。

### finish

```text
finish() -> None
```

结束写入并关掉流。结束后再调用写入方法会失败。对象被回收时也会结束。

### finish_with_responses

```text
finish_with_responses() -> list[BulkResponse]
```

结束写入，并返回收尾时还没被收走的响应。

## Rows

由 `BulkWriter.alloc_rows` 创建，不能自己构造。

```text
add_row(row) -> None
add_rows(rows) -> int
```

`add_row` 追加一行。`add_rows` 一次追加一个可迭代对象里的全部行，返回追加的行数。某一行不合法时，这一批都不会追加。

`len(rows)` 是当前缓冲里的行数。

一行是 list、tuple，或按列名给的 dict。

## WriteOptions

```text
WriteOptions(compression="lz4", timeout_secs=60.0, parallelism=4)
```

| 参数 | 说明 |
| --- | --- |
| `compression` | `"lz4"`、`"zstd"` 或 `"none"` |
| `timeout_secs` | 批量写入超时，秒，必须 `>= 0` |
| `parallelism` | 在途请求数，必须 `>= 1` |

三个字段都可以读：`options.compression`、`options.timeout_secs`、`options.parallelism`。

## Column

```text
Column(name, data_type, semantic_type, precision=None, scale=None)
```

| 参数 | 说明 |
| --- | --- |
| `name` | 列名，不能为空 |
| `data_type` | `ColumnDataType` |
| `semantic_type` | `SemanticType` |
| `precision`、`scale` | 只用于 `DECIMAL128`。`precision` 是 1 到 38，`scale` 是 0 到 `precision` |

可读字段：`name`、`data_type`、`semantic_type`、`precision`、`scale`。

## SemanticType

`SemanticType.TAG`、`SemanticType.FIELD`、`SemanticType.TIMESTAMP`。一张表用一个 `TIMESTAMP` 列做时间索引。

## ColumnDataType

`BOOLEAN`、`INT8`、`INT16`、`INT32`、`INT64`、`UINT8`、`UINT16`、`UINT32`、`UINT64`、`FLOAT32`、`FLOAT64`、`BINARY`、`STRING`、`DATE`、`TIMESTAMP_SECOND`、`TIMESTAMP_MILLISECOND`、`TIMESTAMP_MICROSECOND`、`TIMESTAMP_NANOSECOND`、`TIME_SECOND`、`TIME_MILLISECOND`、`TIME_MICROSECOND`、`TIME_NANOSECOND`、`DECIMAL128`、`JSON`。

## BulkResponse

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `request_id` | `int` | 这一批的 id |
| `affected_rows` | `int` | 写入行数 |

## GreptimeError

失败时抛出。`error.retriable` 为 `False` 表示参数不合法，重试没用。为 `True` 表示多数传输失败，可以再试。
