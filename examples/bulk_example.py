"""Bulk stream write. The table must already exist unless auto_create_table is enabled."""

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
    rows.add_rows(
        [
            [1_234_567_890_000, "device_001", 23.5],
            {"ts": 1_234_567_890_001, "device_id": "device_002", "temperature": 24.0},
        ]
    )
    request_ids = writer.write_async(rows)
    responses = writer.wait_all()

print(request_ids)
for response in responses:
    print(response.request_id, response.affected_rows)
