"""Low-latency insert. Requires GreptimeDB listening on 127.0.0.1:4001."""

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
        ["device_001", 1_234_567_890_000, 23.5],
        {"device_id": "device_002", "ts": 1_234_567_890_001, "temperature": 24.0},
    ],
)
print(f"inserted {affected} rows")
