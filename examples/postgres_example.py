"""CSV COPY insert through the PostgreSQL extra (port 4003)."""

import os
from greptimedb_ingester import Column, ColumnDataType, SemanticType
from greptimedb_ingester.postgres import Client


def main():
    columns = [
        Column("host", ColumnDataType.STRING, SemanticType.TAG),
        Column("ts", ColumnDataType.TIMESTAMP_MILLISECOND, SemanticType.TIMESTAMP),
        Column("reading", ColumnDataType.FLOAT64, SemanticType.FIELD),
    ]
    rows = (("edge", 1793066400000 + i, i / 10) for i in range(20003))
    with Client(
        [os.getenv("GREPTIMEDB_POSTGRES", "127.0.0.1:4003")], batch_size=5000
    ) as client:
        client.health_check()
        affected = client.insert("py_postgres_example", columns, rows)
        affected += client.insert(
            "py_postgres_example",
            columns,
            [
                {"host": "edge", "ts": 1793066420003, "reading": None},
            ],
        )
    print(f"COPY affected={affected}")


if __name__ == "__main__":
    main()
