"""Bulk-write one row into a table that already exists.

Requires GreptimeDB listening on 127.0.0.1:4001, HTTP SQL on 127.0.0.1:4000.
bulk does not create tables. This example inserts one row first, then bulk-writes
another row into py_ingester_bulk_existing.
"""

from datetime import timedelta

from greptimedb_ingester import Client, WriteOptions

from ddl import drop_table
from type_samples import BULK_COLUMNS, BULK_ROW, WHEN


def row_at(when):
    row = list(BULK_ROW)
    for index, column in enumerate(BULK_COLUMNS):
        if column.name in {"ts_s", "ts_ms", "ts_us", "ts_ns"}:
            row[index] = when
    return row


def main():
    client = Client(["127.0.0.1:4001"], database="public")
    table = "py_ingester_bulk_existing"
    drop_table(table)
    client.insert(table, BULK_COLUMNS, [row_at(WHEN)])
    with client.bulk_writer(
        table,
        BULK_COLUMNS,
        options=WriteOptions(compression="zstd", parallelism=8, timeout_secs=60),
    ) as writer:
        rows = writer.alloc_rows(1)
        rows.add_rows([row_at(WHEN + timedelta(milliseconds=1))])
        request_ids = writer.write_async(rows)
        responses = writer.wait_all()

    print(f"{table} request_ids={request_ids}")
    for response in responses:
        print(response.request_id, response.affected_rows)


if __name__ == "__main__":
    main()
