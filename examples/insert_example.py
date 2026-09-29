"""Insert one row of every column type, with and without an existing table.

Requires GreptimeDB listening on 127.0.0.1:4001, HTTP SQL on 127.0.0.1:4000.
- py_ingester_insert_new: table is absent; insert creates it.
- py_ingester_insert_existing: the first insert creates the table, the second
  insert writes into that existing table.
"""

from datetime import timedelta

from greptimedb_ingester import Client

from ddl import drop_table
from type_samples import INSERT_COLUMNS, INSERT_ROW, WHEN


def row_at(when):
    row = list(INSERT_ROW)
    for index, column in enumerate(INSERT_COLUMNS):
        if column.name in {"ts_s", "ts_ms", "ts_us", "ts_ns"}:
            row[index] = when
    return row


def main():
    client = Client(["127.0.0.1:4001"], database="public")

    new_table = "py_ingester_insert_new"
    drop_table(new_table)
    affected = client.insert(new_table, INSERT_COLUMNS, [row_at(WHEN)])
    print(f"insert creates the table: {new_table} affected={affected}")

    existing_table = "py_ingester_insert_existing"
    drop_table(existing_table)
    client.insert(existing_table, INSERT_COLUMNS, [row_at(WHEN)])
    affected = client.insert(
        existing_table,
        INSERT_COLUMNS,
        [row_at(WHEN + timedelta(milliseconds=1))],
    )
    print(f"insert into an existing table: {existing_table} affected={affected}")


if __name__ == "__main__":
    main()
