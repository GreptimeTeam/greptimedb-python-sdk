"""Write throughput against a local GreptimeDB.

Usage:
    uv run python examples/bench_write.py
"""

import argparse
import time

from greptimedb_ingester import Client, Column, ColumnDataType, SemanticType, WriteOptions

COLUMNS = [
    Column("device_id", ColumnDataType.STRING, SemanticType.TAG),
    Column("ts", ColumnDataType.TIMESTAMP_MILLISECOND, SemanticType.TIMESTAMP),
    Column("temperature", ColumnDataType.FLOAT64, SemanticType.FIELD),
]


def make_batch(count, ts0):
    rows = []
    for i in range(count):
        rows.append((f"d{i % 128}", ts0 + i, 20.0 + (i % 50)))
    return rows


def fill(buffer, batch, ts0):
    for i in range(batch):
        buffer.add_row((f"d{i % 128}", ts0 + i, 20.0 + (i % 50)))


def ensure_table(client, table, ts0):
    client.insert(table, COLUMNS, [["d0", ts0, 0.0]])


def bench_insert(client, table, batch, seconds, ts0):
    ensure_table(client, table, ts0)
    ts0 += 1
    client.insert(table, COLUMNS, make_batch(batch, ts0))
    ts0 += batch

    deadline = time.perf_counter() + seconds
    started = time.perf_counter()
    written = 0
    build_s = 0.0
    rpc_s = 0.0
    while time.perf_counter() < deadline:
        t0 = time.perf_counter()
        rows = make_batch(batch, ts0)
        t1 = time.perf_counter()
        client.insert(table, COLUMNS, rows)
        t2 = time.perf_counter()
        build_s += t1 - t0
        rpc_s += t2 - t1
        written += batch
        ts0 += batch
    elapsed = time.perf_counter() - started
    return written, elapsed, build_s, rpc_s


def bench_bulk(client, table, batch, seconds, parallelism, compression, ts0):
    ensure_table(client, table, ts0)
    ts0 += 1
    options = WriteOptions(compression=compression, parallelism=parallelism, timeout_secs=60)
    with client.bulk_writer(table, COLUMNS, options=options) as writer:
        warmup = writer.alloc_rows(batch)
        fill(warmup, batch, ts0)
        ts0 += batch
        writer.write(warmup)

        deadline = time.perf_counter() + seconds
        started = time.perf_counter()
        written = 0
        build_s = 0.0
        submit_s = 0.0
        while time.perf_counter() < deadline:
            t0 = time.perf_counter()
            rows = writer.alloc_rows(batch)
            fill(rows, batch, ts0)
            t1 = time.perf_counter()
            writer.write_async(rows)
            t2 = time.perf_counter()
            build_s += t1 - t0
            submit_s += t2 - t1
            written += batch
            ts0 += batch
        writer.wait_all()
        elapsed = time.perf_counter() - started
    return written, elapsed, build_s, submit_s


def report(label, written, elapsed, left_s, right_s, left_name, right_name):
    rate = written / elapsed if elapsed else 0
    print(
        f"{label}: {written} rows in {elapsed:.2f}s = {rate:,.0f} rows/s"
        f"  ({left_name} {left_s:.2f}s, {right_name} {right_s:.2f}s)"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="127.0.0.1:4001")
    parser.add_argument("--seconds", type=float, default=8.0)
    args = parser.parse_args()

    client = Client([args.url], database="public")
    client.health_check()
    ts0 = time.time_ns() // 1_000_000
    print(f"target {args.url} public, {args.seconds:.0f}s per case")

    written, elapsed, build_s, rpc_s = bench_insert(
        client, "py_ingester_bench_insert", 500, args.seconds, ts0
    )
    report("insert  batch=500", written, elapsed, build_s, rpc_s, "build", "rpc")

    for batch, parallelism, compression in (
        (2_000, 8, "lz4"),
        (20_000, 8, "lz4"),
        (20_000, 8, "zstd"),
    ):
        written, elapsed, build_s, submit_s = bench_bulk(
            client,
            f"py_ingester_bench_bulk_{compression}_{batch}",
            batch,
            args.seconds,
            parallelism,
            compression,
            ts0,
        )
        report(
            f"bulk    batch={batch:<6} parallelism={parallelism} {compression}",
            written,
            elapsed,
            build_s,
            submit_s,
            "build",
            "submit",
        )


if __name__ == "__main__":
    main()
