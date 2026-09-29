# Agent notes

Python write client for GreptimeDB. Package name `greptimedb-ingester`, import `greptimedb_ingester`. Binds `greptimedb-ingester` 0.19.0. Python 3.10+, sync API, PyO3 `abi3-py310`, maturin, uv.

## Write paths

- `Client.insert` may create the table from the request schema. `Client.delete` uses the same row shape. Delete columns are TAG plus the time index, not FIELD.
- `Client.bulk_writer` does not create tables. Do not add an `auto_create_table` flag. Callers create the table first, usually with `insert`.
- A table has one `SemanticType.TIMESTAMP` column. That column cannot be NULL. A `TIMESTAMP_*` column with `SemanticType.FIELD` may be NULL.
- Do not add `JSON2`, `DATETIME`, or `INTERVAL_*` column types. GreptimeDB rejects `DATETIME` and `INTERVAL` at table creation. `JSON2` was removed on purpose.
- Convert a bulk batch to `Row` once, before sending it to the writer thread.

## Docs and language

- `README.md` is English and is the default. `README.zh-CN.md` is Chinese. Keep them in sync.
- Usage and API reference stay in `docs/usage.md` and `docs/api.md`.
- In prose for this repo, keep API names as written: `insert`, `bulk`, `GreptimeDB`, `wheel`, `gRPC`. Do not invent shorthand for them.

## Build and release

- Wheels are built on the target OS: Linux x86_64, Linux aarch64, Windows x64, macOS Apple Silicon. Do not cross-compile those wheels.
- After a wheel build, install it and run `pytest`. Tests must not require a running GreptimeDB.
- Publish to PyPI only from a `v*` tag, using `secrets.PYPI_TOKEN`. The uploaded version is `version` in `pyproject.toml`, not the tag text.
- `pytest` covers value conversion. Live writes belong in `examples/`, which need GreptimeDB on `127.0.0.1:4001`.
