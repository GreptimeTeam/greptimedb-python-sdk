# Agent notes

Python write client for GreptimeDB. Package name `greptimedb-ingester`, import `greptimedb_ingester`. Python 3.10+, sync API, uv. Root is a pure Python setuptools facade with optional `[grpc]` and `[postgres]` extras. The native plugin `greptimedb-ingester-grpc` in `packages/grpc` binds `greptimedb-ingester` 0.19.0 using PyO3 `abi3-py310` and maturin. Rust sources and Cargo files live there, not at the root. Postgres-only installations must not depend on the native plugin.

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

- Root `uv build` builds the pure Python facade. Build the native dependency from `packages/grpc` with maturin. Keep facade/plugin versions, the exact grpc extra pin and lock files synchronized with `scripts/set-version.py`.
- Test base, postgres-only, gRPC-only and combined installed wheels. The root exports only shared schema and GreptimeError. Import clients explicitly from `.grpc` or `.postgres`; do not add automatic backend selection or legacy import shims.
- `v*` tags publish the native dependency first, then the public facade, using `PYPI_TOKEN` with access to both distributions. Both backend integrations must pass.

- Wheels are built on the target OS: Linux x86_64, Linux aarch64, Windows x64, macOS Apple Silicon. Do not cross-compile those wheels.
- After a wheel build, install it and run `pytest`. Tests must not require a running GreptimeDB.
- Publish to PyPI only from a `v*` tag, using `secrets.PYPI_TOKEN`. The uploaded version is `version` in `pyproject.toml`, not the tag text.
- Default `pytest` skips tests marked `integration` and must not require GreptimeDB.
- CI job `integration` starts GreptimeDB and runs `pytest -m integration` on Linux. Cover `insert` (including TAG on auto-create), `delete`, and `bulk` into an existing table.
