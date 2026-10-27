# Backend and COPY test coverage

The public facade is pure Python; the native gRPC plugin lives in `packages/grpc`.
Tests run against four installation modes: no extra, postgres-only, gRPC-only, and
both. `tests/test_backends.py` checks wheel dependency metadata, backend selection,
missing-extra errors, schema availability and the common public error type.
`tests/test_postgres_schema_compat.py` checks enum names/values and Column fields
against the native SDK when present. Existing gRPC tests remain; COPY has no bulk API.

| Capability | Offline tests | Real GreptimeDB tests |
|---|---|---|
| Independent installation | COPY import loads no native SDK | Isolated [postgres] environment has no greptimedb-ingester-grpc installed |
| Construction and connection | Lazy connect, explicit/URI/default database priority, TLS parameters, credentials, batch size, timeout validation | health_check against PostgreSQL port |
| Column definitions | Compatible Column input, pure Python Column validation, duplicates, missing/invalid time index, unsupported types | Auto-create, TAG primary key, time index, FIELD schema, existing schema preserved |
| Every supported data type | Enumeration coverage guard, all widths/ranges, booleans, float range/nonfinite values, nullable fields | 20,003 rows of all supported types, integer extremes, floats, strings and timestamp fields, full read-back |
| Timestamp conversion | s/ms/us/ns units, pre-epoch values, exact nanoseconds, aware datetime offsets, lossy/naive datetime rejection | Every time-index unit, no-tag tables, aware datetime, nullable timestamp fields |
| Input forms and CSV | list/tuple/dict rows, ordering, escaping, leading BOM at batch boundaries, malformed row shape, empty input, input immutability | Generator and dict insert, Unicode/commas/quotes/newlines, BOM preservation in TAG/FIELD across batches, reserved/space identifiers |
| Batching | Full and partial batches, bounded generator consumption, acknowledged counts | 20,003 rows across four full 5,000-row batches plus tail; existing-table follow-up insert |
| Failure behavior | No retry, reconnection after a broken connection, generator errors, incorrect acknowledgement, late validation leaves prefix | Late validation preserves acknowledged rows, real COPY failure, socket disconnection and reconnection, continued writes |
| Resource lifecycle | close, repeated close, use-after-close, with normal/exceptional exit | with-managed real client connections |
| TAG/time keys | SQL construction | Same key overwrite, different TAG with same time remains separate |

Unsupported COPY types (BINARY, DATE, TIME, DECIMAL128, JSON) are tested for explicit
local rejection. Existing gRPC support for these types is unchanged. COPY requires
timezone-aware datetime values and rejects precision loss; this differs from the
existing gRPC conversion rules and has dedicated tests. Empty strings are rejected
because this server CSV decoder maps them to NULL.

```bash
uv sync --python 3.10 --locked --extra postgres
uv run --extra postgres pytest
GREPTIMEDB_POSTGRES=127.0.0.1:4003 uv run --extra postgres pytest \
  tests/test_postgres_integration.py -m integration -o addopts= -v

uv sync --locked --all-extras
uv run --all-extras pytest
GREPTIMEDB_GRPC=127.0.0.1:4001 GREPTIMEDB_HTTP=http://127.0.0.1:4000 \
  uv run --all-extras pytest tests/test_integration.py -m integration -o addopts= -v
```

`wheels.yml` builds the universal facade once, tests its base installation, and
checks installed wheels on Linux x86_64/aarch64, Windows x64 and macOS arm64 for
postgres-only, gRPC-only and combined modes. Only gRPC jobs build Rust. Separate Linux
jobs run real gRPC and COPY integration tests against a COPY-capable GreptimeDB
nightly pinned by multi-platform digest in the workflow (nightly-20261027-2d00ae25c,
containing COPY FROM STDIN #9297). Fixtures use unique tables and clean them up.

`v*` release tags wait for all builds and tests before publishing the native plugin
and public facade. Versions and the exact optional gRPC dependency must match;
use `uv run python scripts/set-version.py 0.1.3` to update them and lock files.
`PYPI_TOKEN` must authorize both greptimedb-ingester and greptimedb-ingester-grpc.
Installing a released `[postgres]` facade never resolves the gRPC plugin.
