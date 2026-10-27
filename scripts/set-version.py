#!/usr/bin/env python3
"""Keep facade, native plugin and their exact optional dependency in sync."""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def set_first_line(path, prefix, new_line):
    file = ROOT / path
    lines = file.read_text().splitlines(keepends=True)
    for index, line in enumerate(lines):
        if line.startswith(prefix):
            lines[index] = new_line + "\n"
            file.write_text("".join(lines))
            return
    raise SystemExit(f"{path}: no line starting with {prefix!r}")


def main():
    if len(sys.argv) != 2:
        raise SystemExit("usage: uv run python scripts/set-version.py 0.1.2")
    version = sys.argv[1]
    parts = version.split(".")
    if len(parts) != 3 or not all(part.isdigit() for part in parts):
        raise SystemExit("version must look like 0.1.2")
    for path in [
        "pyproject.toml",
        "packages/grpc/pyproject.toml",
        "packages/grpc/Cargo.toml",
    ]:
        set_first_line(path, 'version = "', f'version = "{version}"')
    for path in [
        "python/greptimedb_ingester/__init__.py",
        "packages/grpc/python/greptimedb_ingester_grpc/__init__.py",
    ]:
        set_first_line(path, '__version__ = "', f'__version__ = "{version}"')
    set_first_line(
        "pyproject.toml", "grpc = [", f'grpc = ["greptimedb-ingester-grpc=={version}"]'
    )
    subprocess.run(
        ["cargo", "check", "--manifest-path", "packages/grpc/Cargo.toml", "--offline"],
        cwd=ROOT,
        check=True,
    )
    subprocess.run(["uv", "lock"], cwd=ROOT, check=True)
    subprocess.run(["uv", "lock", "--project", "packages/grpc"], cwd=ROOT, check=True)
    print(version)


if __name__ == "__main__":
    main()
