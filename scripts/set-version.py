#!/usr/bin/env python3
"""Set this package's version in three fixed lines, then compile.

Cargo rewrites Cargo.lock from Cargo.toml during that compile. This script
does not edit the lock file itself.
"""

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
        raise SystemExit("usage: uv run python scripts/set-version.py 0.1.1")
    version = sys.argv[1]
    parts = version.split(".")
    if len(parts) != 3 or not all(part.isdigit() for part in parts):
        raise SystemExit("version must look like 0.1.1")
    set_first_line("pyproject.toml", 'version = "', f'version = "{version}"')
    set_first_line("Cargo.toml", 'version = "', f'version = "{version}"')
    set_first_line(
        "python/greptimedb_ingester/__init__.py",
        '__version__ = "',
        f'__version__ = "{version}"',
    )
    subprocess.run(["cargo", "check", "--offline"], cwd=ROOT, check=True)
    print(version)


if __name__ == "__main__":
    main()
