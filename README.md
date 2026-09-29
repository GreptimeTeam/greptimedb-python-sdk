# greptimedb-ingester

GreptimeDB 的 Python 写入客户端，绑定 [greptimedb-ingester](https://github.com/GreptimeTeam/greptimedb-ingester-rust) 0.19.0。支持 Python 3.10 及以上。调用是同步的。

使用说明见 [docs/usage.md](docs/usage.md)。

## 给别人用

对方不需要安装 Rust。这个包用了 Python stable ABI（`abi3`），同一个操作系统和 CPU 上的 Python 3.10 及以上共用一个 wheel。

GitHub Actions 在一台 Ubuntu 上交叉编译三个 wheel，不上传 PyPI：

| 产物 | 平台 |
| --- | --- |
| `wheels-linux-x86_64` | Linux x86_64，glibc 2.28 及以上 |
| `wheels-linux-aarch64` | Linux aarch64，glibc 2.28 及以上 |
| `wheels-windows-x64` | Windows x64 |

push 和 pull request 的产物在 Actions 页面下载，默认保留约 90 天。推送 `v*` tag 时，这三个 wheel 会挂到对应的 GitHub Release 上。macOS wheel 还没有。

对方按自己的系统安装，例如：

```bash
pip install greptimedb_ingester-0.1.0-cp310-abi3-manylinux_2_28_x86_64.whl
```

从源码装（对方机器上要有 Rust 1.85+）：

```bash
pip install maturin
maturin build --release
pip install dist/greptimedb_ingester-*.whl
```

## 开发

改这个库本身时：

```bash
uv python pin 3.10
uv sync --group dev
uv run maturin develop
uv run pytest
```

`pytest` 只覆盖值转换，不连接 GreptimeDB。
