# greptimedb-ingester-grpc

`greptimedb-ingester[grpc]` 的内部原生后端。用户通过公共包的 grpc extra 安装。
本发行包只包含 Rust/PyO3 扩展，不反向依赖公共包，避免依赖环。
原生 wheel 使用 abi3-py310，在对应系统和架构构建。
