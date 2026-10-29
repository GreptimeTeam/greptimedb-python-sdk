# greptimedb-ingester-grpc

Internal native backend for `greptimedb-ingester[grpc]`. Users install the public
facade with that extra. This distribution contains the Rust/PyO3 extension only;
it has no dependency on the public facade, so the dependency graph has no cycle.
Native wheels use abi3-py310 and are built on their target OS/architecture.
