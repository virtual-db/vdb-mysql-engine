# Contributing to vdb-mysql-engine

> [!IMPORTANT]
> ### Public Contributions Are Not Yet Open
>
> VirtualDB is not currently accepting public pull requests. Please open an issue instead.

## What belongs here

Everything that knows MySQL or InnoDB, in Go: the `Driver` (`engine.go`), the plug-in protocol (`protocol/`, public),
the Source adapter, pushdown, statistics, identity, discovery, schema polling, the supervisor and rebuild, the `vdb`
schema, and InnoDB's lock, isolation, foreign-key and auto-increment rules. What a PostgreSQL engine could use
unchanged belongs in `vdb-core` (D-88); C++ belongs in `vdb-mysql-plugins`; the binary, the image and the oracle
belong in `vdb-mysql`.

## Rules

- The engine reaches `vdb-core` only through `core.DriverAPI`; the root package exposes only `Config`,
  `ConfigFromEnv`, `NewDriver` and `Driver`.
- One package per concern under `internal/`; unit tests in `internal/<pkg>/tests/` need no database and no network.
- New direct dependencies need BMO's approval.
- `log/slog` with the `pkg` field; never log a password or a row value.
- `gofmt`, `go vet` and `go test -race ./...` are clean; `make proto` leaves no diff.
