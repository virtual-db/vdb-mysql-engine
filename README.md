# vdb-mysql-engine

VDB's MySQL-family engine: the Go driver that `vdb-core` runs for MySQL. It serves the `mysqld` plug-ins of
`vdb-mysql-plugins` over the plug-in protocol, reads the Source, relays logins, mirrors grants, supervises `mysqld`,
and holds InnoDB's lock, isolation, foreign-key and auto-increment rules on top of `vdb-core`.

The `vdb-mysql` repository assembles it into the `vdb-mysql` binary and the environment image.

## Requirements

- Go 1.27.4 (the version every VDB module uses)
- Module: `github.com/virtual-db/vdb-mysql-engine`, public; the plug-in protocol is the public `protocol` package

## Use

```go
cfg, err := engine.ConfigFromEnv()        // docs/configuration.md
app := core.New(cfg.CoreConfig())
app.UseDriver(engine.NewDriver(cfg, app.DriverAPI())) // also registers its Source and memory probe
err = app.Run()
```

That is all of `vdb-mysql`'s `main.go`. MariaDB is a variant of this engine, not a separate one (D-90).

## Docs

| Doc | Covers |
|---|---|
| [how-it-works.md](docs/how-it-works.md) | Start, reads, writes, logins, Source changes, restarts |
| [configuration.md](docs/configuration.md) | Every `VDB_*` setting |
| [tls.md](docs/tls.md) | TLS to the Source and from clients |
| [vdb-schema.md](docs/vdb-schema.md) | The `vdb` schema and status variables |
| [pipelines-and-events.md](docs/pipelines-and-events.md) | When `vdb-core`'s pipelines and events run in VDB for MySQL |
| [plugins.md](docs/plugins.md) | VDB plugins |
| [known-limitations.md](docs/known-limitations.md) | What behaves differently from MySQL, and why |

## Development

`make test` runs `go test -race ./...`; `make proto` regenerates `protocol/vdb.pb.go` from `protocol/vdb.proto` in a
`golang:1.27.4` container, and CI fails if it leaves a diff. CI runs Lint and Test on pull requests.

## License

Elastic License 2.0 (ELv2). See [LICENSE.md](LICENSE.md).
