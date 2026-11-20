# vdb-mysql-engine

VDB's MySQL-family engine: the Go driver that `vdb-core` runs for MySQL. It serves the `mysqld` plug-ins of
`vdb-mysql-plugins` over the plug-in protocol, reads the Source, relays logins, mirrors grants, supervises `mysqld`,
and holds InnoDB's lock, isolation, foreign-key and auto-increment rules on top of `vdb-core`.

The `vdb-mysql` repository assembles it into the `vdb-mysql` binary and the environment image.
