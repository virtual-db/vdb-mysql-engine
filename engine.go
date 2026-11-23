// Package engine is VDB's MySQL-family engine (implementation plan doc 09
// §1): one Driver that vdb-core runs, built from a Config and core.DriverAPI,
// in the shape vdb-mysql-driver had. Everything else is internal: the
// plug-in protocol server, the Source adapter, identity, discovery, the
// supervisor and the vdb schema. The composition root never reaches inside.
//
//	cfg, err := engine.ConfigFromEnv()
//	app := core.New(cfg.CoreConfig())
//	driver := engine.NewDriver(cfg, app.DriverAPI())
//	app.UseDriver(driver)
//	err = app.Run()
package engine

import (
	"log/slog"
	"os"

	core "github.com/virtual-db/vdb-core"

	"github.com/virtual-db/vdb-mysql-engine/internal/collations"
	"github.com/virtual-db/vdb-mysql-engine/internal/config"
	"github.com/virtual-db/vdb-mysql-engine/internal/server"
	"github.com/virtual-db/vdb-mysql-engine/internal/source"
	"github.com/virtual-db/vdb-mysql-engine/internal/supervisor"
)

// CollationsProven reports whether the collation proof suite's results
// (proven.yaml) cover a mysqld release. vdb-mysql's image build runs it and
// fails without them (vdb-mysql#23): on an unproven release every
// non-binary string index would read whole tables.
func CollationsProven(release string) bool { return collations.Listed(release) }

// Config holds every engine setting (plan doc 13 §3).
type Config struct {
	c *config.Config
}

// ConfigFromEnv reads VDB_* variables and /etc/vdb/vdb.yaml, applies
// defaults and environment classes, and validates. It also installs the
// log/slog handler the configuration names (D-82): JSON by default, text with
// VDB_LOG_FORMAT=text.
func ConfigFromEnv() (Config, error) {
	c, err := config.Load()
	if err != nil {
		setupLogging("json")
		return Config{}, err
	}
	setupLogging(c.LogFormat)
	return Config{c: c}, nil
}

// CoreConfig returns the settings the engine passes to vdb-core.
func (c Config) CoreConfig() core.Config {
	return core.Config{
		PluginDir:        c.c.PluginDir,
		MemoryReserve:    c.c.MemoryReserve,
		DeltaMaxBytes:    c.c.DeltaMaxBytes,
		CountDeclineKeys: c.c.CountMaxShadowed,
		LookupBatch:      c.c.Source.LookupBatch,
	}
}

// Driver is the engine. It implements core.Server, core.SourceProvider and
// core.MemoryProber.
type Driver struct {
	srv *server.Server
	src *source.Adapter
	sup *supervisor.Supervisor
}

var (
	_ core.Server         = (*Driver)(nil)
	_ core.SourceProvider = (*Driver)(nil)
	_ core.MemoryProber   = (*Driver)(nil)
)

// Version is set at build time by the binary that embeds the engine.
var Version = "dev"

// NewDriver builds the engine. Nothing starts until Run.
func NewDriver(cfg Config, api core.DriverAPI) *Driver {
	src := source.New(cfg.c.Source) // not connected yet
	sup := supervisor.New(cfg.c.Mysqld, cfg.c.Socket, cfg.c.MysqldSocket)
	srv := server.New(cfg.c, api, src, sup)
	srv.Version = Version
	return &Driver{srv: srv, src: src, sup: sup}
}

// Run executes the start-up steps and blocks until Stop or a fatal error.
func (d *Driver) Run() error { return d.srv.Run() }

// Stop stops the engine.
func (d *Driver) Stop() error { return d.srv.Stop() }

// Source returns the Source adapter the core reads through.
func (d *Driver) Source() core.Source { return d.src }

// MemoryProbe returns the resident memory of vdb-mysql and mysqld together.
func (d *Driver) MemoryProbe() (uint64, error) { return supervisor.MemoryProbe(d.sup)() }

func setupLogging(format string) {
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	if os.Getenv("VDB_LOG_LEVEL") == "debug" {
		opts.Level = slog.LevelDebug
	}
	var h slog.Handler = slog.NewJSONHandler(os.Stderr, opts)
	if format == "text" {
		h = slog.NewTextHandler(os.Stderr, opts)
	}
	slog.SetDefault(slog.New(h))
}
