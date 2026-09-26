package sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/sllt/pi/pkg/pi/config"
	"github.com/sllt/pi/pkg/pi/datasource"
)

// OpenContext opens and verifies a SQL connection without starting background
// retry/metrics loops or servers. The caller owns Close. It is intended for
// bounded commands such as migrations; NewSQL retains its existing behavior.
// logger must be non-nil. No telemetry provider is installed by this function.
func OpenContext(ctx context.Context, configs config.Config, logger datasource.Logger, metrics Metrics) (*DB, error) {
	if configs == nil || logger == nil {
		return nil, fmt.Errorf("sql: config and logger are required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cfg := getDBConfig(configs)
	if cfg.Dialect == supabaseDialect {
		setupSupabaseDefaults(cfg, configs, logger)
	}
	if cfg.Database == "" {
		return nil, fmt.Errorf("sql: DB_NAME is required")
	}
	if cfg.Dialect != sqlite && cfg.HostName == "" {
		return nil, fmt.Errorf("sql: DB_HOST is required for %q", cfg.Dialect)
	}
	if err := registerMySQLTLSConfig(cfg, logger); err != nil {
		return nil, fmt.Errorf("sql: configure TLS: %w", err)
	}
	dsn, err := getDBConnectionString(cfg)
	if err != nil {
		return nil, err
	}
	driver := cfg.Dialect
	if driver == supabaseDialect || driver == cockroachDB {
		driver = dialectPostgres
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("sql: open: %w", err)
	}
	db.SetMaxIdleConns(cfg.MaxIdleConn)
	db.SetMaxOpenConns(cfg.MaxOpenConn)
	if err = db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sql: ping: %w", err)
	}
	return &DB{DB: db, config: cfg, logger: logger, metrics: metrics}, nil
}
