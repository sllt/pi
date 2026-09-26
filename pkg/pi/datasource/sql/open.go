package sql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/go-sql-driver/mysql"

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
	dsn, err := getDBConnectionString(cfg)
	if err != nil {
		return nil, err
	}
	driver := cfg.Dialect
	if driver == supabaseDialect || driver == cockroachDB {
		driver = dialectPostgres
	}
	var db *sql.DB
	if driver == dialectMysql {
		var connector sqldriver.Connector
		connector, err = snapshotMySQLConnector(cfg, configs)
		if err == nil {
			db = sql.OpenDB(connector)
		}
	} else {
		db, err = sql.Open(driver, dsn)
	}
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

// Unlike legacy NewSQL, the bounded path never installs MySQL's global TLS
// registry or reads TLS paths from ambient environment variables.
func snapshotMySQLConnector(c *DBConfig, values config.Config) (sqldriver.Connector, error) {
	cfg := mysql.NewConfig()
	cfg.User = c.User
	cfg.Passwd = c.Password
	cfg.Net = "tcp"
	cfg.Addr = net.JoinHostPort(c.HostName, c.Port)
	cfg.DBName = c.Database
	cfg.ParseTime = true
	cfg.InterpolateParams = true
	if c.Charset != "" {
		cfg.Params = map[string]string{"charset": c.Charset}
	}
	mode := strings.ToLower(c.SSLMode)
	switch mode {
	case "", "disable", "false":
	case "require", "true", "skip-verify", "preferred":
		cfg.TLS = &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true} // Explicit configured compatibility mode.
		cfg.AllowFallbackToPlaintext = mode == "preferred"
	case "verify-ca", "verify-full":
		cfg.TLS = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: getServerName(c.HostName)}
		if path := values.Get("DB_TLS_CA_CERT"); path != "" {
			pem, err := os.ReadFile(path)
			if err != nil {
				return nil, fmt.Errorf("read DB_TLS_CA_CERT: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, errFailedCACerts
			}
			cfg.TLS.RootCAs = pool
		}
	default:
		return nil, fmt.Errorf("invalid DB_SSL_MODE %q", c.SSLMode)
	}
	cert, key := values.Get("DB_TLS_CLIENT_CERT"), values.Get("DB_TLS_CLIENT_KEY")
	if (cert == "") != (key == "") {
		return nil, fmt.Errorf("DB_TLS_CLIENT_CERT and DB_TLS_CLIENT_KEY must be configured together")
	}
	if cert != "" {
		if cfg.TLS == nil {
			return nil, fmt.Errorf("client certificates require DB_SSL_MODE")
		}
		pair, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, err
		}
		cfg.TLS.Certificates = []tls.Certificate{pair}
	}
	return mysql.NewConnector(cfg)
}
