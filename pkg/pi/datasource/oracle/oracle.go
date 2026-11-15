package oracle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	// Import for Oracle driver registration.
	_ "github.com/godror/godror"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var _ Txn = (*oracleTx)(nil)

type Config struct {
	Host             string
	Port             int
	Username         string
	Password         string
	Service          string // or SID.
	ConnectionString string
	LibDir           string
	WalletLocation   string
}

type Client struct {
	mu      sync.RWMutex
	conn    Connection
	config  Config
	logger  Logger
	metrics Metrics
	tracer  trace.Tracer
}

var (
	errStatusDown      = errors.New("status down")
	errInvalidDestType = errors.New("dest must be *[]map[string]any")
	errNoConnection    = errors.New("oracle connection not established")
	errInvalidConnType = errors.New("invalid connection type")
	errEmptyHost       = errors.New("invalid OracleDB host: host is empty")
	errInvalidPort     = errors.New("invalid OracleDB port")
)

const (
	StatusUp        = "UP"
	StatusDown      = "DOWN"
	defaultPort     = 1521
	maxValidTCPPort = 65535
)

func New(config *Config) *Client {
	if config == nil {
		return &Client{}
	}
	return &Client{config: *config}
}

// FromEnv constructs a zero-I/O Oracle *Client from an environment/config lookup function.
//
// Supported keys:
//   - ORACLE_HOST
//   - ORACLE_PORT (default: 1521)
//   - ORACLE_USER (fallback: ORACLE_USERNAME)
//   - ORACLE_PASSWORD
//   - ORACLE_SERVICE
//   - ORACLE_CONNECTION_STRING
//   - ORACLE_LIB_DIR
//   - ORACLE_WALLET_LOCATION
func FromEnv(get func(string) string) (*Client, error) {
	if get == nil {
		return nil, errEmptyHost
	}

	host := strings.TrimSpace(get("ORACLE_HOST"))
	connStr := strings.TrimSpace(get("ORACLE_CONNECTION_STRING"))
	if host == "" && connStr == "" {
		return nil, errEmptyHost
	}

	port := defaultPort
	if pStr := strings.TrimSpace(get("ORACLE_PORT")); pStr != "" {
		p, err := strconv.Atoi(pStr)
		if err != nil || p <= 0 || p > maxValidTCPPort {
			return nil, fmt.Errorf("%w: %s", errInvalidPort, pStr)
		}
		port = p
	}

	user := strings.TrimSpace(get("ORACLE_USER"))
	if user == "" {
		user = strings.TrimSpace(get("ORACLE_USERNAME"))
	}

	cfg := &Config{
		Host:             host,
		Port:             port,
		Username:         user,
		Password:         get("ORACLE_PASSWORD"),
		Service:          strings.TrimSpace(get("ORACLE_SERVICE")),
		ConnectionString: connStr,
		LibDir:           strings.TrimSpace(get("ORACLE_LIB_DIR")),
		WalletLocation:   strings.TrimSpace(get("ORACLE_WALLET_LOCATION")),
	}
	if cfg.Host == "" {
		cfg.Host = "localhost"
	}

	return New(cfg), nil
}

func (c *Client) UseLogger(logger any) {
	if c == nil {
		return
	}
	if l, ok := logger.(Logger); ok {
		c.mu.Lock()
		c.logger = l
		c.mu.Unlock()
	}
}

func (c *Client) UseMetrics(metrics any) {
	if c == nil {
		return
	}
	if m, ok := metrics.(Metrics); ok {
		c.mu.Lock()
		c.metrics = m
		c.mu.Unlock()
	}
}

func (c *Client) UseTracer(tracer any) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	switch t := tracer.(type) {
	case trace.Tracer:
		c.tracer = t
	case trace.TracerProvider:
		c.tracer = t.Tracer("pi-oracle")
	}
}

func (c *Client) buildDSN() (string, error) {
	if c.config.Host == "" && c.config.ConnectionString == "" {
		if c.logger != nil {
			c.logger.Errorf("invalid OracleDB host: host is empty")
		}
		return "", errEmptyHost
	}

	if c.config.Port <= 0 || c.config.Port > maxValidTCPPort {
		if c.logger != nil {
			c.logger.Errorf("invalid OracleDB port: %v", c.config.Port)
		}
		return "", fmt.Errorf("%w: %d", errInvalidPort, c.config.Port)
	}

	connStr := c.config.ConnectionString
	if connStr == "" && c.config.Host != "" && c.config.Service != "" {
		connStr = fmt.Sprintf("%s:%d/%s", c.config.Host, c.config.Port, c.config.Service)
	}

	parts := []string{
		fmt.Sprintf("user=%q", c.config.Username),
		fmt.Sprintf("password=%q", c.config.Password),
		fmt.Sprintf("connectString=%q", connStr),
	}
	if c.config.LibDir != "" {
		parts = append(parts, fmt.Sprintf("libDir=%q", c.config.LibDir))
	}
	if c.config.WalletLocation != "" {
		parts = append(parts, fmt.Sprintf("wallet_location=%q", c.config.WalletLocation))
	}

	return strings.Join(parts, " "), nil
}

// ConnectContext opens and verifies an Oracle connection bounded by ctx.
// Calling ConnectContext on an already-connected Client is an idempotent no-op.
func (c *Client) ConnectContext(ctx context.Context) error {
	if c == nil {
		return errNoConnection
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	c.mu.RLock()
	if c.conn != nil {
		c.mu.RUnlock()
		return nil
	}
	logger := c.logger
	c.mu.RUnlock()

	dsn, err := c.buildDSN()
	if err != nil {
		return err
	}

	if logger != nil {
		logger.Debugf("connecting to OracleDB using connection string")
	}

	db, err := sql.Open("godror", dsn)
	if err != nil {
		if logger != nil {
			logger.Errorf("error while connecting to OracleDB: %v", err)
		}
		return err
	}

	if err = db.PingContext(ctx); err != nil {
		_ = db.Close()
		if logger != nil {
			logger.Errorf("ping failed with error %v", err)
		}
		return err
	}

	c.mu.Lock()
	if c.conn != nil {
		c.mu.Unlock()
		_ = db.Close()
		return nil
	}
	c.conn = &sqlConn{db: db}
	c.mu.Unlock()

	if logger != nil {
		logger.Logf("successfully connected to OracleDB")
	}

	return nil
}

func (c *Client) Connect() {
	if c == nil {
		return
	}

	c.mu.RLock()
	logger := c.logger
	c.mu.RUnlock()

	if c.config.Host == "" {
		if logger != nil {
			logger.Errorf("invalid OracleDB host: host is empty")
		}
		return
	}

	if c.config.Port <= 0 || c.config.Port > maxValidTCPPort {
		if logger != nil {
			logger.Errorf("invalid OracleDB port: %v", c.config.Port)
		}
		return
	}

	if logger != nil {
		logger.Debugf("connecting to OracleDB using connection string")
	}

	dsn, err := c.buildDSN()
	if err != nil {
		return
	}

	db, err := sql.Open("godror", dsn)
	if err != nil {
		if logger != nil {
			logger.Errorf("error while connecting to OracleDB: %v", err)
		}
		return
	}

	conn := &sqlConn{db: db}
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	if err = conn.Ping(context.Background()); err != nil {
		if logger != nil {
			logger.Errorf("ping failed with error %v", err)
		}
	} else if logger != nil {
		logger.Logf("successfully connected to OracleDB")
	}
}

// Close closes the underlying Oracle connection if open. Safe to call multiple times.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()

	if conn == nil {
		return nil
	}
	if closer, ok := conn.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// Exec executes a non-query SQL statement (such as INSERT, UPDATE, or DELETE) against the Oracle database.
func (c *Client) Exec(ctx context.Context, query string, args ...any) error {
	if c == nil {
		return errNoConnection
	}
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()
	if conn == nil {
		return errNoConnection
	}

	tracedCtx, span := c.addTrace(ctx, "exec", query)
	start := time.Now()
	err := conn.Exec(tracedCtx, query, args...)
	defer c.sendOperationStats(start, "Exec", query, "exec", span, args...)

	return err
}

// Select executes a SELECT query and scans the resulting rows into dest.
func (c *Client) Select(ctx context.Context, dest any, query string, args ...any) error {
	if c == nil {
		return errNoConnection
	}
	if dest == nil || reflect.TypeOf(dest).Kind() != reflect.Ptr || reflect.TypeOf(dest).Elem().Kind() != reflect.Slice {
		return errInvalidDestType
	}

	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()
	if conn == nil {
		return errNoConnection
	}

	tracedCtx, span := c.addTrace(ctx, "select", query)
	start := time.Now()
	err := conn.Select(tracedCtx, dest, query, args...)
	defer c.sendOperationStats(start, "Select", query, "select", span, args...)

	return err
}

// oracleTx wraps a sql.Tx to implement the Txn interface.
type oracleTx struct {
	tx     *sql.Tx
	logger Logger
}

// Begin starts a new transaction.
func (c *Client) Begin() (Txn, error) {
	if c == nil {
		return nil, errNoConnection
	}
	c.mu.RLock()
	conn := c.conn
	logger := c.logger
	c.mu.RUnlock()

	if conn == nil {
		return nil, errNoConnection
	}

	start := time.Now()

	sc, ok := conn.(*sqlConn)
	if !ok {
		return nil, errInvalidConnType
	}

	tx, err := sc.db.BeginTx(context.Background(), nil)
	if err != nil {
		if logger != nil {
			logger.Errorf("failed to begin transaction: %v", err)
		}
		return nil, err
	}

	if logger != nil {
		logger.Debug(&Log{
			Type:     "Begin",
			Duration: time.Since(start).Microseconds(),
		})
	}

	return &oracleTx{tx: tx, logger: logger}, nil
}

func (t *oracleTx) ExecContext(ctx context.Context, query string, args ...any) error {
	start := time.Now()
	_, err := t.tx.ExecContext(ctx, query, args...)

	if t.logger != nil {
		t.logger.Debug(&Log{
			Type:     "Tx-Exec",
			Query:    query,
			Duration: time.Since(start).Microseconds(),
			Args:     args,
		})
	}

	return err
}

func scanRows(rows *sql.Rows) ([]map[string]any, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []map[string]any

	for rows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))

		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		rowMap := make(map[string]any)
		for columnIndex, columnName := range columns {
			rowMap[columnName] = values[columnIndex]
		}

		results = append(results, rowMap)
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return results, nil
}

func (t *oracleTx) SelectContext(ctx context.Context, dest any, query string, args ...any) error {
	start := time.Now()

	if dest == nil || reflect.TypeOf(dest).Kind() != reflect.Ptr || reflect.TypeOf(dest).Elem().Kind() != reflect.Slice {
		return errInvalidDestType
	}

	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	results, err := scanRows(rows)
	if err != nil {
		return err
	}

	type Destination = []map[string]any

	p, ok := dest.(*Destination)
	if !ok {
		return errInvalidDestType
	}

	*p = results

	if t.logger != nil {
		t.logger.Debug(&Log{
			Type:     "Tx-Select",
			Query:    query,
			Duration: time.Since(start).Microseconds(),
			Args:     args,
		})
	}

	return nil
}

func (t *oracleTx) Commit() error {
	start := time.Now()
	err := t.tx.Commit()

	if t.logger != nil {
		t.logger.Debug(&Log{
			Type:     "Tx-Commit",
			Duration: time.Since(start).Microseconds(),
		})

		if err != nil {
			t.logger.Errorf("transaction commit failed: %v", err)
		}
	}

	return err
}

func (t *oracleTx) Rollback() error {
	start := time.Now()
	err := t.tx.Rollback()

	if t.logger != nil {
		t.logger.Debug(&Log{
			Type:     "Tx-Rollback",
			Duration: time.Since(start).Microseconds(),
		})

		if err != nil {
			t.logger.Errorf("transaction rollback failed: %v", err)
		}
	}

	return err
}

func (c *Client) sendOperationStats(start time.Time, methodType, query, method string, span trace.Span, args ...any) {
	duration := time.Since(start).Microseconds()

	c.mu.RLock()
	logger := c.logger
	c.mu.RUnlock()

	if logger != nil {
		logger.Debug(&Log{
			Type:     methodType,
			Query:    query,
			Duration: duration,
			Args:     args,
		})
	}

	if span != nil {
		defer span.End()

		span.SetAttributes(attribute.Int64(fmt.Sprintf("oracle.%v.duration", method), duration))
	}
}

type Health struct {
	Status  string         `json:"status,omitempty"`
	Details map[string]any `json:"details,omitempty"`
}

func (c *Client) HealthCheck(ctx context.Context) (any, error) {
	if c == nil {
		return &Health{Status: StatusDown}, errStatusDown
	}
	if ctx == nil {
		ctx = context.Background()
	}

	h := Health{
		Details: map[string]any{
			"host":     c.config.Host,
			"database": c.config.Service,
		},
	}

	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()

	if conn == nil {
		h.Status = StatusDown
		h.Details["error"] = errNoConnection.Error()
		return &h, errStatusDown
	}

	if err := conn.Ping(ctx); err != nil {
		h.Status = StatusDown
		return &h, errStatusDown
	}

	h.Status = StatusUp
	return &h, nil
}

func (c *Client) addTrace(ctx context.Context, method, query string) (context.Context, trace.Span) {
	c.mu.RLock()
	tracer := c.tracer
	c.mu.RUnlock()

	if tracer != nil {
		ctxWithTrace, span := tracer.Start(ctx, fmt.Sprintf("oracle-%v", method))
		span.SetAttributes(
			attribute.String("oracle.query", query),
		)
		return ctxWithTrace, span
	}

	return ctx, nil
}

type sqlConn struct{ db *sql.DB }

func (s *sqlConn) Exec(ctx context.Context, query string, args ...any) error {
	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

func (s *sqlConn) Select(ctx context.Context, dest any, query string, args ...any) error {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	results, err := scanRows(rows)
	if err != nil {
		return err
	}

	type Destination = []map[string]any

	p, ok := dest.(*Destination)
	if !ok {
		return errInvalidDestType
	}

	*p = results

	return nil
}

func (s *sqlConn) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

func (s *sqlConn) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}
