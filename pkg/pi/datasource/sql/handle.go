package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"

	"go.opentelemetry.io/otel/trace"

	"github.com/sllt/pi/pkg/pi/config"
	"github.com/sllt/pi/pkg/pi/datasource"
)

var ErrNotStarted = errors.New("sql: managed connection has not started")

// Handle has stable identity during dependency injection. Construction opens no
// connection and starts no goroutines; Start activates it before application hooks.
type Handle struct {
	mu             sync.RWMutex
	db             *DB
	closed         bool
	config         config.Config
	logger         datasource.Logger
	metrics        Metrics
	tracerProvider trace.TracerProvider
}

func NewHandle(c config.Config, l datasource.Logger, m Metrics) *Handle {
	return &Handle{config: c, logger: l, metrics: m}
}

func NewHandleWithTracerProvider(c config.Config, l datasource.Logger, m Metrics, tp trace.TracerProvider) *Handle {
	return &Handle{config: c, logger: l, metrics: m, tracerProvider: tp}
}

func (h *Handle) IsStarted() bool {
	if h == nil {
		return false
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return !h.closed && h.db != nil
}

func (h *Handle) UseLogger(v any) {
	if h == nil {
		return
	}
	l, ok := v.(datasource.Logger)
	if !ok || l == nil {
		return
	}
	h.mu.Lock()
	h.logger = l
	h.mu.Unlock()
}

func (h *Handle) UseMetrics(v any) {
	if h == nil {
		return
	}
	m, ok := v.(Metrics)
	if !ok || m == nil {
		return
	}
	h.mu.Lock()
	h.metrics = m
	h.mu.Unlock()
}

func (h *Handle) UseTracerProvider(tp trace.TracerProvider) {
	if h == nil || tp == nil {
		return
	}
	h.mu.Lock()
	h.tracerProvider = tp
	h.mu.Unlock()
}

func (h *Handle) Start(ctx context.Context) error {
	if h == nil {
		return sql.ErrConnDone
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return sql.ErrConnDone
	}
	if h.db != nil {
		h.mu.Unlock()
		return nil
	}
	cfg, logger, m, tp := h.config, h.logger, h.metrics, h.tracerProvider
	h.mu.Unlock()

	db, err := OpenContextWithTracerProvider(ctx, cfg, logger, m, tp)
	if err != nil {
		return err
	}

	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		_ = db.Close()
		return sql.ErrConnDone
	}
	if h.db != nil {
		h.mu.Unlock()
		_ = db.Close()
		return nil
	}
	h.db = db
	h.mu.Unlock()
	return nil
}
func (h *Handle) ready() (*DB, error) {
	if h == nil {
		return nil, sql.ErrConnDone
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.closed {
		return nil, sql.ErrConnDone
	}
	if h.db == nil {
		return nil, ErrNotStarted
	}
	return h.db, nil
}
func (h *Handle) Close() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	db := h.db
	h.db = nil
	h.mu.Unlock()
	if db != nil {
		return db.Close()
	}
	return nil
}
func (h *Handle) Dialect() string {
	if h == nil || h.config == nil {
		return ""
	}
	return h.config.Get("DB_DIALECT")
}
func (h *Handle) Exec(q string, a ...any) (sql.Result, error) {
	return h.ExecContext(context.Background(), q, a...)
}
func (h *Handle) ExecContext(ctx context.Context, q string, a ...any) (sql.Result, error) {
	d, e := h.ready()
	if e != nil {
		return nil, e
	}
	return d.ExecContext(ctx, q, a...)
}
func (h *Handle) Query(q string, a ...any) (*sql.Rows, error) {
	return h.QueryContext(context.Background(), q, a...)
}
func (h *Handle) QueryContext(ctx context.Context, q string, a ...any) (*sql.Rows, error) {
	d, e := h.ready()
	if e != nil {
		return nil, e
	}
	return d.QueryContext(ctx, q, a...)
}
func (h *Handle) QueryRow(q string, a ...any) *sql.Row {
	return h.QueryRowContext(context.Background(), q, a...)
}
func (h *Handle) QueryRowContext(ctx context.Context, q string, a ...any) *sql.Row {
	d, e := h.ready()
	if e == nil {
		return d.QueryRowContext(ctx, q, a...)
	}
	// sql.Row has no public error constructor; produce an error-only row via a
	// connector that cannot dial. This path runs only on calls before Start/after Stop.
	db := sql.OpenDB(errorConnector{e})
	row := db.QueryRowContext(ctx, q, a...)
	_ = db.Close()
	return row
}
func (h *Handle) Prepare(q string) (*sql.Stmt, error) {
	d, e := h.ready()
	if e != nil {
		return nil, e
	}
	return d.Prepare(q)
}
func (h *Handle) Begin() (*Tx, error) { return h.BeginTxContext(context.Background(), nil) }
func (h *Handle) BeginTxContext(ctx context.Context, o *sql.TxOptions) (*Tx, error) {
	d, e := h.ready()
	if e != nil {
		return nil, e
	}
	return d.BeginTxContext(ctx, o)
}
func (h *Handle) Select(ctx context.Context, dest any, q string, a ...any) error {
	d, e := h.ready()
	if e != nil {
		return e
	}
	return d.Select(ctx, dest, q, a...)
}
func (h *Handle) HealthCheck() *datasource.Health {
	return h.HealthCheckContext(context.Background())
}

func (h *Handle) HealthCheckContext(ctx context.Context) *datasource.Health {
	d, e := h.ready()
	if e != nil {
		return &datasource.Health{Status: datasource.StatusDown, Details: map[string]any{"error": e.Error()}}
	}
	return d.HealthCheckContext(ctx)
}

type errorConnector struct{ err error }

func (h *Handle) Owns(tx *Tx) bool { d, err := h.ready(); return err == nil && d.Owns(tx) }

func (c errorConnector) Connect(context.Context) (driver.Conn, error) { return nil, c.err }
func (c errorConnector) Driver() driver.Driver                        { return c }
func (c errorConnector) Open(string) (driver.Conn, error)             { return nil, c.err }
