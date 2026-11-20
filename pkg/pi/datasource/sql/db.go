// Package sql provides functionalities to interact with SQL databases using the database/sql package.This package
// includes a wrapper around sql.DB and sql.Tx to provide additional features such as query logging, metrics recording,
// and error handling.
package sql

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel/trace"

	"github.com/sllt/pi/pkg/pi/datasource"
)

// DB is a wrapper around sql.DB which provides some more features.
type DB struct {
	// contains unexported or private fields
	*sql.DB
	logger  datasource.Logger
	config  *DBConfig
	metrics Metrics

	stopOnce   sync.Once
	bgCancel   context.CancelFunc
	bgWg       sync.WaitGroup
	closeOnce  sync.Once
	closeDBErr error
}

type Log struct {
	Type     string `json:"type"`
	Query    string `json:"query"`
	Duration int64  `json:"duration"`
	Args     []any  `json:"args,omitempty"`
}

const maxSlowQuerySQLLen = 512

var whitespaceRegex = regexp.MustCompile(`\s+`)

func (l *Log) PrettyPrint(writer io.Writer) {
	fmt.Fprintf(writer, "\u001B[38;5;8m%-32s \u001B[38;5;24m%-6s\u001B[0m %8d\u001B[38;5;8mµs\u001B[0m %s\n",
		l.Type, "SQL", l.Duration, clean(l.Query))
}

func clean(query string) string {
	query = whitespaceRegex.ReplaceAllString(query, " ")
	query = strings.TrimSpace(query)

	return query
}

func truncateSQL(query string, maxLen int) string {
	cleaned := clean(query)
	if maxLen <= 0 || len(cleaned) <= maxLen {
		return cleaned
	}

	const ellipsis = "..."
	if maxLen <= len(ellipsis) {
		return cleaned[:maxLen]
	}

	limit := maxLen - len(ellipsis)
	for limit > 0 && !utf8.RuneStart(cleaned[limit]) {
		limit--
	}

	return cleaned[:limit] + ellipsis
}

func sendStats(ctx context.Context, logger datasource.Logger, metrics Metrics, config *DBConfig, start time.Time, queryType, query string, args ...any) {
	elapsed := time.Since(start)
	duration := elapsed.Milliseconds()

	var (
		hostName      string
		database      string
		slowThreshold time.Duration
	)
	if config != nil {
		hostName = config.HostName
		database = config.Database
		slowThreshold = config.SlowQueryThreshold
	}

	if logger != nil {
		logger.Debug(&Log{
			Type:     queryType,
			Query:    query,
			Duration: duration,
			// Query arguments can contain credentials or password hashes.
		})

		if slowThreshold > 0 && elapsed >= slowThreshold {
			var traceID string
			if ctx != nil {
				if sc := trace.SpanFromContext(ctx).SpanContext(); sc.HasTraceID() {
					traceID = sc.TraceID().String()
				}
			}

			truncatedSQL := truncateSQL(query, maxSlowQuerySQLLen)
			if traceID != "" {
				logger.Warnf("slow SQL query (%s >= %s) op=%s trace_id=%s sql=%s",
					elapsed, slowThreshold, queryType, traceID, truncatedSQL)
			} else {
				logger.Warnf("slow SQL query (%s >= %s) op=%s sql=%s",
					elapsed, slowThreshold, queryType, truncatedSQL)
			}
		}
	}

	// This contains the fix for the nil pointer dereference
	if metrics != nil {
		metrics.RecordHistogram(context.Background(), "app_sql_stats", float64(duration), "hostname", hostName,
			"database", database, "type", getOperationType(query))
	}
}

func (d *DB) sendOperationStats(ctx context.Context, start time.Time, queryType, query string, args ...any) {
	sendStats(ctx, d.logger, d.metrics, d.config, start, queryType, query, args...)
}

func getOperationType(query string) string {
	query = strings.TrimSpace(query)
	words := strings.Split(query, " ")

	return strings.ToUpper(words[0])
}

func (d *DB) Query(query string, args ...any) (*sql.Rows, error) {
	defer d.sendOperationStats(context.Background(), time.Now(), "Query", query, args...)
	return d.DB.QueryContext(context.Background(), query, args...)
}

func (d *DB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	defer d.sendOperationStats(ctx, time.Now(), "QueryContext", query, args...)
	return d.DB.QueryContext(ctx, query, args...)
}

func (d *DB) Dialect() string {
	if d == nil || d.config == nil {
		return ""
	}
	return d.config.Dialect
}

func (d *DB) QueryRow(query string, args ...any) *sql.Row {
	defer d.sendOperationStats(context.Background(), time.Now(), "QueryRow", query, args...)
	return d.DB.QueryRowContext(context.Background(), query, args...)
}

func (d *DB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	defer d.sendOperationStats(ctx, time.Now(), "QueryRowContext", query, args...)
	return d.DB.QueryRowContext(ctx, query, args...)
}

func (d *DB) Exec(query string, args ...any) (sql.Result, error) {
	defer d.sendOperationStats(context.Background(), time.Now(), "Exec", query, args...)
	return d.DB.ExecContext(context.Background(), query, args...)
}

func (d *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	defer d.sendOperationStats(ctx, time.Now(), "ExecContext", query, args...)
	return d.DB.ExecContext(ctx, query, args...)
}

func (d *DB) Prepare(query string) (*sql.Stmt, error) {
	defer d.sendOperationStats(context.Background(), time.Now(), "Prepare", query)
	return d.DB.PrepareContext(context.Background(), query)
}

func (d *DB) Begin() (*Tx, error) {
	return d.BeginTxContext(context.Background(), nil)
}

// BeginTxContext starts a Pi transaction whose lifetime is bounded by ctx.
// It leaves the embedded database/sql BeginTx method and existing Begin callers compatible.
func (d *DB) BeginTxContext(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := d.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &Tx{Tx: tx, config: d.config, logger: d.logger, metrics: d.metrics, owner: d}, nil
}

func (d *DB) stopBackgroundLoops() {
	if d == nil {
		return
	}
	d.stopOnce.Do(func() {
		if d.bgCancel != nil {
			d.bgCancel()
		}
	})
	d.bgWg.Wait()
}

func (d *DB) Close() error {
	if d == nil {
		return nil
	}
	d.stopBackgroundLoops()
	d.closeOnce.Do(func() {
		if d.DB != nil {
			d.closeDBErr = d.DB.Close()
		}
	})

	return d.closeDBErr
}

type Tx struct {
	*sql.Tx
	owner   *DB
	config  *DBConfig
	logger  datasource.Logger
	metrics Metrics
}

// Owns reports transaction identity, not merely matching database configuration.
func (d *DB) Owns(tx *Tx) bool { return d != nil && tx != nil && tx.owner == d }

func (t *Tx) Dialect() string {
	if t == nil || t.config == nil {
		return ""
	}
	return t.config.Dialect
}

func (t *Tx) sendOperationStats(ctx context.Context, start time.Time, queryType, query string, args ...any) {
	sendStats(ctx, t.logger, t.metrics, t.config, start, queryType, query, args...)
}

func (t *Tx) Query(query string, args ...any) (*sql.Rows, error) {
	defer t.sendOperationStats(context.Background(), time.Now(), "TxQuery", query, args...)
	return t.Tx.QueryContext(context.Background(), query, args...)
}

func (t *Tx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	defer t.sendOperationStats(ctx, time.Now(), "TxQueryContext", query, args...)
	return t.Tx.QueryContext(ctx, query, args...)
}

func (t *Tx) QueryRow(query string, args ...any) *sql.Row {
	defer t.sendOperationStats(context.Background(), time.Now(), "TxQueryRow", query, args...)
	return t.Tx.QueryRowContext(context.Background(), query, args...)
}

func (t *Tx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	defer t.sendOperationStats(ctx, time.Now(), "TxQueryRowContext", query, args...)
	return t.Tx.QueryRowContext(ctx, query, args...)
}

func (t *Tx) Exec(query string, args ...any) (sql.Result, error) {
	defer t.sendOperationStats(context.Background(), time.Now(), "TxExec", query, args...)
	return t.Tx.ExecContext(context.Background(), query, args...)
}

func (t *Tx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	defer t.sendOperationStats(ctx, time.Now(), "TxExecContext", query, args...)
	return t.Tx.ExecContext(ctx, query, args...)
}

func (t *Tx) Prepare(query string) (*sql.Stmt, error) {
	defer t.sendOperationStats(context.Background(), time.Now(), "TxPrepare", query)
	return t.Tx.PrepareContext(context.Background(), query)
}

func (t *Tx) Commit() error {
	defer t.sendOperationStats(context.Background(), time.Now(), "TxCommit", "COMMIT")
	return t.Tx.Commit()
}

func (t *Tx) Rollback() error {
	defer t.sendOperationStats(context.Background(), time.Now(), "TxRollback", "ROLLBACK")
	return t.Tx.Rollback()
}

// Select runs a query with args and binds the result of the query to data.
// data should be a pointer to a slice or struct.
//
// Example:
//
//  1. Get multiple rows with only one column
//     ids := make([]int, 0)
//     err := db.Select(ctx, &ids, "select id from users")
//
//  2. Get a single object from database
//     type user struct {
//     Name  string
//     ID    int
//     Image string
//     }
//     u := user{}
//     err := db.Select(ctx, &u, "select * from users where id=?", 1)
//
//  3. Get array of objects from multiple rows
//     type user struct {
//     Name  string
//     ID    int
//     Image string `db:"image_url"`
//     }
//     users := []user{}
//     err := db.Select(ctx, &users, "select * from users")
//
//nolint:exhaustive // We only support slice and struct destinations.
func (d *DB) Select(ctx context.Context, data any, query string, args ...any) error {
	return selectData(ctx, d.logger, d.QueryContext, data, query, args...)
}

// Select executes query using the active transaction and binds rows into data.
func (t *Tx) Select(ctx context.Context, data any, query string, args ...any) error {
	return selectData(ctx, t.logger, t.QueryContext, data, query, args...)
}
