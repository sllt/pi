package sql

import (
	"context"
	"database/sql"

	"github.com/sllt/pi/pkg/pi/datasource"
)

// Client is the canonical SQL database capability interface exposed by App.SQL() and Context.SQL().
// It is satisfied by *DB, *Handle, and *dbresolver.Resolver.
type Client interface {
	Executor

	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
	Exec(query string, args ...any) (sql.Result, error)
	Prepare(query string) (*sql.Stmt, error)

	Begin() (*Tx, error)
	BeginTxContext(ctx context.Context, opts *sql.TxOptions) (*Tx, error)

	HealthCheck() *datasource.Health
	HealthCheckContext(ctx context.Context) *datasource.Health
	Owns(tx *Tx) bool
	Dialect() string
	Close() error
}

var (
	_ Client = (*DB)(nil)
	_ Client = (*Handle)(nil)
)
