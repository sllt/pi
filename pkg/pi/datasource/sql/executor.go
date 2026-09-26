package sql

import (
	"context"
	"database/sql"
)

// Executor captures the query operations shared by DB and Tx.
// It is useful for transaction-aware repositories that can run against either.
type Executor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	Select(ctx context.Context, data any, query string, args ...any) error
}
