package sql

import (
	"context"
	"database/sql"
	"errors"

	"github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
	sqliteDriver "modernc.org/sqlite"
)

func IsUniqueViolation(err error) bool {
	var sqliteErr *sqliteDriver.Error
	if errors.As(err, &sqliteErr) {
		return sqliteErr.Code() == 1555 || sqliteErr.Code() == 2067
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		return mysqlErr.Number == 1062
	}
	var pgErr *pq.Error
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// ErrorExecutor fails every operation without touching a database. It lets
// compatibility APIs returning Executor reject an invalid transaction scope.
func ErrorExecutor(err error) Executor { return rejectedExecutor{err} }

type rejectedExecutor struct{ err error }

func (e rejectedExecutor) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return nil, e.err
}
func (e rejectedExecutor) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, e.err
}
func (e rejectedExecutor) Select(context.Context, any, string, ...any) error { return e.err }
func (e rejectedExecutor) QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row {
	db := sql.OpenDB(errorConnector{e.err})
	row := db.QueryRowContext(ctx, q, args...)
	_ = db.Close()
	return row
}
