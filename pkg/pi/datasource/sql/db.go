// Package sql provides functionalities to interact with SQL databases using the database/sql package.This package
// includes a wrapper around sql.DB and sql.Tx to provide additional features such as query logging, metrics recording,
// and error handling.
package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/sllt/pi/pkg/pi/datasource"
)

// DB is a wrapper around sql.DB which provides some more features.
type DB struct {
	// contains unexported or private fields
	*sql.DB
	logger  datasource.Logger
	config  *DBConfig
	metrics Metrics
}

type Log struct {
	Type     string `json:"type"`
	Query    string `json:"query"`
	Duration int64  `json:"duration"`
	Args     []any  `json:"args,omitempty"`
}

var (
	errSelectDataNotPointer = errors.New("data is not a pointer")
	errSelectUnsupported    = errors.New("unsupported select destination type")
)

func (l *Log) PrettyPrint(writer io.Writer) {
	fmt.Fprintf(writer, "\u001B[38;5;8m%-32s \u001B[38;5;24m%-6s\u001B[0m %8d\u001B[38;5;8mµs\u001B[0m %s\n",
		l.Type, "SQL", l.Duration, clean(l.Query))
}

func clean(query string) string {
	query = regexp.MustCompile(`\s+`).ReplaceAllString(query, " ")
	query = strings.TrimSpace(query)

	return query
}

func sendStats(logger datasource.Logger, metrics Metrics, config *DBConfig, start time.Time, queryType, query string, args ...any) {
	duration := time.Since(start).Milliseconds()

	if logger != nil {
		logger.Debug(&Log{
			Type:     queryType,
			Query:    query,
			Duration: duration,
			Args:     args,
		})
	}

	// This contains the fix for the nil pointer dereference
	if metrics != nil {
		metrics.RecordHistogram(context.Background(), "app_sql_stats", float64(duration), "hostname", config.HostName,
			"database", config.Database, "type", getOperationType(query))
	}
}

func (d *DB) sendOperationStats(start time.Time, queryType, query string, args ...any) {
	sendStats(d.logger, d.metrics, d.config, start, queryType, query, args...)
}

func getOperationType(query string) string {
	query = strings.TrimSpace(query)
	words := strings.Split(query, " ")

	return strings.ToUpper(words[0])
}

func (d *DB) Query(query string, args ...any) (*sql.Rows, error) {
	defer d.sendOperationStats(time.Now(), "Query", query, args...)
	return d.DB.QueryContext(context.Background(), query, args...)
}

func (d *DB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	defer d.sendOperationStats(time.Now(), "QueryContext", query, args...)
	return d.DB.QueryContext(ctx, query, args...)
}

func (d *DB) Dialect() string {
	return d.config.Dialect
}

func (d *DB) QueryRow(query string, args ...any) *sql.Row {
	defer d.sendOperationStats(time.Now(), "QueryRow", query, args...)
	return d.DB.QueryRowContext(context.Background(), query, args...)
}

func (d *DB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	defer d.sendOperationStats(time.Now(), "QueryRowContext", query, args...)
	return d.DB.QueryRowContext(ctx, query, args...)
}

func (d *DB) Exec(query string, args ...any) (sql.Result, error) {
	defer d.sendOperationStats(time.Now(), "Exec", query, args...)
	return d.DB.ExecContext(context.Background(), query, args...)
}

func (d *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	defer d.sendOperationStats(time.Now(), "ExecContext", query, args...)
	return d.DB.ExecContext(ctx, query, args...)
}

func (d *DB) Prepare(query string) (*sql.Stmt, error) {
	defer d.sendOperationStats(time.Now(), "Prepare", query)
	return d.DB.PrepareContext(context.Background(), query)
}

func (d *DB) Begin() (*Tx, error) {
	tx, err := d.DB.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, err
	}

	return &Tx{Tx: tx, config: d.config, logger: d.logger, metrics: d.metrics}, nil
}

func (d *DB) Close() error {
	if d.DB != nil {
		return d.DB.Close()
	}

	return nil
}

type Tx struct {
	*sql.Tx
	config  *DBConfig
	logger  datasource.Logger
	metrics Metrics
}

func (t *Tx) sendOperationStats(start time.Time, queryType, query string, args ...any) {
	sendStats(t.logger, t.metrics, t.config, start, queryType, query, args...)
}

func (t *Tx) Query(query string, args ...any) (*sql.Rows, error) {
	defer t.sendOperationStats(time.Now(), "TxQuery", query, args...)
	return t.Tx.QueryContext(context.Background(), query, args...)
}

func (t *Tx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	defer t.sendOperationStats(time.Now(), "TxQueryContext", query, args...)
	return t.Tx.QueryContext(ctx, query, args...)
}

func (t *Tx) QueryRow(query string, args ...any) *sql.Row {
	defer t.sendOperationStats(time.Now(), "TxQueryRow", query, args...)
	return t.Tx.QueryRowContext(context.Background(), query, args...)
}

func (t *Tx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	defer t.sendOperationStats(time.Now(), "TxQueryRowContext", query, args...)
	return t.Tx.QueryRowContext(ctx, query, args...)
}

func (t *Tx) Exec(query string, args ...any) (sql.Result, error) {
	defer t.sendOperationStats(time.Now(), "TxExec", query, args...)
	return t.Tx.ExecContext(context.Background(), query, args...)
}

func (t *Tx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	defer t.sendOperationStats(time.Now(), "TxExecContext", query, args...)
	return t.Tx.ExecContext(ctx, query, args...)
}

func (t *Tx) Prepare(query string) (*sql.Stmt, error) {
	defer t.sendOperationStats(time.Now(), "TxPrepare", query)
	return t.Tx.PrepareContext(context.Background(), query)
}

func (t *Tx) Commit() error {
	defer t.sendOperationStats(time.Now(), "TxCommit", "COMMIT")
	return t.Tx.Commit()
}

func (t *Tx) Rollback() error {
	defer t.sendOperationStats(time.Now(), "TxRollback", "ROLLBACK")
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

type queryFunc func(ctx context.Context, query string, args ...any) (*sql.Rows, error)

//nolint:exhaustive // We only support slice and struct destinations.
func selectData(ctx context.Context, logger datasource.Logger, queryContext queryFunc, data any, query string, args ...any) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// Destination must be settable so callers can read scanned results.
	rvo := reflect.ValueOf(data)
	if !rvo.IsValid() || rvo.Kind() != reflect.Ptr || rvo.IsNil() {
		if logger != nil {
			logger.Error("we did not get a pointer. data is not settable.")
		}

		return errSelectDataNotPointer
	}

	rv := rvo.Elem()

	switch rv.Kind() {
	case reflect.Slice:
		return selectSlice(ctx, logger, queryContext, query, args, rvo, rv)
	case reflect.Struct:
		return selectStruct(ctx, logger, queryContext, query, args, rv)
	default:
		if logger != nil {
			logger.Debugf("a pointer to %v was not expected.", rv.Kind().String())
		}

		return fmt.Errorf("%w: %s", errSelectUnsupported, rv.Kind())
	}
}

func selectSlice(ctx context.Context, logger datasource.Logger, queryContext queryFunc, query string, args []any, rvo, rv reflect.Value) error {
	rows, err := queryContext(ctx, query, args...)
	if err != nil {
		if logger != nil {
			logger.Errorf("error running query: %v", err)
		}

		return err
	}

	defer rows.Close()

	for rows.Next() {
		val := reflect.New(rv.Type().Elem())

		if rv.Type().Elem().Kind() == reflect.Struct {
			if err := rowsToStruct(rows, val); err != nil {
				return err
			}
		} else if err := rows.Scan(val.Interface()); err != nil {
			return err
		}

		rv = reflect.Append(rv, val.Elem())
	}

	if err := rows.Err(); err != nil {
		if logger != nil {
			logger.Errorf("error parsing rows : %v", err)
		}

		return err
	}

	if rvo.Elem().CanSet() {
		rvo.Elem().Set(rv)
	}

	return nil
}

func selectStruct(ctx context.Context, logger datasource.Logger, queryContext queryFunc, query string, args []any, rv reflect.Value) error {
	rows, err := queryContext(ctx, query, args...)
	if err != nil {
		if logger != nil {
			logger.Errorf("error running query: %v", err)
		}

		return err
	}

	defer rows.Close()

	rowFound := false

	for rows.Next() {
		rowFound = true
		if err := rowsToStruct(rows, rv); err != nil {
			return err
		}
	}

	if err := rows.Err(); err != nil {
		if logger != nil {
			logger.Errorf("error parsing rows : %v", err)
		}

		return err
	}

	if !rowFound {
		return sql.ErrNoRows
	}

	return nil
}

func rowsToStruct(rows *sql.Rows, vo reflect.Value) error {
	v := vo
	if vo.Kind() == reflect.Ptr {
		v = vo.Elem()
	}

	// Map fields and their indexes by normalized name
	fieldNameIndex := map[string]int{}

	for i := 0; i < v.Type().NumField(); i++ {
		var name string

		f := v.Type().Field(i)
		tag := f.Tag.Get("db")

		if tag != "" {
			name = tag
		} else {
			name = ToSnakeCase(f.Name)
		}

		fieldNameIndex[name] = i
	}

	fields := []any{}
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	for _, c := range columns {
		if i, ok := fieldNameIndex[c]; ok {
			fields = append(fields, v.Field(i).Addr().Interface())
		} else {
			var i any

			fields = append(fields, &i)
		}
	}

	if err := rows.Scan(fields...); err != nil {
		return err
	}

	if vo.CanSet() {
		vo.Set(v)
	}

	return nil
}

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

func ToSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")

	return strings.ToLower(snake)
}
