package qb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strings"
)

var (
	errNilQuery             = errors.New("[qb] query is nil")
	errEmptyTable           = errors.New("[qb] table name cannot be empty")
	errEmptyJoinClause      = errors.New("[qb] join clause cannot be empty")
	errEmptyWhereColumn     = errors.New("[qb] column name cannot be empty")
	errNegativeLimit        = errors.New("[qb] limit cannot be negative")
	errNegativeOffset       = errors.New("[qb] offset cannot be negative")
	errInvalidPageSize      = errors.New("[qb] page size must be greater than 0")
	errPageOverflow         = errors.New("[qb] page offset overflows int")
	errScanPageDestNotSlice = errors.New("[qb] ScanPage dest must be a non-nil pointer to a slice")

	distinctKeywordPattern = regexp.MustCompile(`(?i)\bdistinct\b`)
)

// Executor is the minimal query execution interface required by Query.
// It is satisfied by *sql.DB, *sql.Handle, *sql.Tx, sql.Client, sql.Executor,
// and *dbresolver.Resolver without importing the parent sql package.
type Executor interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	Select(ctx context.Context, data any, query string, args ...any) error
}

type joinClause struct {
	joinType string
	clause   string
	args     []any
}

type condClause struct {
	sql  string
	args []any
}

// Query is a lightweight, fluent SQL query builder and page executor.
type Query struct {
	table   string
	dialect string
	columns []string
	joins   []joinClause
	wheres  []condClause
	groupBy []string
	havings []condClause
	orderBy []string
	limit   *int
	offset  *int
	err     error
}

// From creates a new Query targeting table (which may include an alias, e.g. "posts p").
func From(table string) *Query {
	trimmed := strings.TrimSpace(table)
	if trimmed == "" {
		return &Query{err: errEmptyTable}
	}
	return &Query{table: trimmed}
}

func (q *Query) setErr(err error) {
	if q != nil && q.err == nil {
		q.err = err
	}
}

// Dialect pins the SQL dialect used by ToSQL and ToCountSQL (e.g. "postgres", "mysql", "sqlite").
// Leaving it empty allows Scan/First/Count/ScanPage to auto-detect from exec when it implements DialectProvider.
func (q *Query) Dialect(dialect string) *Query {
	if q == nil {
		return nil
	}
	trimmed := strings.TrimSpace(dialect)
	if trimmed == "" {
		q.dialect = ""
		return q
	}
	if _, err := normalizeDialect(trimmed); err != nil {
		q.setErr(err)
		return q
	}
	q.dialect = trimmed
	return q
}

// Select appends projected columns or expressions. Defaults to "*" when none are added.
func (q *Query) Select(columns ...string) *Query {
	if q == nil {
		return nil
	}
	for _, col := range columns {
		if trimmed := strings.TrimSpace(col); trimmed != "" {
			q.columns = append(q.columns, trimmed)
		}
	}
	return q
}

// Join appends an INNER/plain JOIN clause (e.g. "users u ON u.id = p.user_id").
func (q *Query) Join(clause string, args ...any) *Query {
	return q.addJoin("JOIN", clause, args...)
}

// LeftJoin appends a LEFT JOIN clause (e.g. "users u ON u.id = p.user_id").
func (q *Query) LeftJoin(clause string, args ...any) *Query {
	return q.addJoin("LEFT JOIN", clause, args...)
}

func (q *Query) addJoin(joinType, clause string, args ...any) *Query {
	if q == nil {
		return nil
	}
	trimmed := strings.TrimSpace(clause)
	if trimmed == "" {
		q.setErr(errEmptyJoinClause)
		return q
	}
	q.joins = append(q.joins, joinClause{
		joinType: joinType,
		clause:   trimmed,
		args:     append([]any(nil), args...),
	})
	return q
}

// Where appends a WHERE condition with optional bind arguments.
func (q *Query) Where(cond string, args ...any) *Query {
	if q == nil {
		return nil
	}
	trimmed := strings.TrimSpace(cond)
	if trimmed == "" {
		if len(args) > 0 {
			q.setErr(errors.New("[qb] where condition cannot be empty when args are provided"))
		}
		return q
	}
	q.wheres = append(q.wheres, condClause{
		sql:  trimmed,
		args: append([]any(nil), args...),
	})
	return q
}

// WhereIf appends a WHERE condition only when ok is true.
func (q *Query) WhereIf(ok bool, cond string, args ...any) *Query {
	if !ok {
		return q
	}
	return q.Where(cond, args...)
}

// WhereIn appends a "column IN (?, ...)" condition accepting any typed Go slice
// (e.g. []int64, []string). Empty or non-slice values record an error on the Query.
func (q *Query) WhereIn(column string, values any) *Query {
	if q == nil {
		return nil
	}
	col := strings.TrimSpace(column)
	if col == "" {
		q.setErr(errEmptyWhereColumn)
		return q
	}
	vals, ok := convertInterfaceToMap(values)
	if !ok {
		q.setErr(fmt.Errorf("[qb] WhereIn %q: values must be a slice", col))
		return q
	}
	if len(vals) == 0 {
		q.setErr(fmt.Errorf("[qb] WhereIn %q: slice cannot be empty", col))
		return q
	}

	placeholders := strings.TrimRight(strings.Repeat("?,", len(vals)), ",")
	q.wheres = append(q.wheres, condClause{
		sql:  fmt.Sprintf("%s IN (%s)", col, placeholders),
		args: vals,
	})
	return q
}

// WhereInIf appends a WhereIn condition only when ok is true (no-op when !ok).
func (q *Query) WhereInIf(ok bool, column string, values any) *Query {
	if !ok {
		return q
	}
	return q.WhereIn(column, values)
}

// GroupBy appends GROUP BY expressions.
func (q *Query) GroupBy(cols ...string) *Query {
	if q == nil {
		return nil
	}
	for _, col := range cols {
		if trimmed := strings.TrimSpace(col); trimmed != "" {
			q.groupBy = append(q.groupBy, trimmed)
		}
	}
	return q
}

// Having appends a HAVING condition with optional bind arguments.
func (q *Query) Having(cond string, args ...any) *Query {
	if q == nil {
		return nil
	}
	trimmed := strings.TrimSpace(cond)
	if trimmed == "" {
		if len(args) > 0 {
			q.setErr(errors.New("[qb] having condition cannot be empty when args are provided"))
		}
		return q
	}
	q.havings = append(q.havings, condClause{
		sql:  trimmed,
		args: append([]any(nil), args...),
	})
	return q
}

// OrderBy appends ORDER BY expressions.
func (q *Query) OrderBy(exprs ...string) *Query {
	if q == nil {
		return nil
	}
	for _, expr := range exprs {
		if trimmed := strings.TrimSpace(expr); trimmed != "" {
			q.orderBy = append(q.orderBy, trimmed)
		}
	}
	return q
}

// Limit sets the LIMIT clause.
func (q *Query) Limit(n int) *Query {
	if q == nil {
		return nil
	}
	if n < 0 {
		q.setErr(errNegativeLimit)
		return q
	}
	v := n
	q.limit = &v
	return q
}

// Offset sets the OFFSET clause.
func (q *Query) Offset(n int) *Query {
	if q == nil {
		return nil
	}
	if n < 0 {
		q.setErr(errNegativeOffset)
		return q
	}
	v := n
	q.offset = &v
	return q
}

// Page sets Limit and Offset from 1-based page and pageSize.
// page values < 1 are clamped to 1; pageSize must be > 0.
func (q *Query) Page(page, pageSize int) *Query {
	if q == nil {
		return nil
	}
	if pageSize <= 0 {
		q.setErr(errInvalidPageSize)
		return q
	}
	if page < 1 {
		page = 1
	}
	if page-1 > math.MaxInt/pageSize {
		q.setErr(errPageOverflow)
		return q
	}
	lim := pageSize
	off := (page - 1) * pageSize
	q.limit = &lim
	q.offset = &off
	return q
}

// Clone returns a deep copy of q so callers can fork a base query safely.
func (q *Query) Clone() *Query {
	if q == nil {
		return nil
	}
	cp := &Query{
		table:   q.table,
		dialect: q.dialect,
		columns: append([]string(nil), q.columns...),
		groupBy: append([]string(nil), q.groupBy...),
		orderBy: append([]string(nil), q.orderBy...),
		err:     q.err,
	}
	if len(q.joins) > 0 {
		cp.joins = make([]joinClause, len(q.joins))
		for i, j := range q.joins {
			cp.joins[i] = joinClause{
				joinType: j.joinType,
				clause:   j.clause,
				args:     append([]any(nil), j.args...),
			}
		}
	}
	if len(q.wheres) > 0 {
		cp.wheres = make([]condClause, len(q.wheres))
		for i, w := range q.wheres {
			cp.wheres[i] = condClause{
				sql:  w.sql,
				args: append([]any(nil), w.args...),
			}
		}
	}
	if len(q.havings) > 0 {
		cp.havings = make([]condClause, len(q.havings))
		for i, h := range q.havings {
			cp.havings[i] = condClause{
				sql:  h.sql,
				args: append([]any(nil), h.args...),
			}
		}
	}
	if q.limit != nil {
		v := *q.limit
		cp.limit = &v
	}
	if q.offset != nil {
		v := *q.offset
		cp.offset = &v
	}
	return cp
}

// ToSQL builds the SELECT statement and bind arguments, rebinding "?" to "$1, $2, ..."
// when the configured dialect is PostgreSQL.
func (q *Query) ToSQL() (string, []any, error) {
	return q.toSQLWithDialect(q.dialectOrEmpty())
}

// ToCountSQL builds a COUNT(1) query with ORDER BY, LIMIT, and OFFSET stripped.
// When GROUP BY or HAVING is present, it wraps the grouped query in
// "SELECT COUNT(1) FROM (<subquery>) AS _qb_count".
func (q *Query) ToCountSQL() (string, []any, error) {
	return q.toCountSQLWithDialect(q.dialectOrEmpty())
}

func (q *Query) dialectOrEmpty() string {
	if q == nil {
		return ""
	}
	return q.dialect
}

func (q *Query) resolveBuilder(dialectOverride string) (Builder, error) {
	if strings.TrimSpace(dialectOverride) == "" {
		return *defaultBuilder, nil
	}
	b, err := New(dialectOverride)
	if err != nil {
		return Builder{}, err
	}
	return *b, nil
}

func (q *Query) effectiveDialect(exec Executor) string {
	if q != nil && q.dialect != "" {
		return q.dialect
	}
	if dp, ok := exec.(DialectProvider); ok {
		if d := strings.TrimSpace(dp.Dialect()); d != "" {
			return d
		}
	}
	return ""
}

func (q *Query) toSQLWithDialect(dialectOverride string) (string, []any, error) {
	if q == nil {
		return "", nil, errNilQuery
	}
	if q.err != nil {
		return "", nil, q.err
	}
	if q.table == "" {
		return "", nil, errEmptyTable
	}

	b, err := q.resolveBuilder(dialectOverride)
	if err != nil {
		return "", nil, err
	}

	sqlStr, args := q.buildSelectSQL(true, b.dialect)
	return b.finalizeQuery(sqlStr, args)
}

func (q *Query) hasDistinctColumns() bool {
	for _, col := range q.columns {
		if distinctKeywordPattern.MatchString(col) {
			return true
		}
	}
	return false
}

func (q *Query) toCountSQLWithDialect(dialectOverride string) (string, []any, error) {
	if q == nil {
		return "", nil, errNilQuery
	}
	if q.err != nil {
		return "", nil, q.err
	}
	if q.table == "" {
		return "", nil, errEmptyTable
	}

	b, err := q.resolveBuilder(dialectOverride)
	if err != nil {
		return "", nil, err
	}

	if len(q.groupBy) > 0 || len(q.havings) > 0 || q.hasDistinctColumns() {
		innerSQL, args := q.buildSelectSQL(false, b.dialect)
		return b.finalizeQuery("SELECT COUNT(1) FROM ("+innerSQL+") AS _qb_count", args)
	}

	var sb strings.Builder
	var args []any

	sb.WriteString("SELECT COUNT(1) FROM ")
	sb.WriteString(q.table)
	q.writeJoinsAndWheres(&sb, &args)

	return b.finalizeQuery(sb.String(), args)
}

func (q *Query) buildSelectSQL(includePaginationAndOrder bool, dialect Dialect) (string, []any) {
	var sb strings.Builder
	var args []any

	sb.WriteString("SELECT ")
	if len(q.columns) > 0 {
		sb.WriteString(strings.Join(q.columns, ", "))
	} else {
		sb.WriteByte('*')
	}
	sb.WriteString(" FROM ")
	sb.WriteString(q.table)

	q.writeJoinsAndWheres(&sb, &args)

	if len(q.groupBy) > 0 {
		sb.WriteString(" GROUP BY ")
		sb.WriteString(strings.Join(q.groupBy, ", "))
	}

	if len(q.havings) > 0 {
		sb.WriteString(" HAVING ")
		writeCondClauses(&sb, &args, q.havings)
	}

	if includePaginationAndOrder {
		if len(q.orderBy) > 0 {
			sb.WriteString(" ORDER BY ")
			sb.WriteString(strings.Join(q.orderBy, ", "))
		}
		switch {
		case q.limit != nil && q.offset != nil:
			sb.WriteString(" LIMIT ? OFFSET ?")
			args = append(args, *q.limit, *q.offset)
		case q.limit != nil:
			sb.WriteString(" LIMIT ?")
			args = append(args, *q.limit)
		case q.offset != nil:
			switch dialect {
			case DialectSQLite:
				sb.WriteString(" LIMIT -1 OFFSET ?")
			case DialectPostgres:
				sb.WriteString(" OFFSET ?")
			default:
				sb.WriteString(" LIMIT 18446744073709551615 OFFSET ?")
			}
			args = append(args, *q.offset)
		}
	}

	return sb.String(), args
}

func (q *Query) writeJoinsAndWheres(sb *strings.Builder, args *[]any) {
	for _, j := range q.joins {
		sb.WriteByte(' ')
		sb.WriteString(j.joinType)
		sb.WriteByte(' ')
		sb.WriteString(j.clause)
		*args = append(*args, j.args...)
	}

	if len(q.wheres) > 0 {
		sb.WriteString(" WHERE ")
		writeCondClauses(sb, args, q.wheres)
	}
}

func writeCondClauses(sb *strings.Builder, args *[]any, clauses []condClause) {
	for i, c := range clauses {
		if i > 0 {
			sb.WriteString(" AND ")
		}
		sb.WriteByte('(')
		sb.WriteString(c.sql)
		sb.WriteByte(')')
		*args = append(*args, c.args...)
	}
}

// Scan executes the built SELECT query via exec.Select and binds rows into dest.
func (q *Query) Scan(ctx context.Context, exec Executor, dest any) error {
	if q == nil {
		return errNilQuery
	}
	if exec == nil {
		return errNilExecutor
	}
	if ctx == nil {
		ctx = context.Background()
	}

	query, args, err := q.toSQLWithDialect(q.effectiveDialect(exec))
	if err != nil {
		return err
	}

	return exec.Select(ctx, dest, query, args...)
}

// First clones the query, applies LIMIT 1 if no limit is already set, and binds via exec.Select.
func (q *Query) First(ctx context.Context, exec Executor, dest any) error {
	if q == nil {
		return errNilQuery
	}
	if exec == nil {
		return errNilExecutor
	}
	if q.err != nil {
		return q.err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	cloned := q.Clone()
	if cloned.limit == nil {
		cloned.Limit(1)
	}

	query, args, err := cloned.toSQLWithDialect(cloned.effectiveDialect(exec))
	if err != nil {
		return err
	}

	return exec.Select(ctx, dest, query, args...)
}

// Count executes the COUNT query and returns the matching row count.
func (q *Query) Count(ctx context.Context, exec Executor) (int64, error) {
	if q == nil {
		return 0, errNilQuery
	}
	if exec == nil {
		return 0, errNilExecutor
	}
	if ctx == nil {
		ctx = context.Background()
	}

	query, args, err := q.toCountSQLWithDialect(q.effectiveDialect(exec))
	if err != nil {
		return 0, err
	}

	row := exec.QueryRowContext(ctx, query, args...)
	if row == nil {
		return 0, errNilExecutor
	}

	var total int64
	if err := row.Scan(&total); err != nil {
		return 0, err
	}

	return total, nil
}

// ScanPage executes Count first; if total == 0 it clears dest to an empty non-nil
// slice and returns (0, nil) without running the page query.
// Otherwise it executes Scan and returns (total, err).
func (q *Query) ScanPage(ctx context.Context, exec Executor, dest any) (int64, error) {
	rvo := reflect.ValueOf(dest)
	if !rvo.IsValid() || rvo.Kind() != reflect.Ptr || rvo.IsNil() || rvo.Elem().Kind() != reflect.Slice {
		return 0, errScanPageDestNotSlice
	}

	total, err := q.Count(ctx, exec)
	if err != nil {
		return 0, err
	}
	if total == 0 {
		rvo.Elem().Set(reflect.MakeSlice(rvo.Elem().Type(), 0, 0))
		return 0, nil
	}

	err = q.Scan(ctx, exec, dest)
	return total, err
}
