package qb

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Dialect represents a SQL dialect that qb can generate queries for.
type Dialect string

const (
	DialectMySQL    Dialect = "mysql"
	DialectPostgres Dialect = "postgres"
	DialectSQLite   Dialect = "sqlite"
)

var (
	errUnsupportedDialect        = errors.New("[builder] unsupported dialect")
	errFeatureUnsupportedDialect = errors.New("[builder] feature is not supported for dialect")
	errEmptyConflictColumns      = errors.New("[builder] conflict columns cannot be empty")
	errNilDialectProvider        = errors.New("[builder] dialect provider is nil")
)

// Builder builds SQL for a specific dialect.
type Builder struct {
	dialect Dialect
}

// DialectProvider describes a type that can expose SQL dialect.
type DialectProvider interface {
	Dialect() string
}

var defaultBuilder = &Builder{dialect: DialectMySQL}

// New returns a Builder for the provided dialect.
//
// Supported values include:
//   - mysql, mariadb
//   - postgres, postgresql, supabase, cockroachdb
//   - sqlite, sqlite3
func New(dialect string) (*Builder, error) {
	d, err := normalizeDialect(dialect)
	if err != nil {
		return nil, err
	}

	return &Builder{dialect: d}, nil
}

// FromDB creates a Builder from a provider that exposes Dialect().
func FromDB(db DialectProvider) (*Builder, error) {
	if db == nil {
		return nil, errNilDialectProvider
	}

	return New(db.Dialect())
}

// BuildSelectWithDialect builds a SELECT query for the given dialect.
func BuildSelectWithDialect(dialect, table string, where map[string]interface{}, selectField []string) (string, []interface{}, error) {
	b, err := New(dialect)
	if err != nil {
		return "", nil, err
	}

	return b.BuildSelect(table, where, selectField)
}

// BuildUpdateWithDialect builds an UPDATE query for the given dialect.
func BuildUpdateWithDialect(dialect, table string, where map[string]interface{}, update map[string]interface{}) (string, []interface{}, error) {
	b, err := New(dialect)
	if err != nil {
		return "", nil, err
	}

	return b.BuildUpdate(table, where, update)
}

// BuildDeleteWithDialect builds a DELETE query for the given dialect.
func BuildDeleteWithDialect(dialect, table string, where map[string]interface{}) (string, []interface{}, error) {
	b, err := New(dialect)
	if err != nil {
		return "", nil, err
	}

	return b.BuildDelete(table, where)
}

// BuildInsertWithDialect builds an INSERT query for the given dialect.
func BuildInsertWithDialect(dialect, table string, data []map[string]interface{}) (string, []interface{}, error) {
	b, err := New(dialect)
	if err != nil {
		return "", nil, err
	}

	return b.BuildInsert(table, data)
}

// BuildInsertIgnoreWithDialect builds an INSERT IGNORE-style query for the given dialect.
func BuildInsertIgnoreWithDialect(dialect, table string, data []map[string]interface{}) (string, []interface{}, error) {
	b, err := New(dialect)
	if err != nil {
		return "", nil, err
	}

	return b.BuildInsertIgnore(table, data)
}

// BuildReplaceInsertWithDialect builds a REPLACE-style query for the given dialect.
func BuildReplaceInsertWithDialect(dialect, table string, data []map[string]interface{}) (string, []interface{}, error) {
	b, err := New(dialect)
	if err != nil {
		return "", nil, err
	}

	return b.BuildReplaceInsert(table, data)
}

// BuildInsertOnDuplicateWithDialect builds an INSERT ... ON DUPLICATE KEY UPDATE query for the given dialect.
func BuildInsertOnDuplicateWithDialect(dialect, table string, data []map[string]interface{}, update map[string]interface{}) (string, []interface{}, error) {
	b, err := New(dialect)
	if err != nil {
		return "", nil, err
	}

	return b.BuildInsertOnDuplicate(table, data, update)
}

// NamedQueryWithDialect expands named placeholders using the given dialect placeholder format.
func NamedQueryWithDialect(dialect, sql string, data map[string]interface{}) (string, []interface{}, error) {
	b, err := New(dialect)
	if err != nil {
		return "", nil, err
	}

	return b.NamedQuery(sql, data)
}

func normalizeDialect(dialect string) (Dialect, error) {
	switch strings.ToLower(strings.TrimSpace(dialect)) {
	case "", string(DialectMySQL), "mariadb":
		return DialectMySQL, nil
	case string(DialectPostgres), "postgresql", "supabase", "cockroachdb":
		return DialectPostgres, nil
	case string(DialectSQLite), "sqlite3":
		return DialectSQLite, nil
	default:
		return "", fmt.Errorf("%w: %q", errUnsupportedDialect, dialect)
	}
}

func (b Builder) rebindQuery(query string) string {
	if b.dialect != DialectPostgres {
		return query
	}

	var (
		counter int = 1
		out     strings.Builder
	)

	out.Grow(len(query) + 8)

	for i := 0; i < len(query); i++ {
		if query[i] != '?' {
			out.WriteByte(query[i])
			continue
		}

		out.WriteByte('$')
		out.WriteString(strconv.Itoa(counter))
		counter++
	}

	return out.String()
}

func (b Builder) finalizeQuery(query string, vals []interface{}) (string, []interface{}, error) {
	return b.rebindQuery(query), vals, nil
}

func (b Builder) lockClause(lockMode string) (string, error) {
	switch b.dialect {
	case DialectMySQL:
		switch lockMode {
		case "share":
			return " LOCK IN SHARE MODE", nil
		case "exclusive":
			return " FOR UPDATE", nil
		default:
			return "", errNotAllowedLockMode
		}
	case DialectPostgres:
		switch lockMode {
		case "share":
			return " FOR SHARE", nil
		case "exclusive":
			return " FOR UPDATE", nil
		default:
			return "", errNotAllowedLockMode
		}
	case DialectSQLite:
		return "", errNotAllowedLockMode
	default:
		return "", fmt.Errorf("%w: %q", errUnsupportedDialect, b.dialect)
	}
}

func (b Builder) limitIdentifier() string {
	switch b.dialect {
	case DialectPostgres:
		return "ctid"
	case DialectSQLite:
		return "rowid"
	default:
		return ""
	}
}

func (b Builder) unsupportedFeature(feature string) error {
	return fmt.Errorf("%w: %s for %s", errFeatureUnsupportedDialect, feature, b.dialect)
}
