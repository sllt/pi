package qb

import (
	"fmt"
	"strings"
)

// BuildUpsert builds an upsert query using MySQL dialect for backward-compatible defaults.
//
// For MySQL, conflictColumns is ignored and ON DUPLICATE KEY semantics are used.
// For PostgreSQL and SQLite, conflictColumns is required when update is non-empty.
func BuildUpsert(table string, data []map[string]interface{}, conflictColumns []string, update map[string]interface{}) (string, []interface{}, error) {
	return defaultBuilder.BuildUpsert(table, data, conflictColumns, update)
}

// BuildUpsertWithDialect builds an upsert query for the provided dialect.
func BuildUpsertWithDialect(dialect, table string, data []map[string]interface{}, conflictColumns []string, update map[string]interface{}) (string, []interface{}, error) {
	b, err := New(dialect)
	if err != nil {
		return "", nil, err
	}

	return b.BuildUpsert(table, data, conflictColumns, update)
}

// BuildUpsert builds an upsert query for the current builder dialect.
func (b Builder) BuildUpsert(table string, data []map[string]interface{}, conflictColumns []string, update map[string]interface{}) (string, []interface{}, error) {
	if len(update) == 0 {
		switch b.dialect {
		case DialectMySQL:
			return b.buildInsert(table, data, ignoreInsert)
		case DialectPostgres, DialectSQLite:
			insertCond, insertVals, err := b.buildInsertRaw(table, data, commonInsert)
			if err != nil {
				return "", nil, err
			}

			target, err := buildConflictTarget(conflictColumns)
			if err != nil {
				return "", nil, err
			}

			if target == "" {
				insertCond += " ON CONFLICT DO NOTHING"
			} else {
				insertCond += fmt.Sprintf(" ON CONFLICT %s DO NOTHING", target)
			}

			return b.finalizeQuery(insertCond, insertVals)
		default:
			return "", nil, fmt.Errorf("%w: %q", errUnsupportedDialect, b.dialect)
		}
	}

	switch b.dialect {
	case DialectMySQL:
		insertCond, insertVals, err := b.buildInsertRaw(table, data, commonInsert)
		if err != nil {
			return "", nil, err
		}

		sets, updateVals, err := resolveUpdate(update)
		if err != nil {
			return "", nil, err
		}
		cond := fmt.Sprintf("%s ON DUPLICATE KEY UPDATE %s", insertCond, sets)
		vals := make([]interface{}, 0, len(insertVals)+len(updateVals))
		vals = append(vals, insertVals...)
		vals = append(vals, updateVals...)

		return b.finalizeQuery(cond, vals)

	case DialectPostgres, DialectSQLite:
		target, err := buildConflictTarget(conflictColumns)
		if err != nil {
			return "", nil, err
		}

		if target == "" {
			return "", nil, errEmptyConflictColumns
		}

		insertCond, insertVals, err := b.buildInsertRaw(table, data, commonInsert)
		if err != nil {
			return "", nil, err
		}

		sets, updateVals, err := resolveUpdate(update)
		if err != nil {
			return "", nil, err
		}
		cond := fmt.Sprintf("%s ON CONFLICT %s DO UPDATE SET %s", insertCond, target, sets)
		vals := make([]interface{}, 0, len(insertVals)+len(updateVals))
		vals = append(vals, insertVals...)
		vals = append(vals, updateVals...)

		return b.finalizeQuery(cond, vals)

	default:
		return "", nil, fmt.Errorf("%w: %q", errUnsupportedDialect, b.dialect)
	}
}

func buildConflictTarget(conflictColumns []string) (string, error) {
	if len(conflictColumns) == 0 {
		return "", nil
	}

	columns := make([]string, 0, len(conflictColumns))
	for _, col := range conflictColumns {
		c := strings.TrimSpace(col)
		if c == "" {
			return "", errEmptyConflictColumns
		}

		columns = append(columns, quoteField(c))
	}

	return fmt.Sprintf("(%s)", strings.Join(columns, ",")), nil
}
