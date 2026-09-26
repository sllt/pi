// Package qb provides SQL query builder utilities for dynamic WHERE/ORDER/GROUP/LIMIT
// style queries and bulk insert/update/delete statements.
//
// By default package-level helpers keep MySQL-compatible behavior.
// Use New(...) or *WithDialect helpers to generate SQL for sqlite and postgres.
// You can also use FromDB(...) with a datasource that exposes Dialect().
//
// JSON helper functions (JsonContains/JsonSet/JsonArrayAppend/JsonArrayInsert/JsonRemove)
// generate MySQL JSON function syntax.
package qb
