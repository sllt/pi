package qb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubDialectDB struct {
	d string
}

func (s stubDialectDB) Dialect() string {
	return s.d
}

func TestNewBuilder_DialectAliases(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Dialect
	}{
		{name: "mysql", input: "mysql", expected: DialectMySQL},
		{name: "mariadb alias", input: "mariadb", expected: DialectMySQL},
		{name: "postgres", input: "postgres", expected: DialectPostgres},
		{name: "supabase alias", input: "supabase", expected: DialectPostgres},
		{name: "cockroach alias", input: "cockroachdb", expected: DialectPostgres},
		{name: "sqlite", input: "sqlite", expected: DialectSQLite},
		{name: "sqlite3 alias", input: "sqlite3", expected: DialectSQLite},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, err := New(tc.input)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, b.dialect)
		})
	}
}

func TestFromDB(t *testing.T) {
	b, err := FromDB(stubDialectDB{d: "postgres"})
	require.NoError(t, err)
	assert.Equal(t, DialectPostgres, b.dialect)

	_, err = FromDB(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, errNilDialectProvider)
}

func TestBuildSelectWithDialect_Postgres(t *testing.T) {
	cond, vals, err := BuildSelectWithDialect("postgres", "users", map[string]interface{}{
		"name":      "pi",
		"_limit":    []uint{10, 20},
		"_lockMode": "share",
	}, []string{"id", "name"})

	require.NoError(t, err)
	assert.Equal(t, "SELECT id,name FROM users WHERE (name=$1) LIMIT $2 OFFSET $3 FOR SHARE", cond)
	assert.Equal(t, []interface{}{"pi", 20, 10}, vals)
}

func TestBuildSelectWithDialect_SQLite(t *testing.T) {
	cond, vals, err := BuildSelectWithDialect("sqlite", "users", map[string]interface{}{
		"age >": 18,
		"_limit": []uint{
			5, 15,
		},
	}, nil)

	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM users WHERE (age>?) LIMIT ? OFFSET ?", cond)
	assert.Equal(t, []interface{}{18, 15, 5}, vals)
}

func TestBuildSelectWithDialect_SQLiteLockModeRejected(t *testing.T) {
	_, _, err := BuildSelectWithDialect("sqlite", "users", map[string]interface{}{
		"_lockMode": "share",
	}, nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, errNotAllowedLockMode)
}

func TestBuildUpdateWithDialect_PostgresLimit(t *testing.T) {
	cond, vals, err := BuildUpdateWithDialect("postgres", "users", map[string]interface{}{
		"id >":   100,
		"_limit": uint(2),
	}, map[string]interface{}{
		"name": "updated",
	})

	require.NoError(t, err)
	assert.Equal(t, "UPDATE users SET name=$1 WHERE ctid IN (SELECT ctid FROM users WHERE (id>$2) LIMIT $3)", cond)
	assert.Equal(t, []interface{}{"updated", 100, 2}, vals)
}

func TestBuildDeleteWithDialect_PostgresLimit(t *testing.T) {
	cond, vals, err := BuildDeleteWithDialect("postgres", "users", map[string]interface{}{
		"status": "active",
		"_limit": uint(5),
	})

	require.NoError(t, err)
	assert.Equal(t, "DELETE FROM users WHERE ctid IN (SELECT ctid FROM users WHERE (status=$1) LIMIT $2)", cond)
	assert.Equal(t, []interface{}{"active", 5}, vals)
}

func TestBuildInsertIgnoreWithDialect_Postgres(t *testing.T) {
	cond, vals, err := BuildInsertIgnoreWithDialect("postgres", "users", []map[string]interface{}{
		{
			"id":   1,
			"name": "pi",
		},
	})

	require.NoError(t, err)
	assert.Equal(t, "INSERT INTO users (id,name) VALUES ($1,$2) ON CONFLICT DO NOTHING", cond)
	assert.Equal(t, []interface{}{1, "pi"}, vals)
}

func TestBuildReplaceInsertWithDialect_PostgresUnsupported(t *testing.T) {
	_, _, err := BuildReplaceInsertWithDialect("postgres", "users", []map[string]interface{}{
		{"id": 1},
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, errFeatureUnsupportedDialect)
}

func TestNamedQueryWithDialect_Postgres(t *testing.T) {
	cond, vals, err := NamedQueryWithDialect("postgres", "SELECT * FROM users WHERE id={{id}} AND status IN {{statuses}}", map[string]interface{}{
		"id":       7,
		"statuses": []string{"active", "pending"},
	})

	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM users WHERE id=$1 AND status IN ($2,$3)", cond)
	assert.Equal(t, []interface{}{7, "active", "pending"}, vals)
}
