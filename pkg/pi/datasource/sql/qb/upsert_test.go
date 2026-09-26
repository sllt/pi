package qb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildUpsertWithDialect_MySQL(t *testing.T) {
	cond, vals, err := BuildUpsertWithDialect("mysql", "users", []map[string]interface{}{
		{
			"id":   1,
			"name": "pi",
		},
	}, []string{"id"}, map[string]interface{}{
		"name": "updated",
	})

	require.NoError(t, err)
	assert.Equal(t, "INSERT INTO users (id,name) VALUES (?,?) ON DUPLICATE KEY UPDATE name=?", cond)
	assert.Equal(t, []interface{}{1, "pi", "updated"}, vals)
}

func TestBuildUpsertWithDialect_MySQLDoNothing(t *testing.T) {
	cond, vals, err := BuildUpsertWithDialect("mysql", "users", []map[string]interface{}{
		{
			"id":   1,
			"name": "pi",
		},
	}, nil, nil)

	require.NoError(t, err)
	assert.Equal(t, "INSERT IGNORE INTO users (id,name) VALUES (?,?)", cond)
	assert.Equal(t, []interface{}{1, "pi"}, vals)
}

func TestBuildUpsertWithDialect_Postgres(t *testing.T) {
	cond, vals, err := BuildUpsertWithDialect("postgres", "users", []map[string]interface{}{
		{
			"id":   1,
			"name": "pi",
		},
	}, []string{"id"}, map[string]interface{}{
		"name": "updated",
	})

	require.NoError(t, err)
	assert.Equal(t, "INSERT INTO users (id,name) VALUES ($1,$2) ON CONFLICT (id) DO UPDATE SET name=$3", cond)
	assert.Equal(t, []interface{}{1, "pi", "updated"}, vals)
}

func TestBuildUpsertWithDialect_SQLite(t *testing.T) {
	cond, vals, err := BuildUpsertWithDialect("sqlite", "users", []map[string]interface{}{
		{
			"id":   1,
			"name": "pi",
		},
	}, []string{"id"}, map[string]interface{}{
		"name": "updated",
	})

	require.NoError(t, err)
	assert.Equal(t, "INSERT INTO users (id,name) VALUES (?,?) ON CONFLICT (id) DO UPDATE SET name=?", cond)
	assert.Equal(t, []interface{}{1, "pi", "updated"}, vals)
}

func TestBuildUpsertWithDialect_PostgresRequiresConflictColumns(t *testing.T) {
	_, _, err := BuildUpsertWithDialect("postgres", "users", []map[string]interface{}{
		{"id": 1},
	}, nil, map[string]interface{}{
		"name": "updated",
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, errEmptyConflictColumns)
}

func TestBuildUpsertWithDialect_PostgresDoNothingWithoutTarget(t *testing.T) {
	cond, vals, err := BuildUpsertWithDialect("postgres", "users", []map[string]interface{}{
		{"id": 1},
	}, nil, nil)

	require.NoError(t, err)
	assert.Equal(t, "INSERT INTO users (id) VALUES ($1) ON CONFLICT DO NOTHING", cond)
	assert.Equal(t, []interface{}{1}, vals)
}
