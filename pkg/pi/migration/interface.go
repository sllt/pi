package migration

import (
	"context"
	"database/sql"
	"time"

	goRedis "github.com/redis/go-redis/v9"
)

type Redis interface {
	Get(ctx context.Context, key string) *goRedis.StringCmd
	Set(ctx context.Context, key string, value any, expiration time.Duration) *goRedis.StatusCmd
	Del(ctx context.Context, keys ...string) *goRedis.IntCmd
	Rename(ctx context.Context, key, newKey string) *goRedis.StatusCmd
}

type SQL interface {
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	Exec(query string, args ...any) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type PubSub interface {
	Query(ctx context.Context, query string, args ...any) ([]byte, error)
	CreateTopic(context context.Context, name string) error
	DeleteTopic(context context.Context, name string) error
}

type migrator interface {
	checkAndCreateMigrationTable(ctx context.Context, deps *Deps) error
	listApplied(ctx context.Context, deps *Deps) ([]Record, error)
	getLastMigration(ctx context.Context, deps *Deps) (int64, error)

	beginTransaction(ctx context.Context, deps *Deps) (transactionData, error)

	commitMigration(ctx context.Context, deps *Deps, data transactionData) error
	rollback(ctx context.Context, deps *Deps, data transactionData) error
}
