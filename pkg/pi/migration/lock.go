package migration

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"modernc.org/sqlite"

	"github.com/sllt/pi/pkg/pi/infra"
)

const (
	migrationLockName           = "default"
	migrationLockTTL            = 15 * time.Minute
	migrationLockReleaseTimeout = 5 * time.Second
	migrationRedisKey           = "kite:migration:lock"

	createSQLMigrationLocksTable = `CREATE TABLE IF NOT EXISTS kite_migration_locks (
    name VARCHAR(255) PRIMARY KEY,
    owner VARCHAR(255) NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);`
	insertSQLMigrationLockMySQL    = `INSERT INTO kite_migration_locks (name, owner, expires_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?);`
	insertSQLMigrationLockPostgres = `INSERT INTO kite_migration_locks (name, owner, expires_at, created_at, updated_at) VALUES ($1, $2, $3, $4, $5);`
	updateSQLMigrationLockMySQL    = `UPDATE kite_migration_locks SET owner = ?, expires_at = ?, updated_at = ? WHERE name = ? AND expires_at < ?;`
	updateSQLMigrationLockPostgres = `UPDATE kite_migration_locks SET owner = $1, expires_at = $2, updated_at = $3 WHERE name = $4 AND expires_at < $5;`
	deleteSQLMigrationLockMySQL    = `DELETE FROM kite_migration_locks WHERE name = ? AND owner = ?;`
	deleteSQLMigrationLockPostgres = `DELETE FROM kite_migration_locks WHERE name = $1 AND owner = $2;`

	releaseRedisMigrationLockScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`
)

type releaseFunc func(context.Context) error

func acquireMigrationLock(ctx context.Context, c *infra.Container, options Options) (releaseFunc, error) {
	if options.Lock != LockEnabled {
		return nil, nil
	}
	if options.LockTTL <= 0 {
		return nil, fmt.Errorf("%w: lock TTL must be greater than zero", ErrInvalidOption)
	}

	if c == nil {
		return nil, fmt.Errorf("%w: no datasource available for migration lock", ErrMigrationLockUnavailable)
	}

	if !isNil(c.SQL) {
		return acquireSQLMigrationLock(ctx, c, options.LockTTL)
	}

	if !isNil(c.Redis) {
		return acquireRedisMigrationLock(ctx, c, options.LockTTL)
	}

	return nil, fmt.Errorf("%w: no supported datasource available for migration lock", ErrMigrationLockUnavailable)
}

func acquireSQLMigrationLock(ctx context.Context, c *infra.Container, ttl time.Duration) (releaseFunc, error) {
	insertQuery, updateQuery, deleteQuery, err := sqlMigrationLockQueries(c.SQL.Dialect())
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrMigrationLockUnavailable, err)
	}

	if _, err := c.SQL.ExecContext(ctx, createSQLMigrationLocksTable); err != nil {
		return nil, fmt.Errorf("%w: create SQL migration lock table: %w", ErrMigrationLockUnavailable, err)
	}

	now := time.Now().UTC()
	owner := newMigrationLockOwner(now)
	expiresAt := now.Add(ttl)
	if _, err := c.SQL.ExecContext(ctx, insertQuery, migrationLockName, owner, expiresAt, now, now); err != nil {
		if !isDuplicateLock(err) {
			return nil, fmt.Errorf("%w: insert SQL migration lock: %w", ErrMigrationLockUnavailable, err)
		}
		result, updateErr := c.SQL.ExecContext(ctx, updateQuery, owner, expiresAt, now, migrationLockName, now)
		if updateErr != nil {
			return nil, fmt.Errorf(
				"%w: acquire SQL migration lock: %v; update expired lock: %w",
				ErrMigrationLockUnavailable,
				err,
				updateErr,
			)
		}

		rowsAffected, rowsErr := result.RowsAffected()
		if rowsErr != nil {
			return nil, fmt.Errorf("%w: verify SQL migration lock update: %w", ErrMigrationLockUnavailable, rowsErr)
		}
		if rowsAffected == 0 {
			return nil, fmt.Errorf("%w: acquire SQL migration lock: %v", ErrMigrationLocked, err)
		}
	}

	return func(releaseCtx context.Context) error {
		if releaseCtx == nil {
			releaseCtx = context.Background()
		}

		_, err := c.SQL.ExecContext(releaseCtx, deleteQuery, migrationLockName, owner)
		if err != nil {
			return fmt.Errorf("release SQL migration lock: %w", err)
		}

		return nil
	}, nil
}

func sqlMigrationLockQueries(dialect string) (insertQuery, updateQuery, deleteQuery string, err error) {
	postgresPlaceholders, err := sqlUsesPostgresPlaceholders(dialect)
	if err != nil {
		return "", "", "", err
	}
	if postgresPlaceholders {
		return insertSQLMigrationLockPostgres, updateSQLMigrationLockPostgres, deleteSQLMigrationLockPostgres, nil
	}

	return insertSQLMigrationLockMySQL, updateSQLMigrationLockMySQL, deleteSQLMigrationLockMySQL, nil
}

func acquireRedisMigrationLock(ctx context.Context, c *infra.Container, ttl time.Duration) (releaseFunc, error) {
	now := time.Now().UTC()
	owner := newMigrationLockOwner(now)

	acquired, err := c.Redis.SetNX(ctx, migrationRedisKey, owner, ttl).Result()
	if err != nil {
		return nil, fmt.Errorf("%w: acquire Redis migration lock: %w", ErrMigrationLockUnavailable, err)
	}
	if !acquired {
		return nil, fmt.Errorf("%w: acquire Redis migration lock", ErrMigrationLocked)
	}

	return func(releaseCtx context.Context) error {
		if releaseCtx == nil {
			releaseCtx = context.Background()
		}

		if err := c.Redis.Eval(releaseCtx, releaseRedisMigrationLockScript, []string{migrationRedisKey}, owner).Err(); err != nil {
			return fmt.Errorf("release Redis migration lock: %w", err)
		}

		return nil
	}, nil
}

func releaseMigrationLock(parent context.Context, release releaseFunc) error {
	if release == nil {
		return nil
	}

	releaseParent := context.Background()
	if parent != nil {
		releaseParent = context.WithoutCancel(parent)
	}
	releaseCtx, cancel := context.WithTimeout(releaseParent, migrationLockReleaseTimeout)
	defer cancel()

	return release(releaseCtx)
}

func newMigrationLockOwner(now time.Time) string {
	return uuid.NewString()
}

func isDuplicateLock(err error) bool {
	var mysqlErr *mysql.MySQLError
	var pgErr *pq.Error
	var sqliteErr *sqlite.Error
	switch {
	case errors.As(err, &mysqlErr):
		return mysqlErr.Number == 1062
	case errors.As(err, &pgErr):
		return pgErr.Code == "23505"
	case errors.As(err, &sqliteErr):
		return sqliteErr.Code() == 1555 || sqliteErr.Code() == 2067 // PRIMARYKEY / UNIQUE
	default:
		return false
	}
}
