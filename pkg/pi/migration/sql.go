package migration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	piSql "github.com/sllt/pi/pkg/pi/datasource/sql"
)

var errInvalidSQLTransaction = errors.New("migration: invalid SQL transaction")

const (
	createSQLPiMigrationsTable = `CREATE TABLE IF NOT EXISTS kite_migrations (
    version BIGINT not null ,
    method VARCHAR(4) not null ,
    start_time TIMESTAMP not null ,
    duration BIGINT,
    constraint primary_key primary key (version, method)
);`

	getLastSQLPiMigration = `SELECT COALESCE(MAX(version), 0) FROM kite_migrations;`

	listSQLPiMigrations = `SELECT version, method, start_time, duration FROM kite_migrations ORDER BY version;`

	insertPiMigrationRowMySQL = `INSERT INTO kite_migrations (version, method, start_time,duration) VALUES (?, ?, ?, ?);`

	insertPiMigrationRowPostgres = `INSERT INTO kite_migrations (version, method, start_time,duration) VALUES ($1, $2, $3, $4);`
)

// database/sql is the package imported so named it sqlDS.
type sqlDS struct {
	SQL
}

func (s *sqlDS) apply(m migrator) migrator {
	return sqlMigrator{
		SQL:      s.SQL,
		migrator: m,
	}
}

type sqlMigrator struct {
	SQL

	migrator
}

func (d sqlMigrator) checkAndCreateMigrationTable(ctx context.Context, deps *Deps) error {
	if _, err := deps.SQL.ExecContext(ctx, createSQLPiMigrationsTable); err != nil {
		return err
	}

	if d.migrator == nil {
		return nil
	}

	return d.migrator.checkAndCreateMigrationTable(ctx, deps)
}

func (d sqlMigrator) listApplied(ctx context.Context, deps *Deps) ([]Record, error) {
	rows, err := deps.SQL.QueryContext(ctx, listSQLPiMigrations)
	if err != nil {
		return nil, fmt.Errorf("sql: %w", err)
	}
	defer rows.Close()

	records := make([]Record, 0)
	for rows.Next() {
		var (
			record     Record
			durationMS sql.NullInt64
		)
		if err = rows.Scan(&record.Version, &record.Method, &record.StartedAt, &durationMS); err != nil {
			return nil, fmt.Errorf("sql: %w", err)
		}
		if record.Method != "UP" {
			continue
		}
		record.Duration = time.Duration(durationMS.Int64) * time.Millisecond
		records = append(records, record)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("sql: %w", err)
	}

	if d.migrator == nil {
		return records, nil
	}

	nested, err := d.migrator.listApplied(ctx, deps)
	if err != nil {
		return nil, err
	}

	return mergeAppliedRecords(records, nested), nil
}

func (d sqlMigrator) getLastMigration(ctx context.Context, deps *Deps) (int64, error) {
	var lastMigration int64

	err := deps.SQL.QueryRowContext(ctx, getLastSQLPiMigration).Scan(&lastMigration)
	if err != nil {
		return -1, fmt.Errorf("sql: %w", err)
	}

	deps.log().Debugf("SQL last migration fetched value is: %v", lastMigration)

	if d.migrator == nil {
		return lastMigration, nil
	}

	lm2, err := d.migrator.getLastMigration(ctx, deps)
	if err != nil {
		return -1, err
	}

	return max(lastMigration, lm2), nil
}

func (d sqlMigrator) commitMigration(ctx context.Context, deps *Deps, data transactionData) error {
	if data.SQLTx == nil {
		return errInvalidSQLTransaction
	}

	insertQuery, err := sqlMigrationInsertQuery(deps.SQL.Dialect())
	if err != nil {
		return err
	}

	if err = insertMigrationRecord(ctx, data.SQLTx, insertQuery, data.MigrationNumber, data.StartTime); err != nil {
		return err
	}
	deps.log().Debugf("inserted record for migration %v in kite_migrations table", data.MigrationNumber)

	// Commit transaction
	if err := data.SQLTx.Commit(); err != nil {
		return err
	}

	if d.migrator == nil {
		return nil
	}

	return d.migrator.commitMigration(ctx, deps, data)
}

func sqlMigrationInsertQuery(dialect string) (string, error) {
	postgresPlaceholders, err := sqlUsesPostgresPlaceholders(dialect)
	if err != nil {
		return "", err
	}
	if postgresPlaceholders {
		return insertPiMigrationRowPostgres, nil
	}

	return insertPiMigrationRowMySQL, nil
}

func sqlUsesPostgresPlaceholders(dialect string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(dialect)) {
	case "mysql", "sqlite":
		return false, nil
	case "postgres", "supabase", "cockroachdb":
		return true, nil
	default:
		return false, fmt.Errorf("%w: %q", ErrUnsupportedSQLDialect, dialect)
	}
}

func insertMigrationRecord(ctx context.Context, tx *piSql.Tx, query string, version int64, startTime time.Time) error {
	_, err := tx.ExecContext(ctx, query, version, "UP", startTime, time.Since(startTime).Milliseconds())

	return err
}

func (d sqlMigrator) beginTransaction(ctx context.Context, deps *Deps) (transactionData, error) {
	if err := ctx.Err(); err != nil {
		return transactionData{}, err
	}

	sqlTx, err := deps.SQL.BeginTxContext(ctx, nil)
	if err != nil {
		deps.log().Errorf("unable to begin transaction: %v", err)

		return transactionData{}, err
	}

	if err = ctx.Err(); err != nil {
		return transactionData{}, errorsJoin(err, sqlTx.Rollback())
	}

	var cmt transactionData
	if d.migrator != nil {
		cmt, err = d.migrator.beginTransaction(ctx, deps)
		if err != nil {
			rollbackErr := sqlTx.Rollback()
			if rollbackErr != nil {
				rollbackErr = fmt.Errorf("rollback SQL transaction after nested begin failure: %w", rollbackErr)
			}

			return transactionData{}, errorsJoin(err, rollbackErr)
		}
	}

	cmt.SQLTx = sqlTx

	deps.log().Debug("SQL Transaction begin successful")

	return cmt, nil
}

func (d sqlMigrator) rollback(ctx context.Context, deps *Deps, data transactionData) error {
	var rollbackErr error
	if data.SQLTx != nil {
		if err := data.SQLTx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			deps.log().Errorf("unable to rollback transaction: %v", err)
			rollbackErr = err
		}
	}

	if d.migrator == nil {
		return rollbackErr
	}

	return errorsJoin(rollbackErr, d.migrator.rollback(ctx, deps, data))
}
