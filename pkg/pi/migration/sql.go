package migration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	piSql "github.com/sllt/pi/pkg/pi/datasource/sql"
	"github.com/sllt/pi/pkg/pi/infra"
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

func (d sqlMigrator) checkAndCreateMigrationTable(ctx context.Context, c *infra.Container) error {
	if _, err := c.SQL.ExecContext(ctx, createSQLPiMigrationsTable); err != nil {
		return err
	}

	return d.migrator.checkAndCreateMigrationTable(ctx, c)
}

func (d sqlMigrator) listApplied(ctx context.Context, c *infra.Container) ([]Record, error) {
	rows, err := c.SQL.QueryContext(ctx, listSQLPiMigrations)
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

	nested, err := d.migrator.listApplied(ctx, c)
	if err != nil {
		return nil, err
	}

	return mergeAppliedRecords(records, nested), nil
}

func (d sqlMigrator) getLastMigration(ctx context.Context, c *infra.Container) (int64, error) {
	var lastMigration int64

	err := c.SQL.QueryRowContext(ctx, getLastSQLPiMigration).Scan(&lastMigration)
	if err != nil {
		return -1, fmt.Errorf("sql: %w", err)
	}

	c.Debugf("SQL last migration fetched value is: %v", lastMigration)

	lm2, err := d.migrator.getLastMigration(ctx, c)
	if err != nil {
		return -1, err
	}

	return max(lastMigration, lm2), nil
}

func (d sqlMigrator) commitMigration(ctx context.Context, c *infra.Container, data transactionData) error {
	if data.SQLTx == nil {
		return errInvalidSQLTransaction
	}

	insertQuery, err := sqlMigrationInsertQuery(c.SQL.Dialect())
	if err != nil {
		return err
	}

	if err = insertMigrationRecord(ctx, data.SQLTx, insertQuery, data.MigrationNumber, data.StartTime); err != nil {
		return err
	}
	c.Debugf("inserted record for migration %v in kite_migrations table", data.MigrationNumber)

	// Commit transaction
	if err := data.SQLTx.Commit(); err != nil {
		return err
	}

	if d.migrator == nil {
		return nil
	}

	return d.migrator.commitMigration(ctx, c, data)
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

func (d sqlMigrator) beginTransaction(ctx context.Context, c *infra.Container) (transactionData, error) {
	if err := ctx.Err(); err != nil {
		return transactionData{}, err
	}
	var sqlTx *piSql.Tx
	var err error
	if db, ok := c.SQL.(interface {
		BeginTxContext(context.Context, *sql.TxOptions) (*piSql.Tx, error)
	}); ok {
		sqlTx, err = db.BeginTxContext(ctx, nil)
	} else {
		// Compatibility for custom infra.DB implementations. Their Begin call itself
		// cannot be interrupted; new implementations should provide BeginTxContext.
		sqlTx, err = c.SQL.Begin()
	}
	if err != nil {
		c.Errorf("unable to begin transaction: %v", err)

		return transactionData{}, err
	}

	if err = ctx.Err(); err != nil {
		return transactionData{}, errorsJoin(err, sqlTx.Rollback())
	}
	cmt, err := d.migrator.beginTransaction(ctx, c)
	if err != nil {
		rollbackErr := sqlTx.Rollback()
		if rollbackErr != nil {
			rollbackErr = fmt.Errorf("rollback SQL transaction after nested begin failure: %w", rollbackErr)
		}

		return transactionData{}, errorsJoin(err, rollbackErr)
	}

	cmt.SQLTx = sqlTx

	c.Debug("SQL Transaction begin successful")

	return cmt, nil
}

func (d sqlMigrator) rollback(ctx context.Context, c *infra.Container, data transactionData) error {
	var rollbackErr error
	if data.SQLTx != nil {
		if err := data.SQLTx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			c.Errorf("unable to rollback transaction: %v", err)
			rollbackErr = err
		}
	}

	if d.migrator == nil {
		return rollbackErr
	}

	return errorsJoin(rollbackErr, d.migrator.rollback(ctx, c, data))
}
