package migration

import (
	"context"
	"fmt"
	"time"

	"github.com/sllt/pi/pkg/pi/infra"
)

type scyllaDS struct {
	ScyllaDB
}

type scyllaMigrator struct {
	ScyllaDB
	migrator
}

func (ds scyllaDS) apply(m migrator) migrator {
	return scyllaMigrator{
		ScyllaDB: ds.ScyllaDB,
		migrator: m,
	}
}

const (
	scyllaDBMigrationTable = "kite_migrations"
)

func (s scyllaMigrator) checkAndCreateMigrationTable(ctx context.Context, c *infra.Container) error {
	createTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			version bigint PRIMARY KEY,
			method text,
			start_time timestamp,
			duration bigint
		);
	`, scyllaDBMigrationTable)

	err := s.ScyllaDB.ExecWithCtx(ctx, createTableQuery)
	if err != nil {
		c.Errorf("Failed to create migration table: %v", err)
		return err
	}

	if s.migrator == nil {
		return nil
	}

	return s.migrator.checkAndCreateMigrationTable(ctx, c)
}

type migrationRow struct {
	Version int64 `db:"version"`
}

func (s scyllaMigrator) getLastMigration(ctx context.Context, c *infra.Container) (int64, error) {
	var (
		migrations  []migrationRow
		lastVersion int64
	)

	query := fmt.Sprintf("SELECT version FROM %s", scyllaDBMigrationTable)

	err := s.ScyllaDB.QueryWithCtx(ctx, &migrations, query)
	if err != nil {
		return -1, fmt.Errorf("scylladb: %w", err)
	}

	for _, m := range migrations {
		if m.Version > lastVersion {
			lastVersion = m.Version
		}
	}

	c.Debugf("ScyllaDB last migration fetched value is: %v", lastVersion)

	lm2, err := s.migrator.getLastMigration(ctx, c)
	if err != nil {
		return -1, err
	}

	return max(lastVersion, lm2), nil
}

func (s scyllaMigrator) beginTransaction(ctx context.Context, c *infra.Container) (transactionData, error) {
	return s.migrator.beginTransaction(ctx, c)
}

func (s scyllaMigrator) commitMigration(ctx context.Context, c *infra.Container, data transactionData) error {
	insertStmt := fmt.Sprintf(`
		INSERT INTO %s (version, method, start_time, duration)
		VALUES (?, ?, ?, ?);
	`, scyllaDBMigrationTable)

	err := s.ScyllaDB.ExecWithCtx(ctx, insertStmt,
		data.MigrationNumber,
		"UP",
		data.StartTime,
		time.Since(data.StartTime).Milliseconds(),
	)
	if err != nil {
		c.Errorf("Failed to insert migration record: %v", err)
		return err
	}

	c.Debugf("Inserted migration record for version %v into ScyllaDB", data.MigrationNumber)

	return s.migrator.commitMigration(ctx, c, data)
}

func (s scyllaMigrator) rollback(ctx context.Context, c *infra.Container, data transactionData) error {
	return s.migrator.rollback(ctx, c, data)
}
