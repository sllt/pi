package migration

import (
	"context"
	"fmt"
	"time"

	"github.com/sllt/pi/pkg/pi/infra"
)

type cassandraDS struct {
	infra.CassandraWithContext
}

type cassandraMigrator struct {
	infra.CassandraWithContext

	migrator
}

func (cs cassandraDS) apply(m migrator) migrator {
	return cassandraMigrator{
		CassandraWithContext: cs.CassandraWithContext,
		migrator:             m,
	}
}

const (
	checkAndCreateCassandraMigrationTable = `CREATE TABLE IF NOT EXISTS kite_migrations (version bigint,
    method text, start_time timestamp, duration bigint, PRIMARY KEY (version, method));`

	getLastCassandraPiMigration = `SELECT version FROM kite_migrations`

	insertCassandraPiMigrationRow = `INSERT INTO kite_migrations (version, method, start_time, duration) VALUES (?, ?, ?, ?);`
)

func (cs cassandraMigrator) checkAndCreateMigrationTable(ctx context.Context, c *infra.Container) error {
	if err := c.Cassandra.ExecWithCtx(ctx, checkAndCreateCassandraMigrationTable); err != nil {
		return err
	}

	return cs.migrator.checkAndCreateMigrationTable(ctx, c)
}

func (cs cassandraMigrator) getLastMigration(ctx context.Context, c *infra.Container) (int64, error) {
	var (
		lastMigration  int64
		lastMigrations []int64
	)

	err := c.Cassandra.QueryWithCtx(ctx, &lastMigrations, getLastCassandraPiMigration)
	if err != nil {
		return -1, fmt.Errorf("cassandra: %w", err)
	}

	for _, version := range lastMigrations {
		if version > lastMigration {
			lastMigration = version
		}
	}

	c.Debugf("cassandra last migration fetched value is: %v", lastMigration)

	lm2, err := cs.migrator.getLastMigration(ctx, c)
	if err != nil {
		return -1, err
	}

	return max(lastMigration, lm2), nil
}

func (cs cassandraMigrator) beginTransaction(ctx context.Context, c *infra.Container) (transactionData, error) {
	cmt, err := cs.migrator.beginTransaction(ctx, c)
	if err != nil {
		return transactionData{}, err
	}

	c.Debug("cassandra migrator begin successfully")

	return cmt, nil
}

func (cs cassandraMigrator) commitMigration(ctx context.Context, c *infra.Container, data transactionData) error {
	err := cs.CassandraWithContext.ExecWithCtx(ctx, insertCassandraPiMigrationRow, data.MigrationNumber,
		"UP", data.StartTime, time.Since(data.StartTime).Milliseconds())
	if err != nil {
		return err
	}

	c.Debugf("inserted record for migration %v in cassandra kite_migrations table", data.MigrationNumber)

	return cs.migrator.commitMigration(ctx, c, data)
}

func (cs cassandraMigrator) rollback(ctx context.Context, c *infra.Container, data transactionData) error {
	return cs.migrator.rollback(ctx, c, data)
}
