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

func (cs cassandraMigrator) checkAndCreateMigrationTable(c *infra.Container) error {
	if err := c.Cassandra.ExecWithCtx(context.Background(), checkAndCreateCassandraMigrationTable); err != nil {
		return err
	}

	return cs.migrator.checkAndCreateMigrationTable(c)
}

func (cs cassandraMigrator) getLastMigration(c *infra.Container) (int64, error) {
	var (
		lastMigration  int64
		lastMigrations []int64
	)

	err := c.Cassandra.QueryWithCtx(context.Background(), &lastMigrations, getLastCassandraPiMigration)
	if err != nil {
		return -1, fmt.Errorf("cassandra: %w", err)
	}

	for _, version := range lastMigrations {
		if version > lastMigration {
			lastMigration = version
		}
	}

	c.Debugf("cassandra last migration fetched value is: %v", lastMigration)

	lm2, err := cs.migrator.getLastMigration(c)
	if err != nil {
		return -1, err
	}

	return max(lastMigration, lm2), nil
}

func (cs cassandraMigrator) beginTransaction(c *infra.Container) transactionData {
	cmt := cs.migrator.beginTransaction(c)

	c.Debug("cassandra migrator begin successfully")

	return cmt
}

func (cs cassandraMigrator) commitMigration(c *infra.Container, data transactionData) error {
	err := cs.CassandraWithContext.ExecWithCtx(context.Background(), insertCassandraPiMigrationRow, data.MigrationNumber,
		"UP", data.StartTime, time.Since(data.StartTime).Milliseconds())
	if err != nil {
		return err
	}

	c.Debugf("inserted record for migration %v in cassandra kite_migrations table", data.MigrationNumber)

	return cs.migrator.commitMigration(c, data)
}

func (cs cassandraMigrator) rollback(c *infra.Container, data transactionData) {
	cs.migrator.rollback(c, data)

	c.Fatalf("migration %v failed and rolled back", data.MigrationNumber)
}
