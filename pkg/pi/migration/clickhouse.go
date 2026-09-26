package migration

import (
	"context"
	"fmt"
	"time"

	"github.com/sllt/pi/pkg/pi/infra"
)

type clickHouseDS struct {
	Clickhouse
}

type clickHouseMigrator struct {
	Clickhouse

	migrator
}

func (ch clickHouseDS) apply(m migrator) migrator {
	return clickHouseMigrator{
		Clickhouse: ch.Clickhouse,
		migrator:   m,
	}
}

const (
	CheckAndCreateChMigrationTable = `CREATE TABLE IF NOT EXISTS kite_migrations
(
    version    Int64     NOT NULL,
    method     String    NOT NULL,
    start_time DateTime  NOT NULL,
    duration   Int64     NULL,
    PRIMARY KEY (version, method)
) ENGINE = MergeTree()
ORDER BY (version, method);
`

	getLastChPiMigration = `SELECT COALESCE(MAX(version), 0) as last_migration FROM kite_migrations;`

	insertChPiMigrationRow = `INSERT INTO kite_migrations (version, method, start_time, duration) VALUES (?, ?, ?, ?);`
)

func (ch clickHouseMigrator) checkAndCreateMigrationTable(c *infra.Container) error {
	if err := c.Clickhouse.Exec(context.Background(), CheckAndCreateChMigrationTable); err != nil {
		return err
	}

	return ch.migrator.checkAndCreateMigrationTable(c)
}

func (ch clickHouseMigrator) getLastMigration(c *infra.Container) (int64, error) {
	type LastMigration struct {
		Timestamp int64 `ch:"last_migration"`
	}

	var lastMigrations []LastMigration

	var lastMigration int64

	err := c.Clickhouse.Select(context.Background(), &lastMigrations, getLastChPiMigration)
	if err != nil {
		return -1, fmt.Errorf("clickhouse: %w", err)
	}

	if len(lastMigrations) != 0 {
		lastMigration = lastMigrations[0].Timestamp
	}

	c.Debugf("Clickhouse last migration fetched value is: %v", lastMigration)

	lm2, err := ch.migrator.getLastMigration(c)
	if err != nil {
		return -1, err
	}

	return max(lastMigration, lm2), nil
}

func (ch clickHouseMigrator) beginTransaction(c *infra.Container) transactionData {
	cmt := ch.migrator.beginTransaction(c)

	c.Debug("Clickhouse Migrator begin successfully")

	return cmt
}

func (ch clickHouseMigrator) commitMigration(c *infra.Container, data transactionData) error {
	err := ch.Clickhouse.Exec(context.Background(), insertChPiMigrationRow, data.MigrationNumber,
		"UP", data.StartTime, time.Since(data.StartTime).Milliseconds())
	if err != nil {
		return err
	}

	c.Debugf("inserted record for migration %v in clickhouse kite_migrations table", data.MigrationNumber)

	return ch.migrator.commitMigration(c, data)
}

func (ch clickHouseMigrator) rollback(c *infra.Container, data transactionData) {
	ch.migrator.rollback(c, data)

	c.Fatalf("migration %v failed and rolled back", data.MigrationNumber)
}
