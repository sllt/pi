package migration

import (
	"context"
	"fmt"
	"time"

	"github.com/sllt/pi/pkg/pi/infra"
)

type mongoDS struct {
	infra.Mongo
}

type mongoMigrator struct {
	infra.Mongo
	migrator
}

// apply initializes mongoMigrator using the Mongo interface.
func (ds mongoDS) apply(m migrator) migrator {
	return mongoMigrator{
		Mongo:    ds.Mongo,
		migrator: m,
	}
}

const (
	mongoMigrationCollection = "kite_migrations"
)

// checkAndCreateMigrationTable initializes a MongoDB collection if it doesn't exist.
func (mg mongoMigrator) checkAndCreateMigrationTable(ctx context.Context, c *infra.Container) error {
	err := mg.Mongo.CreateCollection(ctx, mongoMigrationCollection)
	if err != nil {
		return err
	}

	if mg.migrator == nil {
		return nil
	}

	return mg.migrator.checkAndCreateMigrationTable(ctx, c)
}

func (mg mongoMigrator) getLastMigration(ctx context.Context, c *infra.Container) (int64, error) {
	var (
		lastMigration int64
		migrations    []struct {
			Version int64 `bson:"version"`
		}
	)

	filter := make(map[string]any)

	err := mg.Mongo.Find(ctx, mongoMigrationCollection, filter, &migrations)
	if err != nil {
		return -1, fmt.Errorf("mongo: %w", err)
	}

	for _, migration := range migrations {
		lastMigration = max(lastMigration, migration.Version)
	}

	c.Debugf("MongoDB last migration fetched value is: %v", lastMigration)

	lm2, err := mg.migrator.getLastMigration(ctx, c)
	if err != nil {
		return -1, err
	}

	return max(lastMigration, lm2), nil
}

func (mg mongoMigrator) beginTransaction(ctx context.Context, c *infra.Container) (transactionData, error) {
	return mg.migrator.beginTransaction(ctx, c)
}

func (mg mongoMigrator) commitMigration(ctx context.Context, c *infra.Container, data transactionData) error {
	migrationDoc := map[string]any{
		"version":    data.MigrationNumber,
		"method":     "UP",
		"start_time": data.StartTime,
		"duration":   time.Since(data.StartTime).Milliseconds(),
	}

	_, err := mg.Mongo.InsertOne(ctx, mongoMigrationCollection, migrationDoc)
	if err != nil {
		return err
	}

	c.Debugf("Inserted record for migration %v in MongoDB kite_migrations collection", data.MigrationNumber)

	return mg.migrator.commitMigration(ctx, c, data)
}

func (mg mongoMigrator) rollback(ctx context.Context, c *infra.Container, data transactionData) error {
	return mg.migrator.rollback(ctx, c, data)
}
