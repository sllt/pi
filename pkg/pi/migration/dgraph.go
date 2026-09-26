package migration

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dgraph-io/dgo/v210/protos/api"

	"github.com/sllt/pi/pkg/pi/infra"
)

// dgraphDS is the adapter struct that implements migration operations.
type dgraphDS struct {
	client DGraph
}

// dgraphMigrator struct implements the migrator interface.
type dgraphMigrator struct {
	dgraphDS
	migrator
}

const (
	// dgraphSchema defines the migration schema with fully qualified predicate names.
	dgraphSchema = `
		migrations.version: int @index(int) .
		migrations.method: string .
		migrations.start_time: datetime .
		migrations.duration: int .
		type Migration {
			migrations.version
			migrations.method
			migrations.start_time
			migrations.duration
		}
	`

	// getLastMigrationQuery fetches the most recent migration version.
	getLastMigrationQuery = `
		{
			migrations(func: type(Migration), orderdesc: migrations.version, first: 1) {
				migrations.version
			}
		}
	`
)

// apply creates a new dgraphMigrator.
func (ds dgraphDS) apply(m migrator) migrator {
	return dgraphMigrator{
		dgraphDS: ds,
		migrator: m,
	}
}

// ApplySchema applies the given schema to DGraph. It takes a context and schema string as parameters
// and returns an error if the schema application fails.
func (ds dgraphDS) ApplySchema(ctx context.Context, schema string) error {
	return ds.client.ApplySchema(ctx, schema)
}

// AddOrUpdateField adds a new field or updates an existing field in DGraph schema.
// Parameters:
//   - ctx: The context for the operation
//   - fieldName: Name of the field to add or update
//   - fieldType: Data type of the field
//   - directives: Additional DGraph directives for the field
//
// Returns an error if the operation fails.
func (ds dgraphDS) AddOrUpdateField(ctx context.Context, fieldName, fieldType, directives string) error {
	return ds.client.AddOrUpdateField(ctx, fieldName, fieldType, directives)
}

// DropField removes a field from DGraph schema.
// Parameters:
//   - ctx: The context for the operation
//   - fieldName: Name of the field to remove
//
// Returns an error if the field deletion fails.
func (ds dgraphDS) DropField(ctx context.Context, fieldName string) error {
	return ds.client.DropField(ctx, fieldName)
}

// checkAndCreateMigrationTable ensures migration schema exists.
func (dm dgraphMigrator) checkAndCreateMigrationTable(ctx context.Context, c *infra.Container) error {
	err := dm.ApplySchema(ctx, dgraphSchema)
	if err != nil {
		c.Debug("Migration schema might already exist:", err)
	}

	return dm.migrator.checkAndCreateMigrationTable(ctx, c)
}

// getLastMigration retrieves the last applied migration version.
func (dm dgraphMigrator) getLastMigration(ctx context.Context, c *infra.Container) (int64, error) {
	var response struct {
		Migrations []struct {
			Version int64 `json:"version"`
		} `json:"migrations"`
	}

	resp, err := c.DGraph.Query(ctx, getLastMigrationQuery)
	if err != nil {
		return -1, fmt.Errorf("dgraph: %w", err)
	}

	if resp != nil {
		var b []byte

		b, err = json.Marshal(resp)
		if err != nil {
			return 0, fmt.Errorf("dgraph: %w", err)
		}

		err = json.Unmarshal(b, &response)
		if err != nil {
			return 0, fmt.Errorf("dgraph: %w", err)
		}
	}

	var lastMigration int64
	if len(response.Migrations) > 0 {
		lastMigration = response.Migrations[0].Version
	}

	lm2, err := dm.migrator.getLastMigration(ctx, c)
	if err != nil {
		return -1, err
	}

	return max(lastMigration, lm2), nil
}

// beginTransaction starts a new migration transaction.
func (dm dgraphMigrator) beginTransaction(ctx context.Context, c *infra.Container) (transactionData, error) {
	data, err := dm.migrator.beginTransaction(ctx, c)
	if err != nil {
		return transactionData{}, err
	}

	c.Debug("Dgraph migrator begin successfully")

	return data, nil
}

// commitMigration commits the migration and records its metadata.
func (dm dgraphMigrator) commitMigration(ctx context.Context, c *infra.Container, data transactionData) error {
	// Build the JSON payload for the migration record.
	payload := map[string]any{
		"migrations": []map[string]any{
			{
				"migrations.version":    data.MigrationNumber,
				"migrations.method":     "UP",
				"migrations.start_time": data.StartTime.Format(time.RFC3339),
				"migrations.duration":   time.Since(data.StartTime).Milliseconds(),
			},
		},
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	_, err = c.DGraph.Mutate(ctx, &api.Mutation{
		SetJson: jsonPayload,
	})
	if err != nil {
		return err
	}

	c.Debugf("Inserted record for migration %v in Dgraph migrations", data.MigrationNumber)

	return dm.migrator.commitMigration(ctx, c, data)
}

// rollback handles migration failure and rollback.
func (dm dgraphMigrator) rollback(ctx context.Context, c *infra.Container, data transactionData) error {
	return dm.migrator.rollback(ctx, c, data)
}
