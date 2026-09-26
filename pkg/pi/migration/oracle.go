package migration

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/sllt/pi/pkg/pi/infra"
)

var (
	errInvalidOracleTransaction      = errors.New("invalid Oracle transaction")
	errNestedTransactionNotSupported = errors.New("nested transactions not supported")
)

type oracleDS struct {
	Oracle
}

type oracleMigrator struct {
	Oracle
	migrator
}

// Provides a wrapper to apply the oracle migrator logic.
func (od oracleDS) apply(m migrator) migrator {
	return oracleMigrator{
		Oracle:   od.Oracle,
		migrator: m,
	}
}

const (
	checkAndCreateOracleMigrationTable = `
BEGIN
    EXECUTE IMMEDIATE 'CREATE TABLE kite_migrations (
        version NUMBER NOT NULL,
        method VARCHAR2(64) NOT NULL,
        start_time TIMESTAMP NOT NULL,
        duration NUMBER NULL,
        PRIMARY KEY (version, method)
    )';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -955 THEN RAISE; END IF;
END;
`
	getLastOraclePiMigration = `
SELECT NVL(MAX(version), 0) AS last_migration
FROM kite_migrations
`
	insertOraclePiMigrationRow = `
INSERT INTO kite_migrations (version, method, start_time, duration)
VALUES (:1, :2, :3, :4)
`
)

// Create migration table if it doesn't exist.
func (om oracleMigrator) checkAndCreateMigrationTable(ctx context.Context, c *infra.Container) error {
	err := om.Oracle.Exec(ctx, checkAndCreateOracleMigrationTable)
	if err != nil {
		c.Errorf("Failed to create Oracle migration table: %v", err)
		return err
	}
	c.Infof("Oracle migration table checked/created successfully")

	if om.migrator == nil {
		return nil
	}

	return om.migrator.checkAndCreateMigrationTable(ctx, c)
}

// Get the last applied migration version.
func (om oracleMigrator) getLastMigration(ctx context.Context, c *infra.Container) (int64, error) {
	var (
		results             []map[string]any
		oracleLastMigration int64
	)

	err := om.Oracle.Select(ctx, &results, getLastOraclePiMigration)
	if err != nil {
		return -1, fmt.Errorf("oracle: %w", err)
	}

	if len(results) != 0 {
		oracleLastMigration = om.extractLastMigrationFromResults(results)
	}

	c.Debugf("Oracle last migration fetched value is: %v", oracleLastMigration)

	baseLastMigration, err := om.migrator.getLastMigration(ctx, c)
	if err != nil {
		return -1, err
	}

	return max(baseLastMigration, oracleLastMigration), nil
}

// extractLastMigrationFromResults handles Oracle number type conversion.
func (om oracleMigrator) extractLastMigrationFromResults(results []map[string]any) int64 {
	if len(results) == 0 {
		return 0
	}

	lastMigVal, exists := results[0]["LAST_MIGRATION"]
	if !exists {
		return 0
	}

	return om.convertToInt64(lastMigVal)
}

// convertToInt64 converts various Oracle number types to int64.
func (om oracleMigrator) convertToInt64(value any) int64 {
	switch v := value.(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	default:
		return om.parseStringValue(v)
	}
}

// parseStringValue handles godror.Number type by converting to string then parsing.
func (oracleMigrator) parseStringValue(value any) int64 {
	str := fmt.Sprintf("%v", value)
	if str == "" || str == "<nil>" {
		return 0
	}

	parsed, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return 0
	}

	return parsed
}

// Commit the migration and insert a record into the migration table.
func (om oracleMigrator) commitMigration(ctx context.Context, c *infra.Container, data transactionData) error {
	if data.OracleTx == nil {
		c.Error("invalid Oracle transaction")
		return errInvalidOracleTransaction
	}

	// Insert migration record using the transaction.
	err := data.OracleTx.ExecContext(ctx, insertOraclePiMigrationRow,
		data.MigrationNumber, "UP", data.StartTime, time.Since(data.StartTime).Milliseconds())
	if err != nil {
		c.Errorf("failed to insert migration record: %v", err)

		return err
	}

	c.Debugf("inserted record for migration %v in Oracle kite_migrations table", data.MigrationNumber)

	// Commit the transaction.
	if err := data.OracleTx.Commit(); err != nil {
		c.Errorf("failed to commit Oracle transaction: %v", err)
		return err
	}

	return om.migrator.commitMigration(ctx, c, data)
}

// Rollback the migration transaction.
func (om oracleMigrator) rollback(ctx context.Context, c *infra.Container, data transactionData) error {
	var rollbackErr error
	if data.OracleTx != nil {
		if err := data.OracleTx.Rollback(); err != nil {
			c.Errorf("unable to rollback Oracle transaction: %v", err)
			rollbackErr = err
		}
	}

	// Call the base migrator's rollback.
	return errorsJoin(rollbackErr, om.migrator.rollback(ctx, c, data))
}

// Begin a new migration transaction.
func (om oracleMigrator) beginTransaction(ctx context.Context, c *infra.Container) (transactionData, error) {
	// Begin a proper transaction
	tx, err := om.Oracle.Begin()
	if err != nil {
		c.Errorf("unable to begin Oracle transaction: %v", err)

		return transactionData{}, err
	}

	td, err := om.migrator.beginTransaction(ctx, c)
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			rollbackErr = fmt.Errorf("rollback Oracle transaction after nested begin failure: %w", rollbackErr)
		}

		return transactionData{}, errorsJoin(err, rollbackErr)
	}
	td.OracleTx = tx // Store the transaction in transactionData

	c.Debug("Oracle Transaction begin successful")

	return td, nil
}

type oracleTransactionWrapper struct {
	tx infra.OracleTx
}

func (otw *oracleTransactionWrapper) Exec(ctx context.Context, query string, args ...any) error {
	return otw.tx.ExecContext(ctx, query, args...)
}

func (otw *oracleTransactionWrapper) Select(ctx context.Context, dest any, query string, args ...any) error {
	return otw.tx.SelectContext(ctx, dest, query, args...)
}

func (*oracleTransactionWrapper) Begin() (infra.OracleTx, error) {
	return nil, errNestedTransactionNotSupported
}
