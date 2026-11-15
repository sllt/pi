package migration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

var errInvalidRedisTransaction = errors.New("migration: invalid Redis transaction")

type redisDS struct {
	Redis
}

func (r redisDS) apply(m migrator) migrator {
	return redisMigrator{
		Redis:    r.Redis,
		migrator: m,
	}
}

type redisMigrator struct {
	Redis

	migrator
}

type redisData struct {
	Method    string    `json:"method"`
	StartTime time.Time `json:"startTime"`
	Duration  int64     `json:"duration"`
}

func (m redisMigrator) listApplied(ctx context.Context, deps *Deps) ([]Record, error) {
	table, err := deps.Redis.HGetAll(ctx, "kite_migrations").Result()
	if err != nil {
		return nil, fmt.Errorf("redis: %w", err)
	}

	records := make([]Record, 0, len(table))
	for key, value := range table {
		version, err := strconv.ParseInt(key, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("redis: invalid migration version %q: %w", key, err)
		}

		var data redisData
		if err = json.Unmarshal([]byte(value), &data); err != nil {
			return nil, fmt.Errorf("redis: %w", err)
		}
		if data.Method != "UP" {
			continue
		}

		records = append(records, Record{
			Version:   version,
			Method:    data.Method,
			StartedAt: data.StartTime,
			Duration:  time.Duration(data.Duration) * time.Millisecond,
		})
	}

	if m.migrator == nil {
		return records, nil
	}

	nested, err := m.migrator.listApplied(ctx, deps)
	if err != nil {
		return nil, err
	}

	return mergeAppliedRecords(records, nested), nil
}

func (m redisMigrator) getLastMigration(ctx context.Context, deps *Deps) (int64, error) {
	var lastMigration int64

	table, err := deps.Redis.HGetAll(ctx, "kite_migrations").Result()
	if err != nil {
		return -1, fmt.Errorf("redis: %w", err)
	}

	for key, value := range table {
		integerValue, _ := strconv.ParseInt(key, 10, 64)

		if integerValue > lastMigration {
			lastMigration = integerValue
		}

		var data redisData

		err = json.Unmarshal([]byte(value), &data)
		if err != nil {
			return -1, fmt.Errorf("redis: %w", err)
		}
	}

	deps.log().Debugf("Redis last migration fetched value is: %v", lastMigration)

	if m.migrator == nil {
		return lastMigration, nil
	}

	last, err := m.migrator.getLastMigration(ctx, deps)
	if err != nil {
		return -1, err
	}

	return max(lastMigration, last), nil
}

func (m redisMigrator) beginTransaction(ctx context.Context, deps *Deps) (transactionData, error) {
	redisTx := deps.Redis.TxPipeline()

	var (
		cmt transactionData
		err error
	)
	if m.migrator != nil {
		cmt, err = m.migrator.beginTransaction(ctx, deps)
		if err != nil {
			redisTx.Discard()

			return transactionData{}, err
		}
	}

	cmt.RedisTx = redisTx

	deps.log().Debug("Redis Transaction begin successful")

	return cmt, nil
}

func (m redisMigrator) commitMigration(ctx context.Context, deps *Deps, data transactionData) error {
	if data.RedisTx == nil {
		return errInvalidRedisTransaction
	}

	migrationVersion := strconv.FormatInt(data.MigrationNumber, 10)

	jsonData, err := json.Marshal(redisData{
		Method:    "UP",
		StartTime: data.StartTime,
		Duration:  time.Since(data.StartTime).Milliseconds(),
	})
	if err != nil {
		deps.log().Errorf("migration %v for Redis failed with err: %v", migrationVersion, err)

		return err
	}

	_, err = data.RedisTx.HSet(ctx, "kite_migrations", map[string]string{migrationVersion: string(jsonData)}).Result()
	if err != nil {
		deps.log().Errorf("migration %v for Redis failed with err: %v", migrationVersion, err)

		return err
	}

	_, err = data.RedisTx.Exec(ctx)
	if err != nil {
		deps.log().Errorf("migration %v for Redis failed with err: %v", migrationVersion, err)

		return err
	}

	if m.migrator == nil {
		return nil
	}

	return m.migrator.commitMigration(ctx, deps, data)
}

func (m redisMigrator) rollback(ctx context.Context, deps *Deps, data transactionData) error {
	if data.RedisTx != nil {
		data.RedisTx.Discard()
	}
	if m.migrator == nil {
		return nil
	}

	return m.migrator.rollback(ctx, deps, data)
}
