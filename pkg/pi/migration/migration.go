package migration

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/gogo/protobuf/sortkeys"
	goRedis "github.com/redis/go-redis/v9"

	piSql "github.com/sllt/pi/pkg/pi/datasource/sql"
	"github.com/sllt/pi/pkg/pi/infra"
)

type MigrateFunc func(d Datasource) error

type ContextFunc func(ctx context.Context, d Datasource) error

type Migrate struct {
	UP MigrateFunc

	// Down is reserved for explicit rollback/down flows. The first Migration v2
	// delivery keeps UP-compatible migrations working and does not automatically
	// call Down on UP failures.
	Down MigrateFunc

	// UpContext is preferred for new migrations because it allows deployment
	// cancellation and timeouts to propagate into datasource calls.
	UpContext ContextFunc

	// DownContext is reserved for explicit down flows.
	DownContext ContextFunc

	// Name is optional human-readable metadata used in structured results.
	Name string
}

type transactionData struct {
	StartTime       time.Time
	MigrationNumber int64

	SQLTx    *piSql.Tx
	RedisTx  goRedis.Pipeliner
	OracleTx infra.OracleTx
}

// Run applies pending migrations and returns a structured result.
func Run(ctx context.Context, migrationsMap map[int64]Migrate, c *infra.Container, opts ...Option) (result Result, err error) {
	if ctx == nil {
		ctx = context.Background()
	}

	options := applyOptions(opts)
	result.Direction = options.Direction
	result.StartedAt = time.Now()
	defer func() {
		result.FinishedAt = time.Now()
	}()
	if err = validateOptions(options); err != nil {
		return result, err
	}

	if options.Direction != DirectionUp {
		return result, fmt.Errorf("%w: direction %q is not implemented", ErrDownNotDefined, options.Direction)
	}

	invalidKeys, keys := getKeys(migrationsMap)
	if len(invalidKeys) > 0 {
		err = fmt.Errorf("%w: versions must be positive and define UP or UpContext: %v", ErrInvalidMigration, invalidKeys)
		if c != nil {
			c.Errorf("migration run failed! UP not defined for the following keys: %v", invalidKeys)
		}

		return result, err
	}

	sortkeys.Int64s(keys)

	if err = ctx.Err(); err != nil {
		return result, err
	}

	ds, mg, ok := getMigrator(c)
	if c != nil {
		ds.Logger = c.Logger
	}

	// Returning with an error as migration would eventually fail if no databases are initialized.
	// Pub/Sub is considered initialized if its configurations are given.
	if !ok {
		if c != nil {
			c.Errorf("no migrations are running as datasources are not initialized")
		}

		return result, ErrNoDatasource
	}

	stateReader := selectMigrationStateReader(c, mg)
	result.StateSource = stateReader.source
	result.StatePrecise = stateReader.precise
	if err = mg.checkAndCreateMigrationTable(ctx, c); err != nil {
		return result, fmt.Errorf("migration: ensure state store: %w", err)
	}

	releaseLock, err := acquireMigrationLock(ctx, c, options)
	if err != nil {
		return result, err
	}
	if releaseLock != nil {
		defer func() {
			if releaseErr := releaseMigrationLock(ctx, releaseLock); releaseErr != nil {
				err = errorsJoin(err, releaseErr)
			}
		}()
	}

	state, err := readMigrationState(ctx, c, stateReader, migrationsMap)
	if err != nil {
		return result, err
	}
	result.StatePrecise = state.Precise

	if gaps := state.detectGaps(keys, options.Target); len(gaps) > 0 {
		return result, fmt.Errorf("%w: versions %v", ErrMigrationGap, gaps)
	}

	for _, currentMigration := range keys {
		migrationDef := migrationsMap[currentMigration]
		name := migrationDef.displayName(currentMigration)

		if options.Target > 0 && currentMigration > options.Target {
			result.Skipped = append(result.Skipped, VersionResult{Version: currentMigration, Name: name, Reason: SkipAboveTarget})
			continue
		}

		if state.shouldSkipAsApplied(currentMigration) {
			if c != nil {
				c.Infof("skipping migration %v", currentMigration)
			}

			result.Skipped = append(result.Skipped, VersionResult{Version: currentMigration, Name: name, Reason: SkipAlreadyApplied})
			continue
		}

		if options.DryRun {
			result.Skipped = append(result.Skipped, VersionResult{Version: currentMigration, Name: name, Reason: SkipDryRun})
			continue
		}

		if err = ctx.Err(); err != nil {
			return result, err
		}

		if c != nil {
			c.Logger.Infof("running migration %v", currentMigration)
		}

		migrationInfo, err := mg.beginTransaction(ctx, c)
		if err != nil {
			ve := &VersionError{Version: currentMigration, Name: name, Op: "begin", Err: err}
			result.Failed = &VersionResult{Version: currentMigration, Name: name, Error: ve}
			return result, joinVersionError(ve, nil)
		}

		// Replacing the objects in datasource object only for those Datasources which support transactions.
		if migrationInfo.SQLTx != nil {
			ds.SQL = migrationInfo.SQLTx
		}
		if migrationInfo.RedisTx != nil {
			ds.Redis = migrationInfo.RedisTx
		}

		if migrationInfo.OracleTx != nil {
			ds.Oracle = &oracleTransactionWrapper{tx: migrationInfo.OracleTx}
		}

		migrationInfo.StartTime = time.Now()
		migrationInfo.MigrationNumber = currentMigration

		err = migrationDef.runUp(ctx, ds)
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			if c != nil {
				c.Logger.Errorf("failed to run migration : [%v], err: %v", currentMigration, err)
			}

			rollbackErr := rollbackMigration(ctx, mg, c, migrationInfo)
			ve := &VersionError{Version: currentMigration, Name: name, Op: "up", Err: err}
			result.Failed = &VersionResult{Version: currentMigration, Name: name, Duration: time.Since(migrationInfo.StartTime), Error: ve}

			return result, joinVersionError(ve, rollbackErr)
		}

		err = invokeMigration(func() error { return mg.commitMigration(ctx, c, migrationInfo) })
		if err != nil {
			if c != nil {
				c.Errorf("failed to commit migration, err: %v", err)
			}

			rollbackErr := rollbackMigration(ctx, mg, c, migrationInfo)
			ve := &VersionError{Version: currentMigration, Name: name, Op: "commit", Err: err}
			result.Failed = &VersionResult{Version: currentMigration, Name: name, Duration: time.Since(migrationInfo.StartTime), Error: ve}

			return result, joinVersionError(ve, rollbackErr)
		}

		result.Applied = append(result.Applied, VersionResult{Version: currentMigration, Name: name, Duration: time.Since(migrationInfo.StartTime)})
	}

	return result, nil
}

func joinVersionError(ve *VersionError, rollbackErr error) error {
	if rollbackErr == nil {
		return fmt.Errorf("%w: %w", ErrMigrationFailed, ve)
	}

	return fmt.Errorf("%w: %w", ErrMigrationFailed, errorsJoin(ve, fmt.Errorf("rollback: %w", rollbackErr)))
}

func (m Migrate) hasUp() bool {
	return m.UP != nil || m.UpContext != nil
}

func (m Migrate) runUp(ctx context.Context, ds Datasource) error {
	return invokeMigration(func() error {
		if m.UpContext != nil {
			return m.UpContext(ctx, ds)
		}
		return m.UP(ds)
	})
}

// Recover inside the transaction boundary so a user panic follows the same
// rollback and structured-error path as a returned error.
func invokeMigration(fn func() error) (err error) {
	defer func() {
		if value := recover(); value != nil {
			if cause, ok := value.(error); ok {
				err = fmt.Errorf("%w: %w", ErrMigrationPanic, cause)
			} else {
				err = fmt.Errorf("%w: %v", ErrMigrationPanic, value)
			}
		}
	}()
	return fn()
}

func rollbackMigration(ctx context.Context, mg migrator, c *infra.Container, data transactionData) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return invokeMigration(func() error { return mg.rollback(cleanupCtx, c, data) })
}

func (m Migrate) displayName(version int64) string {
	if m.Name != "" {
		return m.Name
	}

	return fmt.Sprintf("%d", version)
}

func getKeys(migrationsMap map[int64]Migrate) (invalidKey, keys []int64) {
	invalidKey = make([]int64, 0, len(migrationsMap))
	keys = make([]int64, 0, len(migrationsMap))

	for k, v := range migrationsMap {
		if k <= 0 || !v.hasUp() {
			invalidKey = append(invalidKey, k)

			continue
		}

		keys = append(keys, k)
	}

	sortkeys.Int64s(invalidKey)
	return invalidKey, keys
}

func getMigrator(c *infra.Container) (Datasource, migrator, bool) {
	var (
		ds Datasource
		mg migrator = &ds
		ok bool
	)

	mg, ok = initializeDatasources(c, &ds, mg)

	return ds, mg, ok
}

type datasourceInitializer struct {
	condition     func() bool
	setDS         func()
	apply         func(m migrator) migrator
	logIdentifier string
}

func initializeDatasources(c *infra.Container, ds *Datasource, mg migrator) (migrator, bool) {
	if c == nil {
		return mg, false
	}

	var initialized bool

	initializers := []datasourceInitializer{
		{
			condition:     func() bool { return !isNil(c.SQL) },
			setDS:         func() { ds.SQL = c.SQL },
			apply:         func(m migrator) migrator { return (&sqlDS{ds.SQL}).apply(m) },
			logIdentifier: "SQL",
		},
		{
			condition:     func() bool { return !isNil(c.Redis) },
			setDS:         func() { ds.Redis = c.Redis },
			apply:         func(m migrator) migrator { return redisDS{ds.Redis}.apply(m) },
			logIdentifier: "Redis",
		},
		{
			condition:     func() bool { return !isNil(c.DGraph) },
			setDS:         func() { ds.DGraph = dgraphDS{c.DGraph} },
			apply:         func(m migrator) migrator { return dgraphDS{c.DGraph}.apply(m) },
			logIdentifier: "DGraph",
		},
		{
			condition:     func() bool { return !isNil(c.Clickhouse) },
			setDS:         func() { ds.Clickhouse = c.Clickhouse },
			apply:         func(m migrator) migrator { return clickHouseDS{ds.Clickhouse}.apply(m) },
			logIdentifier: "Clickhouse",
		},
		{
			condition:     func() bool { return !isNil(c.Oracle) },
			setDS:         func() { ds.Oracle = c.Oracle },
			apply:         func(m migrator) migrator { return oracleDS{c.Oracle}.apply(m) },
			logIdentifier: "Oracle",
		},

		{
			condition:     func() bool { return c.PubSub != nil },
			setDS:         func() { ds.PubSub = c.PubSub },
			apply:         func(m migrator) migrator { return pubsubDS{c.PubSub}.apply(m) },
			logIdentifier: "PubSub",
		},
		{
			condition:     func() bool { return !isNil(c.Cassandra) },
			setDS:         func() { ds.Cassandra = cassandraDS{c.Cassandra} },
			apply:         func(m migrator) migrator { return cassandraDS{c.Cassandra}.apply(m) },
			logIdentifier: "Cassandra",
		},
		{
			condition:     func() bool { return !isNil(c.Mongo) },
			setDS:         func() { ds.Mongo = mongoDS{c.Mongo} },
			apply:         func(m migrator) migrator { return mongoDS{c.Mongo}.apply(m) },
			logIdentifier: "Mongo",
		},
		{
			condition:     func() bool { return !isNil(c.ArangoDB) },
			setDS:         func() { ds.ArangoDB = arangoDS{c.ArangoDB} },
			apply:         func(m migrator) migrator { return arangoDS{c.ArangoDB}.apply(m) },
			logIdentifier: "ArangoDB",
		},
		{
			condition:     func() bool { return !isNil(c.SurrealDB) },
			setDS:         func() { ds.SurrealDB = surrealDS{c.SurrealDB} },
			apply:         func(m migrator) migrator { return surrealDS{c.SurrealDB}.apply(m) },
			logIdentifier: "SurrealDB",
		},
		{
			condition:     func() bool { return !isNil(c.Elasticsearch) },
			setDS:         func() { ds.Elasticsearch = c.Elasticsearch },
			apply:         func(m migrator) migrator { return elasticsearchDS{c.Elasticsearch}.apply(m) },
			logIdentifier: "Elasticsearch",
		},
		{
			condition:     func() bool { return !isNil(c.OpenTSDB) },
			setDS:         func() { ds.OpenTSDB = c.OpenTSDB },
			apply:         func(m migrator) migrator { return openTSDBDS{c.OpenTSDB, "kite_migrations.json"}.apply(m) },
			logIdentifier: "OpenTSDB",
		},
		{
			condition:     func() bool { return !isNil(c.ScyllaDB) },
			setDS:         func() { ds.ScyllaDB = c.ScyllaDB },
			apply:         func(m migrator) migrator { return scyllaDS{c.ScyllaDB}.apply(m) },
			logIdentifier: "ScyllaDB",
		},
	}

	for _, init := range initializers {
		if !init.condition() {
			continue
		}

		init.setDS()
		mg = init.apply(mg)
		initialized = true

		c.Debugf("initialized data source for %s", init.logIdentifier)
	}

	return mg, initialized
}

func isNil(i any) bool {
	if i == nil {
		return true
	}

	val := reflect.ValueOf(i)

	switch val.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return val.IsNil()
	default:
		return false
	}
}
