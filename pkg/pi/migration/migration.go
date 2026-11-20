package migration

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"time"

	goRedis "github.com/redis/go-redis/v9"

	piSql "github.com/sllt/pi/pkg/pi/datasource/sql"
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

	SQLTx   *piSql.Tx
	RedisTx goRedis.Pipeliner
}

// Run applies pending migrations and returns a structured result.
func Run(ctx context.Context, migrationsMap map[int64]Migrate, target Target, opts ...Option) (result Result, err error) {
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
		if deps, _ := resolveDeps(target); deps != nil {
			deps.log().Errorf("migration run failed! UP not defined for the following keys: %v", invalidKeys)
		}

		return result, err
	}

	slices.Sort(keys)

	if err = ctx.Err(); err != nil {
		return result, err
	}

	deps, _ := resolveDeps(target)
	ds, mg, ok := getMigrator(deps)
	if !ok {
		if deps != nil {
			deps.log().Errorf("no migrations are running as datasources are not initialized")
		}

		return result, ErrNoDatasource
	}

	stateReader := selectMigrationStateReader(deps, mg)
	result.StateSource = stateReader.source
	result.StatePrecise = stateReader.precise
	if err = mg.checkAndCreateMigrationTable(ctx, deps); err != nil {
		return result, fmt.Errorf("migration: ensure state store: %w", err)
	}

	releaseLock, err := acquireMigrationLock(ctx, deps, options)
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

	state, err := readMigrationState(ctx, deps, stateReader, migrationsMap)
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
			deps.log().Infof("skipping migration %v", currentMigration)

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

		deps.log().Infof("running migration %v", currentMigration)

		migrationInfo, err := mg.beginTransaction(ctx, deps)
		if err != nil {
			ve := &VersionError{Version: currentMigration, Name: name, Op: "begin", Err: err}
			result.Failed = &VersionResult{Version: currentMigration, Name: name, Error: ve}
			return result, joinVersionError(ve, nil)
		}

		execDS := ds
		if migrationInfo.SQLTx != nil {
			execDS.SQL = migrationInfo.SQLTx
		}
		if migrationInfo.RedisTx != nil {
			execDS.Redis = migrationInfo.RedisTx
		}

		migrationInfo.StartTime = time.Now()
		migrationInfo.MigrationNumber = currentMigration

		err = migrationDef.runUp(ctx, execDS)
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			deps.log().Errorf("failed to run migration : [%v], err: %v", currentMigration, err)

			rollbackErr := rollbackMigration(ctx, mg, deps, migrationInfo)
			ve := &VersionError{Version: currentMigration, Name: name, Op: "up", Err: err}
			result.Failed = &VersionResult{Version: currentMigration, Name: name, Duration: time.Since(migrationInfo.StartTime), Error: ve}

			return result, joinVersionError(ve, rollbackErr)
		}

		err = invokeMigration(func() error { return mg.commitMigration(ctx, deps, migrationInfo) })
		if err != nil {
			deps.log().Errorf("failed to commit migration, err: %v", err)

			rollbackErr := rollbackMigration(ctx, mg, deps, migrationInfo)
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

func rollbackMigration(ctx context.Context, mg migrator, deps *Deps, data transactionData) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return invokeMigration(func() error { return mg.rollback(cleanupCtx, deps, data) })
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

	slices.Sort(invalidKey)
	return invalidKey, keys
}

func getMigrator(deps *Deps) (Datasource, migrator, bool) {
	var (
		ds Datasource
		mg migrator = &ds
		ok bool
	)

	if deps != nil {
		ds.Logger = deps.log()
		ds.adapters = deps.Adapters
	} else {
		ds.Logger = nopLogger{}
	}

	mg, ok = initializeDatasources(deps, &ds, mg)

	return ds, mg, ok
}

type datasourceInitializer struct {
	condition     func() bool
	setDS         func()
	apply         func(m migrator) migrator
	logIdentifier string
}

func initializeDatasources(deps *Deps, ds *Datasource, mg migrator) (migrator, bool) {
	if deps == nil {
		return mg, false
	}

	var initialized bool

	initializers := []datasourceInitializer{
		{
			condition:     func() bool { return !isNil(deps.SQL) },
			setDS:         func() { ds.SQL = deps.SQL },
			apply:         func(m migrator) migrator { return (&sqlDS{ds.SQL}).apply(m) },
			logIdentifier: "SQL",
		},
		{
			condition:     func() bool { return !isNil(deps.Redis) },
			setDS:         func() { ds.Redis = deps.Redis },
			apply:         func(m migrator) migrator { return redisDS{ds.Redis}.apply(m) },
			logIdentifier: "Redis",
		},
		{
			condition:     func() bool { return !isNil(deps.PubSub) },
			setDS:         func() { ds.PubSub = deps.PubSub },
			apply:         func(m migrator) migrator { return pubsubDS{deps.PubSub}.apply(m) },
			logIdentifier: "PubSub",
		},
	}

	for _, init := range initializers {
		if !init.condition() {
			continue
		}

		init.setDS()
		mg = init.apply(mg)
		initialized = true

		deps.log().Debugf("initialized data source for %s", init.logIdentifier)
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
