package migration

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	goRedis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sllt/pi/pkg/pi/logging"
)

func TestRunReturnsInvalidMigrationError(t *testing.T) {
	result, err := Run(t.Context(), map[int64]Migrate{1: {}}, nil)

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidMigration))
	assert.Empty(t, result.Applied)
}

func TestRunReturnsNoDatasourceError(t *testing.T) {
	result, err := Run(t.Context(), map[int64]Migrate{1: {UP: func(Datasource) error { return nil }}}, nil)

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNoDatasource))
	assert.Empty(t, result.Applied)
}

func TestRunReturnsResultOnUserMigrationFailure(t *testing.T) {
	ps := &fakePubSub{
		createTopicFn: func(_ context.Context, name string) error {
			if name == "users" {
				return sql.ErrConnDone
			}
			return nil
		},
	}
	deps := Deps{
		Logger: logging.NewWriterLogger(logging.INFO, io.Discard, io.Discard),
		PubSub: ps,
	}

	result, err := Run(t.Context(), map[int64]Migrate{
		1: {Name: "fail_users", UP: func(d Datasource) error {
			return d.PubSub.CreateTopic(t.Context(), "users")
		}},
	}, deps)

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrMigrationFailed))
	require.NotNil(t, result.Failed)
	assert.Equal(t, int64(1), result.Failed.Version)
	assert.Equal(t, "fail_users", result.Failed.Name)
	assert.Equal(t, StateSourceLegacy, result.StateSource)
	assert.False(t, result.StatePrecise)
}

func TestRunDryRunDoesNotExecuteMigration(t *testing.T) {
	deps := Deps{
		Logger: logging.NewWriterLogger(logging.INFO, io.Discard, io.Discard),
		PubSub: &fakePubSub{},
	}

	executed := false
	result, err := Run(t.Context(), map[int64]Migrate{
		1: {UP: func(Datasource) error {
			executed = true
			return nil
		}},
	}, deps, WithDryRun())

	require.NoError(t, err)
	assert.False(t, executed)
	require.Len(t, result.Skipped, 1)
	assert.Equal(t, SkipDryRun, result.Skipped[0].Reason)
}

func TestRunHonorsCanceledContextBeforeDatasourceWork(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := Run(ctx, map[int64]Migrate{1: {UP: func(Datasource) error { return nil }}}, nil)

	require.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled))
}

func TestRunWithLockRequiresSupportedLockBackend(t *testing.T) {
	deps := Deps{
		Logger: logging.NewWriterLogger(logging.INFO, io.Discard, io.Discard),
		PubSub: &fakePubSub{},
	}

	_, err := Run(t.Context(), map[int64]Migrate{1: {UP: func(Datasource) error { return nil }}}, deps, WithLock())

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrMigrationLockUnavailable))
	assert.False(t, errors.Is(err, ErrMigrationLocked))
}

func TestAcquireMigrationLockUsesRedisWhenSQLMissing(t *testing.T) {
	var setCalled, evalCalled bool
	deps := &Deps{
		Logger: logging.NewWriterLogger(logging.INFO, io.Discard, io.Discard),
		Redis: &fakeRedis{
			setNXFn: func(_ context.Context, key string, _ any, ttl time.Duration) *goRedis.BoolCmd {
				assert.Equal(t, migrationRedisKey, key)
				assert.Equal(t, migrationLockTTL, ttl)
				setCalled = true
				return goRedis.NewBoolResult(true, nil)
			},
			evalFn: func(_ context.Context, script string, keys []string, _ ...any) *goRedis.Cmd {
				assert.Equal(t, releaseRedisMigrationLockScript, script)
				assert.Equal(t, []string{migrationRedisKey}, keys)
				evalCalled = true
				return goRedis.NewCmdResult(int64(1), nil)
			},
		},
	}

	release, err := acquireMigrationLock(t.Context(), deps, applyOptions([]Option{WithLock()}))
	require.NoError(t, err)
	require.NotNil(t, release)
	require.NoError(t, release(t.Context()))
	assert.True(t, setCalled)
	assert.True(t, evalCalled)
}

func TestAcquireMigrationLockReturnsLockedWhenRedisSetNXFails(t *testing.T) {
	deps := &Deps{
		Logger: logging.NewWriterLogger(logging.INFO, io.Discard, io.Discard),
		Redis: &fakeRedis{
			setNXFn: func(context.Context, string, any, time.Duration) *goRedis.BoolCmd {
				return goRedis.NewBoolResult(false, nil)
			},
		},
	}

	release, err := acquireMigrationLock(t.Context(), deps, applyOptions([]Option{WithLock()}))

	require.Error(t, err)
	assert.Nil(t, release)
	assert.True(t, errors.Is(err, ErrMigrationLocked))
}

func TestAcquireMigrationLockCanStealExpiredSQLLock(t *testing.T) {
	deps, _, mock := newTestSQLDeps(t, "mysql")

	mock.ExpectExec(createSQLMigrationLocksTable).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(insertSQLMigrationLockMySQL).WillReturnError(&mysql.MySQLError{Number: 1062})
	mock.ExpectExec(updateSQLMigrationLockMySQL).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(deleteSQLMigrationLockMySQL).WillReturnResult(sqlmock.NewResult(0, 1))

	release, err := acquireMigrationLock(t.Context(), deps, applyOptions([]Option{WithLock()}))
	require.NoError(t, err)
	require.NotNil(t, release)
	require.NoError(t, release(t.Context()))
}

func TestPlanReturnsApplyAndSkipActions(t *testing.T) {
	deps, _, mock := newTestSQLDeps(t, "mysql")

	mock.ExpectExec(createSQLPiMigrationsTable).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(listSQLPiMigrations).
		WillReturnRows(mock.NewRows([]string{"version", "method", "start_time", "duration"}).
			AddRow(1, "UP", time.Now(), nil))

	plan, err := Plan(t.Context(), map[int64]Migrate{
		1: {Name: "already", UP: func(Datasource) error { return nil }},
		2: {Name: "pending", UP: func(Datasource) error { return nil }},
	}, *deps)

	require.NoError(t, err)
	assert.Equal(t, StateSourceSQL, plan.StateSource)
	assert.True(t, plan.StatePrecise)
	require.Len(t, plan.Items, 2)
	assert.Equal(t, PlanSkip, plan.Items[0].Action)
	assert.Equal(t, PlanApply, plan.Items[1].Action)
}

func TestStatusDetectsSQLAppliedGaps(t *testing.T) {
	deps, _, mock := newTestSQLDeps(t, "mysql")

	mock.ExpectExec(createSQLPiMigrationsTable).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(listSQLPiMigrations).
		WillReturnRows(mock.NewRows([]string{"version", "method", "start_time", "duration"}).
			AddRow(1, "UP", time.Now(), 10).
			AddRow(3, "UP", time.Now(), 20))

	status, err := Status(t.Context(), map[int64]Migrate{
		1: {Name: "one", UP: func(Datasource) error { return nil }},
		2: {Name: "two", UP: func(Datasource) error { return nil }},
		3: {Name: "three", UP: func(Datasource) error { return nil }},
	}, *deps)

	require.NoError(t, err)
	assert.Equal(t, StateSourceSQL, status.StateSource)
	assert.True(t, status.StatePrecise)
	assert.Equal(t, []int64{2}, status.Gaps)
	assert.Equal(t, []int64{1, 3}, versionsFromResults(status.Applied))
	assert.Equal(t, []int64{2}, versionsFromResults(status.Pending))
}

func TestStatusDetectsRedisAppliedGaps(t *testing.T) {
	applied := map[string]string{
		"1": `{"method":"UP","startTime":"2026-05-01T00:00:00Z","duration":10}`,
		"3": `{"method":"UP","startTime":"2026-05-01T00:00:01Z","duration":20}`,
	}
	deps := Deps{
		Logger: logging.NewWriterLogger(logging.INFO, io.Discard, io.Discard),
		Redis: &fakeRedis{
			hgetAllFn: func(_ context.Context, key string) *goRedis.MapStringStringCmd {
				assert.Equal(t, "kite_migrations", key)
				return goRedis.NewMapStringStringResult(applied, nil)
			},
		},
	}

	status, err := Status(t.Context(), map[int64]Migrate{
		1: {Name: "one", UP: func(Datasource) error { return nil }},
		2: {Name: "two", UP: func(Datasource) error { return nil }},
		3: {Name: "three", UP: func(Datasource) error { return nil }},
	}, deps)

	require.NoError(t, err)
	assert.Equal(t, StateSourceRedis, status.StateSource)
	assert.True(t, status.StatePrecise)
	assert.Equal(t, []int64{2}, status.Gaps)
	assert.Equal(t, []int64{1, 3}, versionsFromResults(status.Applied))
	assert.Equal(t, []int64{2}, versionsFromResults(status.Pending))
}

func TestRunReturnsGapErrorBeforeExecutingMissingSQLMigration(t *testing.T) {
	deps, _, mock := newTestSQLDeps(t, "mysql")

	mock.ExpectExec(createSQLPiMigrationsTable).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(listSQLPiMigrations).
		WillReturnRows(mock.NewRows([]string{"version", "method", "start_time", "duration"}).
			AddRow(1, "UP", time.Now(), 10).
			AddRow(3, "UP", time.Now(), 20))

	executed := false
	_, err := Run(t.Context(), map[int64]Migrate{
		1: {Name: "one", UP: func(Datasource) error { return nil }},
		2: {Name: "two", UP: func(Datasource) error {
			executed = true
			return nil
		}},
		3: {Name: "three", UP: func(Datasource) error { return nil }},
	}, *deps)

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrMigrationGap))
	assert.False(t, executed)
}

func TestStatusPrefersSQLStateWhenRedisIsAlsoConfigured(t *testing.T) {
	deps, _, mock := newTestSQLDeps(t, "mysql")
	deps.Redis = &fakeRedis{
		hgetAllFn: func(context.Context, string) *goRedis.MapStringStringCmd {
			t.Fatal("Redis state should not be queried when SQL is configured")
			return nil
		},
	}

	mock.ExpectExec(createSQLPiMigrationsTable).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(listSQLPiMigrations).
		WillReturnRows(mock.NewRows([]string{"version", "method", "start_time", "duration"}).
			AddRow(1, "UP", time.Now(), 0))

	status, err := Status(t.Context(), map[int64]Migrate{
		1: {UP: func(Datasource) error { return nil }},
		2: {UP: func(Datasource) error { return nil }},
	}, *deps)

	require.NoError(t, err)
	assert.Equal(t, StateSourceSQL, status.StateSource)
	assert.True(t, status.StatePrecise)
	assert.Equal(t, []int64{1}, versionsFromResults(status.Applied))
	assert.Equal(t, []int64{2}, versionsFromResults(status.Pending))
}

func TestPlanOnlyInitializesAuthoritativeStateStore(t *testing.T) {
	deps, _, mock := newTestSQLDeps(t, "mysql")
	deps.PubSub = &fakePubSub{
		createTopicFn: func(context.Context, string) error {
			t.Fatal("PubSub topic should not be created during Plan when SQL is authoritative")
			return nil
		},
	}

	mock.ExpectExec(createSQLPiMigrationsTable).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(listSQLPiMigrations).
		WillReturnRows(mock.NewRows([]string{"version", "method", "start_time", "duration"}))

	plan, err := Plan(t.Context(), map[int64]Migrate{
		1: {UP: func(Datasource) error { return nil }},
	}, *deps)

	require.NoError(t, err)
	assert.Equal(t, StateSourceSQL, plan.StateSource)
	assert.True(t, plan.StatePrecise)
	require.Len(t, plan.Items, 1)
	assert.Equal(t, PlanApply, plan.Items[0].Action)
}

func TestSQLMigrationQueriesSupportPostgresCompatibleDialects(t *testing.T) {
	for _, dialect := range []string{"postgres", "supabase", "cockroachdb"} {
		t.Run(dialect, func(t *testing.T) {
			insertQuery, err := sqlMigrationInsertQuery(dialect)
			require.NoError(t, err)
			assert.Equal(t, insertPiMigrationRowPostgres, insertQuery)

			lockInsert, lockUpdate, lockDelete, err := sqlMigrationLockQueries(dialect)
			require.NoError(t, err)
			assert.Equal(t, insertSQLMigrationLockPostgres, lockInsert)
			assert.Equal(t, updateSQLMigrationLockPostgres, lockUpdate)
			assert.Equal(t, deleteSQLMigrationLockPostgres, lockDelete)
		})
	}
}

func versionsFromResults(results []VersionResult) []int64 {
	versions := make([]int64, 0, len(results))
	for _, result := range results {
		versions = append(versions, result.Version)
	}

	return versions
}
