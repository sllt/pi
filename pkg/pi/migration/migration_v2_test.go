package migration

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	goRedis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/sllt/pi/pkg/pi/infra"
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
	mockClickHouse, mockContainer := initializeClickHouseRunMocks(t)

	mockClickHouse.EXPECT().Exec(gomock.Any(), CheckAndCreateChMigrationTable).Return(nil)
	mockClickHouse.EXPECT().Select(gomock.Any(), gomock.Any(), getLastChPiMigration).Return(nil)
	mockClickHouse.EXPECT().Exec(gomock.Any(), "SELECT * FROM users").Return(sql.ErrConnDone)

	result, err := Run(t.Context(), map[int64]Migrate{
		1: {Name: "fail_users", UP: func(d Datasource) error {
			return d.Clickhouse.Exec(t.Context(), "SELECT * FROM users")
		}},
	}, mockContainer)

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrMigrationFailed))
	require.NotNil(t, result.Failed)
	assert.Equal(t, int64(1), result.Failed.Version)
	assert.Equal(t, "fail_users", result.Failed.Name)
	assert.Equal(t, StateSourceLegacy, result.StateSource)
	assert.False(t, result.StatePrecise)
}

func TestRunDryRunDoesNotExecuteMigration(t *testing.T) {
	mockClickHouse, mockContainer := initializeClickHouseRunMocks(t)

	mockClickHouse.EXPECT().Exec(gomock.Any(), CheckAndCreateChMigrationTable).Return(nil)
	mockClickHouse.EXPECT().Select(gomock.Any(), gomock.Any(), getLastChPiMigration).Return(nil)

	executed := false
	result, err := Run(t.Context(), map[int64]Migrate{
		1: {UP: func(Datasource) error {
			executed = true
			return nil
		}},
	}, mockContainer, WithDryRun())

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
	mockClickHouse, mockContainer := initializeClickHouseRunMocks(t)

	mockClickHouse.EXPECT().Exec(gomock.Any(), CheckAndCreateChMigrationTable).Return(nil)

	_, err := Run(t.Context(), map[int64]Migrate{1: {UP: func(Datasource) error { return nil }}}, mockContainer, WithLock())

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrMigrationLockUnavailable))
	assert.False(t, errors.Is(err, ErrMigrationLocked))
}

func TestAcquireMigrationLockUsesRedisWhenSQLMissing(t *testing.T) {
	mockContainer, mocks := newRedisOnlyMockContainer(t)

	mocks.Redis.EXPECT().
		SetNX(gomock.Any(), migrationRedisKey, gomock.Any(), migrationLockTTL).
		Return(goRedis.NewBoolResult(true, nil))
	mocks.Redis.EXPECT().
		Eval(gomock.Any(), releaseRedisMigrationLockScript, []string{migrationRedisKey}, gomock.Any()).
		Return(goRedis.NewCmdResult(int64(1), nil))

	release, err := acquireMigrationLock(t.Context(), mockContainer, applyOptions([]Option{WithLock()}))
	require.NoError(t, err)
	require.NotNil(t, release)
	require.NoError(t, release(t.Context()))
}

func TestAcquireMigrationLockReturnsLockedWhenRedisSetNXFails(t *testing.T) {
	mockContainer, mocks := newRedisOnlyMockContainer(t)

	mocks.Redis.EXPECT().
		SetNX(gomock.Any(), migrationRedisKey, gomock.Any(), migrationLockTTL).
		Return(goRedis.NewBoolResult(false, nil))

	release, err := acquireMigrationLock(t.Context(), mockContainer, applyOptions([]Option{WithLock()}))

	require.Error(t, err)
	assert.Nil(t, release)
	assert.True(t, errors.Is(err, ErrMigrationLocked))
}

func TestAcquireMigrationLockCanStealExpiredSQLLock(t *testing.T) {
	mockContainer, mocks := newSQLOnlyMockContainer(t)

	mocks.SQL.ExpectExec(createSQLMigrationLocksTable).WillReturnResult(mocks.SQL.NewResult(1, 1))
	mocks.SQL.ExpectDialect().WillReturnString("mysql")
	mocks.SQL.ExpectExec(insertSQLMigrationLockMySQL).WillReturnError(&mysql.MySQLError{Number: 1062})
	mocks.SQL.ExpectExec(updateSQLMigrationLockMySQL).WillReturnResult(mocks.SQL.NewResult(0, 1))
	mocks.SQL.ExpectExec(deleteSQLMigrationLockMySQL).WillReturnResult(mocks.SQL.NewResult(0, 1))

	release, err := acquireMigrationLock(t.Context(), mockContainer, applyOptions([]Option{WithLock()}))
	require.NoError(t, err)
	require.NotNil(t, release)
	require.NoError(t, release(t.Context()))
}

func TestPlanReturnsApplyAndSkipActions(t *testing.T) {
	mockContainer, mocks := newSQLOnlyMockContainer(t)

	mocks.SQL.ExpectExec(createSQLPiMigrationsTable).WillReturnResult(mocks.SQL.NewResult(1, 1))
	mocks.SQL.ExpectQuery(listSQLPiMigrations).
		WillReturnRows(mocks.SQL.NewRows([]string{"version", "method", "start_time", "duration"}).
			AddRow(1, "UP", time.Now(), nil))

	plan, err := Plan(t.Context(), map[int64]Migrate{
		1: {Name: "already", UP: func(Datasource) error { return nil }},
		2: {Name: "pending", UP: func(Datasource) error { return nil }},
	}, mockContainer)

	require.NoError(t, err)
	assert.Equal(t, StateSourceSQL, plan.StateSource)
	assert.True(t, plan.StatePrecise)
	require.Len(t, plan.Items, 2)
	assert.Equal(t, PlanSkip, plan.Items[0].Action)
	assert.Equal(t, PlanApply, plan.Items[1].Action)
}

func TestStatusDetectsSQLAppliedGaps(t *testing.T) {
	mockContainer, mocks := newSQLOnlyMockContainer(t)

	mocks.SQL.ExpectExec(createSQLPiMigrationsTable).WillReturnResult(mocks.SQL.NewResult(1, 1))
	mocks.SQL.ExpectQuery(listSQLPiMigrations).
		WillReturnRows(mocks.SQL.NewRows([]string{"version", "method", "start_time", "duration"}).
			AddRow(1, "UP", time.Now(), 10).
			AddRow(3, "UP", time.Now(), 20))

	status, err := Status(t.Context(), map[int64]Migrate{
		1: {Name: "one", UP: func(Datasource) error { return nil }},
		2: {Name: "two", UP: func(Datasource) error { return nil }},
		3: {Name: "three", UP: func(Datasource) error { return nil }},
	}, mockContainer)

	require.NoError(t, err)
	assert.Equal(t, StateSourceSQL, status.StateSource)
	assert.True(t, status.StatePrecise)
	assert.Equal(t, []int64{2}, status.Gaps)
	assert.Equal(t, []int64{1, 3}, versionsFromResults(status.Applied))
	assert.Equal(t, []int64{2}, versionsFromResults(status.Pending))
}

func TestStatusDetectsRedisAppliedGaps(t *testing.T) {
	mockContainer, mocks := newRedisOnlyMockContainer(t)
	applied := map[string]string{
		"1": `{"method":"UP","startTime":"2026-05-01T00:00:00Z","duration":10}`,
		"3": `{"method":"UP","startTime":"2026-05-01T00:00:01Z","duration":20}`,
	}

	mocks.Redis.EXPECT().HGetAll(gomock.Any(), "kite_migrations").
		Return(goRedis.NewMapStringStringResult(applied, nil))

	status, err := Status(t.Context(), map[int64]Migrate{
		1: {Name: "one", UP: func(Datasource) error { return nil }},
		2: {Name: "two", UP: func(Datasource) error { return nil }},
		3: {Name: "three", UP: func(Datasource) error { return nil }},
	}, mockContainer)

	require.NoError(t, err)
	assert.Equal(t, StateSourceRedis, status.StateSource)
	assert.True(t, status.StatePrecise)
	assert.Equal(t, []int64{2}, status.Gaps)
	assert.Equal(t, []int64{1, 3}, versionsFromResults(status.Applied))
	assert.Equal(t, []int64{2}, versionsFromResults(status.Pending))
}

func TestRunReturnsGapErrorBeforeExecutingMissingSQLMigration(t *testing.T) {
	mockContainer, mocks := newSQLOnlyMockContainer(t)

	mocks.SQL.ExpectExec(createSQLPiMigrationsTable).WillReturnResult(mocks.SQL.NewResult(1, 1))
	mocks.SQL.ExpectQuery(listSQLPiMigrations).
		WillReturnRows(mocks.SQL.NewRows([]string{"version", "method", "start_time", "duration"}).
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
	}, mockContainer)

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrMigrationGap))
	assert.False(t, executed)
}

func TestStatusPrefersSQLStateWhenRedisIsAlsoConfigured(t *testing.T) {
	mockContainer, mocks := newSQLOnlyMockContainer(t)
	mockContainer.Redis = mocks.Redis

	mocks.SQL.ExpectExec(createSQLPiMigrationsTable).WillReturnResult(mocks.SQL.NewResult(1, 1))
	mocks.SQL.ExpectQuery(listSQLPiMigrations).
		WillReturnRows(mocks.SQL.NewRows([]string{"version", "method", "start_time", "duration"}).
			AddRow(1, "UP", time.Now(), 0))

	status, err := Status(t.Context(), map[int64]Migrate{
		1: {UP: func(Datasource) error { return nil }},
		2: {UP: func(Datasource) error { return nil }},
	}, mockContainer)

	require.NoError(t, err)
	assert.Equal(t, StateSourceSQL, status.StateSource)
	assert.True(t, status.StatePrecise)
	assert.Equal(t, []int64{1}, versionsFromResults(status.Applied))
	assert.Equal(t, []int64{2}, versionsFromResults(status.Pending))
}

func TestPlanOnlyInitializesAuthoritativeStateStore(t *testing.T) {
	mockContainer, mocks := newSQLOnlyMockContainer(t)
	mockContainer.Clickhouse = mocks.Clickhouse

	mocks.SQL.ExpectExec(createSQLPiMigrationsTable).WillReturnResult(mocks.SQL.NewResult(1, 1))
	mocks.SQL.ExpectQuery(listSQLPiMigrations).
		WillReturnRows(mocks.SQL.NewRows([]string{"version", "method", "start_time", "duration"}))

	plan, err := Plan(t.Context(), map[int64]Migrate{
		1: {UP: func(Datasource) error { return nil }},
	}, mockContainer)

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

func newSQLOnlyMockContainer(t *testing.T) (*infra.Container, *infra.Mocks) {
	t.Helper()

	mockContainer, mocks := infra.NewMockContainer(t)
	mockContainer.Redis = nil
	mockContainer.Mongo = nil
	mockContainer.Cassandra = nil
	mockContainer.PubSub = nil
	mockContainer.ArangoDB = nil
	mockContainer.SurrealDB = nil
	mockContainer.DGraph = nil
	mockContainer.Elasticsearch = nil
	mockContainer.OpenTSDB = nil
	mockContainer.ScyllaDB = nil
	mockContainer.Oracle = nil
	mockContainer.Clickhouse = nil

	return mockContainer, mocks
}

func newRedisOnlyMockContainer(t *testing.T) (*infra.Container, *infra.Mocks) {
	t.Helper()

	mockContainer, mocks := infra.NewMockContainer(t)
	mockContainer.SQL = nil
	mockContainer.Mongo = nil
	mockContainer.Cassandra = nil
	mockContainer.PubSub = nil
	mockContainer.ArangoDB = nil
	mockContainer.SurrealDB = nil
	mockContainer.DGraph = nil
	mockContainer.Elasticsearch = nil
	mockContainer.OpenTSDB = nil
	mockContainer.ScyllaDB = nil
	mockContainer.Oracle = nil
	mockContainer.Clickhouse = nil

	return mockContainer, mocks
}

func versionsFromResults(results []VersionResult) []int64 {
	versions := make([]int64, 0, len(results))
	for _, result := range results {
		versions = append(versions, result.Version)
	}

	return versions
}
