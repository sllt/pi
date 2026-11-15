package migration

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/sllt/pi/pkg/pi/testutil"
)

func TestMigration_InvalidKeys(t *testing.T) {
	logs := testutil.StderrOutputForFunc(func() {
		deps := Deps{Logger: logging.NewLogger(logging.DEBUG)}

		_, _ = Run(t.Context(), map[int64]Migrate{
			1: {UP: nil},
		}, deps)
	})

	assert.Contains(t, logs, "migration run failed! UP not defined for the following keys: [1]")
}

func TestMigration_NoDatasource(t *testing.T) {
	logs := testutil.StderrOutputForFunc(func() {
		deps := Deps{Logger: logging.NewLogger(logging.DEBUG)}

		_, _ = Run(t.Context(), map[int64]Migrate{
			1: {UP: func(d Datasource) error {
				_, err := d.SQL.Exec("CREATE table customer(id int not null);")
				return err
			}},
		}, deps)
	})

	assert.Contains(t, logs, "no migrations are running")
}

func Test_getMigratorDBInitialisation(t *testing.T) {
	deps, _, _ := newTestSQLDeps(t, "mysql")
	deps.Redis = &fakeRedis{}

	datasource, _, isInitialized := getMigrator(deps)

	assert.NotNil(t, datasource.SQL, "TEST Failed \nSQL not initialized, but should have been initialized")
	assert.NotNil(t, datasource.Redis, "TEST Failed \nRedis not initialized, but should have been initialized")
	assert.True(t, isInitialized, "TEST Failed \nNo datastores are Initialized")
}

func TestMigrationRunPubSubSuccess(t *testing.T) {
	logs := testutil.StdoutOutputForFunc(func() {
		ps := &fakePubSub{}
		deps := Deps{
			Logger: logging.NewMockLogger(logging.DEBUG),
			PubSub: ps,
		}

		migrationMap := map[int64]Migrate{
			1: {UP: func(d Datasource) error {
				if err := d.PubSub.CreateTopic(t.Context(), "orders"); err != nil {
					return err
				}
				d.Logger.Infof("PubSub Migration Ran Successfully")
				return nil
			}},
		}

		_, _ = Run(t.Context(), migrationMap, deps)
	})

	assert.Contains(t, logs, "Migration 1 ran successfully")
	assert.Contains(t, logs, "PubSub Migration Ran Successfully")
}

func TestMigrationRunPubSubMigrationFailure(t *testing.T) {
	logs := testutil.StderrOutputForFunc(func() {
		ps := &fakePubSub{
			createTopicFn: func(_ context.Context, name string) error {
				if name == "orders" {
					return sql.ErrConnDone
				}
				return nil
			},
		}
		deps := Deps{
			Logger: logging.NewMockLogger(logging.DEBUG),
			PubSub: ps,
		}

		migrationMap := map[int64]Migrate{
			1: {UP: func(d Datasource) error {
				return d.PubSub.CreateTopic(t.Context(), "orders")
			}},
		}

		_, _ = Run(t.Context(), migrationMap, deps)
	})

	assert.Contains(t, logs, "failed to run migration : [1], err: sql: connection is already closed")
}

func TestMigrationRunPubSubCurrentMigrationEqualLastMigration(t *testing.T) {
	logs := testutil.StdoutOutputForFunc(func() {
		ps := &fakePubSub{
			queryFn: func(context.Context, string, ...any) ([]byte, error) {
				return []byte(`{"version":1,"method":"UP","start_time":1625000000000,"duration":100}`), nil
			},
		}
		deps := Deps{
			Logger: logging.NewMockLogger(logging.DEBUG),
			PubSub: ps,
		}

		migrationMap := map[int64]Migrate{
			1: {UP: func(d Datasource) error {
				return d.PubSub.CreateTopic(t.Context(), "orders")
			}},
		}

		_, _ = Run(t.Context(), migrationMap, deps)
	})

	assert.Contains(t, logs, "skipping migration 1")
}

func TestMigrationRunPubSubCommitError(t *testing.T) {
	logs := testutil.StderrOutputForFunc(func() {
		ps := &fakePubSub{
			publishFn: func(context.Context, string, []byte) error {
				return sql.ErrConnDone
			},
		}
		deps := Deps{
			Logger: logging.NewMockLogger(logging.DEBUG),
			PubSub: ps,
		}

		migrationMap := map[int64]Migrate{
			1: {UP: func(Datasource) error {
				return nil
			}},
		}

		_, _ = Run(t.Context(), migrationMap, deps)
	})

	assert.Contains(t, logs, "failed to commit migration, err: sql: connection is already closed")
}
