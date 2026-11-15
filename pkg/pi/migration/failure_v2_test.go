package migration

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	piSQL "github.com/sllt/pi/pkg/pi/datasource/sql"
	"github.com/sllt/pi/pkg/pi/logging"
)

func TestMigrationSQLFailureStages(t *testing.T) {
	for _, stage := range []string{"begin", "up", "commit", "rollback"} {
		t.Run(stage, func(t *testing.T) {
			db, mock, _ := piSQL.NewSQLMocksWithConfig(t, &piSQL.DBConfig{Dialect: "mysql"})
			deps := Deps{SQL: db, Logger: logging.NewWriterLogger(logging.INFO, io.Discard, io.Discard)}
			cause := errors.New(stage + " failure")
			rollbackCause := errors.New("cleanup failure")
			mock.ExpectExec(createSQLPiMigrationsTable).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(listSQLPiMigrations).WillReturnRows(sqlmock.NewRows([]string{"version", "method", "start_time", "duration"}))
			begin := mock.ExpectBegin()
			if stage == "begin" {
				begin.WillReturnError(cause)
			}
			if stage == "commit" {
				mock.ExpectExec(insertPiMigrationRowMySQL).WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit().WillReturnError(cause)
			}
			if stage == "up" {
				mock.ExpectRollback()
			}
			if stage == "rollback" {
				mock.ExpectRollback().WillReturnError(rollbackCause)
			}
			result, err := Run(t.Context(), map[int64]Migrate{12: {Name: "failure", UP: func(Datasource) error {
				if stage == "up" || stage == "rollback" {
					return cause
				}
				return nil
			}}}, deps)
			require.ErrorIs(t, err, cause)
			require.ErrorIs(t, err, ErrMigrationFailed)
			if stage == "rollback" {
				require.ErrorIs(t, err, rollbackCause)
			}
			var ve *VersionError
			require.ErrorAs(t, err, &ve)
			require.Equal(t, int64(12), ve.Version)
			want := stage
			if stage == "rollback" {
				want = "up"
			}
			require.Equal(t, want, ve.Op)
			require.NotNil(t, result.Failed)
			require.Empty(t, result.Applied)
			require.False(t, result.FinishedAt.IsZero())
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestMigrationNestedBeginRollsBackSQL(t *testing.T) {
	db, mock, _ := piSQL.NewSQLMocksWithConfig(t, &piSQL.DBConfig{Dialect: "mysql"})
	deps := &Deps{SQL: db, Logger: logging.NewWriterLogger(logging.INFO, io.Discard, io.Discard)}
	cause := errors.New("nested begin")
	nested := &fakeMigrator{
		beginFn: func(context.Context, *Deps) (transactionData, error) {
			return transactionData{}, cause
		},
	}
	mock.ExpectBegin()
	mock.ExpectRollback()
	_, err := (sqlMigrator{migrator: nested}).beginTransaction(t.Context(), deps)
	require.ErrorIs(t, err, cause)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMigrationRollbackSurvivesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	nested := &fakeMigrator{
		rollbackFn: func(ctx context.Context, _ *Deps, _ transactionData) error {
			require.NoError(t, ctx.Err())
			_, ok := ctx.Deadline()
			require.True(t, ok)
			return nil
		},
	}
	require.NoError(t, rollbackMigration(ctx, nested, nil, transactionData{}))
}

func TestMigrationLockDoesNotMaskBackendFailure(t *testing.T) {
	deps, _, mock := newTestSQLDeps(t, "mysql")
	cause := errors.New("insert permission denied")
	mock.ExpectExec(createSQLMigrationLocksTable).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(insertSQLMigrationLockMySQL).WillReturnError(cause)
	_, err := acquireMigrationLock(t.Context(), deps, applyOptions([]Option{WithLock()}))
	require.ErrorIs(t, err, ErrMigrationLockUnavailable)
	require.ErrorIs(t, err, cause)
	require.NotErrorIs(t, err, ErrMigrationLocked)
}
