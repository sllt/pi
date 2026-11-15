package migration

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errCreateTable = errors.New("create table error")

func TestQuery(t *testing.T) {
	t.Run("successful query", func(t *testing.T) {
		var id int
		var name string

		expectedResult := []struct {
			id   int
			name string
		}{
			{1, "Alex"},
			{2, "John"},
		}

		_, db, mock := newTestSQLDeps(t, "mysql")

		expectedRows := mock.NewRows([]string{"id", "name"}).
			AddRow(expectedResult[0].id, expectedResult[0].name).
			AddRow(expectedResult[1].id, expectedResult[1].name)

		mock.ExpectQuery("SELECT * FROM users").WithoutArgs().WillReturnRows(expectedRows)

		rows, err := db.Query("SELECT * FROM users")
		require.NoError(t, err)
		defer rows.Close()

		i := 0
		for rows.Next() {
			require.NoError(t, rows.Err())
			err = rows.Scan(&id, &name)
			require.NoError(t, err)
			require.Equal(t, expectedResult[i].id, id)
			require.Equal(t, expectedResult[i].name, name)
			i++
		}
	})

	t.Run("query error", func(t *testing.T) {
		var id int
		var name string

		_, db, mock := newTestSQLDeps(t, "mysql")
		expectedErr := sql.ErrNoRows
		expectedRows := mock.NewRows([]string{"id", "name"})
		mock.ExpectQuery("SELECT * FROM unknown_table").WithoutArgs().WillReturnRows(expectedRows)

		rows, err := db.Query("SELECT * FROM unknown_table")
		require.NoError(t, err)
		defer rows.Close()

		for rows.Next() {
			require.NoError(t, rows.Err())
			err = rows.Scan(&id, &name)
			require.Error(t, err)
			require.Equal(t, expectedErr, err)
		}
	})
}

func TestQueryRow(t *testing.T) {
	t.Run("successful query row", func(t *testing.T) {
		var name string
		var id int

		_, db, mock := newTestSQLDeps(t, "mysql")
		expectedRows := mock.NewRows([]string{"id", "name"}).AddRow(1, "Alex")
		mock.ExpectQuery("SELECT * FROM users WHERE id = ?").WithArgs(1).WillReturnRows(expectedRows)

		err := db.QueryRow("SELECT * FROM users WHERE id = ?", 1).Scan(&id, &name)
		require.NoError(t, err)
		require.Equal(t, 1, id)
		require.Equal(t, "Alex", name)
	})
}

func TestQueryRowContext(t *testing.T) {
	ctx := t.Context()

	t.Run("successful query row context", func(t *testing.T) {
		var id int
		var name string

		_, db, mock := newTestSQLDeps(t, "mysql")
		expectedRows := mock.NewRows([]string{"id", "name"}).AddRow(1, "Alex")
		mock.ExpectQuery("SELECT * FROM users WHERE id = ?").WithArgs(1).WillReturnRows(expectedRows)

		err := db.QueryRowContext(ctx, "SELECT * FROM users WHERE id = ?", 1).Scan(&id, &name)
		require.NoError(t, err)
		require.Equal(t, 1, id)
		require.Equal(t, "Alex", name)
	})
}

func TestExec(t *testing.T) {
	t.Run("successful exec", func(t *testing.T) {
		_, db, mock := newTestSQLDeps(t, "mysql")
		expectedResult := sqlmock.NewResult(10, 1)
		mock.ExpectExec("DELETE FROM users WHERE id = ?").WithArgs(1).WillReturnResult(expectedResult)

		result, err := db.Exec("DELETE FROM users WHERE id = ?", 1)
		require.NoError(t, err)

		expectedLastInserted, err := expectedResult.LastInsertId()
		require.NoError(t, err)
		resultLastInserted, err := result.LastInsertId()
		require.NoError(t, err)
		expectedRowsAffected, err := expectedResult.RowsAffected()
		require.NoError(t, err)
		resultRowsAffected, err := result.RowsAffected()
		require.NoError(t, err)

		require.Equal(t, expectedLastInserted, resultLastInserted)
		require.Equal(t, expectedRowsAffected, resultRowsAffected)
	})

	t.Run("exec error", func(t *testing.T) {
		_, db, mock := newTestSQLDeps(t, "mysql")
		expectedErr := sql.ErrNoRows
		mock.ExpectExec("UPDATE unknown_table SET name = ?").WithArgs("John").WillReturnError(expectedErr)

		_, err := db.Exec("UPDATE unknown_table SET name = ?", "John")
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
	})
}

func TestExecContext(t *testing.T) {
	ctx := t.Context()

	t.Run("successful exec context", func(t *testing.T) {
		_, db, mock := newTestSQLDeps(t, "mysql")
		expectedResult := sqlmock.NewResult(10, 1)
		mock.ExpectExec("DELETE FROM users WHERE id = ?").WithArgs(1).WillReturnResult(expectedResult)

		result, err := db.ExecContext(ctx, "DELETE FROM users WHERE id = ?", 1)
		require.NoError(t, err)

		expectedLastInserted, err := expectedResult.LastInsertId()
		require.NoError(t, err)
		resultLastInserted, err := result.LastInsertId()
		require.NoError(t, err)
		expectedRowsAffected, err := expectedResult.RowsAffected()
		require.NoError(t, err)
		resultRowsAffected, err := result.RowsAffected()
		require.NoError(t, err)

		require.Equal(t, expectedLastInserted, resultLastInserted)
		require.Equal(t, expectedRowsAffected, resultRowsAffected)
	})
}

func TestCheckAndCreateMigrationTableSuccess(t *testing.T) {
	deps, db, mock := newTestSQLDeps(t, "mysql")
	called := false
	next := &fakeMigrator{
		checkTableFn: func(_ context.Context, got *Deps) error {
			assert.Same(t, deps, got)
			called = true
			return nil
		},
	}

	mock.ExpectExec(createSQLPiMigrationsTable).WillReturnResult(sqlmock.NewResult(1, 1))

	m := sqlMigrator{
		SQL:      db,
		migrator: next,
	}

	err := m.checkAndCreateMigrationTable(t.Context(), deps)
	require.NoError(t, err)
	require.True(t, called)
}

func TestCheckAndCreateMigrationTableExecError(t *testing.T) {
	deps, db, mock := newTestSQLDeps(t, "mysql")
	expectedErr := sql.ErrNoRows
	mock.ExpectExec(createSQLPiMigrationsTable).WillReturnError(expectedErr)

	m := sqlMigrator{
		SQL:      db,
		migrator: &fakeMigrator{},
	}

	err := m.checkAndCreateMigrationTable(t.Context(), deps)
	require.ErrorIs(t, err, expectedErr)
}

func TestBeginTransactionSuccess(t *testing.T) {
	deps, db, mock := newTestSQLDeps(t, "mysql")
	mock.ExpectBegin()

	m := sqlMigrator{
		SQL:      db,
		migrator: &fakeMigrator{},
	}

	data, err := m.beginTransaction(t.Context(), deps)
	require.NoError(t, err)
	require.NotNil(t, data.SQLTx.Tx)
}

var errBeginTx = errors.New("failed to begin transaction")

func TestBeginTransactionDBError(t *testing.T) {
	deps, db, mock := newTestSQLDeps(t, "mysql")
	mock.ExpectBegin().WillReturnError(errBeginTx)

	m := sqlMigrator{
		SQL:      db,
		migrator: &fakeMigrator{},
	}

	data, err := m.beginTransaction(t.Context(), deps)
	require.Error(t, err)
	require.Nil(t, data.SQLTx)
}

func TestRollbackNoTransaction(t *testing.T) {
	deps, _, _ := newTestSQLDeps(t, "mysql")
	m := sqlMigrator{}
	require.NoError(t, m.rollback(t.Context(), deps, transactionData{}))
}

func TestApply(t *testing.T) {
	_, db, _ := newTestSQLDeps(t, "mysql")
	next := &fakeMigrator{}

	ds := &sqlDS{SQL: db}
	result := ds.apply(next)

	sqlMig, ok := result.(sqlMigrator)
	require.True(t, ok)
	require.Equal(t, db, sqlMig.SQL)
	require.Equal(t, next, sqlMig.migrator)
}

func TestGetLastMigration_UseMigratorFallback(t *testing.T) {
	deps, db, mock := newTestSQLDeps(t, "mysql")
	mock.ExpectQuery(getLastSQLPiMigration).
		WillReturnRows(mock.NewRows([]string{"version"}).AddRow(2))

	next := &fakeMigrator{
		lastFn: func(context.Context, *Deps) (int64, error) {
			return 5, nil
		},
	}

	m := sqlMigrator{SQL: db, migrator: next}
	last, err := m.getLastMigration(t.Context(), deps)
	require.NoError(t, err)
	require.Equal(t, int64(5), last)
}

func TestGetLastMigration_MigratorReturnsLesser(t *testing.T) {
	deps, db, mock := newTestSQLDeps(t, "mysql")
	mock.ExpectQuery(getLastSQLPiMigration).
		WillReturnRows(mock.NewRows([]string{"version"}).AddRow(7))

	next := &fakeMigrator{
		lastFn: func(context.Context, *Deps) (int64, error) {
			return 5, nil
		},
	}

	m := sqlMigrator{SQL: db, migrator: next}
	last, err := m.getLastMigration(t.Context(), deps)
	require.NoError(t, err)
	require.Equal(t, int64(7), last)
}

func TestBeginTransaction_ReplaceSQLTx(t *testing.T) {
	deps, db, mock := newTestSQLDeps(t, "mysql")
	mock.ExpectBegin()

	next := &fakeMigrator{
		beginFn: func(context.Context, *Deps) (transactionData, error) {
			return transactionData{MigrationNumber: 123}, nil
		},
	}

	m := sqlMigrator{SQL: db, migrator: next}
	data, err := m.beginTransaction(t.Context(), deps)
	require.NoError(t, err)
	require.NotNil(t, data.SQLTx)
	require.Equal(t, int64(123), data.MigrationNumber)
}

func TestCheckAndCreateMigrationTable_ErrorCreatingTable(t *testing.T) {
	deps, _, mock := newTestSQLDeps(t, "mysql")
	mock.ExpectExec(createSQLPiMigrationsTable).WillReturnError(errCreateTable)

	m := sqlMigrator{}
	err := m.checkAndCreateMigrationTable(t.Context(), deps)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create table error")
}
