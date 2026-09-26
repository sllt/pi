package migration

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/sllt/pi/pkg/pi/infra"
	"github.com/sllt/pi/pkg/pi/testutil"
)

func cassandraSetup(t *testing.T) (migrator, *infra.MockCassandraWithContext, *infra.Container) {
	t.Helper()

	mockContainer, mocks := infra.NewMockContainer(t)

	mockCassandra := mocks.Cassandra

	ds := Datasource{Cassandra: mockContainer.Cassandra}

	cassandraDB := cassandraDS{CassandraWithContext: mockCassandra}
	migratorWithCassandra := cassandraDB.apply(&ds)

	mockContainer.Cassandra = mockCassandra

	return migratorWithCassandra, mockCassandra, mockContainer
}

func Test_CassandraCheckAndCreateMigrationTable(t *testing.T) {
	migratorWithCassandra, mockCassandra, mockContainer := cassandraSetup(t)

	testCases := []struct {
		desc string
		err  error
	}{
		{"no error", nil},
		{"connection failed", sql.ErrConnDone},
	}

	for i, tc := range testCases {
		mockCassandra.EXPECT().ExecWithCtx(gomock.Any(), checkAndCreateCassandraMigrationTable).Return(tc.err)

		err := migratorWithCassandra.checkAndCreateMigrationTable(t.Context(), mockContainer)

		assert.Equal(t, tc.err, err, "TEST[%v]\n %v Failed! ", i, tc.desc)
	}
}

func Test_CassandraGetLastMigration(t *testing.T) {
	migratorWithCassandra, mockCassandra, mockContainer := cassandraSetup(t)

	testCases := []struct {
		desc string
		err  error
		resp int64
	}{
		{"no error", nil, 0},
		{"connection failed", sql.ErrConnDone, -1},
	}

	var lastMigration []int64

	for i, tc := range testCases {
		mockCassandra.EXPECT().QueryWithCtx(gomock.Any(), &lastMigration, getLastCassandraPiMigration).Return(tc.err)

		resp, err := migratorWithCassandra.getLastMigration(t.Context(), mockContainer)

		assert.Equal(t, tc.resp, resp, "TEST[%v]\n %v Failed! ", i, tc.desc)

		if tc.err != nil {
			assert.ErrorContains(t, err, tc.err.Error(), "TEST[%v]\n %v Failed! ", i, tc.desc)
		} else {
			assert.NoError(t, err, "TEST[%v]\n %v Failed! ", i, tc.desc)
		}
	}
}

func Test_CassandraCommitMigration(t *testing.T) {
	migratorWithCassandra, mockCassandra, mockContainer := cassandraSetup(t)

	testCases := []struct {
		desc string
		err  error
	}{
		{"no error", nil},
		{"connection failed", sql.ErrConnDone},
	}

	timeNow := time.Now()

	td := transactionData{
		StartTime:       timeNow,
		MigrationNumber: 10,
	}

	for i, tc := range testCases {
		mockCassandra.EXPECT().ExecWithCtx(gomock.Any(), insertCassandraPiMigrationRow, td.MigrationNumber,
			"UP", td.StartTime, gomock.Any()).Return(tc.err)

		err := migratorWithCassandra.commitMigration(t.Context(), mockContainer, td)

		assert.Equal(t, tc.err, err, "TEST[%v]\n %v Failed! ", i, tc.desc)
	}
}

func Test_CassandraBeginTransaction(t *testing.T) {
	logs := testutil.StdoutOutputForFunc(func() {
		migratorWithCassandra, _, mockContainer := cassandraSetup(t)
		migratorWithCassandra.beginTransaction(t.Context(), mockContainer)
	})

	assert.Contains(t, logs, "cassandra migrator begin successfully")
}
