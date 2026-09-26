package migration

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/sllt/pi/pkg/pi/infra"
	"github.com/sllt/pi/pkg/pi/logging"
)

func oracleSetup(t *testing.T) (migrator, *infra.MockOracleDB, *infra.Container) {
	t.Helper()

	mockContainer, mocks := infra.NewMockContainer(t)

	mockOracle := mocks.Oracle

	ds := Datasource{Oracle: mockOracle}

	oracleDB := oracleDS{Oracle: mockOracle}
	migrationWithOracle := oracleDB.apply(&ds)

	mockContainer.Oracle = mockOracle

	return migrationWithOracle, mockOracle, mockContainer
}

func Test_OracleCheckAndCreateMigrationTable(t *testing.T) {
	mg, mockOracle, mockContainer := oracleSetup(t)

	testCases := []struct {
		desc string
		err  error
	}{
		{"no error", nil},
		{"connection failed", sql.ErrConnDone},
	}

	for i, tc := range testCases {
		mockOracle.EXPECT().Exec(gomock.Any(), checkAndCreateOracleMigrationTable).Return(tc.err)
		err := mg.checkAndCreateMigrationTable(t.Context(), mockContainer)
		assert.Equal(t, tc.err, err, "TEST[%d]: %s failed", i, tc.desc)
	}
}

func Test_OracleGetLastMigration(t *testing.T) {
	mg, mockOracle, mockContainer := oracleSetup(t)

	testCases := []struct {
		desc string
		err  error
		resp int64
	}{
		{"no error", nil, 0},
		{"connection failed", sql.ErrConnDone, -1},
	}

	for i, tc := range testCases {
		mockOracle.EXPECT().Select(gomock.Any(), gomock.Any(), getLastOraclePiMigration).Return(tc.err)

		resp, err := mg.getLastMigration(t.Context(), mockContainer)
		assert.Equal(t, tc.resp, resp, "TEST[%d]: %s failed", i, tc.desc)

		if tc.err != nil {
			assert.ErrorContains(t, err, tc.err.Error(), "TEST[%d]: %s failed", i, tc.desc)
		} else {
			assert.NoError(t, err, "TEST[%d]: %s failed", i, tc.desc)
		}
	}
}

func Test_OracleCommitMigration(t *testing.T) {
	mg, _, mockContainer := oracleSetup(t)
	ctrl := gomock.NewController(t)
	timeNow := time.Now()

	// Success case
	mockTxSuccess := infra.NewMockOracleTx(ctrl)
	tdSuccess := transactionData{
		StartTime:       timeNow,
		MigrationNumber: 10,
		OracleTx:        mockTxSuccess,
	}

	mockTxSuccess.EXPECT().
		ExecContext(gomock.Any(), insertOraclePiMigrationRow,
			tdSuccess.MigrationNumber, "UP", tdSuccess.StartTime, gomock.Any()).
		Return(nil)

	mockTxSuccess.EXPECT().Commit().Return(nil)

	err := mg.commitMigration(t.Context(), mockContainer, tdSuccess)
	require.NoError(t, err, "Success case failed")

	// Error case
	mockTxError := infra.NewMockOracleTx(ctrl)
	tdError := transactionData{
		StartTime:       timeNow,
		MigrationNumber: 10,
		OracleTx:        mockTxError,
	}

	mockTxError.EXPECT().
		ExecContext(gomock.Any(), insertOraclePiMigrationRow,
			tdError.MigrationNumber, "UP", tdError.StartTime, gomock.Any()).
		Return(sql.ErrConnDone)

	mockTxError.EXPECT().Rollback().Return(nil).AnyTimes()

	err = mg.commitMigration(t.Context(), mockContainer, tdError)
	assert.Equal(t, sql.ErrConnDone, err, "Error case failed")
}

func TestOracleMigration_RunMigrationSuccess(t *testing.T) {
	mockOracle, mockContainer := initializeOracleRunMocks(t)
	ctrl := gomock.NewController(t)

	mockTx := infra.NewMockOracleTx(ctrl)

	ds := Datasource{Oracle: mockOracle}
	od := oracleDS{Oracle: mockOracle}
	_ = od.apply(&ds)

	migrationMap := map[int64]Migrate{
		1: {UP: func(d Datasource) error {
			return d.Oracle.Exec(context.Background(), "CREATE TABLE test (id INT)")
		}},
	}

	// Set up mock expectations in the correct order
	mockOracle.EXPECT().Exec(gomock.Any(), checkAndCreateOracleMigrationTable).Return(nil)
	mockOracle.EXPECT().Select(gomock.Any(), gomock.Any(), getLastOraclePiMigration).Return(nil)
	mockOracle.EXPECT().Begin().Return(mockTx, nil)

	// Migration execution through transaction wrapper
	mockTx.EXPECT().ExecContext(gomock.Any(), "CREATE TABLE test (id INT)").Return(nil)

	// Migration record insertion and commit
	mockTx.EXPECT().ExecContext(gomock.Any(), insertOraclePiMigrationRow,
		int64(1), "UP", gomock.Any(), gomock.Any()).Return(nil)
	mockTx.EXPECT().Commit().Return(nil)

	Run(t.Context(), migrationMap, mockContainer)
}

func TestOracleMigration_FailCreateMigrationTable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockContainer, mocks := infra.NewMockContainer(t)
	mockOracle := mocks.Oracle
	mockContainer.Oracle = mockOracle

	ds := Datasource{Oracle: mockOracle}
	od := oracleDS{Oracle: mockOracle}
	mg := od.apply(&ds)

	mockOracle.EXPECT().Exec(gomock.Any(), checkAndCreateOracleMigrationTable).Return(sql.ErrConnDone)

	err := mg.checkAndCreateMigrationTable(t.Context(), mockContainer)
	assert.Equal(t, sql.ErrConnDone, err)
}

func TestOracleMigration_GetLastMigration_ReturnsZeroOnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockContainer, mocks := infra.NewMockContainer(t)
	mockOracle := mocks.Oracle
	mockContainer.Oracle = mockOracle

	ds := Datasource{Oracle: mockOracle}
	od := oracleDS{Oracle: mockOracle}
	mg := od.apply(&ds)

	mockOracle.EXPECT().Select(gomock.Any(), gomock.Any(), getLastOraclePiMigration).Return(sql.ErrConnDone)

	lastMigration, err := mg.getLastMigration(t.Context(), mockContainer)
	assert.Equal(t, int64(-1), lastMigration)
	assert.ErrorContains(t, err, sql.ErrConnDone.Error())
}

func initializeOracleRunMocks(t *testing.T) (*infra.MockOracleDB, *infra.Container) {
	t.Helper()

	mockContainer, mocks := infra.NewMockContainer(t)
	mockOracle := mocks.Oracle

	// Disable all other datasources by setting to nil.
	mockContainer.SQL = nil
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
	mockContainer.Clickhouse = nil

	// Initialize Oracle mock and Logger.
	mockContainer.Oracle = mockOracle
	mockContainer.Logger = logging.NewMockLogger(logging.DEBUG)

	return mockOracle, mockContainer
}
