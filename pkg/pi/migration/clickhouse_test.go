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

func clickHouseSetup(t *testing.T) (migrator, *MockClickhouse, *infra.Container) {
	t.Helper()

	ctrl := gomock.NewController(t)

	mockContainer, _ := infra.NewMockContainer(t)

	mockClickhouse := NewMockClickhouse(ctrl)

	ds := Datasource{Clickhouse: mockClickhouse}

	ch := clickHouseDS{Clickhouse: mockClickhouse}
	mg := ch.apply(&ds)

	mockContainer.Clickhouse = mockClickhouse

	return mg, mockClickhouse, mockContainer
}

func Test_ClickHouseCheckAndCreateMigrationTable(t *testing.T) {
	mg, mockClickhouse, mockContainer := clickHouseSetup(t)

	testCases := []struct {
		desc string
		err  error
	}{
		{"no error", nil},
		{"connection failed", sql.ErrConnDone},
	}

	for i, tc := range testCases {
		mockClickhouse.EXPECT().Exec(gomock.Any(), CheckAndCreateChMigrationTable).Return(tc.err)

		err := mg.checkAndCreateMigrationTable(t.Context(), mockContainer)

		assert.Equal(t, tc.err, err, "TEST[%v]\n %v Failed! ", i, tc.desc)
	}
}

func Test_ClickHouseGetLastMigration(t *testing.T) {
	mg, mockClickhouse, mockContainer := clickHouseSetup(t)

	testCases := []struct {
		desc string
		err  error
		resp int64
	}{
		{"no error", nil, 0},
		{"connection failed", sql.ErrConnDone, -1},
	}

	for i, tc := range testCases {
		mockClickhouse.EXPECT().Select(gomock.Any(), gomock.Any(), getLastChPiMigration).Return(tc.err)

		resp, err := mg.getLastMigration(t.Context(), mockContainer)

		assert.Equal(t, tc.resp, resp, "TEST[%v]\n %v Failed! ", i, tc.desc)

		if tc.err != nil {
			assert.ErrorContains(t, err, tc.err.Error(), "TEST[%v]\n %v Failed! ", i, tc.desc)
		} else {
			assert.NoError(t, err, "TEST[%v]\n %v Failed! ", i, tc.desc)
		}
	}
}

func Test_ClickHouseCommitMigration(t *testing.T) {
	mg, mockClickhouse, mockContainer := clickHouseSetup(t)

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
		mockClickhouse.EXPECT().Exec(gomock.Any(), insertChPiMigrationRow, td.MigrationNumber,
			"UP", td.StartTime, gomock.Any()).Return(tc.err)

		err := mg.commitMigration(t.Context(), mockContainer, td)

		assert.Equal(t, tc.err, err, "TEST[%v]\n %v Failed! ", i, tc.desc)
	}
}

func Test_ClickHouseBeginTransaction(t *testing.T) {
	logs := testutil.StdoutOutputForFunc(func() {
		mg, _, mockContainer := clickHouseSetup(t)
		mg.beginTransaction(t.Context(), mockContainer)
	})

	assert.Contains(t, logs, "Clickhouse Migrator begin successfully")
}
