package migration

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sllt/pi/pkg/pi/infra"
	"github.com/sllt/pi/pkg/pi/testutil"
)

func Test_getMigratorDatastoreNotInitialized(t *testing.T) {
	logs := testutil.StdoutOutputForFunc(func() {
		mockContainer, _ := infra.NewMockContainer(t)
		mockContainer.SQL = nil
		mockContainer.Redis = nil

		mg := Datasource{}

		mg.rollback(t.Context(), mockContainer, transactionData{})

		lastMigration, err := mg.getLastMigration(t.Context(), mockContainer)
		require.NoError(t, err)
		assert.Equal(t, int64(0), lastMigration, "TEST Failed \n Last Migration is not 0")
		require.NoError(t, mg.checkAndCreateMigrationTable(t.Context(), mockContainer), "TEST Failed")
		data, err := mg.beginTransaction(t.Context(), mockContainer)
		require.NoError(t, err)
		assert.Equal(t, transactionData{}, data, "TEST Failed")
		require.NoError(t, mg.commitMigration(t.Context(), mockContainer, transactionData{}), "TEST Failed")
	})

	assert.Contains(t, logs, "Migration 0 ran successfully", "TEST Failed")
}
