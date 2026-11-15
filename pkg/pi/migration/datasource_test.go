package migration

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/sllt/pi/pkg/pi/testutil"
)

func Test_getMigratorDatastoreNotInitialized(t *testing.T) {
	logs := testutil.StdoutOutputForFunc(func() {
		deps := &Deps{
			Logger: logging.NewLogger(logging.INFO),
		}

		mg := Datasource{}

		require.NoError(t, mg.rollback(t.Context(), deps, transactionData{}))

		lastMigration, err := mg.getLastMigration(t.Context(), deps)
		require.NoError(t, err)
		assert.Equal(t, int64(0), lastMigration, "TEST Failed \n Last Migration is not 0")
		require.NoError(t, mg.checkAndCreateMigrationTable(t.Context(), deps), "TEST Failed")
		data, err := mg.beginTransaction(t.Context(), deps)
		require.NoError(t, err)
		assert.Equal(t, transactionData{}, data, "TEST Failed")
		require.NoError(t, mg.commitMigration(t.Context(), deps, transactionData{}), "TEST Failed")
	})

	assert.Contains(t, logs, "Migration 0 ran successfully", "TEST Failed")
}
