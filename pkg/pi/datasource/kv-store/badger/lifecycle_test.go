package badger

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLifecycle_FromEnv_ConnectContext_Close(t *testing.T) {
	_, err := FromEnv(nil)
	require.ErrorIs(t, err, errInvalidConfig)

	dir := t.TempDir()
	client, err := FromEnv(func(k string) string {
		if k == "BADGER_DIR_PATH" {
			return dir
		}
		return ""
	})
	require.NoError(t, err)
	require.NotNil(t, client)

	health, err := client.HealthCheck(context.Background())
	require.ErrorIs(t, err, errStatusDown)
	assert.Equal(t, "DOWN", health.(*Health).Status)

	require.NoError(t, client.ConnectContext(context.Background()))
	health, err = client.HealthCheck(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "UP", health.(*Health).Status)

	require.NoError(t, client.Close())
	require.NoError(t, client.Close())

	health, err = client.HealthCheck(context.Background())
	require.ErrorIs(t, err, errStatusDown)
	assert.Equal(t, "DOWN", health.(*Health).Status)
}
