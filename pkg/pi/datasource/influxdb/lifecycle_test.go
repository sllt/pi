package influxdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLifecycle_FromEnv_HealthCheck_Close(t *testing.T) {
	_, err := FromEnv(nil)
	require.ErrorIs(t, err, errInvalidConfig)

	client, err := FromEnv(func(k string) string {
		if k == "INFLUXDB_URL" {
			return "http://127.0.0.1:1"
		}
		return ""
	})
	require.NoError(t, err)
	require.NotNil(t, client)

	health, err := client.HealthCheck(context.Background())
	require.ErrorIs(t, err, errHealthCheckFailed)
	assert.Equal(t, statusDown, health.(*Health).Status)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, client.ConnectContext(ctx), context.Canceled)

	require.NoError(t, client.Close())
	require.NoError(t, client.Close())
}
