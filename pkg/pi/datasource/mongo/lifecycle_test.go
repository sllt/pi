package mongo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLifecycle_FromEnv_HealthCheck_Close(t *testing.T) {
	_, err := FromEnv(nil)
	require.Error(t, err)

	_, err = FromEnv(func(string) string { return "" })
	require.ErrorIs(t, err, errMissingField)

	client, err := FromEnv(func(k string) string {
		switch k {
		case "MONGO_HOST":
			return "127.0.0.1"
		case "MONGO_DATABASE":
			return "appdb"
		case "MONGO_CONNECTION_TIMEOUT":
			return "200ms"
		default:
			return ""
		}
	})
	require.NoError(t, err)
	require.NotNil(t, client)

	health, err := client.HealthCheck(context.Background())
	require.ErrorIs(t, err, errStatusDown)
	assert.Equal(t, "DOWN", health.(*Health).Status)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, client.ConnectContext(ctx), context.Canceled)

	require.NoError(t, client.Close())
	require.NoError(t, client.Close())
}
