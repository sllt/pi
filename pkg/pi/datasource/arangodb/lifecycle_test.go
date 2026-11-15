package arangodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLifecycle_FromEnv_HealthCheck_Close(t *testing.T) {
	_, err := FromEnv(nil)
	require.ErrorIs(t, err, errMissingField)

	client, err := FromEnv(func(k string) string {
		switch k {
		case "ARANGODB_HOST":
			return "127.0.0.1"
		case "ARANGODB_PORT":
			return "1"
		case "ARANGODB_USER":
			return "root"
		case "ARANGODB_PASSWORD":
			return "secret"
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
