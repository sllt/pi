package surrealdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLifecycle_FromEnv_HealthCheck_Close(t *testing.T) {
	_, err := FromEnv(nil)
	require.ErrorIs(t, err, errInvalidConfig)

	_, err = FromEnv(func(k string) string {
		switch k {
		case "SURREALDB_HOST":
			return "127.0.0.1"
		case "SURREALDB_NAMESPACE":
			return "ns"
		case "SURREALDB_DATABASE":
			return "db"
		case "SURREALDB_USERNAME":
			return "root"
		default:
			return ""
		}
	})
	require.ErrorIs(t, err, errInvalidCredentialsConfig)

	client, err := FromEnv(func(k string) string {
		switch k {
		case "SURREALDB_HOST":
			return "127.0.0.1"
		case "SURREALDB_NAMESPACE":
			return "ns"
		case "SURREALDB_DATABASE":
			return "db"
		default:
			return ""
		}
	})
	require.NoError(t, err)
	require.NotNil(t, client)

	health, err := client.HealthCheck(context.Background())
	require.ErrorIs(t, err, errNotConnected)
	assert.Equal(t, "DOWN", health.(*Health).Status)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, client.ConnectContext(ctx), context.Canceled)

	require.NoError(t, client.Shutdown(context.Background()))
	require.NoError(t, client.Close())
}
