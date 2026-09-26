package migration

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goRedis "github.com/redis/go-redis/v9"
	"github.com/sllt/pi/pkg/pi/datasource"
	"github.com/sllt/pi/pkg/pi/infra"
	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/stretchr/testify/require"
)

type testRedis struct{ *goRedis.Client }

func (r testRedis) HealthCheck() datasource.Health { return datasource.Health{} }

func redisContainer(t *testing.T, addr string) *infra.Container {
	t.Helper()
	r := testRedis{goRedis.NewClient(&goRedis.Options{Addr: addr})}
	t.Cleanup(func() { require.NoError(t, r.Close()) })
	return &infra.Container{Redis: r, Logger: logging.NewWriterLogger(logging.INFO, io.Discard, io.Discard)}
}

func TestMigrationRedisBackend(t *testing.T) {
	addr := os.Getenv("PI_MIGRATION_TEST_REDIS_ADDR")
	if addr == "" {
		addr = miniredis.RunT(t).Addr()
	}
	c := redisContainer(t, addr)
	// Use only the dedicated test Redis: these keys belong to this suite.
	require.NoError(t, c.Redis.Del(t.Context(), "kite_migrations", migrationRedisKey, "pi-v030-test").Err())
	defs := map[int64]Migrate{1: {UpContext: func(ctx context.Context, d Datasource) error { return d.Redis.Set(ctx, "pi-v030-test", "one", 0).Err() }}}
	result, err := Run(t.Context(), defs, c, WithLock())
	require.NoError(t, err)
	require.Equal(t, StateSourceRedis, result.StateSource)
	require.True(t, result.StatePrecise)
	result, err = Run(t.Context(), defs, c, WithLock())
	require.NoError(t, err)
	require.Len(t, result.Skipped, 1)
	cause := errors.New("panic before EXEC")
	defs[2] = Migrate{UpContext: func(ctx context.Context, d Datasource) error {
		d.Redis.Set(ctx, "pi-v030-test", "two", 0)
		panic(cause)
	}}
	_, err = Run(t.Context(), defs, c, WithLock())
	require.ErrorIs(t, err, cause)
	require.Equal(t, "one", c.Redis.Get(t.Context(), "pi-v030-test").Val())
	status, err := Status(t.Context(), defs, c)
	require.NoError(t, err)
	require.Len(t, status.Applied, 1)
	require.Len(t, status.Pending, 1)
	release, err := acquireRedisMigrationLock(t.Context(), c, time.Minute)
	require.NoError(t, err)
	require.NoError(t, c.Redis.Set(t.Context(), migrationRedisKey, "new-owner", time.Minute).Err())
	require.NoError(t, release(t.Context()))
	require.Equal(t, "new-owner", c.Redis.Get(t.Context(), migrationRedisKey).Val())
	require.NoError(t, c.Redis.Del(t.Context(), migrationRedisKey).Err())
}

func TestMigrationRedisProcesses(t *testing.T) {
	addr := os.Getenv("PI_MIGRATION_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("isolated Redis not configured")
	}
	c := redisContainer(t, addr)
	if os.Getenv("PI_MIGRATION_REDIS_CHILD") == "1" {
		_, err := acquireRedisMigrationLock(t.Context(), c, time.Minute)
		require.ErrorIs(t, err, ErrMigrationLocked)
		return
	}
	release, err := acquireRedisMigrationLock(t.Context(), c, time.Minute)
	require.NoError(t, err)
	defer func() { require.NoError(t, release(t.Context())) }()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMigrationRedisProcesses$")
	cmd.Env = append(os.Environ(), "PI_MIGRATION_REDIS_CHILD=1")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
}
