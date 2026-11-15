package migration

import (
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	goRedis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sllt/pi/pkg/pi/logging"
)

func TestRedis_Get(t *testing.T) {
	mockCmd := &fakeRedis{
		getFn: func(_ context.Context, key string) *goRedis.StringCmd {
			assert.Equal(t, "test_key", key)
			return &goRedis.StringCmd{}
		},
	}

	r := redisDS{mockCmd}
	_, err := r.Get(t.Context(), "test_key").Result()
	require.NoError(t, err)
}

func TestRedis_Set(t *testing.T) {
	mockCmd := &fakeRedis{
		setFn: func(_ context.Context, key string, value any, expiration time.Duration) *goRedis.StatusCmd {
			assert.Equal(t, "test_key", key)
			assert.Equal(t, "test_value", value)
			assert.Equal(t, time.Duration(0), expiration)
			return &goRedis.StatusCmd{}
		},
	}

	r := redisDS{mockCmd}
	_, err := r.Set(t.Context(), "test_key", "test_value", 0).Result()
	require.NoError(t, err)
}

func TestRedis_Del(t *testing.T) {
	mockCmd := &fakeRedis{
		delFn: func(_ context.Context, keys ...string) *goRedis.IntCmd {
			assert.Equal(t, []string{"test_key"}, keys)
			return &goRedis.IntCmd{}
		},
	}

	r := redisDS{mockCmd}
	_, err := r.Del(t.Context(), "test_key").Result()
	require.NoError(t, err)
}

func TestRedis_Rename(t *testing.T) {
	mockCmd := &fakeRedis{
		renameFn: func(_ context.Context, key, newKey string) *goRedis.StatusCmd {
			assert.Equal(t, "test_key", key)
			assert.Equal(t, "test_new_key", newKey)
			return &goRedis.StatusCmd{}
		},
	}

	r := redisDS{mockCmd}
	_, err := r.Rename(t.Context(), "test_key", "test_new_key").Result()
	require.NoError(t, err)
}

func TestRedisMigrator_GetLastMigration(t *testing.T) {
	tests := []struct {
		desc                  string
		mockedData            map[string]string
		redisErr              error
		migratorLastMigration int64
		migratorErr           error
		expectedLastMigration int64
		expectedErr           error
	}{
		{
			desc: "Successful",
			mockedData: map[string]string{
				"1": `{"method":"UP","startTime":"2024-01-01T00:00:00Z","duration":1000}`,
				"2": `{"method":"UP","startTime":"2024-01-02T00:00:00Z","duration":2000}`,
			},
			migratorLastMigration: 3,
			expectedLastMigration: 3,
		},
		{
			desc:                  "ErrorFromHGetAll",
			redisErr:              goRedis.ErrClosed,
			expectedLastMigration: -1,
			expectedErr:           goRedis.ErrClosed,
		},
		{
			desc: "UnmarshalError",
			mockedData: map[string]string{
				"1": `{"method":"UP","startTime":"2024-01-01T00:00:00Z","duration":1000}`,
				"2": "invalid JSON data",
			},
			expectedLastMigration: -1,
			expectedErr:           &json.SyntaxError{},
		},
	}

	for i, tc := range tests {
		mockR := &fakeRedis{
			hgetAllFn: func(_ context.Context, key string) *goRedis.MapStringStringCmd {
				assert.Equal(t, "kite_migrations", key)
				return goRedis.NewMapStringStringResult(tc.mockedData, tc.redisErr)
			},
		}
		deps := &Deps{
			Logger: logging.NewWriterLogger(logging.DEBUG, io.Discard, io.Discard),
			Redis:  mockR,
		}
		next := &fakeMigrator{
			lastFn: func(context.Context, *Deps) (int64, error) {
				return tc.migratorLastMigration, tc.migratorErr
			},
		}
		m := redisMigrator{
			Redis:    mockR,
			migrator: next,
		}

		lastMigration, err := m.getLastMigration(t.Context(), deps)
		assert.Equal(t, tc.expectedLastMigration, lastMigration, "TEST[%d], Failed.\n%s", i, tc.desc)
		if tc.expectedErr != nil {
			assert.Error(t, err, "TEST[%d], Failed.\n%s", i, tc.desc)
		} else {
			assert.NoError(t, err, "TEST[%d], Failed.\n%s", i, tc.desc)
		}
	}
}

func TestRedisMigrator_beginTransaction(t *testing.T) {
	txCalled := false
	mockR := &fakeRedis{
		txPipelineFn: func() goRedis.Pipeliner {
			txCalled = true
			return nil
		},
	}
	deps := &Deps{
		Logger: logging.NewWriterLogger(logging.DEBUG, io.Discard, io.Discard),
		Redis:  mockR,
	}
	m := redisMigrator{
		Redis:    mockR,
		migrator: &fakeMigrator{},
	}

	data, err := m.beginTransaction(t.Context(), deps)
	require.NoError(t, err)
	assert.True(t, txCalled)
	assert.Equal(t, transactionData{}, data)
}
