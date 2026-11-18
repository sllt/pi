package cache

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/redis/go-redis/v9"

	piRedis "github.com/sllt/pi/pkg/pi/datasource/redis"
)

const defaultRedisKeyPrefix = "pi:cache:"

var errNilRedisClient = errors.New("cache: redis client is nil")

type redisConfig struct {
	keyPrefix string
}

// RedisOption configures a RedisStore (and implements cache.Option for NewRedis).
type RedisOption func(*redisConfig)

func (o RedisOption) applyOption(cfg *allOptionsConfig) {
	if o != nil {
		o(&cfg.redis)
	}
}

// WithKeyPrefix sets the key prefix prepended to all keys stored in RedisStore.
func WithKeyPrefix(prefix string) RedisOption {
	return func(c *redisConfig) {
		c.keyPrefix = prefix
	}
}

// RedisStore implements ByteStore backed by a pi datasource Redis client.
type RedisStore struct {
	client    piRedis.Client
	keyPrefix string
}

// NewRedisStore constructs a RedisStore using client (nil-safe if client is nil).
func NewRedisStore(client piRedis.Client, opts ...RedisOption) *RedisStore {
	cfg := defaultRedisConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return newRedisStoreFromConfig(client, cfg)
}

func defaultRedisConfig() redisConfig {
	return redisConfig{
		keyPrefix: defaultRedisKeyPrefix,
	}
}

func newRedisStoreFromConfig(client piRedis.Client, cfg redisConfig) *RedisStore {
	if isNilRedisClient(client) {
		client = nil
	}
	return &RedisStore{
		client:    client,
		keyPrefix: cfg.keyPrefix,
	}
}

func isNilRedisClient(c piRedis.Client) bool {
	if c == nil {
		return true
	}
	rv := reflect.ValueOf(c)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
		return rv.IsNil()
	default:
		return false
	}
}

func (s *RedisStore) prefixedKey(key string) string {
	return s.keyPrefix + key
}

// Get retrieves cached bytes from Redis, returning (nil, false, nil) on redis.Nil.
func (s *RedisStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if s == nil || s.client == nil {
		return nil, false, errNilRedisClient
	}
	if ctx == nil {
		ctx = context.Background()
	}
	b, err := s.client.Get(ctx, s.prefixedKey(key)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

func (s *RedisStore) getWithTTL(ctx context.Context, key string) ([]byte, bool, time.Duration, error) {
	if s == nil || s.client == nil {
		return nil, false, 0, errNilRedisClient
	}
	if ctx == nil {
		ctx = context.Background()
	}
	fullKey := s.prefixedKey(key)
	pipe := s.client.Pipeline()
	getCmd := pipe.Get(ctx, fullKey)
	pttlCmd := pipe.PTTL(ctx, fullKey)
	_, _ = pipe.Exec(ctx)

	b, err := getCmd.Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, 0, nil
	}
	if err != nil {
		return nil, false, 0, err
	}
	pttl, pttlErr := pttlCmd.Result()
	if pttlErr != nil {
		return b, true, 0, nil
	}
	if pttl == time.Duration(-2) || pttl == -2*time.Millisecond {
		return nil, false, 0, nil
	}
	if pttl == time.Duration(-1) || pttl == -1*time.Millisecond {
		return b, true, 0, nil
	}
	if pttl == 0 {
		return b, true, time.Millisecond, nil
	}
	if pttl < 0 {
		return b, true, 0, nil
	}
	return b, true, pttl, nil
}

// Set writes value to Redis under the prefixed key with ttl (0 means no expiration; <0 deletes).
func (s *RedisStore) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if s == nil || s.client == nil {
		return errNilRedisClient
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ttl < 0 {
		return s.Delete(ctx, key)
	}
	if ttl > 0 && ttl < time.Millisecond {
		ttl = time.Millisecond
	}
	return s.client.Set(ctx, s.prefixedKey(key), value, ttl).Err()
}

// Delete removes one or more keys from Redis.
func (s *RedisStore) Delete(ctx context.Context, keys ...string) error {
	if s == nil || s.client == nil {
		return errNilRedisClient
	}
	if len(keys) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	prefixed := make([]string, len(keys))
	for i, k := range keys {
		prefixed[i] = s.prefixedKey(k)
	}
	return s.client.Del(ctx, prefixed...).Err()
}

// Close is a no-op because the underlying piRedis.Client lifecycle is owned by pi.App.
func (s *RedisStore) Close() error {
	return nil
}
