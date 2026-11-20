package middleware

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"time"

	piRedis "github.com/sllt/pi/pkg/pi/datasource/redis"
)

const defaultRateLimitRedisPrefix = "pi:rl:"

var (
	errRateLimitNilRedis    = errors.New("rate limit: redis client is nil")
	errRateLimitRedisResult = errors.New("rate limit: unexpected redis eval result")
)

const redisRateLimitTokenBucketScript = `
pcall(redis.replicate_commands)
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])

local t = redis.call('TIME')
local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local refill_rate_per_ms = limit / window_ms

local bucket = redis.call("HMGET", key, "tokens", "last_refill_ms")
local tokens = tonumber(bucket[1])
local last_refill_ms = tonumber(bucket[2])

if tokens == nil or last_refill_ms == nil then
	tokens = burst
	last_refill_ms = now_ms
else
	local elapsed_ms = math.max(0, now_ms - last_refill_ms)
	tokens = math.min(burst, tokens + elapsed_ms * refill_rate_per_ms)
	last_refill_ms = now_ms
end

local allowed = 0
local retry_after_ms = 0

if tokens >= 1 then
	allowed = 1
	tokens = tokens - 1
else
	allowed = 0
	retry_after_ms = math.ceil((1 - tokens) / refill_rate_per_ms)
	if retry_after_ms < 1 then
		retry_after_ms = 1
	end
end

local remaining = math.floor(tokens)
if remaining < 0 then
	remaining = 0
end

if allowed == 1 then
	local ttl_ms = math.ceil(burst / refill_rate_per_ms)
	if ttl_ms < window_ms then
		ttl_ms = window_ms
	end
	ttl_ms = ttl_ms * 2

	redis.call("HSET", key, "tokens", string.format("%.6f", tokens), "last_refill_ms", tostring(last_refill_ms))
	redis.call("PEXPIRE", key, ttl_ms)
end

return {allowed, remaining, retry_after_ms}
`

// RedisRateLimitStore implements RateLimitStore using an atomic Redis Lua token bucket.
type RedisRateLimitStore struct {
	client    piRedis.Client
	keyPrefix string
}

// NewRedisRateLimitStore constructs a Redis-backed RateLimitStore.
// If keyPrefix is omitted or empty, it defaults to "pi:rl:".
func NewRedisRateLimitStore(client piRedis.Client, keyPrefix ...string) *RedisRateLimitStore {
	prefix := defaultRateLimitRedisPrefix
	if len(keyPrefix) > 0 && strings.TrimSpace(keyPrefix[0]) != "" {
		prefix = keyPrefix[0]
	}
	if isNilRateLimitRedisClient(client) {
		client = nil
	}
	return &RedisRateLimitStore{
		client:    client,
		keyPrefix: prefix,
	}
}

func isNilRateLimitRedisClient(c piRedis.Client) bool {
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

// Allow checks and consumes one token for key using Redis server TIME and a float token bucket.
func (r *RedisRateLimitStore) Allow(
	ctx context.Context,
	key string,
	limit int,
	window time.Duration,
	burst int,
) (RateLimitDecision, error) {
	if r == nil || r.client == nil {
		return RateLimitDecision{}, errRateLimitNilRedis
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if limit <= 0 {
		return RateLimitDecision{}, errRateLimitInvalidLimit
	}
	if window <= 0 {
		window = time.Minute
	}
	if burst <= 0 {
		burst = limit
	}

	windowMs := window.Milliseconds()
	if windowMs <= 0 {
		windowMs = 1
	}

	res, err := r.client.Eval(
		ctx,
		redisRateLimitTokenBucketScript,
		[]string{r.keyPrefix + key},
		limit,
		windowMs,
		burst,
	).Result()
	if err != nil {
		return RateLimitDecision{}, err
	}

	arr, ok := res.([]any)
	if !ok || len(arr) != 3 {
		return RateLimitDecision{}, errRateLimitRedisResult
	}
	allowed, ok0 := redisReplyInt64(arr[0])
	remaining, ok1 := redisReplyInt64(arr[1])
	retryAfterMs, ok2 := redisReplyInt64(arr[2])
	if !ok0 || !ok1 || !ok2 {
		return RateLimitDecision{}, errRateLimitRedisResult
	}
	if remaining < 0 {
		remaining = 0
	}

	var retryAfter time.Duration
	if retryAfterMs > 0 {
		retryAfter = time.Duration(retryAfterMs) * time.Millisecond
	}

	return RateLimitDecision{
		Allowed:    allowed == 1,
		Limit:      limit,
		Remaining:  int(remaining),
		RetryAfter: retryAfter,
	}, nil
}

// Close is a no-op because the underlying piRedis.Client lifecycle is owned by the caller or pi.App.
func (r *RedisRateLimitStore) Close() error {
	return nil
}

var (
	_ RateLimitStore = (*MemoryRateLimitStore)(nil)
	_ RateLimitStore = (*RedisRateLimitStore)(nil)
)
