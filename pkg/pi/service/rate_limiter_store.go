package service

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	piRedis "github.com/sllt/pi/pkg/pi/datasource/redis"
)

const (
	cleanupInterval = 5 * time.Minute  // How often to clean up unused buckets
	bucketTTL       = 10 * time.Minute // How long to keep unused buckets
)

// RateLimiterStore abstracts the storage and cleanup for rate limiter buckets.
type RateLimiterStore interface {
	Allow(ctx context.Context, key string, config RateLimiterConfig) (allowed bool, retryAfter time.Duration, err error)
	StartCleanup(ctx context.Context)
	StopCleanup()
}

// tokenBucket implements fractional token refill so sub-1-RPS limits work accurately without integer truncation.
type tokenBucket struct {
	mu             sync.Mutex
	tokens         float64
	lastRefillTime int64
	maxTokens      float64
	refillRate     float64
}

// bucketEntry holds bucket with last access time for cleanup.
type bucketEntry struct {
	bucket     *tokenBucket
	lastAccess int64 // Unix timestamp
}

// newTokenBucket creates a new token bucket supporting arbitrary positive refill rates.
func newTokenBucket(config *RateLimiterConfig) *tokenBucket {
	maxTokens := float64(config.Burst)
	refillRate := config.RequestsPerSecond()

	return &tokenBucket{
		tokens:         maxTokens,
		lastRefillTime: time.Now().UnixNano(),
		maxTokens:      maxTokens,
		refillRate:     refillRate,
	}
}

// allow checks if a token can be consumed.
func (tb *tokenBucket) allow() (allowed bool, waitTime time.Duration) {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now().UnixNano()
	if tb.refillRate <= 0 {
		if tb.tokens >= 1 {
			tb.tokens--
			tb.lastRefillTime = now
			return true, 0
		}
		return false, time.Second
	}

	if elapsed := now - tb.lastRefillTime; elapsed > 0 {
		tokensToAdd := (float64(elapsed) / float64(time.Second)) * tb.refillRate
		tb.tokens = math.Min(tb.maxTokens, tb.tokens+tokensToAdd)
		tb.lastRefillTime = now
	}

	if tb.tokens < 1 {
		waitSec := (1.0 - tb.tokens) / tb.refillRate
		waitNs := math.Ceil(waitSec * float64(time.Second))
		if waitNs > float64(math.MaxInt64) {
			waitTime = time.Duration(math.MaxInt64)
		} else {
			waitTime = time.Duration(waitNs)
		}
		if waitTime < time.Millisecond {
			waitTime = time.Millisecond
		}
		return false, waitTime
	}

	tb.tokens--
	return true, 0
}

// LocalRateLimiterStore implements RateLimiterStore using in-memory buckets.
type LocalRateLimiterStore struct {
	buckets *sync.Map
	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
	doneCh  chan struct{}
}

func NewLocalRateLimiterStore() *LocalRateLimiterStore {
	return &LocalRateLimiterStore{
		buckets: &sync.Map{},
	}
}

func (l *LocalRateLimiterStore) Allow(_ context.Context, key string, config RateLimiterConfig) (bool, time.Duration, error) {
	now := time.Now().Unix()
	entry, _ := l.buckets.LoadOrStore(key, &bucketEntry{
		bucket:     newTokenBucket(&config),
		lastAccess: now,
	})

	bucketEntry := entry.(*bucketEntry)

	atomic.StoreInt64(&bucketEntry.lastAccess, now)

	allowed, retryAfter := bucketEntry.bucket.allow()

	return allowed, retryAfter, nil
}

func (l *LocalRateLimiterStore) StartCleanup(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	l.mu.Lock()
	if l.running {
		l.mu.Unlock()
		return
	}
	stopCh := make(chan struct{})
	doneCh := make(chan struct{})
	l.running = true
	l.stopCh = stopCh
	l.doneCh = doneCh
	l.mu.Unlock()

	go func() {
		defer close(doneCh)

		ticker := time.NewTicker(cleanupInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				l.cleanupExpiredBuckets()
			case <-stopCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (l *LocalRateLimiterStore) StopCleanup() {
	l.mu.Lock()
	if !l.running {
		l.mu.Unlock()
		return
	}
	stopCh := l.stopCh
	doneCh := l.doneCh
	l.running = false
	l.stopCh = nil
	l.doneCh = nil
	l.mu.Unlock()

	close(stopCh)
	<-doneCh
}

func (l *LocalRateLimiterStore) Close() error {
	l.StopCleanup()
	return nil
}

func (l *LocalRateLimiterStore) cleanupExpiredBuckets() {
	cutoff := time.Now().Unix() - int64(bucketTTL.Seconds())
	cleaned := 0

	l.buckets.Range(func(key, value any) bool {
		entry := value.(*bucketEntry)
		if atomic.LoadInt64(&entry.lastAccess) < cutoff {
			l.buckets.Delete(key)

			cleaned++
		}

		return true
	})
}

// tokenBucketScript is a Lua script for atomic token bucket rate limiting in Redis.
// Updated to use integer-only token math for simplicity
//
//nolint:gosec // This is a Lua script for Redis, not credentials
const tokenBucketScript = `
local key = KEYS[1]
local burst = tonumber(ARGV[1])
local requests = tonumber(ARGV[2]) 
local window_seconds = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

-- Calculate refill rate as requests per second
local refill_rate = requests / window_seconds

-- Fetch bucket
local bucket = redis.call("HMGET", key, "tokens", "last_refill")
local tokens = tonumber(bucket[1])
local last_refill = tonumber(bucket[2])

if tokens == nil then
    tokens = burst
    last_refill = now
end

-- Refill tokens (integer math only)
local delta = math.max(0, (now - last_refill)/1e9)
local tokens_to_add = math.floor(delta * refill_rate)
local new_tokens = math.min(burst, tokens + tokens_to_add)

local allowed = 0
local retryAfter = 0

if new_tokens >= 1 then
    allowed = 1
    new_tokens = new_tokens - 1
else
    retryAfter = math.ceil((1 - new_tokens) / refill_rate * 1000) -- ms
end

redis.call("HSET", key, "tokens", new_tokens, "last_refill", now)
redis.call("EXPIRE", key, 600)

return {allowed, retryAfter}
`

// RedisRateLimiterStore implements RateLimiterStore using Redis.
type RedisRateLimiterStore struct {
	client piRedis.Client
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

func NewRedisRateLimiterStore(client piRedis.Client) *RedisRateLimiterStore {
	if isNilRedisClient(client) {
		return &RedisRateLimiterStore{client: nil}
	}
	return &RedisRateLimiterStore{client: client}
}

func (r *RedisRateLimiterStore) Allow(ctx context.Context, key string, config RateLimiterConfig) (bool, time.Duration, error) {
	if r == nil || r.client == nil {
		return true, 0, errors.New("redis rate limiter client is nil") // Fail open
	}
	now := time.Now().UnixNano()
	cmd := r.client.Eval(
		ctx,
		tokenBucketScript,
		[]string{"kite:ratelimit:" + key},
		config.Burst,                   // ARGV[1]: burst
		config.Requests,                // ARGV[2]: requests
		int64(config.Window.Seconds()), // ARGV[3]: window_seconds
		now,                            // ARGV[4]: now (nanoseconds)
	)

	result, err := cmd.Result()
	if err != nil {
		return true, 0, err // Fail open
	}

	resultArray, ok := result.([]any)
	if !ok || len(resultArray) != 2 {
		return true, 0, errInvalidRedisResultType // Fail open
	}

	allowed, _ := toInt64(resultArray[0])
	retryAfterMs, _ := toInt64(resultArray[1])

	return allowed == 1, time.Duration(retryAfterMs) * time.Millisecond, nil
}

func (*RedisRateLimiterStore) StartCleanup(_ context.Context) {
	// No-op: Redis handles cleanup automatically via EXPIRE commands in Lua script.
}

func (*RedisRateLimiterStore) StopCleanup() {
	// No-op: Redis handles cleanup automatically.
}

// toInt64 safely converts Redis result to int64.
func toInt64(i any) (int64, error) {
	switch v := i.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case string:
		if v == "" {
			return 0, nil
		}

		return strconv.ParseInt(v, 10, 64)
	default:
		return 0, fmt.Errorf("%w: %T", errInvalidRedisResultType, i)
	}
}
