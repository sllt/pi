package middleware

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	piRedis "github.com/sllt/pi/pkg/pi/datasource/redis"
)

func newTestRateLimitRedis(t *testing.T) (*miniredis.Miniredis, piRedis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })
	return mr, &piRedis.Redis{Client: rc}
}

func TestRedisRateLimitStore_TokenBucketAndFloatPrecision(t *testing.T) {
	mr, client := newTestRateLimitRedis(t)
	store := NewRedisRateLimitStore(client)
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()

	// Burst=3, Limit=3 per 300ms (1 token per 100ms).
	d1, err := store.Allow(ctx, "user:alice", 3, 300*time.Millisecond, 3)
	require.NoError(t, err)
	assert.True(t, d1.Allowed)
	assert.Equal(t, 3, d1.Limit)
	assert.Equal(t, 2, d1.Remaining)
	assert.Zero(t, d1.RetryAfter)

	// Verify key uses default pi:rl: prefix and stores float-formatted tokens (no integer truncation).
	rawTokens := mr.HGet("pi:rl:user:alice", "tokens")
	assert.Contains(t, rawTokens, ".")

	d2, err := store.Allow(ctx, "user:alice", 3, 300*time.Millisecond, 3)
	require.NoError(t, err)
	assert.True(t, d2.Allowed)
	assert.Equal(t, 1, d2.Remaining)

	d3, err := store.Allow(ctx, "user:alice", 3, 300*time.Millisecond, 3)
	require.NoError(t, err)
	assert.True(t, d3.Allowed)
	assert.Equal(t, 0, d3.Remaining)

	// 4th request exceeds burst.
	d4, err := store.Allow(ctx, "user:alice", 3, 300*time.Millisecond, 3)
	require.NoError(t, err)
	assert.False(t, d4.Allowed)
	assert.Equal(t, 3, d4.Limit)
	assert.Equal(t, 0, d4.Remaining)
	assert.Greater(t, d4.RetryAfter, time.Duration(0))

	// Other key has independent bucket.
	other, err := store.Allow(ctx, "user:bob", 3, 300*time.Millisecond, 3)
	require.NoError(t, err)
	assert.True(t, other.Allowed)
	assert.Equal(t, 2, other.Remaining)

	// Wait for at least 1 token to refill using server TIME.
	time.Sleep(150 * time.Millisecond)
	dRefill, err := store.Allow(ctx, "user:alice", 3, 300*time.Millisecond, 3)
	require.NoError(t, err)
	assert.True(t, dRefill.Allowed)

	// PEXPIRE is set and expires the bucket key cleanly.
	assert.Greater(t, mr.TTL("pi:rl:user:alice"), time.Duration(0))
	mr.FastForward(5 * time.Second)
	assert.False(t, mr.Exists("pi:rl:user:alice"))
}

func TestRedisRateLimitStore_CustomPrefixAndErrorGuards(t *testing.T) {
	mr, client := newTestRateLimitRedis(t)
	store := NewRedisRateLimitStore(client, "custom:rl:")

	d, err := store.Allow(context.Background(), "k1", 5, time.Minute, 5)
	require.NoError(t, err)
	assert.True(t, d.Allowed)
	assert.True(t, mr.Exists("custom:rl:k1"))

	_, err = store.Allow(context.Background(), "k1", 0, time.Minute, 1)
	require.Error(t, err)

	var typedNil *piRedis.Redis
	nilStore := NewRedisRateLimitStore(typedNil)
	_, err = nilStore.Allow(context.Background(), "k1", 5, time.Minute, 5)
	require.Error(t, err)

	require.NoError(t, (*RedisRateLimitStore)(nil).Close())
}

func TestRedisRateLimitStore_RejectedPollsDoNotStarveSlowRefill(t *testing.T) {
	mr, client := newTestRateLimitRedis(t)
	store := NewRedisRateLimitStore(client)
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()

	// Very slow window (1 per 10h): 1ms accrues < 1e-6 tokens.
	// Rejected polls must NOT overwrite tokens/last_refill_ms in Redis.
	d1, err := store.Allow(ctx, "slow-hour", 1, 10*time.Hour, 1)
	require.NoError(t, err)
	assert.True(t, d1.Allowed)
	initialLastRefill := mr.HGet("pi:rl:slow-hour", "last_refill_ms")
	require.NotEmpty(t, initialLastRefill)

	for i := 0; i < 5; i++ {
		time.Sleep(2 * time.Millisecond)
		rejected, err := store.Allow(ctx, "slow-hour", 1, 10*time.Hour, 1)
		require.NoError(t, err)
		assert.False(t, rejected.Allowed)
		assert.Equal(t, initialLastRefill, mr.HGet("pi:rl:slow-hour", "last_refill_ms"),
			"rejected poll must not advance last_refill_ms and starve refill")
	}

	// Verify rapid rejected polls do not prevent eventual refill once window elapses.
	dFirst, err := store.Allow(ctx, "slow-refill", 1, 120*time.Millisecond, 1)
	require.NoError(t, err)
	assert.True(t, dFirst.Allowed)

	for i := 0; i < 4; i++ {
		time.Sleep(10 * time.Millisecond)
		rej, err := store.Allow(ctx, "slow-refill", 1, 120*time.Millisecond, 1)
		require.NoError(t, err)
		assert.False(t, rej.Allowed)
	}

	time.Sleep(100 * time.Millisecond)
	dAfter, err := store.Allow(ctx, "slow-refill", 1, 120*time.Millisecond, 1)
	require.NoError(t, err)
	assert.True(t, dAfter.Allowed)
}

func TestMemoryRateLimitStore_GoroutineFreeBoundedAndRefill(t *testing.T) {
	beforeG := runtime.NumGoroutine()
	store := NewMemoryRateLimitStore(3)
	t.Cleanup(func() { _ = store.Close() })
	afterG := runtime.NumGoroutine()
	assert.LessOrEqual(t, afterG, beforeG+1, "NewMemoryRateLimitStore must not spawn background goroutines")

	now := time.Unix(1_700_000_000, 0)
	store.now = func() time.Time { return now }

	ctx := context.Background()
	d1, err := store.Allow(ctx, "k1", 2, time.Minute, 2)
	require.NoError(t, err)
	assert.True(t, d1.Allowed)
	assert.Equal(t, 1, d1.Remaining)

	d2, err := store.Allow(ctx, "k1", 2, time.Minute, 2)
	require.NoError(t, err)
	assert.True(t, d2.Allowed)
	assert.Equal(t, 0, d2.Remaining)

	d3, err := store.Allow(ctx, "k1", 2, time.Minute, 2)
	require.NoError(t, err)
	assert.False(t, d3.Allowed)
	assert.Equal(t, 0, d3.Remaining)
	assert.Equal(t, 30*time.Second, d3.RetryAfter)

	// Advance clock by 30s -> 1 token refilled.
	now = now.Add(30 * time.Second)
	d4, err := store.Allow(ctx, "k1", 2, time.Minute, 2)
	require.NoError(t, err)
	assert.True(t, d4.Allowed)

	// Bounded maxKeys=3 evicts when adding a 4th key.
	_, err = store.Allow(ctx, "k2", 2, time.Minute, 2)
	require.NoError(t, err)
	_, err = store.Allow(ctx, "k3", 2, time.Minute, 2)
	require.NoError(t, err)
	_, err = store.Allow(ctx, "k4", 2, time.Minute, 2)
	require.NoError(t, err)

	store.mu.Lock()
	assert.LessOrEqual(t, len(store.buckets), 3)
	store.mu.Unlock()

	require.NoError(t, store.Close())
}

func TestMemoryRateLimitStore_ConcurrentSafety(t *testing.T) {
	store := NewMemoryRateLimitStore(100)
	t.Cleanup(func() { _ = store.Close() })

	var allowed atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("k-%d", idx%2)
			d, err := store.Allow(context.Background(), key, 5, time.Minute, 5)
			if err == nil && d.Allowed {
				allowed.Add(1)
			}
		}(i)
	}
	wg.Wait()
	assert.Equal(t, int32(10), allowed.Load())
}

func TestLegacyMemoryRateLimiterStore_RespectsPerCallConfigAndTrustedProxyCIDRs(t *testing.T) {
	store := NewMemoryRateLimiterStore(RateLimiterConfig{
		RequestsPerSecond: 100,
		Burst:             100,
	})
	t.Cleanup(store.StopCleanup)

	// Per-call config overrides initial store config for new key and existing key.
	strictCfg := RateLimiterConfig{RequestsPerSecond: 1, Burst: 1}
	ok1, _, err := store.Allow(context.Background(), "per-call-key", strictCfg)
	require.NoError(t, err)
	assert.True(t, ok1)

	ok2, _, err := store.Allow(context.Background(), "per-call-key", strictCfg)
	require.NoError(t, err)
	assert.False(t, ok2)

	// Updating existing bucket with higher RPS/Burst refills at the new rate.
	relaxedCfg := RateLimiterConfig{RequestsPerSecond: 100, Burst: 5}
	okSetup, _, err := store.Allow(context.Background(), "per-call-key", relaxedCfg)
	require.NoError(t, err)
	assert.False(t, okSetup)
	time.Sleep(25 * time.Millisecond)
	ok3, _, err := store.Allow(context.Background(), "per-call-key", relaxedCfg)
	require.NoError(t, err)
	assert.True(t, ok3)

	// Switching an existing high-burst bucket down to Burst=1 caps available tokens.
	okHigh, _, err := store.Allow(context.Background(), "high-to-low-key", relaxedCfg)
	require.NoError(t, err)
	assert.True(t, okHigh)
	okLow1, _, err := store.Allow(context.Background(), "high-to-low-key", strictCfg)
	require.NoError(t, err)
	assert.True(t, okLow1)
	okLow2, _, err := store.Allow(context.Background(), "high-to-low-key", strictCfg)
	require.NoError(t, err)
	assert.False(t, okLow2)

	// Legacy RateLimiter with TrustedProxyCIDRs ignores spoofed XFF from untrusted peers
	// and strips trusted proxy hops from trusted peers.
	m := newRateLimiterMockMetrics()
	rlMW, closer := RateLimiterWithCloser(RateLimiterConfig{
		RequestsPerSecond: 1,
		Burst:             1,
		PerIP:             true,
		TrustedProxyCIDRs: []string{"10.0.0.0/8"},
	}, m)
	t.Cleanup(func() { _ = closer() })
	mw := rlMW(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Untrusted peer cannot spoof XFF to bypass rate limit.
	req1 := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	req1.RemoteAddr = "198.51.100.10:1111"
	req1.Header.Set("X-Forwarded-For", "203.0.113.1")
	w1 := httptest.NewRecorder()
	mw.ServeHTTP(w1, req1)
	assert.Equal(t, http.StatusOK, w1.Code)

	req2 := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	req2.RemoteAddr = "198.51.100.10:2222"
	req2.Header.Set("X-Forwarded-For", "203.0.113.2")
	w2 := httptest.NewRecorder()
	mw.ServeHTTP(w2, req2)
	assert.Equal(t, http.StatusTooManyRequests, w2.Code)

	// Trusted peer strips trusted hop and keys on real client IP.
	req3 := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	req3.RemoteAddr = "10.0.0.2:3333"
	req3.Header.Set("X-Forwarded-For", "203.0.113.50, 10.0.0.9")
	w3 := httptest.NewRecorder()
	mw.ServeHTTP(w3, req3)
	assert.Equal(t, http.StatusOK, w3.Code)
}
