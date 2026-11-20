package middleware

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// RateLimiterStore abstracts the storage and cleanup for rate limiter buckets.
// This interface matches the one defined in pkg/pi/service for consistency.
//
// Note: The config parameter in Allow() is provided for interface compatibility.
// Implementations may use a stored configuration and ignore this parameter.
type RateLimiterStore interface {
	Allow(ctx context.Context, key string, config RateLimiterConfig) (allowed bool, retryAfter time.Duration, err error)
	StartCleanup(ctx context.Context)
	StopCleanup()
}

// memoryRateLimiterStore implements RateLimiterStore using in-memory token buckets.
type memoryRateLimiterStore struct {
	limiters sync.Map // map[string]*limiterEntry
	keyCount int64    // atomic counter for tracking number of keys
	maxKeys  int64    // maximum allowed keys (0 = unlimited)

	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
	doneCh  chan struct{}

	config RateLimiterConfig // Store config for consistency
}

type limiterEntry struct {
	limiter    *rate.Limiter
	lastAccess int64 // Unix timestamp for cleanup
}

// Default limits for delay calculation bounds checking.
const (
	minDelay       = time.Millisecond
	maxDelay       = time.Minute
	defaultMaxKeys = 100000
)

// NewMemoryRateLimiterStore creates a new in-memory rate limiter store.
// The config is stored to ensure consistent rate limiting for all keys.
func NewMemoryRateLimiterStore(config RateLimiterConfig) RateLimiterStore {
	maxKeys := config.MaxKeys
	if maxKeys == 0 {
		maxKeys = defaultMaxKeys // Default max keys to prevent memory exhaustion
	}

	return &memoryRateLimiterStore{
		config:  config,
		maxKeys: maxKeys,
	}
}

// Allow checks if a request should be allowed based on the rate limit.
func (m *memoryRateLimiterStore) Allow(_ context.Context, key string, config RateLimiterConfig) (bool, time.Duration, error) {
	now := time.Now().Unix()

	rps := config.RequestsPerSecond
	if rps <= 0 {
		rps = m.config.RequestsPerSecond
	}
	burst := config.Burst
	if burst <= 0 {
		burst = m.config.Burst
	}

	val, loaded := m.limiters.Load(key)
	if !loaded {
		newEntry := &limiterEntry{
			limiter:    rate.NewLimiter(rate.Limit(rps), burst),
			lastAccess: now,
		}
		actual, existed := m.limiters.LoadOrStore(key, newEntry)
		if existed {
			val = actual
			loaded = true
		} else {
			val = newEntry
			newCount := atomic.AddInt64(&m.keyCount, 1)
			if m.maxKeys > 0 && newCount > m.maxKeys {
				// Exceeded max keys - remove the entry we just added and fail open
				m.limiters.Delete(key)
				atomic.AddInt64(&m.keyCount, -1)
				// Fail open to prevent service denial
				return true, 0, nil
			}
		}
	}

	entry := val.(*limiterEntry)

	if loaded {
		atomic.StoreInt64(&entry.lastAccess, now)
		if rps > 0 && rate.Limit(rps) != entry.limiter.Limit() {
			entry.limiter.SetLimit(rate.Limit(rps))
		}
		if burst > 0 && burst != entry.limiter.Burst() {
			entry.limiter.SetBurst(burst)
		}
	}

	// Use only Reserve() instead of Allow() + Reserve() to avoid race conditions
	// Reserve() atomically checks and reserves a token, giving accurate delay information
	reservation := entry.limiter.Reserve()
	if !reservation.OK() {
		// Should not happen with valid config, but handle gracefully
		// Use bounds-checked delay calculation
		return false, m.calculateSafeDelay(rps), nil
	}

	delay := reservation.Delay()
	if delay > 0 {
		// Request would need to wait - cancel reservation and return the delay
		reservation.Cancel()
		return false, delay, nil
	}

	// Request is allowed immediately (delay == 0)
	return true, 0, nil
}

// calculateSafeDelay calculates delay with bounds checking to prevent overflow or zero values.
// Ensures delay is always within reasonable bounds.
func (*memoryRateLimiterStore) calculateSafeDelay(requestsPerSecond float64) time.Duration {
	if requestsPerSecond <= 0 {
		return maxDelay
	}

	delay := time.Duration(float64(time.Second) / requestsPerSecond)

	if delay < minDelay {
		return minDelay
	}

	if delay > maxDelay {
		return maxDelay
	}

	return delay
}

// StartCleanup starts a background goroutine to clean up stale limiters.
// This method is safe to call multiple times - only one cleanup goroutine runs at a time.
func (m *memoryRateLimiterStore) StartCleanup(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	m.mu.Lock()
	if m.running {
		m.mu.Unlock()
		return
	}
	stopCh := make(chan struct{})
	doneCh := make(chan struct{})
	m.running = true
	m.stopCh = stopCh
	m.doneCh = doneCh
	m.mu.Unlock()

	go func() {
		defer close(doneCh)

		const cleanupInterval = 5 * time.Minute
		const staleThreshold = 10 * time.Minute

		ticker := time.NewTicker(cleanupInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				m.cleanup(staleThreshold)
			case <-stopCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// StopCleanup stops the cleanup goroutine and waits for it to exit.
// This method is safe to call multiple times.
func (m *memoryRateLimiterStore) StopCleanup() {
	m.mu.Lock()
	if !m.running {
		m.mu.Unlock()
		return
	}
	stopCh := m.stopCh
	doneCh := m.doneCh
	m.running = false
	m.stopCh = nil
	m.doneCh = nil
	m.mu.Unlock()

	close(stopCh)
	<-doneCh
}

// Close stops the cleanup goroutine, satisfying io.Closer.
func (m *memoryRateLimiterStore) Close() error {
	m.StopCleanup()
	return nil
}

// cleanup removes stale limiters that haven't been accessed recently.
func (m *memoryRateLimiterStore) cleanup(staleThreshold time.Duration) {
	cutoff := time.Now().Unix() - int64(staleThreshold.Seconds())

	m.limiters.Range(func(key, value any) bool {
		entry := value.(*limiterEntry)
		if atomic.LoadInt64(&entry.lastAccess) < cutoff {
			m.limiters.Delete(key)
			// Decrement key count when removing stale entries
			atomic.AddInt64(&m.keyCount, -1)
		}

		return true
	})
}

const (
	rateLimitSweepBatchSize   = 8
	rateLimitEvictSampleLimit = 32
)

var (
	errRateLimitInvalidLimit = errors.New("rate limit: limit must be positive")
	errRateLimitNilStore     = errors.New("rate limit: memory store is nil")
)

// RateLimitDecision represents the outcome of a single rate limit check.
type RateLimitDecision struct {
	Allowed    bool
	Limit      int
	Remaining  int
	RetryAfter time.Duration
}

// RateLimitStore abstracts atomic token-bucket rate limiting across in-memory
// and distributed backends.
type RateLimitStore interface {
	Allow(ctx context.Context, key string, limit int, window time.Duration, burst int) (RateLimitDecision, error)
	Close() error
}

type memoryTokenBucket struct {
	tokens     float64
	lastRefill time.Time
	expiresAt  time.Time
	seq        uint64
}

// MemoryRateLimitStore is a goroutine-free, bounded-capacity in-memory RateLimitStore
// using a token bucket with lazy expiry and opportunistic eviction.
type MemoryRateLimitStore struct {
	mu      sync.Mutex
	buckets map[string]memoryTokenBucket
	maxKeys int
	nextSeq uint64
	now     func() time.Time
}

// NewMemoryRateLimitStore creates a goroutine-free in-memory RateLimitStore.
// If maxKeys is omitted or <= 0, it defaults to 100000.
func NewMemoryRateLimitStore(maxKeys ...int) *MemoryRateLimitStore {
	limit := defaultMaxKeys
	if len(maxKeys) > 0 && maxKeys[0] > 0 {
		limit = maxKeys[0]
	}
	return &MemoryRateLimitStore{
		buckets: make(map[string]memoryTokenBucket),
		maxKeys: limit,
		now:     time.Now,
	}
}

// Allow checks and consumes one token for key under the given limit, window, and burst.
func (m *MemoryRateLimitStore) Allow(
	ctx context.Context,
	key string,
	limit int,
	window time.Duration,
	burst int,
) (RateLimitDecision, error) {
	if m == nil {
		return RateLimitDecision{}, errRateLimitNilStore
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return RateLimitDecision{}, err
		}
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

	refillRate := float64(limit) / window.Seconds()
	fullRefill := time.Duration(float64(burst) / refillRate * float64(time.Second))
	ttl := window
	if fullRefill > ttl {
		ttl = fullRefill
	}
	ttl *= 2

	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	b, exists := m.buckets[key]
	if exists && now.Before(b.lastRefill) {
		now = b.lastRefill
	}

	var tokens float64
	if !exists || !now.Before(b.expiresAt) {
		if exists {
			delete(m.buckets, key)
			exists = false
		}
		m.sweepExpiredLocked(now, rateLimitSweepBatchSize)
		if m.maxKeys > 0 && len(m.buckets) >= m.maxKeys {
			m.evictOneLocked(now)
		}
		tokens = float64(burst)
	} else {
		elapsed := now.Sub(b.lastRefill).Seconds()
		if elapsed < 0 {
			elapsed = 0
		}
		tokens = math.Min(float64(burst), b.tokens+elapsed*refillRate)
	}

	decision := RateLimitDecision{
		Limit: limit,
	}
	if tokens >= 1.0 {
		tokens -= 1.0
		decision.Allowed = true
		decision.Remaining = int(math.Floor(tokens))
		if decision.Remaining < 0 {
			decision.Remaining = 0
		}
	} else {
		decision.Allowed = false
		decision.Remaining = 0
		waitMs := math.Ceil(((1.0 - tokens) / refillRate) * 1000.0)
		if waitMs < 1 {
			waitMs = 1
		}
		decision.RetryAfter = time.Duration(waitMs) * time.Millisecond
	}

	m.nextSeq++
	m.buckets[key] = memoryTokenBucket{
		tokens:     tokens,
		lastRefill: now,
		expiresAt:  now.Add(ttl),
		seq:        m.nextSeq,
	}
	return decision, nil
}

// Close clears all buckets in the in-memory store.
func (m *MemoryRateLimitStore) Close() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	clear(m.buckets)
	m.mu.Unlock()
	return nil
}

func (m *MemoryRateLimitStore) sweepExpiredLocked(now time.Time, limit int) {
	checked := 0
	for k, b := range m.buckets {
		if !now.Before(b.expiresAt) {
			delete(m.buckets, k)
		}
		checked++
		if checked >= limit {
			break
		}
	}
}

func (m *MemoryRateLimitStore) evictOneLocked(now time.Time) {
	var (
		oldestKey string
		oldestSeq uint64
		hasOldest bool
		scanned   int
	)
	for k, b := range m.buckets {
		if !now.Before(b.expiresAt) {
			delete(m.buckets, k)
			return
		}
		if !hasOldest || b.seq < oldestSeq {
			oldestKey = k
			oldestSeq = b.seq
			hasOldest = true
		}
		scanned++
		if scanned >= rateLimitEvictSampleLimit {
			break
		}
	}
	if hasOldest {
		delete(m.buckets, oldestKey)
	}
}
