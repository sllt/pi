package cache_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sllt/pi/pkg/pi/apperror"
	"github.com/sllt/pi/pkg/pi/cache"
	piRedis "github.com/sllt/pi/pkg/pi/datasource/redis"
)

type testUser struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func newTestRedisClient(t *testing.T) (*miniredis.Miniredis, piRedis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })

	return mr, &piRedis.Redis{Client: rc}
}

type recordingStore struct {
	mu       sync.Mutex
	inner    cache.ByteStore
	getErr   error
	setErr   error
	delErr   error
	setTTLs  []time.Duration
	getCalls atomic.Int32
	setCalls atomic.Int32
}

func newRecordingStore(inner cache.ByteStore) *recordingStore {
	return &recordingStore{inner: inner}
}

func (s *recordingStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	s.getCalls.Add(1)
	s.mu.Lock()
	err := s.getErr
	s.mu.Unlock()
	if err != nil {
		return nil, false, err
	}
	return s.inner.Get(ctx, key)
}

func (s *recordingStore) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	s.setCalls.Add(1)
	s.mu.Lock()
	s.setTTLs = append(s.setTTLs, ttl)
	err := s.setErr
	s.mu.Unlock()
	if err != nil {
		return err
	}
	return s.inner.Set(ctx, key, value, ttl)
}

func (s *recordingStore) Delete(ctx context.Context, keys ...string) error {
	s.mu.Lock()
	err := s.delErr
	s.mu.Unlock()
	if err != nil {
		return err
	}
	return s.inner.Delete(ctx, keys...)
}

func (s *recordingStore) Close() error {
	if s.inner != nil {
		return s.inner.Close()
	}
	return nil
}

func TestMemoryStore_NoGoroutinesAndCloneIsolation(t *testing.T) {
	runtime.GC()
	before := runtime.NumGoroutine()
	for i := 0; i < 10; i++ {
		s := cache.NewMemoryStore(cache.WithMaxEntries(100))
		require.NoError(t, s.Close())
	}
	ms := cache.NewMemoryStore(cache.WithMaxEntries(100))
	defer func() { _ = ms.Close() }()
	after := runtime.NumGoroutine()
	assert.LessOrEqual(t, after, before+1, "NewMemoryStore must not spawn background goroutines")

	ctx := context.Background()
	input := []byte("hello-world")
	require.NoError(t, ms.Set(ctx, "k1", input, time.Minute))
	// Mutating caller slice after Set must not mutate cached bytes.
	input[0] = 'X'

	got, ok, err := ms.Get(ctx, "k1")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, []byte("hello-world"), got)

	// Mutating returned slice after Get must not mutate cached bytes.
	got[0] = 'Y'
	got2, ok, err := ms.Get(ctx, "k1")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, []byte("hello-world"), got2)

	// Negative TTL deletes entry.
	require.NoError(t, ms.Set(ctx, "k1", []byte("ignored"), -time.Second))
	_, ok, err = ms.Get(ctx, "k1")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestMemoryStore_CapacityEvictionAndExpiry(t *testing.T) {
	ms := cache.NewMemoryStore(cache.WithMaxEntries(4))
	defer func() { _ = ms.Close() }()
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		require.NoError(t, ms.Set(ctx, fmt.Sprintf("key-%d", i), []byte{byte(i)}, time.Minute))
	}

	present := 0
	for i := 0; i < 10; i++ {
		if _, ok, err := ms.Get(ctx, fmt.Sprintf("key-%d", i)); err == nil && ok {
			present++
		}
	}
	assert.LessOrEqual(t, present, 4)

	require.NoError(t, ms.Set(ctx, "short", []byte("v"), 20*time.Millisecond))
	time.Sleep(35 * time.Millisecond)
	_, ok, err := ms.Get(ctx, "short")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestRedisStore_NilSafeAndMiniredis(t *testing.T) {
	ctx := context.Background()

	nilStore := cache.NewRedisStore(nil)
	_, ok, err := nilStore.Get(ctx, "k")
	assert.False(t, ok)
	assert.Error(t, err)
	assert.Error(t, nilStore.Set(ctx, "k", []byte("v"), time.Minute))
	assert.Error(t, nilStore.Delete(ctx, "k"))
	assert.NoError(t, nilStore.Close())

	mr, client := newTestRedisClient(t)
	rs := cache.NewRedisStore(client, cache.WithKeyPrefix("app:test:"))

	// Miss returns (nil, false, nil)
	val, ok, err := rs.Get(ctx, "missing")
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, val)

	require.NoError(t, rs.Set(ctx, "u1", []byte("alice"), 5*time.Second))
	assert.True(t, mr.Exists("app:test:u1"))

	val, ok, err = rs.Get(ctx, "u1")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, []byte("alice"), val)

	// Close is a no-op and leaves underlying client alive.
	require.NoError(t, rs.Close())
	_, ok, err = rs.Get(ctx, "u1")
	require.NoError(t, err)
	assert.True(t, ok)

	// Expiration via miniredis fast-forward.
	mr.FastForward(6 * time.Second)
	_, ok, err = rs.Get(ctx, "u1")
	require.NoError(t, err)
	assert.False(t, ok)

	require.NoError(t, rs.Set(ctx, "u2", []byte("bob"), time.Minute))
	require.NoError(t, rs.Delete(ctx, "u2"))
	_, ok, err = rs.Get(ctx, "u2")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestTieredStore_L1BackfillUsesMinTTLAndJoinsDeleteErrors(t *testing.T) {
	mr, client := newTestRedisClient(t)
	ctx := context.Background()

	l1 := cache.NewMemoryStore()
	l2 := cache.NewRedisStore(client, cache.WithKeyPrefix("tiered:"))
	tiered := cache.NewTieredStore(l1, l2, 30*time.Second)
	defer func() { _ = tiered.Close() }()

	// Seed L2 directly with a shorter 80ms TTL.
	require.NoError(t, l2.Set(ctx, "item", []byte("from-l2"), 80*time.Millisecond))

	// First Get misses L1, hits L2, and populates L1 bounded by remaining L2 TTL (~80ms, not 30s).
	val, ok, err := tiered.Get(ctx, "item")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, []byte("from-l2"), val)

	l1Val, l1Ok, err := l1.Get(ctx, "item")
	require.NoError(t, err)
	require.True(t, l1Ok)
	assert.Equal(t, []byte("from-l2"), l1Val)

	// Wait past L2's remaining TTL; L1 entry must also have expired.
	time.Sleep(110 * time.Millisecond)
	_, l1OkAfter, err := l1.Get(ctx, "item")
	require.NoError(t, err)
	assert.False(t, l1OkAfter)

	// Set writes both L2 and L1.
	require.NoError(t, tiered.Set(ctx, "both", []byte("v2"), time.Minute))
	assert.True(t, mr.Exists("tiered:both"))
	l1Both, ok, err := l1.Get(ctx, "both")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, []byte("v2"), l1Both)

	// Delete removes from both L1 and L2.
	require.NoError(t, tiered.Delete(ctx, "both"))
	assert.False(t, mr.Exists("tiered:both"))
	_, ok, err = l1.Get(ctx, "both")
	require.NoError(t, err)
	assert.False(t, ok)

	// Delete joins errors from L1 and L2.
	errL1 := errors.New("l1 delete err")
	errL2 := errors.New("l2 delete err")
	r1 := newRecordingStore(cache.NewMemoryStore())
	r1.delErr = errL1
	r2 := newRecordingStore(cache.NewMemoryStore())
	r2.delErr = errL2
	errTiered := cache.NewTieredStore(r1, r2, time.Second)
	delErr := errTiered.Delete(ctx, "k")
	require.Error(t, delErr)
	assert.ErrorIs(t, delErr, errL1)
	assert.ErrorIs(t, delErr, errL2)
}

func TestCache_RememberStampedeSingleflight(t *testing.T) {
	c := cache.NewMemory()
	defer func() { _ = c.Close() }()

	var loadCount atomic.Int32
	gate := make(chan struct{})
	const callers = 40

	var wg sync.WaitGroup
	wg.Add(callers)
	results := make([]testUser, callers)
	errs := make([]error, callers)

	for i := 0; i < callers; i++ {
		idx := i
		go func() {
			defer wg.Done()
			results[idx], errs[idx] = cache.Remember(context.Background(), c, "user:1", time.Minute, func(ctx context.Context) (testUser, error) {
				loadCount.Add(1)
				<-gate
				return testUser{ID: 1, Name: "alice"}, nil
			})
		}()
	}

	time.Sleep(25 * time.Millisecond)
	close(gate)
	wg.Wait()

	assert.Equal(t, int32(1), loadCount.Load(), "concurrent callers must share one loader execution")
	for i := 0; i < callers; i++ {
		require.NoError(t, errs[i])
		assert.Equal(t, testUser{ID: 1, Name: "alice"}, results[i])
	}

	// Subsequent Get hits cached value.
	got, ok, err := cache.Get[testUser](context.Background(), c, "user:1")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, testUser{ID: 1, Name: "alice"}, got)
}

func TestCache_SingleflightPartialVsAllCallerCancellation(t *testing.T) {
	t.Run("partial caller cancellation lets remaining waiters succeed", func(t *testing.T) {
		c := cache.NewMemory()
		defer func() { _ = c.Close() }()

		loaderStarted := make(chan struct{})
		releaseLoader := make(chan struct{})
		var flightCanceled atomic.Bool

		ctxA, cancelA := context.WithCancel(context.Background())
		ctxB := context.Background()

		var (
			resA, resB testUser
			errA, errB error
			wg         sync.WaitGroup
		)
		wg.Add(2)

		go func() {
			defer wg.Done()
			resA, errA = cache.Remember(ctxA, c, "partial", time.Minute, func(ctx context.Context) (testUser, error) {
				close(loaderStarted)
				select {
				case <-releaseLoader:
					return testUser{ID: 7, Name: "bob"}, nil
				case <-ctx.Done():
					flightCanceled.Store(true)
					return testUser{}, ctx.Err()
				}
			})
		}()

		<-loaderStarted

		go func() {
			defer wg.Done()
			resB, errB = cache.Remember(ctxB, c, "partial", time.Minute, func(ctx context.Context) (testUser, error) {
				return testUser{}, errors.New("should not run second loader")
			})
		}()

		time.Sleep(20 * time.Millisecond)
		// Cancel only Caller A; Caller B is still waiting so flightCtx must stay alive.
		cancelA()
		time.Sleep(20 * time.Millisecond)
		assert.False(t, flightCanceled.Load())

		close(releaseLoader)
		wg.Wait()

		assert.ErrorIs(t, errA, context.Canceled)
		assert.Equal(t, testUser{}, resA)
		require.NoError(t, errB)
		assert.Equal(t, testUser{ID: 7, Name: "bob"}, resB)
	})

	t.Run("all callers canceled immediately cancels detached flightCtx", func(t *testing.T) {
		c := cache.NewMemory()
		defer func() { _ = c.Close() }()

		loaderStarted := make(chan struct{})
		flightDone := make(chan error, 1)

		ctxA, cancelA := context.WithCancel(context.Background())
		ctxB, cancelB := context.WithCancel(context.Background())

		var (
			errA, errB error
			wg         sync.WaitGroup
		)
		wg.Add(2)

		go func() {
			defer wg.Done()
			_, errA = cache.Remember(ctxA, c, "all-cancel", time.Minute, func(ctx context.Context) (testUser, error) {
				close(loaderStarted)
				<-ctx.Done()
				flightDone <- ctx.Err()
				return testUser{}, ctx.Err()
			})
		}()

		<-loaderStarted

		go func() {
			defer wg.Done()
			_, errB = cache.Remember(ctxB, c, "all-cancel", time.Minute, func(ctx context.Context) (testUser, error) {
				return testUser{}, errors.New("should not run second loader")
			})
		}()

		time.Sleep(20 * time.Millisecond)
		cancelA()
		select {
		case <-flightDone:
			t.Fatal("flightCtx canceled while Caller B was still waiting")
		case <-time.After(15 * time.Millisecond):
		}

		cancelB()
		select {
		case fErr := <-flightDone:
			assert.ErrorIs(t, fErr, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("expected flightCtx to cancel immediately after last caller canceled")
		}

		wg.Wait()
		assert.ErrorIs(t, errA, context.Canceled)
		assert.ErrorIs(t, errB, context.Canceled)
	})
}

func TestCache_LoadTimeoutJitterNegativeCacheNilPtrAndFailOpen(t *testing.T) {
	t.Run("load timeout bounds detached loader", func(t *testing.T) {
		c := cache.New(cache.NewMemoryStore(), cache.WithLoadTimeout(30*time.Millisecond))
		defer func() { _ = c.Close() }()

		_, err := cache.Remember(context.Background(), c, "slow", time.Minute, func(ctx context.Context) (string, error) {
			<-ctx.Done()
			return "", ctx.Err()
		})
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("jitter stays within bounds and zero disables jitter", func(t *testing.T) {
		rec := newRecordingStore(cache.NewMemoryStore())
		c := cache.New(rec, cache.WithJitter(0.2))
		defer func() { _ = c.Close() }()

		baseTTL := 10 * time.Second
		for i := 0; i < 30; i++ {
			require.NoError(t, cache.Set(context.Background(), c, fmt.Sprintf("j-%d", i), i, baseTTL))
		}
		rec.mu.Lock()
		ttls := append([]time.Duration(nil), rec.setTTLs...)
		rec.mu.Unlock()
		require.Len(t, ttls, 30)
		minBound := time.Duration(float64(baseTTL) * 0.8)
		maxBound := time.Duration(float64(baseTTL) * 1.2)
		for _, ttl := range ttls {
			assert.GreaterOrEqual(t, ttl, minBound)
			assert.LessOrEqual(t, ttl, maxBound)
		}

		recNoJitter := newRecordingStore(cache.NewMemoryStore())
		cNoJitter := cache.New(recNoJitter, cache.WithJitter(0))
		require.NoError(t, cache.Set(context.Background(), cNoJitter, "exact", 42, baseTTL))
		assert.Equal(t, []time.Duration{baseTTL}, recNoJitter.setTTLs)
	})

	t.Run("negative cache handles apperror.NotFound, sql.ErrNoRows, custom matcher, and nil pointers cleanly", func(t *testing.T) {
		c := cache.New(cache.NewMemoryStore(), cache.WithNotFoundTTL(60*time.Millisecond), cache.WithJitter(0))
		defer func() { _ = c.Close() }()
		ctx := context.Background()

		// 1. Nil *testUser is cached as JSON null (tag 0x01), distinct from negative cache (tag 0x02).
		var ptrLoads atomic.Int32
		u, err := cache.Remember(ctx, c, "nil-user", time.Minute, func(context.Context) (*testUser, error) {
			ptrLoads.Add(1)
			return nil, nil
		})
		require.NoError(t, err)
		assert.Nil(t, u)

		u2, err := cache.Remember(ctx, c, "nil-user", time.Minute, func(context.Context) (*testUser, error) {
			ptrLoads.Add(1)
			return &testUser{ID: 99}, nil
		})
		require.NoError(t, err)
		assert.Nil(t, u2)
		assert.Equal(t, int32(1), ptrLoads.Load())

		// 2. apperror.NotFound is negatively cached with Kind, Code, and PublicMessage preserved.
		var nfLoads atomic.Int32
		customDomainNotFoundErr := apperror.New(apperror.NotFound, 40401, "missing user")
		_, err = cache.Remember(ctx, c, "nf-app", time.Minute, func(context.Context) (*testUser, error) {
			nfLoads.Add(1)
			return nil, customDomainNotFoundErr
		})
		require.ErrorIs(t, err, customDomainNotFoundErr)

		_, err = cache.Remember(ctx, c, "nf-app", time.Minute, func(context.Context) (*testUser, error) {
			nfLoads.Add(1)
			return &testUser{ID: 1}, nil
		})
		require.ErrorIs(t, err, cache.ErrNotFound)
		require.ErrorIs(t, err, customDomainNotFoundErr)
		resolved := apperror.Resolve(err)
		require.NotNil(t, resolved)
		assert.Equal(t, apperror.NotFound, resolved.Kind())
		assert.Equal(t, 40401, resolved.Code())
		assert.Equal(t, "missing user", resolved.PublicMessage())
		assert.Equal(t, int32(1), nfLoads.Load())

		// 3. sql.ErrNoRows is negatively cached as apperror.NotFound (404) and expires after NotFoundTTL.
		var sqlLoads atomic.Int32
		_, err = cache.Remember(ctx, c, "nf-sql", time.Minute, func(context.Context) (testUser, error) {
			sqlLoads.Add(1)
			return testUser{}, fmt.Errorf("lookup: %w", sql.ErrNoRows)
		})
		require.ErrorIs(t, err, sql.ErrNoRows)

		_, err = cache.Remember(ctx, c, "nf-sql", time.Minute, func(context.Context) (testUser, error) {
			sqlLoads.Add(1)
			return testUser{ID: 2, Name: "now-present"}, nil
		})
		require.ErrorIs(t, err, cache.ErrNotFound)
		sqlResolved := apperror.Resolve(err)
		require.NotNil(t, sqlResolved)
		assert.Equal(t, apperror.NotFound, sqlResolved.Kind())
		assert.Equal(t, 404, sqlResolved.Code())
		assert.Equal(t, "resource not found", sqlResolved.PublicMessage())
		assert.Equal(t, int32(1), sqlLoads.Load())

		time.Sleep(80 * time.Millisecond)
		loaded, err := cache.Remember(ctx, c, "nf-sql", time.Minute, func(context.Context) (testUser, error) {
			sqlLoads.Add(1)
			return testUser{ID: 2, Name: "now-present"}, nil
		})
		require.NoError(t, err)
		assert.Equal(t, testUser{ID: 2, Name: "now-present"}, loaded)
		assert.Equal(t, int32(2), sqlLoads.Load())

		// 4. Custom matcher.
		errCustom := errors.New("custom not found")
		cCustom := cache.New(
			cache.NewMemoryStore(),
			cache.WithNotFoundTTL(time.Minute, func(err error) bool { return errors.Is(err, errCustom) }),
		)
		defer func() { _ = cCustom.Close() }()
		_, err = cache.Remember(ctx, cCustom, "k", time.Minute, func(context.Context) (string, error) {
			return "", errCustom
		})
		require.ErrorIs(t, err, errCustom)
		_, err = cache.Remember(ctx, cCustom, "k", time.Minute, func(context.Context) (string, error) {
			return "never", nil
		})
		require.ErrorIs(t, err, cache.ErrNotFound)
	})

	t.Run("fail-open falls through on transient store errors", func(t *testing.T) {
		ctx := context.Background()
		transient := errors.New("redis connection reset")
		rec := newRecordingStore(cache.NewMemoryStore())
		rec.getErr = transient
		rec.setErr = transient

		openCache := cache.New(rec, cache.WithFailOpen(true))
		val, err := cache.Remember(ctx, openCache, "k", time.Minute, func(context.Context) (string, error) {
			return "db-value", nil
		})
		require.NoError(t, err)
		assert.Equal(t, "db-value", val)

		closedCache := cache.New(rec, cache.WithFailOpen(false))
		_, err = cache.Remember(ctx, closedCache, "k", time.Minute, func(context.Context) (string, error) {
			return "db-value", nil
		})
		require.ErrorIs(t, err, transient)
	})

	t.Run("tiered cache works end-to-end with miniredis", func(t *testing.T) {
		_, client := newTestRedisClient(t)
		l1 := cache.NewMemoryStore()
		l2 := cache.NewRedisStore(client, cache.WithKeyPrefix("e2e:"))
		c := cache.NewTiered(l1, l2, 5*time.Second, cache.WithJitter(0))
		defer func() { _ = c.Close() }()

		ctx := context.Background()
		var calls atomic.Int32
		val, err := cache.Remember(ctx, c, "acct:1", time.Minute, func(context.Context) (testUser, error) {
			calls.Add(1)
			return testUser{ID: 1, Name: "tier"}, nil
		})
		require.NoError(t, err)
		assert.Equal(t, testUser{ID: 1, Name: "tier"}, val)

		// Delete from L1 only; L2 still serves and backfills L1.
		require.NoError(t, l1.Delete(ctx, "acct:1"))
		val2, err := cache.Remember(ctx, c, "acct:1", time.Minute, func(context.Context) (testUser, error) {
			calls.Add(1)
			return testUser{}, nil
		})
		require.NoError(t, err)
		assert.Equal(t, testUser{ID: 1, Name: "tier"}, val2)
		assert.Equal(t, int32(1), calls.Load())

		require.NoError(t, c.Delete(ctx, "acct:1"))
		_, ok, err := cache.Get[testUser](ctx, c, "acct:1")
		require.NoError(t, err)
		assert.False(t, ok)

		// L2 Set failure invalidates stale L1 entry.
		require.NoError(t, l1.Set(ctx, "stale", []byte("old-l1"), time.Minute))
		failL2 := newRecordingStore(cache.NewMemoryStore())
		failL2.setErr = errors.New("l2 down")
		tFail := cache.NewTieredStore(l1, failL2, time.Minute)
		require.Error(t, tFail.Set(ctx, "stale", []byte("new"), time.Minute))
		_, stillInL1, err := l1.Get(ctx, "stale")
		require.NoError(t, err)
		assert.False(t, stillInL1)
	})
}

type mutableUser struct {
	ID   int      `json:"id"`
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}

func TestCache_SingleflightIsolatedDecodedInstancesUnderRace(t *testing.T) {
	c := cache.NewMemory()
	defer func() { _ = c.Close() }()

	const waiters = 20
	gate := make(chan struct{})
	var loadCount atomic.Int32
	var wg sync.WaitGroup
	wg.Add(waiters)

	results := make([]*mutableUser, waiters)
	errs := make([]error, waiters)

	for i := 0; i < waiters; i++ {
		idx := i
		go func() {
			defer wg.Done()
			u, err := cache.Remember(context.Background(), c, "mut:1", time.Minute, func(context.Context) (*mutableUser, error) {
				loadCount.Add(1)
				<-gate
				return &mutableUser{ID: 1, Name: "orig", Tags: []string{"t0"}}, nil
			})
			errs[idx] = err
			results[idx] = u
			if err == nil && u != nil {
				// Mutate returned pointer and slice concurrently across all 20 waiters.
				u.Name = fmt.Sprintf("mut-%d", idx)
				u.Tags[0] = fmt.Sprintf("tag-%d", idx)
			}
		}()
	}

	time.Sleep(20 * time.Millisecond)
	close(gate)
	wg.Wait()

	assert.Equal(t, int32(1), loadCount.Load())
	for i := 0; i < waiters; i++ {
		require.NoError(t, errs[i])
		require.NotNil(t, results[i])
		assert.Equal(t, fmt.Sprintf("mut-%d", i), results[i].Name)
		assert.Equal(t, fmt.Sprintf("tag-%d", i), results[i].Tags[0])
	}

	// Cached value must remain pristine.
	cached, ok, err := cache.Get[*mutableUser](context.Background(), c, "mut:1")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "orig", cached.Name)
	assert.Equal(t, []string{"t0"}, cached.Tags)

	// Remember with negative TTL still returns decoded result to waiters without storing.
	gotNeg, err := cache.Remember(context.Background(), c, "neg-ttl", -time.Second, func(context.Context) (*mutableUser, error) {
		return &mutableUser{ID: 2, Name: "ephemeral", Tags: []string{"x"}}, nil
	})
	require.NoError(t, err)
	require.NotNil(t, gotNeg)
	assert.Equal(t, "ephemeral", gotNeg.Name)
	_, ok, err = cache.Get[*mutableUser](context.Background(), c, "neg-ttl")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestCache_UnifiedOptionsNilPassThroughDeleteAndDetachedWriteCtx(t *testing.T) {
	t.Run("unified options on NewMemory and NewRedis", func(t *testing.T) {
		mem := cache.NewMemory(
			cache.WithMaxEntries(5000),
			cache.WithJitter(0.1),
			cache.WithNotFoundTTL(30*time.Second),
		)
		defer func() { _ = mem.Close() }()
		ctx := context.Background()

		require.NoError(t, cache.Set(ctx, mem, "k1", "v1", time.Minute))
		got, ok, err := cache.Get[string](ctx, mem, "k1")
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, "v1", got)

		require.NoError(t, cache.Delete(ctx, mem, "k1"))
		_, ok, err = cache.Get[string](ctx, mem, "k1")
		require.NoError(t, err)
		assert.False(t, ok)

		mr, client := newTestRedisClient(t)
		rc := cache.NewRedis(
			client,
			cache.WithKeyPrefix("orders:"),
			cache.WithJitter(0),
			cache.WithNotFoundTTL(time.Minute),
		)
		defer func() { _ = rc.Close() }()

		require.NoError(t, cache.Set(ctx, rc, "o1", testUser{ID: 9, Name: "ord"}, time.Minute))
		assert.True(t, mr.Exists("orders:o1"))
		gotUser, ok, err := cache.Get[testUser](ctx, rc, "o1")
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, testUser{ID: 9, Name: "ord"}, gotUser)
	})

	t.Run("nil cache and nil store pass-through", func(t *testing.T) {
		ctx := context.Background()
		var nilCache *cache.Cache

		val, err := cache.Remember(ctx, nilCache, "k", time.Minute, func(context.Context) (string, error) {
			return "direct", nil
		})
		require.NoError(t, err)
		assert.Equal(t, "direct", val)

		got, ok, err := cache.Get[string](ctx, nilCache, "k")
		require.NoError(t, err)
		assert.False(t, ok)
		assert.Empty(t, got)

		require.NoError(t, cache.Set(ctx, nilCache, "k", "v", time.Minute))
		require.NoError(t, nilCache.Delete(ctx, "k"))
		require.NoError(t, cache.Delete(ctx, nilCache, "k"))
		require.NoError(t, nilCache.Close())

		// Non-nil cache with nil store and default failOpen=true also passes through.
		openNilStore := cache.New(nil)
		val, err = cache.Remember(ctx, openNilStore, "k", time.Minute, func(context.Context) (string, error) {
			return "open-direct", nil
		})
		require.NoError(t, err)
		assert.Equal(t, "open-direct", val)
		_, ok, err = cache.Get[string](ctx, openNilStore, "k")
		require.NoError(t, err)
		assert.False(t, ok)
		require.NoError(t, cache.Set(ctx, openNilStore, "k", "v", time.Minute))
		require.NoError(t, cache.Delete(ctx, openNilStore, "k"))

		// When failOpen=false, nil store returns errNilStore.
		closedNilStore := cache.New(nil, cache.WithFailOpen(false))
		_, err = cache.Remember(ctx, closedNilStore, "k", time.Minute, func(context.Context) (string, error) {
			return "x", nil
		})
		require.Error(t, err)
		_, _, err = cache.Get[string](ctx, closedNilStore, "k")
		require.Error(t, err)
		require.Error(t, cache.Set(ctx, closedNilStore, "k", "v", time.Minute))
		require.Error(t, cache.Delete(ctx, closedNilStore, "k"))
	})

	t.Run("post-loader Set uses detached write context when flightCtx expires", func(t *testing.T) {
		inner := cache.NewMemoryStore()
		rec := newRecordingStore(inner)
		c := cache.New(rec, cache.WithLoadTimeout(20*time.Millisecond), cache.WithJitter(0))
		defer func() { _ = c.Close() }()

		// Loader waits until flightCtx deadline expires, then returns a valid result.
		// Because MemoryStore.Set checks ctx.Err(), a non-detached flightCtx would fail Set.
		val, err := cache.Remember(context.Background(), c, "near-deadline", time.Minute, func(flightCtx context.Context) (string, error) {
			<-flightCtx.Done()
			return "loaded-at-deadline", nil
		})
		require.NoError(t, err)
		assert.Equal(t, "loaded-at-deadline", val)

		got, ok, err := cache.Get[string](context.Background(), c, "near-deadline")
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, "loaded-at-deadline", got)
	})
}
