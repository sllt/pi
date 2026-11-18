package middleware_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/golang-jwt/jwt/v5"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sllt/pi/pkg/pi/auth"
	piRedis "github.com/sllt/pi/pkg/pi/datasource/redis"
	"github.com/sllt/pi/pkg/pi/http/middleware"
)

func newTestIdemRedis(t *testing.T) (*miniredis.Miniredis, piRedis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })
	return mr, &piRedis.Redis{Client: rc}
}

func TestMemoryIdempotencyStore_LifecycleAndOwnerSafety(t *testing.T) {
	runtime.GC()
	before := runtime.NumGoroutine()
	for i := 0; i < 10; i++ {
		s := middleware.NewMemoryIdempotencyStore(3)
		require.NoError(t, s.Close())
	}
	store := middleware.NewMemoryIdempotencyStore(3)
	defer func() { _ = store.Close() }()
	after := runtime.NumGoroutine()
	assert.LessOrEqual(t, after, before+1, "NewMemoryIdempotencyStore must be goroutine-free")

	ctx := context.Background()

	// Acquire lock.
	existing, acquired, err := store.TryBegin(ctx, "k1", "fp1", "owner-1", 50*time.Millisecond)
	require.NoError(t, err)
	assert.True(t, acquired)
	assert.Nil(t, existing)

	// Duplicate TryBegin sees InProgress.
	existing, acquired, err = store.TryBegin(ctx, "k1", "fp1", "owner-2", 50*time.Millisecond)
	require.NoError(t, err)
	assert.False(t, acquired)
	require.NotNil(t, existing)
	assert.True(t, existing.InProgress)

	// Wrong owner cannot Complete or Abort.
	assert.Error(t, store.Complete(ctx, "k1", "owner-2", middleware.IdempotencyRecord{Fingerprint: "fp1", StatusCode: 200}, time.Minute))
	require.NoError(t, store.Abort(ctx, "k1", "owner-2"))
	_, acquired, err = store.TryBegin(ctx, "k1", "fp1", "owner-2", 50*time.Millisecond)
	require.NoError(t, err)
	assert.False(t, acquired)

	// Correct owner aborts cleanly.
	require.NoError(t, store.Abort(ctx, "k1", "owner-1"))
	_, acquired, err = store.TryBegin(ctx, "k1", "fp1", "owner-2", time.Minute)
	require.NoError(t, err)
	assert.True(t, acquired)

	// Correct owner completes.
	require.NoError(t, store.Complete(ctx, "k1", "owner-2", middleware.IdempotencyRecord{
		Fingerprint: "fp1",
		StatusCode:  http.StatusCreated,
		Body:        []byte(`{"ok":true}`),
	}, 40*time.Millisecond))

	existing, acquired, err = store.TryBegin(ctx, "k1", "fp1", "owner-3", time.Minute)
	require.NoError(t, err)
	assert.False(t, acquired)
	require.NotNil(t, existing)
	assert.False(t, existing.InProgress)
	assert.Equal(t, http.StatusCreated, existing.StatusCode)

	// Expires after TTL.
	time.Sleep(60 * time.Millisecond)
	_, acquired, err = store.TryBegin(ctx, "k1", "fp1", "owner-3", time.Minute)
	require.NoError(t, err)
	assert.True(t, acquired)
}

func TestRedisIdempotencyStore_LuaAtomicityAndNilSafe(t *testing.T) {
	ctx := context.Background()

	nilStore := middleware.NewRedisIdempotencyStore(nil, "")
	_, _, err := nilStore.TryBegin(ctx, "k", "fp", "o", time.Second)
	assert.Error(t, err)
	assert.Error(t, nilStore.Complete(ctx, "k", "o", middleware.IdempotencyRecord{}, time.Second))
	assert.Error(t, nilStore.Abort(ctx, "k", "o"))
	assert.NoError(t, nilStore.Close())

	mr, client := newTestIdemRedis(t)
	store := middleware.NewRedisIdempotencyStore(client, "test:idem:")
	defer func() { _ = store.Close() }()

	_, acquired, err := store.TryBegin(ctx, "k1", "fp1", "owner-1", 5*time.Second)
	require.NoError(t, err)
	require.True(t, acquired)
	assert.True(t, mr.Exists("test:idem:k1"))

	// Wrong owner cannot abort or complete.
	require.NoError(t, store.Abort(ctx, "k1", "wrong-owner"))
	assert.True(t, mr.Exists("test:idem:k1"))
	assert.Error(t, store.Complete(ctx, "k1", "wrong-owner", middleware.IdempotencyRecord{Fingerprint: "fp1", StatusCode: 200}, time.Minute))

	// Right owner completes and persists with TTL.
	require.NoError(t, store.Complete(ctx, "k1", "owner-1", middleware.IdempotencyRecord{
		Fingerprint: "fp1",
		StatusCode:  http.StatusCreated,
		Headers:     map[string][]string{"Content-Type": {"application/json"}, "X-Custom": {"v1"}},
		Body:        []byte(`{"id":42}`),
	}, 10*time.Second))

	existing, acquired, err := store.TryBegin(ctx, "k1", "fp1", "owner-2", 5*time.Second)
	require.NoError(t, err)
	assert.False(t, acquired)
	require.NotNil(t, existing)
	assert.False(t, existing.InProgress)
	assert.Equal(t, http.StatusCreated, existing.StatusCode)
	assert.Equal(t, []byte(`{"id":42}`), existing.Body)

	// Completed entry cannot be aborted even with old owner token.
	require.NoError(t, store.Abort(ctx, "k1", "owner-1"))
	assert.True(t, mr.Exists("test:idem:k1"))

	// TTL expires cleanly in miniredis.
	mr.FastForward(11 * time.Second)
	_, acquired, err = store.TryBegin(ctx, "k1", "fp1", "owner-3", 5*time.Second)
	require.NoError(t, err)
	assert.True(t, acquired)

	// Right owner aborts InProgress lock.
	require.NoError(t, store.Abort(ctx, "k1", "owner-3"))
	assert.False(t, mr.Exists("test:idem:k1"))

	// Corrupt JSON in Redis is atomically overwritten by TryBegin instead of 503.
	require.NoError(t, mr.Set("test:idem:corrupt", "{bad-json"))
	existing, acquired, err = store.TryBegin(ctx, "corrupt", "fp-new", "owner-4", 5*time.Second)
	require.NoError(t, err)
	assert.True(t, acquired)
	assert.Nil(t, existing)
	require.NoError(t, store.Abort(ctx, "corrupt", "owner-4"))
}

func runIdempotencySuite(t *testing.T, makeStore func(t *testing.T) middleware.IdempotencyStore) {
	t.Run("replay, payload mismatch 422, and cross-user isolation", func(t *testing.T) {
		store := makeStore(t)
		defer func() { _ = store.Close() }()

		var execCount atomic.Int32
		h := middleware.Idempotency(middleware.IdempotencyConfig{
			Store: store,
		})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n := execCount.Add(1)
			var buf bytes.Buffer
			_, _ = buf.ReadFrom(r.Body)
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Order-Seq", string(rune('0'+n)))
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"seq":` + string(rune('0'+n)) + `,"echo":` + buf.String() + `}`))
		}))

		// First POST for user-a.
		req1 := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{"amt":100}`))
		req1 = req1.WithContext(auth.WithPrincipal(req1.Context(), auth.Principal{Subject: "user-a"}))
		req1.Header.Set("Idempotency-Key", "idem-1")
		w1 := httptest.NewRecorder()
		h.ServeHTTP(w1, req1)
		assert.Equal(t, http.StatusCreated, w1.Code)
		assert.Equal(t, "false", w1.Header().Get("X-Idempotent-Replayed"))
		assert.Equal(t, "1", w1.Header().Get("X-Order-Seq"))
		assert.Equal(t, int32(1), execCount.Load())

		// Replay POST for user-a with same key and payload.
		req2 := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{"amt":100}`))
		req2 = req2.WithContext(auth.WithPrincipal(req2.Context(), auth.Principal{Subject: "user-a"}))
		req2.Header.Set("Idempotency-Key", "idem-1")
		w2 := httptest.NewRecorder()
		h.ServeHTTP(w2, req2)
		assert.Equal(t, http.StatusCreated, w2.Code)
		assert.Equal(t, "true", w2.Header().Get("X-Idempotent-Replayed"))
		assert.Equal(t, "1", w2.Header().Get("X-Order-Seq"))
		assert.Equal(t, w1.Body.String(), w2.Body.String())
		assert.Equal(t, int32(1), execCount.Load())

		// Payload mismatch for user-a returns 422.
		req3 := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{"amt":999}`))
		req3 = req3.WithContext(auth.WithPrincipal(req3.Context(), auth.Principal{Subject: "user-a"}))
		req3.Header.Set("Idempotency-Key", "idem-1")
		w3 := httptest.NewRecorder()
		h.ServeHTTP(w3, req3)
		assert.Equal(t, http.StatusUnprocessableEntity, w3.Code)
		var errEnv struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		}
		require.NoError(t, json.Unmarshal(w3.Body.Bytes(), &errEnv))
		assert.Equal(t, http.StatusUnprocessableEntity, errEnv.Code)
		assert.Contains(t, errEnv.Message, "different request payload")
		assert.Equal(t, int32(1), execCount.Load())

		// Cross-user isolation: user-b using the same Idempotency-Key executes independently.
		reqB := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{"amt":200}`))
		reqB = reqB.WithContext(auth.WithPrincipal(reqB.Context(), auth.Principal{Subject: "user-b"}))
		reqB.Header.Set("Idempotency-Key", "idem-1")
		wB := httptest.NewRecorder()
		h.ServeHTTP(wB, reqB)
		assert.Equal(t, http.StatusCreated, wB.Code)
		assert.Equal(t, "false", wB.Header().Get("X-Idempotent-Replayed"))
		assert.Equal(t, int32(2), execCount.Load())

		// Header-based isolation when no auth.Principal is set.
		reqH1 := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{"amt":1}`))
		reqH1.Header.Set("Authorization", "Bearer token-1")
		reqH1.Header.Set("Idempotency-Key", "shared-key")
		wH1 := httptest.NewRecorder()
		h.ServeHTTP(wH1, reqH1)
		assert.Equal(t, http.StatusCreated, wH1.Code)
		assert.Equal(t, int32(3), execCount.Load())

		reqH2 := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{"amt":1}`))
		reqH2.Header.Set("Authorization", "Bearer token-2")
		reqH2.Header.Set("Idempotency-Key", "shared-key")
		wH2 := httptest.NewRecorder()
		h.ServeHTTP(wH2, reqH2)
		assert.Equal(t, http.StatusCreated, wH2.Code)
		assert.Equal(t, "false", wH2.Header().Get("X-Idempotent-Replayed"))
		assert.Equal(t, int32(4), execCount.Load())
	})

	t.Run("concurrent in-flight request returns 409 conflict", func(t *testing.T) {
		store := makeStore(t)
		defer func() { _ = store.Close() }()

		started := make(chan struct{})
		release := make(chan struct{})
		var calls atomic.Int32

		h := middleware.Idempotency(middleware.IdempotencyConfig{
			Store: store,
		})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			close(started)
			<-release
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"ok":true}`))
		}))

		var wg sync.WaitGroup
		wg.Add(1)
		w1 := httptest.NewRecorder()
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "/pay", strings.NewReader(`{}`))
			req.Header.Set("Idempotency-Key", "inflight-1")
			h.ServeHTTP(w1, req)
		}()

		<-started
		req2 := httptest.NewRequest(http.MethodPost, "/pay", strings.NewReader(`{}`))
		req2.Header.Set("Idempotency-Key", "inflight-1")
		w2 := httptest.NewRecorder()
		h.ServeHTTP(w2, req2)
		assert.Equal(t, http.StatusConflict, w2.Code)

		close(release)
		wg.Wait()

		assert.Equal(t, http.StatusCreated, w1.Code)
		assert.Equal(t, int32(1), calls.Load())

		// After completion, retry replays 201.
		req3 := httptest.NewRequest(http.MethodPost, "/pay", strings.NewReader(`{}`))
		req3.Header.Set("Idempotency-Key", "inflight-1")
		w3 := httptest.NewRecorder()
		h.ServeHTTP(w3, req3)
		assert.Equal(t, http.StatusCreated, w3.Code)
		assert.Equal(t, "true", w3.Header().Get("X-Idempotent-Replayed"))
		assert.Equal(t, int32(1), calls.Load())
	})

	t.Run("500 and panic abort lock for retry; deterministic 400 replays", func(t *testing.T) {
		store := makeStore(t)
		defer func() { _ = store.Close() }()

		var attempt atomic.Int32
		h := middleware.Idempotency(middleware.IdempotencyConfig{
			Store: store,
		})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n := attempt.Add(1)
			switch r.URL.Path {
			case "/err500":
				if n == 1 {
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = w.Write([]byte(`{"code":500}`))
					return
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"recovered":true}`))
			case "/panic":
				if n == 1 {
					panic("boom")
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"after_panic":true}`))
			case "/bad400":
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"code":400}`))
			}
		}))

		// 500 -> lock aborted -> retry succeeds.
		r1 := httptest.NewRequest(http.MethodPost, "/err500", http.NoBody)
		r1.Header.Set("Idempotency-Key", "k-500")
		w1 := httptest.NewRecorder()
		h.ServeHTTP(w1, r1)
		assert.Equal(t, http.StatusInternalServerError, w1.Code)

		r2 := httptest.NewRequest(http.MethodPost, "/err500", http.NoBody)
		r2.Header.Set("Idempotency-Key", "k-500")
		w2 := httptest.NewRecorder()
		h.ServeHTTP(w2, r2)
		assert.Equal(t, http.StatusOK, w2.Code)
		assert.Equal(t, "false", w2.Header().Get("X-Idempotent-Replayed"))

		// Panic -> lock aborted -> retry succeeds.
		attempt.Store(0)
		rp1 := httptest.NewRequest(http.MethodPost, "/panic", http.NoBody)
		rp1.Header.Set("Idempotency-Key", "k-panic")
		assert.Panics(t, func() {
			h.ServeHTTP(httptest.NewRecorder(), rp1)
		})
		rp2 := httptest.NewRequest(http.MethodPost, "/panic", http.NoBody)
		rp2.Header.Set("Idempotency-Key", "k-panic")
		wp2 := httptest.NewRecorder()
		h.ServeHTTP(wp2, rp2)
		assert.Equal(t, http.StatusOK, wp2.Code)

		// Deterministic 400 IS cached and replayed.
		attempt.Store(0)
		rb1 := httptest.NewRequest(http.MethodPost, "/bad400", http.NoBody)
		rb1.Header.Set("Idempotency-Key", "k-400")
		wb1 := httptest.NewRecorder()
		h.ServeHTTP(wb1, rb1)
		assert.Equal(t, http.StatusBadRequest, wb1.Code)
		assert.Equal(t, "false", wb1.Header().Get("X-Idempotent-Replayed"))

		rb2 := httptest.NewRequest(http.MethodPost, "/bad400", http.NoBody)
		rb2.Header.Set("Idempotency-Key", "k-400")
		wb2 := httptest.NewRecorder()
		h.ServeHTTP(wb2, rb2)
		assert.Equal(t, http.StatusBadRequest, wb2.Code)
		assert.Equal(t, "true", wb2.Header().Get("X-Idempotent-Replayed"))
		assert.Equal(t, int32(1), attempt.Load())
	})
}

func TestIdempotencyMiddleware_MemoryAndRedisSuites(t *testing.T) {
	t.Run("MemoryStore", func(t *testing.T) {
		runIdempotencySuite(t, func(t *testing.T) middleware.IdempotencyStore {
			return middleware.NewMemoryIdempotencyStore(1000)
		})
	})

	t.Run("RedisStore", func(t *testing.T) {
		runIdempotencySuite(t, func(t *testing.T) middleware.IdempotencyStore {
			_, client := newTestIdemRedis(t)
			return middleware.NewRedisIdempotencyStore(client, "suite:idem:")
		})
	})
}

func TestIdempotencyMiddleware_ValidationLimitsAndStreaming(t *testing.T) {
	store := middleware.NewMemoryIdempotencyStore(100)
	defer func() { _ = store.Close() }()

	var calls atomic.Int32
	h := middleware.Idempotency(middleware.IdempotencyConfig{
		Required:         true,
		MaxBodyBytes:     16,
		MaxResponseBytes: 32,
		Store:            store,
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		switch r.URL.Path {
		case "/large-resp":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(bytes.Repeat([]byte("a"), 64))
		case "/sse":
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			_ = http.NewResponseController(w).Flush()
			_, _ = w.Write([]byte("data: hi\n\n"))
		default:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		}
	}))

	// Missing key when Required: true -> 400.
	reqMissing := httptest.NewRequest(http.MethodPost, "/ok", http.NoBody)
	wMissing := httptest.NewRecorder()
	h.ServeHTTP(wMissing, reqMissing)
	assert.Equal(t, http.StatusBadRequest, wMissing.Code)

	// GET is skipped even when Required: true.
	reqGet := httptest.NewRequest(http.MethodGet, "/ok", http.NoBody)
	wGet := httptest.NewRecorder()
	h.ServeHTTP(wGet, reqGet)
	assert.Equal(t, http.StatusOK, wGet.Code)

	// Key > 256 chars -> 400.
	reqLongKey := httptest.NewRequest(http.MethodPost, "/ok", http.NoBody)
	reqLongKey.Header.Set("Idempotency-Key", strings.Repeat("x", 257))
	wLongKey := httptest.NewRecorder()
	h.ServeHTTP(wLongKey, reqLongKey)
	assert.Equal(t, http.StatusBadRequest, wLongKey.Code)

	// Key with control char -> 400.
	reqCtrl := httptest.NewRequest(http.MethodPost, "/ok", http.NoBody)
	reqCtrl.Header.Set("Idempotency-Key", "bad\x00key")
	wCtrl := httptest.NewRecorder()
	h.ServeHTTP(wCtrl, reqCtrl)
	assert.Equal(t, http.StatusBadRequest, wCtrl.Code)

	// Body > MaxBodyBytes -> 413.
	reqBigBody := httptest.NewRequest(http.MethodPost, "/ok", bytes.NewReader(bytes.Repeat([]byte("b"), 17)))
	reqBigBody.Header.Set("Idempotency-Key", "big-body")
	wBigBody := httptest.NewRecorder()
	h.ServeHTTP(wBigBody, reqBigBody)
	assert.Equal(t, http.StatusRequestEntityTooLarge, wBigBody.Code)

	// Response > MaxResponseBytes streams through and aborts lock (not cached).
	beforeCalls := calls.Load()
	reqLargeResp1 := httptest.NewRequest(http.MethodPost, "/large-resp", http.NoBody)
	reqLargeResp1.Header.Set("Idempotency-Key", "large-resp")
	wLargeResp1 := httptest.NewRecorder()
	h.ServeHTTP(wLargeResp1, reqLargeResp1)
	assert.Equal(t, http.StatusOK, wLargeResp1.Code)
	assert.Len(t, wLargeResp1.Body.Bytes(), 64)

	reqLargeResp2 := httptest.NewRequest(http.MethodPost, "/large-resp", http.NoBody)
	reqLargeResp2.Header.Set("Idempotency-Key", "large-resp")
	wLargeResp2 := httptest.NewRecorder()
	h.ServeHTTP(wLargeResp2, reqLargeResp2)
	assert.Equal(t, http.StatusOK, wLargeResp2.Code)
	assert.Equal(t, beforeCalls+2, calls.Load())

	// SSE response streams through and aborts lock.
	reqSSE := httptest.NewRequest(http.MethodPost, "/sse", http.NoBody)
	reqSSE.Header.Set("Idempotency-Key", "sse-1")
	wSSE := httptest.NewRecorder()
	h.ServeHTTP(wSSE, reqSSE)
	assert.Equal(t, http.StatusOK, wSSE.Code)
	assert.True(t, wSSE.Flushed)
	assert.Equal(t, "data: hi\n\n", wSSE.Body.String())
}

func TestIdempotencyHelpers_ScopeHeadersAndNonCacheableStatuses(t *testing.T) {
	// IsCacheableIdempotencyStatus excludes 401, 403, 408, 409, 425, 429, 499, <200, >=500.
	assert.True(t, middleware.IsCacheableIdempotencyStatus(http.StatusOK))
	assert.True(t, middleware.IsCacheableIdempotencyStatus(http.StatusCreated))
	assert.True(t, middleware.IsCacheableIdempotencyStatus(http.StatusBadRequest))
	assert.True(t, middleware.IsCacheableIdempotencyStatus(http.StatusNotFound))
	assert.True(t, middleware.IsCacheableIdempotencyStatus(http.StatusUnprocessableEntity))
	for _, s := range []int{100, 302, 401, 403, 408, 409, 425, 429, 499, 500, 503} {
		assert.False(t, middleware.IsCacheableIdempotencyStatus(s), "status %d should not be cacheable", s)
	}

	// DefaultIdempotencyScope checks JWTClaim, Username, APIKey, and Cookie.
	rJWT := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	rJWT = rJWT.WithContext(context.WithValue(rJWT.Context(), middleware.JWTClaim, jwt.MapClaims{"sub": "jwt-u1"}))
	assert.Equal(t, "sub:jwt-u1", middleware.DefaultIdempotencyScope(rJWT))

	rUser := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	rUser = rUser.WithContext(context.WithValue(rUser.Context(), middleware.Username, "alice"))
	assert.Equal(t, "user:alice", middleware.DefaultIdempotencyScope(rUser))

	rKey := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	rKey = rKey.WithContext(context.WithValue(rKey.Context(), middleware.APIKey, "secret-1"))
	assert.True(t, strings.HasPrefix(middleware.DefaultIdempotencyScope(rKey), "apikey:"))

	rCookie1 := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	rCookie1.Header.Set("Cookie", "sid=aaa")
	rCookie2 := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	rCookie2.Header.Set("Cookie", "sid=bbb")
	assert.NotEqual(t, middleware.DefaultIdempotencyScope(rCookie1), middleware.DefaultIdempotencyScope(rCookie2))

	// CloneSafeReplayHeaders strips sensitive/per-request headers and Connection-listed headers.
	hdr := http.Header{
		"Content-Type":     {"application/json"},
		"Set-Cookie":       {"sid=secret"},
		"X-Request-Id":     {"req-old"},
		"X-Correlation-Id": {"corr-old"},
		"Date":             {"Mon, 01 Jan 2024 00:00:00 GMT"},
		"Connection":       {"X-Hop-Secret"},
		"X-Hop-Secret":     {"hop"},
		"Vary":             {"Accept-Encoding"},
	}
	cloned := middleware.CloneSafeReplayHeaders(hdr)
	assert.Equal(t, []string{"application/json"}, cloned["Content-Type"])
	assert.Equal(t, []string{"Accept-Encoding"}, cloned["Vary"])
	assert.NotContains(t, cloned, "Set-Cookie")
	assert.NotContains(t, cloned, "X-Request-Id")
	assert.NotContains(t, cloned, "X-Correlation-Id")
	assert.NotContains(t, cloned, "Date")
	assert.NotContains(t, cloned, "Connection")
	assert.NotContains(t, cloned, "X-Hop-Secret")

	// Replay preserves outer Vary and current X-Request-ID, and 401/499 are not cached.
	store := middleware.NewMemoryIdempotencyStore(100)
	defer func() { _ = store.Close() }()
	var attempts atomic.Int32
	outer := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Add("Vary", "Origin")
			w.Header().Set("X-Request-Id", r.Header.Get("X-Test-ReqID"))
			next.ServeHTTP(w, r)
		})
	}
	mw := outer(middleware.Idempotency(middleware.IdempotencyConfig{
		Store: store,
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if r.URL.Path == "/auth-fail" && n == 1 {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"code":401}`))
			return
		}
		w.Header().Add("Vary", "Accept-Encoding")
		w.Header().Set("Set-Cookie", "sid=123")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})))

	r1 := httptest.NewRequest(http.MethodPost, "/vary", strings.NewReader(`{}`))
	r1.Header.Set("Content-Type", "application/json")
	r1.Header.Set("Idempotency-Key", "vary-k")
	r1.Header.Set("X-Test-ReqID", "req-1")
	w1 := httptest.NewRecorder()
	mw.ServeHTTP(w1, r1)
	assert.Equal(t, http.StatusOK, w1.Code)
	assert.Equal(t, "false", w1.Header().Get("X-Idempotent-Replayed"))

	r2 := httptest.NewRequest(http.MethodPost, "/vary", strings.NewReader(`{}`))
	r2.Header.Set("Content-Type", "application/json")
	r2.Header.Set("Idempotency-Key", "vary-k")
	r2.Header.Set("X-Test-ReqID", "req-2")
	w2 := httptest.NewRecorder()
	mw.ServeHTTP(w2, r2)
	assert.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, "true", w2.Header().Get("X-Idempotent-Replayed"))
	assert.Equal(t, "req-2", w2.Header().Get("X-Request-Id"))
	assert.Empty(t, w2.Header().Get("Set-Cookie"))
	assert.ElementsMatch(t, []string{"Origin", "Accept-Encoding"}, w2.Header().Values("Vary"))

	// Content-Type change with same key and body returns 422.
	rCT := httptest.NewRequest(http.MethodPost, "/vary", strings.NewReader(`{}`))
	rCT.Header.Set("Content-Type", "text/plain")
	rCT.Header.Set("Idempotency-Key", "vary-k")
	wCT := httptest.NewRecorder()
	mw.ServeHTTP(wCT, rCT)
	assert.Equal(t, http.StatusUnprocessableEntity, wCT.Code)

	// 401 is not cached; retry executes handler again.
	attempts.Store(0)
	ra1 := httptest.NewRequest(http.MethodPost, "/auth-fail", http.NoBody)
	ra1.Header.Set("Idempotency-Key", "auth-k")
	wa1 := httptest.NewRecorder()
	mw.ServeHTTP(wa1, ra1)
	assert.Equal(t, http.StatusUnauthorized, wa1.Code)

	ra2 := httptest.NewRequest(http.MethodPost, "/auth-fail", http.NoBody)
	ra2.Header.Set("Idempotency-Key", "auth-k")
	wa2 := httptest.NewRecorder()
	mw.ServeHTTP(wa2, ra2)
	assert.Equal(t, http.StatusOK, wa2.Code)
	assert.Equal(t, "false", wa2.Header().Get("X-Idempotent-Replayed"))
}

func TestIdempotency_VariadicDefaultsAndCommaVaryDeduplication(t *testing.T) {
	defaultMem := middleware.NewMemoryIdempotencyStore()
	require.NoError(t, defaultMem.Close())

	_, client := newTestIdemRedis(t)
	defaultRedis := middleware.NewRedisIdempotencyStore(client)
	require.NoError(t, defaultRedis.Close())

	var calls atomic.Int32
	outer := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Vary", "Origin, Accept-Encoding")
			next.ServeHTTP(w, r)
		})
	}
	h := outer(middleware.Idempotency()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Vary", "accept-encoding, X-Tenant, Origin")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})))

	r1 := httptest.NewRequest(http.MethodPost, "/items", strings.NewReader(`{"x":1}`))
	r1.Header.Set("Idempotency-Key", "var-1")
	w1 := httptest.NewRecorder()
	h.ServeHTTP(w1, r1)
	assert.Equal(t, http.StatusCreated, w1.Code)
	assert.Equal(t, "false", w1.Header().Get("X-Idempotent-Replayed"))

	r2 := httptest.NewRequest(http.MethodPost, "/items", strings.NewReader(`{"x":1}`))
	r2.Header.Set("Idempotency-Key", "var-1")
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, r2)
	assert.Equal(t, http.StatusCreated, w2.Code)
	assert.Equal(t, "true", w2.Header().Get("X-Idempotent-Replayed"))
	assert.Equal(t, int32(1), calls.Load())

	var varyTokens []string
	for _, v := range w2.Header().Values("Vary") {
		for _, tok := range strings.Split(v, ",") {
			if trimmed := strings.TrimSpace(tok); trimmed != "" {
				varyTokens = append(varyTokens, strings.ToLower(trimmed))
			}
		}
	}
	assert.ElementsMatch(t, []string{"origin", "accept-encoding", "x-tenant"}, varyTokens)
}
