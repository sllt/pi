package pi_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sllt/pi/pkg/pi"
	"github.com/sllt/pi/pkg/pi/apperror"
	"github.com/sllt/pi/pkg/pi/auth"
	piRedis "github.com/sllt/pi/pkg/pi/datasource/redis"
	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"github.com/sllt/pi/pkg/pi/http/middleware"
	"github.com/sllt/pi/pkg/pi/http/response"
	"github.com/sllt/pi/pkg/pi/testkit"
)

type createOrderReq struct {
	pi.Meta `status:"201"`
	Item    string `json:"item" validate:"required"`
	Qty     int    `json:"qty" validate:"min=1"`
}

type createOrderRes struct {
	OrderID string `json:"order_id"`
	Item    string `json:"item"`
	Qty     int    `json:"qty"`
}

type orderEnvelope struct {
	Code    int            `json:"code"`
	Data    createOrderRes `json:"data"`
	Message string         `json:"message"`
}

type spyIdempotencyStore struct {
	middleware.IdempotencyStore
	tryBeginCalls atomic.Int32
	closed        atomic.Bool
}

func (s *spyIdempotencyStore) TryBegin(
	ctx context.Context,
	key, fingerprint, ownerToken string,
	lockTTL time.Duration,
) (*middleware.IdempotencyRecord, bool, error) {
	s.tryBeginCalls.Add(1)
	return s.IdempotencyStore.TryBegin(ctx, key, fingerprint, ownerToken, lockTTL)
}

func (s *spyIdempotencyStore) Close() error {
	s.closed.Store(true)
	return s.IdempotencyStore.Close()
}

func newTestPiRedisClient(t *testing.T) (*miniredis.Miniredis, piRedis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })
	return mr, &piRedis.Redis{Client: rc}
}

func TestWithEndpointIdempotency_TypedPostReplayMismatchRetryAndCloser(t *testing.T) {
	spyStore := &spyIdempotencyStore{
		IdempotencyStore: middleware.NewMemoryIdempotencyStore(100),
	}

	app, err := pi.Build()
	require.NoError(t, err)

	var execCount atomic.Int32
	var shouldFail atomic.Bool

	app.POST("/orders", func(_ *pi.Context, req *createOrderReq) (*createOrderRes, error) {
		n := execCount.Add(1)
		if shouldFail.Load() {
			return nil, errors.New("downstream failure")
		}
		return &createOrderRes{
			OrderID: "ord-" + string(rune('0'+n)),
			Item:    req.Item,
			Qty:     req.Qty,
		}, nil
	}, pi.WithEndpointIdempotency(middleware.IdempotencyConfig{Store: spyStore}))
	require.NoError(t, app.RouteError())

	h, err := app.HTTPHandler()
	require.NoError(t, err)

	// 1. Initial POST succeeds with 201 and caches result.
	req1 := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{"item":"book","qty":2}`))
	req1.Header.Set("Content-Type", "application/json")
	req1.Header.Set("Idempotency-Key", "order-key-1")
	w1 := httptest.NewRecorder()
	h.ServeHTTP(w1, req1)
	assert.Equal(t, http.StatusCreated, w1.Code)
	assert.Equal(t, "false", w1.Header().Get("X-Idempotent-Replayed"))
	assert.Equal(t, int32(1), execCount.Load())

	var env1 orderEnvelope
	require.NoError(t, json.Unmarshal(w1.Body.Bytes(), &env1))
	assert.Equal(t, 0, env1.Code)
	assert.Equal(t, "ord-1", env1.Data.OrderID)
	assert.Equal(t, "book", env1.Data.Item)
	assert.Equal(t, 2, env1.Data.Qty)

	// 2. Replay with same key & payload returns cached 201 without invoking handler.
	req2 := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{"item":"book","qty":2}`))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("Idempotency-Key", "order-key-1")
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, req2)
	assert.Equal(t, http.StatusCreated, w2.Code)
	assert.Equal(t, "true", w2.Header().Get("X-Idempotent-Replayed"))
	assert.Equal(t, w1.Body.String(), w2.Body.String())
	assert.Equal(t, int32(1), execCount.Load())

	// 3. Different payload with same key returns 422 Unprocessable Entity.
	req3 := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{"item":"pen","qty":5}`))
	req3.Header.Set("Content-Type", "application/json")
	req3.Header.Set("Idempotency-Key", "order-key-1")
	w3 := httptest.NewRecorder()
	h.ServeHTTP(w3, req3)
	assert.Equal(t, http.StatusUnprocessableEntity, w3.Code)
	assert.Equal(t, int32(1), execCount.Load())

	// 4. 500 error aborts lock so retry succeeds.
	shouldFail.Store(true)
	reqErr := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{"item":"desk","qty":1}`))
	reqErr.Header.Set("Content-Type", "application/json")
	reqErr.Header.Set("Idempotency-Key", "order-key-err")
	wErr := httptest.NewRecorder()
	h.ServeHTTP(wErr, reqErr)
	assert.Equal(t, http.StatusInternalServerError, wErr.Code)

	shouldFail.Store(false)
	reqRetry := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{"item":"desk","qty":1}`))
	reqRetry.Header.Set("Content-Type", "application/json")
	reqRetry.Header.Set("Idempotency-Key", "order-key-err")
	wRetry := httptest.NewRecorder()
	h.ServeHTTP(wRetry, reqRetry)
	assert.Equal(t, http.StatusCreated, wRetry.Code)
	assert.Equal(t, "false", wRetry.Header().Get("X-Idempotent-Replayed"))

	// 5. App.Stop invokes WithEndpointIdempotency's registered store closer.
	assert.False(t, spyStore.closed.Load())
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, app.Stop(stopCtx))
	assert.True(t, spyStore.closed.Load())
}

func TestWithEndpointIdempotency_RespectsEndpointMaxBodyBytes(t *testing.T) {
	app := testkit.New(t)
	spyStore := &spyIdempotencyStore{
		IdempotencyStore: middleware.NewMemoryIdempotencyStore(100),
	}
	app.POST("/small", func(_ *pi.Context, req *createOrderReq) (*createOrderRes, error) {
		return &createOrderRes{OrderID: "1", Item: req.Item, Qty: req.Qty}, nil
	}, pi.WithEndpointMaxBodyBytes(32), pi.WithEndpointIdempotency(middleware.IdempotencyConfig{Store: spyStore}))
	require.NoError(t, app.RouteError())

	bigPayload := `{"item":"` + strings.Repeat("x", 40) + `","qty":1}`
	req := httptest.NewRequest(http.MethodPost, "/small", strings.NewReader(bigPayload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "too-big")
	w := testkit.Request(t, app, req)
	assert.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
	assert.Equal(t, int32(0), spyStore.tryBeginCalls.Load())
}

func TestAppUseIdempotency_AuthMiddlewareOrderingJWTRefreshAndNonCacheable(t *testing.T) {
	spyStore := &spyIdempotencyStore{
		IdempotencyStore: middleware.NewMemoryIdempotencyStore(100),
	}

	app := testkit.New(t)

	// Register auth PiMiddleware BEFORE UseIdempotency.
	app.UseMiddleware(func(next pi.Handler) pi.Handler {
		return func(c *pi.Context) (any, error) {
			authz := c.Header("Authorization")
			switch authz {
			case "Bearer jwt1", "Bearer jwt2":
				c.Context = auth.WithPrincipal(c.Context, auth.Principal{Subject: "alice"})
				return next(c)
			case "Bearer bob":
				c.Context = auth.WithPrincipal(c.Context, auth.Principal{Subject: "bob"})
				return next(c)
			default:
				return nil, apperror.New(apperror.Unauthenticated, http.StatusUnauthorized, "unauthorized")
			}
		}
	})
	app.UseIdempotency(middleware.IdempotencyConfig{
		Store:    spyStore,
		Required: true,
	})

	var calls atomic.Int32
	app.POST("/orders", func(_ *pi.Context, req *createOrderReq) (*createOrderRes, error) {
		n := calls.Add(1)
		return &createOrderRes{OrderID: "ord-" + string(rune('0'+n)), Item: req.Item, Qty: req.Qty}, nil
	})
	app.POST("/canceled", func(_ *pi.Context, _ *createOrderReq) (*createOrderRes, error) {
		n := calls.Add(1)
		if n == 1 {
			return nil, context.Canceled
		}
		return &createOrderRes{OrderID: "after-499", Item: "item", Qty: 1}, nil
	})
	require.NoError(t, app.RouteError())

	h, err := app.HTTPHandler()
	require.NoError(t, err)

	// 1. Unauthenticated request returns 401 from authMW BEFORE touching IdempotencyStore.
	rUnauth := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{"item":"book","qty":1}`))
	rUnauth.Header.Set("Content-Type", "application/json")
	rUnauth.Header.Set("Idempotency-Key", "k-unauth")
	wUnauth := httptest.NewRecorder()
	h.ServeHTTP(wUnauth, rUnauth)
	assert.Equal(t, http.StatusUnauthorized, wUnauth.Code)
	assert.Equal(t, int32(0), spyStore.tryBeginCalls.Load())

	// 2. Initial request with jwt1 succeeds for principal "alice".
	r1 := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{"item":"book","qty":1}`))
	r1.Header.Set("Content-Type", "application/json")
	r1.Header.Set("Authorization", "Bearer jwt1")
	r1.Header.Set("Idempotency-Key", "k-refresh")
	w1 := httptest.NewRecorder()
	h.ServeHTTP(w1, r1)
	assert.Equal(t, http.StatusCreated, w1.Code)
	assert.Equal(t, "false", w1.Header().Get("X-Idempotent-Replayed"))
	assert.Equal(t, int32(1), calls.Load())

	// 3. Retry after JWT token refresh (jwt1 -> jwt2, same subject "alice") replays cached 201!
	r2 := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{"item":"book","qty":1}`))
	r2.Header.Set("Content-Type", "application/json")
	r2.Header.Set("Authorization", "Bearer jwt2")
	r2.Header.Set("Idempotency-Key", "k-refresh")
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, r2)
	assert.Equal(t, http.StatusCreated, w2.Code)
	assert.Equal(t, "true", w2.Header().Get("X-Idempotent-Replayed"))
	assert.Equal(t, w1.Body.String(), w2.Body.String())
	assert.Equal(t, int32(1), calls.Load())

	// 4. 499 Client Closed Request is NOT cached; retry runs handler again.
	calls.Store(0)
	rc1 := httptest.NewRequest(http.MethodPost, "/canceled", strings.NewReader(`{"item":"book","qty":1}`))
	rc1.Header.Set("Content-Type", "application/json")
	rc1.Header.Set("Authorization", "Bearer jwt1")
	rc1.Header.Set("Idempotency-Key", "k-499")
	wc1 := httptest.NewRecorder()
	h.ServeHTTP(wc1, rc1)
	assert.Equal(t, 499, wc1.Code)

	rc2 := httptest.NewRequest(http.MethodPost, "/canceled", strings.NewReader(`{"item":"book","qty":1}`))
	rc2.Header.Set("Content-Type", "application/json")
	rc2.Header.Set("Authorization", "Bearer jwt1")
	rc2.Header.Set("Idempotency-Key", "k-499")
	wc2 := httptest.NewRecorder()
	h.ServeHTTP(wc2, rc2)
	assert.Equal(t, http.StatusCreated, wc2.Code)
	assert.Equal(t, "false", wc2.Header().Get("X-Idempotent-Replayed"))
}

func TestAppAndGroupUseIdempotency_RequiredRedisIsolationAndInFlight(t *testing.T) {
	t.Run("App.UseIdempotency with Required true", func(t *testing.T) {
		app := testkit.New(t)
		app.UseIdempotency(middleware.IdempotencyConfig{Required: true})
		app.GET("/ping", func(_ *pi.Context) (any, error) {
			return map[string]string{"status": "ok"}, nil
		})
		app.POST("/items", func(_ *pi.Context, req *createOrderReq) (*createOrderRes, error) {
			return &createOrderRes{OrderID: "1", Item: req.Item, Qty: req.Qty}, nil
		})
		require.NoError(t, app.RouteError())

		// GET is unaffected by Required: true.
		wGet := testkit.Request(t, app, httptest.NewRequest(http.MethodGet, "/ping", http.NoBody))
		assert.Equal(t, http.StatusOK, wGet.Code)

		// POST without Idempotency-Key returns 400.
		reqNoKey := httptest.NewRequest(http.MethodPost, "/items", strings.NewReader(`{"item":"lamp","qty":1}`))
		reqNoKey.Header.Set("Content-Type", "application/json")
		wNoKey := testkit.Request(t, app, reqNoKey)
		assert.Equal(t, http.StatusBadRequest, wNoKey.Code)
	})

	t.Run("RouteGroup.UseIdempotency with Redis store, cross-user isolation, and concurrent 409", func(t *testing.T) {
		_, client := newTestPiRedisClient(t)
		store := middleware.NewRedisIdempotencyStore(client, "pi:test:idem:")

		app := testkit.New(t)
		v1 := app.Group("/v1")
		v1.UseIdempotency(middleware.IdempotencyConfig{
			Store: store,
		})

		started := make(chan struct{})
		release := make(chan struct{})
		var gateOnce sync.Once
		var calls atomic.Int32

		v1.POST("/payments", func(_ *pi.Context, req *createOrderReq) (*createOrderRes, error) {
			n := calls.Add(1)
			if req.Item == "block" {
				gateOnce.Do(func() { close(started) })
				<-release
			}
			return &createOrderRes{OrderID: "pay-" + string(rune('0'+n)), Item: req.Item, Qty: req.Qty}, nil
		})
		require.NoError(t, app.RouteError())

		h, err := app.HTTPHandler()
		require.NoError(t, err)

		// Cross-user isolation on Redis store.
		rA := httptest.NewRequest(http.MethodPost, "/v1/payments", strings.NewReader(`{"item":"a","qty":1}`))
		rA = rA.WithContext(auth.WithPrincipal(rA.Context(), auth.Principal{Subject: "alice"}))
		rA.Header.Set("Content-Type", "application/json")
		rA.Header.Set("Idempotency-Key", "shared-k")
		wA := httptest.NewRecorder()
		h.ServeHTTP(wA, rA)
		assert.Equal(t, http.StatusCreated, wA.Code)
		assert.Equal(t, "false", wA.Header().Get("X-Idempotent-Replayed"))

		rB := httptest.NewRequest(http.MethodPost, "/v1/payments", strings.NewReader(`{"item":"b","qty":2}`))
		rB = rB.WithContext(auth.WithPrincipal(rB.Context(), auth.Principal{Subject: "bob"}))
		rB.Header.Set("Content-Type", "application/json")
		rB.Header.Set("Idempotency-Key", "shared-k")
		wB := httptest.NewRecorder()
		h.ServeHTTP(wB, rB)
		assert.Equal(t, http.StatusCreated, wB.Code)
		assert.Equal(t, "false", wB.Header().Get("X-Idempotent-Replayed"))
		assert.Equal(t, int32(2), calls.Load())

		// Concurrent in-flight request returns 409 Conflict.
		var wg sync.WaitGroup
		wg.Add(1)
		wInflight1 := httptest.NewRecorder()
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "/v1/payments", strings.NewReader(`{"item":"block","qty":1}`))
			req = req.WithContext(auth.WithPrincipal(req.Context(), auth.Principal{Subject: "carol"}))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", "inflight-k")
			h.ServeHTTP(wInflight1, req)
		}()

		<-started
		rInflight2 := httptest.NewRequest(http.MethodPost, "/v1/payments", strings.NewReader(`{"item":"block","qty":1}`))
		rInflight2 = rInflight2.WithContext(auth.WithPrincipal(rInflight2.Context(), auth.Principal{Subject: "carol"}))
		rInflight2.Header.Set("Content-Type", "application/json")
		rInflight2.Header.Set("Idempotency-Key", "inflight-k")
		wInflight2 := httptest.NewRecorder()
		h.ServeHTTP(wInflight2, rInflight2)
		assert.Equal(t, http.StatusConflict, wInflight2.Code)

		close(release)
		wg.Wait()
		assert.Equal(t, http.StatusCreated, wInflight1.Code)
	})
}

func TestIdempotency_ZeroBoilerplateAutoRedisSharedMemoryAndConfigValidation(t *testing.T) {
	t.Run("zero-arg WithEndpointIdempotency shares one default memory store across endpoints", func(t *testing.T) {
		app := testkit.New(t)
		var orderCalls, refundCalls atomic.Int32

		app.POST("/orders", func(_ *pi.Context, req *createOrderReq) (*createOrderRes, error) {
			orderCalls.Add(1)
			return &createOrderRes{OrderID: "ord-1", Item: req.Item, Qty: req.Qty}, nil
		}, pi.WithEndpointIdempotency())

		app.POST("/refunds", func(_ *pi.Context, req *createOrderReq) (*createOrderRes, error) {
			refundCalls.Add(1)
			return &createOrderRes{OrderID: "ref-1", Item: req.Item, Qty: req.Qty}, nil
		}, pi.WithEndpointIdempotency())
		require.NoError(t, app.RouteError())

		h, err := app.HTTPHandler()
		require.NoError(t, err)

		r1 := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{"item":"book","qty":1}`))
		r1.Header.Set("Content-Type", "application/json")
		r1.Header.Set("Idempotency-Key", "shared-default-key")
		w1 := httptest.NewRecorder()
		h.ServeHTTP(w1, r1)
		assert.Equal(t, http.StatusCreated, w1.Code)
		assert.Equal(t, "false", w1.Header().Get("X-Idempotent-Replayed"))

		// Reusing the same Idempotency-Key on a different endpoint hits the shared store and returns 422.
		r2 := httptest.NewRequest(http.MethodPost, "/refunds", strings.NewReader(`{"item":"book","qty":1}`))
		r2.Header.Set("Content-Type", "application/json")
		r2.Header.Set("Idempotency-Key", "shared-default-key")
		w2 := httptest.NewRecorder()
		h.ServeHTTP(w2, r2)
		assert.Equal(t, http.StatusUnprocessableEntity, w2.Code)
		assert.Equal(t, int32(1), orderCalls.Load())
		assert.Equal(t, int32(0), refundCalls.Load())
	})

	t.Run("zero-arg UseIdempotency and WithEndpointIdempotency auto-resolve app.Redis()", func(t *testing.T) {
		mr, client := newTestPiRedisClient(t)
		app, err := pi.Build(pi.WithRedis(client, pi.Borrowed))
		require.NoError(t, err)
		t.Cleanup(func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = app.Stop(stopCtx)
		})

		var calls atomic.Int32
		app.POST("/auto-redis", func(_ *pi.Context, req *createOrderReq) (*createOrderRes, error) {
			calls.Add(1)
			return &createOrderRes{OrderID: "r-1", Item: req.Item, Qty: req.Qty}, nil
		}, pi.WithEndpointIdempotency())
		require.NoError(t, app.RouteError())

		h, err := app.HTTPHandler()
		require.NoError(t, err)

		r1 := httptest.NewRequest(http.MethodPost, "/auto-redis", strings.NewReader(`{"item":"pen","qty":3}`))
		r1.Header.Set("Content-Type", "application/json")
		r1.Header.Set("Idempotency-Key", "redis-auto-k")
		w1 := httptest.NewRecorder()
		h.ServeHTTP(w1, r1)
		assert.Equal(t, http.StatusCreated, w1.Code)
		assert.Equal(t, "false", w1.Header().Get("X-Idempotent-Replayed"))

		// Key must be persisted in miniredis under pi:idem: prefix.
		assert.NotEmpty(t, mr.Keys())
		hasIdemPrefix := false
		for _, k := range mr.Keys() {
			if strings.HasPrefix(k, "pi:idem:") {
				hasIdemPrefix = true
				break
			}
		}
		assert.True(t, hasIdemPrefix)

		r2 := httptest.NewRequest(http.MethodPost, "/auto-redis", strings.NewReader(`{"item":"pen","qty":3}`))
		r2.Header.Set("Content-Type", "application/json")
		r2.Header.Set("Idempotency-Key", "redis-auto-k")
		w2 := httptest.NewRecorder()
		h.ServeHTTP(w2, r2)
		assert.Equal(t, http.StatusCreated, w2.Code)
		assert.Equal(t, "true", w2.Header().Get("X-Idempotent-Replayed"))
		assert.Equal(t, int32(1), calls.Load())
	})

	t.Run("pi re-exports and rejecting multiple configs", func(t *testing.T) {
		mem := pi.NewMemoryIdempotencyStore()
		require.NoError(t, mem.Close())
		_, client := newTestPiRedisClient(t)
		rs := pi.NewRedisIdempotencyStore(client)
		require.NoError(t, rs.Close())

		cfg := pi.IdempotencyConfig{Required: true}
		app1 := testkit.New(t)
		app1.UseIdempotency(cfg, cfg)
		require.Error(t, app1.RouteError())

		app2 := testkit.New(t)
		app2.Group("/v1").UseIdempotency(cfg, cfg)
		require.Error(t, app2.RouteError())

		app3 := testkit.New(t)
		app3.POST("/dup", func(_ *pi.Context, _ *createOrderReq) (*createOrderRes, error) {
			return &createOrderRes{}, nil
		}, pi.WithEndpointIdempotency(cfg, cfg))
		require.Error(t, app3.RouteError())
	})
}

func TestIdempotency_LateCancelPreservationInitial4xxErrAndSpecialResponses(t *testing.T) {
	t.Run("initial cacheable 4xx preserves handlerErr for outer middleware and still replays", func(t *testing.T) {
		app := testkit.New(t)
		var observedOuterErr atomic.Value
		var calls atomic.Int32

		app.UseMiddleware(func(next pi.Handler) pi.Handler {
			return func(c *pi.Context) (any, error) {
				res, err := next(c)
				if err != nil {
					observedOuterErr.Store(err.Error())
				}
				return res, err
			}
		})
		app.POST("/bad", func(_ *pi.Context, _ *createOrderReq) (*createOrderRes, error) {
			calls.Add(1)
			return nil, apperror.New(apperror.InvalidArgument, 40001, "invalid quantity")
		}, pi.WithEndpointIdempotency())
		require.NoError(t, app.RouteError())

		h, err := app.HTTPHandler()
		require.NoError(t, err)

		r1 := httptest.NewRequest(http.MethodPost, "/bad", strings.NewReader(`{"item":"book","qty":1}`))
		r1.Header.Set("Content-Type", "application/json")
		r1.Header.Set("Idempotency-Key", "k-400-preserve")
		w1 := httptest.NewRecorder()
		h.ServeHTTP(w1, r1)
		assert.Equal(t, http.StatusBadRequest, w1.Code)
		assert.Equal(t, "false", w1.Header().Get("X-Idempotent-Replayed"))
		assert.Equal(t, "invalid quantity", observedOuterErr.Load())

		r2 := httptest.NewRequest(http.MethodPost, "/bad", strings.NewReader(`{"item":"book","qty":1}`))
		r2.Header.Set("Content-Type", "application/json")
		r2.Header.Set("Idempotency-Key", "k-400-preserve")
		w2 := httptest.NewRecorder()
		h.ServeHTTP(w2, r2)
		assert.Equal(t, http.StatusBadRequest, w2.Code)
		assert.Equal(t, "true", w2.Header().Get("X-Idempotent-Replayed"))
		assert.Equal(t, w1.Body.String(), w2.Body.String())
		assert.Equal(t, int32(1), calls.Load())
	})

	t.Run("late cancellation after next succeeds still commits idempotency record", func(t *testing.T) {
		store := pi.NewMemoryIdempotencyStore()
		app := testkit.New(t)
		var calls atomic.Int32
		reqCtx, cancelReq := context.WithCancel(context.Background())

		app.POST("/late-cancel", func(_ *pi.Context, req *createOrderReq) (*createOrderRes, error) {
			n := calls.Add(1)
			if n == 1 {
				// Simulate client disconnect / timeout firing right as mutation finishes.
				cancelReq()
			}
			return &createOrderRes{OrderID: "ord-late", Item: req.Item, Qty: req.Qty}, nil
		}, pi.WithEndpointIdempotency(pi.IdempotencyConfig{Store: store}))
		require.NoError(t, app.RouteError())

		h, err := app.HTTPHandler()
		require.NoError(t, err)

		r1 := httptest.NewRequest(http.MethodPost, "/late-cancel", strings.NewReader(`{"item":"book","qty":1}`)).WithContext(reqCtx)
		r1.Header.Set("Content-Type", "application/json")
		r1.Header.Set("Idempotency-Key", "k-late")
		w1 := httptest.NewRecorder()
		h.ServeHTTP(w1, r1)
		assert.Equal(t, int32(1), calls.Load())

		// Retry must replay the committed result instead of executing the mutation a second time.
		r2 := httptest.NewRequest(http.MethodPost, "/late-cancel", strings.NewReader(`{"item":"book","qty":1}`))
		r2.Header.Set("Content-Type", "application/json")
		r2.Header.Set("Idempotency-Key", "k-late")
		w2 := httptest.NewRecorder()
		h.ServeHTTP(w2, r2)
		assert.Equal(t, http.StatusCreated, w2.Code)
		assert.Equal(t, "true", w2.Header().Get("X-Idempotent-Replayed"))
		assert.Equal(t, int32(1), calls.Load())
	})

	t.Run("File and SSE special responses are non-cacheable", func(t *testing.T) {
		app := testkit.New(t)
		var fileCalls, sseCalls atomic.Int32

		app.POST("/export", func(_ *pi.Context) (any, error) {
			fileCalls.Add(1)
			return response.File{
				Content:     []byte("csv-data"),
				ContentType: "text/csv",
			}, nil
		}, pi.WithEndpointIdempotency())

		app.POST("/events", func(_ *pi.Context) (any, error) {
			sseCalls.Add(1)
			return response.NewSSE(func(_ context.Context, sse *response.SSEWriter) error {
				return sse.SendData("hello")
			}, response.WithSSEKeepAlive(0)), nil
		}, pi.WithEndpointIdempotency())
		require.NoError(t, app.RouteError())

		h, err := app.HTTPHandler()
		require.NoError(t, err)

		for i := 0; i < 2; i++ {
			req := httptest.NewRequest(http.MethodPost, "/export", http.NoBody)
			req.Header.Set("Idempotency-Key", "k-file")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			assert.Equal(t, http.StatusCreated, w.Code)
			assert.Equal(t, "csv-data", w.Body.String())

			sseReq := httptest.NewRequest(http.MethodPost, "/events", http.NoBody)
			sseReq.Header.Set("Idempotency-Key", "k-sse")
			sseW := httptest.NewRecorder()
			h.ServeHTTP(sseW, sseReq)
			assert.Equal(t, http.StatusOK, sseW.Code)
			assert.Contains(t, sseW.Body.String(), "data: hello")
		}
		assert.Equal(t, int32(2), fileCalls.Load())
		assert.Equal(t, int32(2), sseCalls.Load())
	})
}

type idempotencyTimeoutNetErr struct{}

func (idempotencyTimeoutNetErr) Error() string   { return "i/o timeout" }
func (idempotencyTimeoutNetErr) Timeout() bool   { return true }
func (idempotencyTimeoutNetErr) Temporary() bool { return false }

type idempotencyHookReadCloser struct {
	read func([]byte) (int, error)
}

func (r idempotencyHookReadCloser) Read(p []byte) (int, error) { return r.read(p) }
func (r idempotencyHookReadCloser) Close() error               { return nil }

func TestWithEndpointIdempotency_BodyReadTimeoutAndCancel(t *testing.T) {
	spyStore := &spyIdempotencyStore{
		IdempotencyStore: middleware.NewMemoryIdempotencyStore(100),
	}
	app := testkit.New(t)
	var calls atomic.Int32
	app.POST("/orders", func(_ *pi.Context, req *createOrderReq) (*createOrderRes, error) {
		calls.Add(1)
		return &createOrderRes{OrderID: "ord-1", Item: req.Item, Qty: req.Qty}, nil
	}, pi.WithEndpointIdempotency(middleware.IdempotencyConfig{Store: spyStore}))
	require.NoError(t, app.RouteError())

	h, err := app.HTTPHandler()
	require.NoError(t, err)

	// Slow body read timeout returns 408 without touching the idempotency store.
	timeoutReq := httptest.NewRequest(http.MethodPost, "/orders", idempotencyHookReadCloser{
		read: func([]byte) (int, error) { return 0, idempotencyTimeoutNetErr{} },
	})
	timeoutReq.Header.Set("Content-Type", "application/json")
	timeoutReq.Header.Set("Idempotency-Key", "idem-timeout-cancel")
	wTimeout := httptest.NewRecorder()
	h.ServeHTTP(wTimeout, timeoutReq)
	assert.Equal(t, http.StatusRequestTimeout, wTimeout.Code)
	assert.Equal(t, int32(0), spyStore.tryBeginCalls.Load())

	// Context canceled while reading body returns 499 without touching the idempotency store.
	reqCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cancelReq := httptest.NewRequest(http.MethodPost, "/orders", idempotencyHookReadCloser{
		read: func([]byte) (int, error) {
			cancel()
			return 0, reqCtx.Err()
		},
	}).WithContext(reqCtx)
	cancelReq.Header.Set("Content-Type", "application/json")
	cancelReq.Header.Set("Idempotency-Key", "idem-timeout-cancel")
	wCancel := httptest.NewRecorder()
	h.ServeHTTP(wCancel, cancelReq)
	assert.Equal(t, piHTTP.StatusClientClosedRequest, wCancel.Code)
	assert.Equal(t, int32(0), spyStore.tryBeginCalls.Load())

	// Subsequent valid retry with the same Idempotency-Key succeeds.
	okReq := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{"item":"book","qty":1}`))
	okReq.Header.Set("Content-Type", "application/json")
	okReq.Header.Set("Idempotency-Key", "idem-timeout-cancel")
	wOK := httptest.NewRecorder()
	h.ServeHTTP(wOK, okReq)
	assert.Equal(t, http.StatusCreated, wOK.Code)
	assert.Equal(t, "false", wOK.Header().Get("X-Idempotent-Replayed"))
	assert.Equal(t, int32(1), calls.Load())
	assert.Equal(t, int32(1), spyStore.tryBeginCalls.Load())
}
