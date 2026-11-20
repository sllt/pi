package pi_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sllt/pi/pkg/pi"
	"github.com/sllt/pi/pkg/pi/auth"
	"github.com/sllt/pi/pkg/pi/http/middleware"
	"github.com/sllt/pi/pkg/pi/testkit"
)

type spyRateLimitMetrics struct {
	mu         sync.Mutex
	registered map[string]bool
	increments map[string]int
	lastLabels []string
}

func newSpyRateLimitMetrics() *spyRateLimitMetrics {
	return &spyRateLimitMetrics{
		registered: make(map[string]bool),
		increments: make(map[string]int),
	}
}

func (m *spyRateLimitMetrics) NewCounter(name, _ string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.registered[name] = true
}

func (m *spyRateLimitMetrics) NewUpDownCounter(string, string)                                {}
func (m *spyRateLimitMetrics) NewHistogram(string, string, ...float64)                        {}
func (m *spyRateLimitMetrics) NewGauge(string, string)                                        {}
func (m *spyRateLimitMetrics) DeltaUpDownCounter(context.Context, string, float64, ...string) {}
func (m *spyRateLimitMetrics) RecordHistogram(context.Context, string, float64, ...string)    {}
func (m *spyRateLimitMetrics) SetGauge(string, float64, ...string)                            {}

func (m *spyRateLimitMetrics) IncrementCounter(_ context.Context, name string, labels ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.increments[name]++
	m.lastLabels = append([]string(nil), labels...)
}

func (m *spyRateLimitMetrics) count(name string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.increments[name]
}

func (m *spyRateLimitMetrics) hasCounter(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.registered[name]
}

type spyRateLimitStore struct {
	middleware.RateLimitStore
	allowCalls atomic.Int32
	closed     atomic.Bool
	forceErr   atomic.Bool
}

func (s *spyRateLimitStore) Allow(
	ctx context.Context,
	key string,
	limit int,
	window time.Duration,
	burst int,
) (middleware.RateLimitDecision, error) {
	s.allowCalls.Add(1)
	if s.forceErr.Load() {
		return middleware.RateLimitDecision{}, errors.New("store failure")
	}
	return s.RateLimitStore.Allow(ctx, key, limit, window, burst)
}

func (s *spyRateLimitStore) Close() error {
	s.closed.Store(true)
	if s.RateLimitStore != nil {
		return s.RateLimitStore.Close()
	}
	return nil
}

type rlPingRes struct {
	OK bool `json:"ok"`
}

func TestAppEnvAndContextEnv(t *testing.T) {
	var nilApp *pi.App
	assert.Equal(t, "", nilApp.Env("ANY").Raw())
	assert.Equal(t, "def", nilApp.Env("ANY").String("def"))

	var nilCtx *pi.Context
	assert.Equal(t, "", nilCtx.Env("ANY").Raw())
	assert.Equal(t, 42, nilCtx.Env("ANY").Int(42))
	assert.Equal(t, "", nilCtx.ClientIP())

	app, err := pi.Build(pi.WithConfig(map[string]string{
		"RATE_LIMIT":  "150",
		"FEATURE_ON":  "true",
		"TIMEOUT_VAL": "2s",
		"LIST_VAL":    " a , b , ",
	}))
	require.NoError(t, err)

	assert.Equal(t, 150, app.Env("RATE_LIMIT").Int())
	assert.True(t, app.Env("FEATURE_ON").Bool())
	assert.Equal(t, 2*time.Second, app.Env("TIMEOUT_VAL").Duration())
	assert.Equal(t, []string{"a", "b"}, app.Env("LIST_VAL").Strings())
	assert.Equal(t, "fallback", app.Env("MISSING").String("fallback"))

	app.GET("/env", func(c *pi.Context) (any, error) {
		return map[string]any{
			"limit":   c.Env("RATE_LIMIT").Int(),
			"enabled": c.Env("FEATURE_ON").Bool(),
			"missing": c.Env("MISSING").Int(77),
		}, nil
	})
	require.NoError(t, app.RouteError())

	w := testkit.Request(t, app, httptest.NewRequest(http.MethodGet, "/env", http.NoBody))
	require.Equal(t, http.StatusOK, w.Code)

	var resp struct {
		Code int `json:"code"`
		Data struct {
			Limit   int  `json:"limit"`
			Enabled bool `json:"enabled"`
			Missing int  `json:"missing"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, 150, resp.Data.Limit)
	assert.True(t, resp.Data.Enabled)
	assert.Equal(t, 77, resp.Data.Missing)
}

func TestTrustedProxies_BuildNewAndClientIP(t *testing.T) {
	t.Run("Build fails on invalid WithTrustedProxies and invalid TRUSTED_PROXIES", func(t *testing.T) {
		_, err := pi.Build(pi.WithTrustedProxies("10.0.0.0/8", "bad-cidr"))
		require.Error(t, err)

		_, err = pi.Build(pi.WithConfig(map[string]string{
			"TRUSTED_PROXIES": "127.0.0.1, invalid-ip",
		}))
		require.Error(t, err)

		// Explicit WithTrustedProxies() overrides invalid/non-empty TRUSTED_PROXIES? No, config still has invalid only if parsed;
		// WithTrustedProxies() with valid/empty overrides valid TRUSTED_PROXIES.
		app, err := pi.Build(
			pi.WithConfig(map[string]string{"TRUSTED_PROXIES": "10.0.0.0/8"}),
			pi.WithTrustedProxies(),
		)
		require.NoError(t, err)
		app.GET("/ip", func(c *pi.Context) (any, error) {
			return map[string]string{"ip": c.ClientIP()}, nil
		})
		req := httptest.NewRequest(http.MethodGet, "/ip", http.NoBody)
		req.RemoteAddr = "10.0.0.5:1234"
		req.Header.Set("X-Forwarded-For", "203.0.113.9")
		w := testkit.Request(t, app, req)
		require.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "10.0.0.5")
	})

	t.Run("WithTrustedProxies and TRUSTED_PROXIES config and per-call override", func(t *testing.T) {
		app, err := pi.Build(pi.WithConfig(map[string]string{
			"TRUSTED_PROXIES": "10.0.0.0/8, 127.0.0.1",
		}))
		require.NoError(t, err)

		app.GET("/ip", func(c *pi.Context) (any, error) {
			return map[string]string{
				"app_ip":      c.ClientIP(),
				"override_ip": c.ClientIP("192.168.0.0/16"),
				"invalid_ip":  c.ClientIP("not-a-cidr"),
			}, nil
		})
		require.NoError(t, app.RouteError())

		req := httptest.NewRequest(http.MethodGet, "/ip", http.NoBody)
		req.RemoteAddr = "10.0.0.2:9000"
		req.Header.Set("X-Forwarded-For", "198.51.100.42, 10.0.0.8")
		w := testkit.Request(t, app, req)
		require.Equal(t, http.StatusOK, w.Code)

		var resp struct {
			Data map[string]string `json:"data"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, "198.51.100.42", resp.Data["app_ip"])
		assert.Equal(t, "10.0.0.2", resp.Data["override_ip"])
		assert.Equal(t, "10.0.0.2", resp.Data["invalid_ip"])
	})

	t.Run("pi.New reads TRUSTED_PROXIES env and surfaces invalid CIDR", func(t *testing.T) {
		t.Setenv("TRUSTED_PROXIES", "10.0.0.0/8")
		app := pi.New()
		t.Cleanup(func() { _ = app.Stop(context.Background()) })
		app.GET("/ip", func(c *pi.Context) (any, error) {
			return map[string]string{"ip": c.ClientIP()}, nil
		})
		require.NoError(t, app.RouteError())

		req := httptest.NewRequest(http.MethodGet, "/ip", http.NoBody)
		req.RemoteAddr = "10.0.0.9:1111"
		req.Header.Set("X-Forwarded-For", "203.0.113.77, 10.0.0.1")
		w := testkit.Request(t, app, req)
		require.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "203.0.113.77")

		t.Setenv("TRUSTED_PROXIES", "bad/cidr")
		badApp := pi.New()
		t.Cleanup(func() { _ = badApp.Stop(context.Background()) })
		require.Error(t, badApp.RouteError())
	})
}

func TestRateLimit_AppGroupEndpointDefaultsHeadersMetricsAndGoroutinePurity(t *testing.T) {
	t.Run("Build + UseRateLimit stays goroutine-pure before Start and records 429 headers + metric", func(t *testing.T) {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
		beforeG := runtime.NumGoroutine()

		m := newSpyRateLimitMetrics()
		app, err := pi.Build(
			pi.WithMetrics(m),
			pi.WithTrustedProxies("10.0.0.0/8"),
		)
		require.NoError(t, err)

		// Default Window=1m and Burst=Limit=2.
		app.UseRateLimit(pi.RateLimitConfig{Limit: 2})
		app.GET("/items", func(*pi.Context) (any, error) {
			return map[string]bool{"ok": true}, nil
		})
		require.NoError(t, app.RouteError())

		time.Sleep(10 * time.Millisecond)
		afterBuildG := runtime.NumGoroutine()
		assert.LessOrEqual(t, afterBuildG, beforeG+1, "Build + UseRateLimit must not spawn background goroutines before Start")
		assert.True(t, m.hasCounter("app_http_rate_limit_exceeded_total"))

		h, err := app.HTTPHandler()
		require.NoError(t, err)

		// Untrusted peer cannot spoof XFF to bypass rate limit.
		for i := 0; i < 2; i++ {
			req := httptest.NewRequest(http.MethodGet, "/items", http.NoBody)
			req.RemoteAddr = "198.51.100.1:1000"
			req.Header.Set("X-Forwarded-For", "203.0.113."+strconv.Itoa(i+1))
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
		}

		reqBlocked := httptest.NewRequest(http.MethodGet, "/items", http.NoBody)
		reqBlocked.RemoteAddr = "198.51.100.1:1001"
		reqBlocked.Header.Set("X-Forwarded-For", "203.0.113.99")
		wBlocked := httptest.NewRecorder()
		h.ServeHTTP(wBlocked, reqBlocked)

		assert.Equal(t, http.StatusTooManyRequests, wBlocked.Code)
		assert.NotEmpty(t, wBlocked.Header().Get("Retry-After"))
		assert.Equal(t, "2", wBlocked.Header().Get("X-RateLimit-Limit"))
		assert.Equal(t, "0", wBlocked.Header().Get("X-RateLimit-Remaining"))
		assert.Equal(t, 1, m.count("app_http_rate_limit_exceeded_total"))

		var env struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		}
		require.NoError(t, json.Unmarshal(wBlocked.Body.Bytes(), &env))
		assert.Equal(t, http.StatusTooManyRequests, env.Code)
		assert.Equal(t, "rate limit exceeded", env.Message)

		// Trusted proxy strips trusted hop and uses client IP bucket.
		reqTrusted := httptest.NewRequest(http.MethodGet, "/items", http.NoBody)
		reqTrusted.RemoteAddr = "10.0.0.4:2000"
		reqTrusted.Header.Set("X-Forwarded-For", "203.0.113.200, 10.0.0.5")
		wTrusted := httptest.NewRecorder()
		h.ServeHTTP(wTrusted, reqTrusted)
		assert.Equal(t, http.StatusOK, wTrusted.Code)

		require.NoError(t, app.Stop(context.Background()))
	})

	t.Run("RouteGroup and WithEndpointRateLimit isolate buckets and validate config", func(t *testing.T) {
		app := testkit.New(t)
		api := app.Group("/api")
		api.UseRateLimit(pi.RateLimitConfig{Limit: 5, Window: time.Minute})

		api.GET("/a", func(*pi.Context) (any, error) { return "a", nil },
			pi.WithEndpointRateLimit(pi.RateLimitConfig{Limit: 1}))
		api.GET("/b", func(*pi.Context) (any, error) { return "b", nil },
			pi.WithEndpointRateLimit(pi.RateLimitConfig{Limit: 2}))
		api.GET("/c", func(*pi.Context) (any, error) { return "c", nil })
		require.NoError(t, app.RouteError())

		// /a allows 1, blocks 2nd (endpoint limit=1; group tokens: 5->4->3).
		rA1 := httptest.NewRequest(http.MethodGet, "/api/a", http.NoBody)
		rA1.RemoteAddr = "192.0.2.10:1234"
		assert.Equal(t, http.StatusOK, testkit.Request(t, app, rA1).Code)

		rA2 := httptest.NewRequest(http.MethodGet, "/api/a", http.NoBody)
		rA2.RemoteAddr = "192.0.2.10:1234"
		assert.Equal(t, http.StatusTooManyRequests, testkit.Request(t, app, rA2).Code)

		// /b has independent endpoint limit=2 (group tokens: 3->2->1->0).
		rB1 := httptest.NewRequest(http.MethodGet, "/api/b", http.NoBody)
		rB1.RemoteAddr = "192.0.2.10:1234"
		assert.Equal(t, http.StatusOK, testkit.Request(t, app, rB1).Code)

		rB2 := httptest.NewRequest(http.MethodGet, "/api/b", http.NoBody)
		rB2.RemoteAddr = "192.0.2.10:1234"
		assert.Equal(t, http.StatusOK, testkit.Request(t, app, rB2).Code)

		rB3 := httptest.NewRequest(http.MethodGet, "/api/b", http.NoBody)
		rB3.RemoteAddr = "192.0.2.10:1234"
		assert.Equal(t, http.StatusTooManyRequests, testkit.Request(t, app, rB3).Code)

		// Group limit=5 is now exhausted, so /c is also rate limited.
		rC1 := httptest.NewRequest(http.MethodGet, "/api/c", http.NoBody)
		rC1.RemoteAddr = "192.0.2.10:1234"
		assert.Equal(t, http.StatusTooManyRequests, testkit.Request(t, app, rC1).Code)

		// Typed Register + unified route validation errors on unfrozen apps.
		type pingReq struct {
			pi.Meta `route:"GET /typed-ping"`
		}
		appBadReg := testkit.New(t)
		err := pi.Register(appBadReg, func(*pi.Context, *pingReq) (*rlPingRes, error) {
			return &rlPingRes{OK: true}, nil
		}, pi.WithEndpointRateLimit(pi.RateLimitConfig{Limit: 0}))
		require.Error(t, err)
		require.Error(t, appBadReg.RouteError())

		appBadRoute := testkit.New(t)
		appBadRoute.GET("/bad", func(*pi.Context) (any, error) {
			return "bad", nil
		}, pi.WithEndpointRateLimit(pi.RateLimitConfig{Limit: 0}))
		require.Error(t, appBadRoute.RouteError())

		appBad1 := testkit.New(t)
		appBad1.UseRateLimit(pi.RateLimitConfig{Limit: -1})
		require.Error(t, appBad1.RouteError())

		appBad2 := testkit.New(t)
		appBad2.Group("/v1").UseRateLimit(pi.RateLimitConfig{Limit: 5, TrustedProxies: []string{"bad-cidr"}})
		require.Error(t, appBad2.RouteError())
	})

	t.Run("Stacked App.UseRateLimit rules and sibling App.Scope groups do not collide", func(t *testing.T) {
		app := testkit.New(t)
		// Stack two App-level rate limiters with different limits and KeyFuncs;
		// they must have distinct bucket scopes so rule #2 doesn't clobber rule #1.
		app.UseRateLimit(pi.RateLimitConfig{
			Limit:   2,
			Window:  time.Minute,
			KeyFunc: func(*pi.Context) string { return "shared-key" },
		})
		app.UseRateLimit(pi.RateLimitConfig{
			Limit:   5,
			Window:  time.Minute,
			KeyFunc: func(*pi.Context) string { return "shared-key" },
		})

		s1 := app.Scope()
		s1.UseRateLimit(pi.RateLimitConfig{Limit: 1, Window: time.Minute})
		s1.GET("/s1", func(*pi.Context) (any, error) { return "s1", nil })

		s2 := app.Scope()
		s2.UseRateLimit(pi.RateLimitConfig{Limit: 2, Window: time.Minute})
		s2.GET("/s2", func(*pi.Context) (any, error) { return "s2", nil })
		require.NoError(t, app.RouteError())

		// /s1: 1st allowed (app#1: 2->1, app#2: 5->4, s1: 1->0); 2nd blocked by s1 (app#1 still 1->0!).
		r1 := httptest.NewRequest(http.MethodGet, "/s1", http.NoBody)
		r1.RemoteAddr = "192.0.2.80:1000"
		assert.Equal(t, http.StatusOK, testkit.Request(t, app, r1).Code)

		r2 := httptest.NewRequest(http.MethodGet, "/s1", http.NoBody)
		r2.RemoteAddr = "192.0.2.80:1000"
		assert.Equal(t, http.StatusTooManyRequests, testkit.Request(t, app, r2).Code)

		// Separate IP on /s2 verifies sibling Scope groups don't share the same bucket.
		// Using another App for Scope-only isolation check without stacked App limiters:
		appScopes := testkit.New(t)
		scA := appScopes.Scope()
		scA.UseRateLimit(pi.RateLimitConfig{Limit: 1, Window: time.Minute})
		scA.GET("/a", func(*pi.Context) (any, error) { return "a", nil })

		scB := appScopes.Scope()
		scB.UseRateLimit(pi.RateLimitConfig{Limit: 2, Window: time.Minute})
		scB.GET("/b", func(*pi.Context) (any, error) { return "b", nil })
		require.NoError(t, appScopes.RouteError())

		reqA1 := httptest.NewRequest(http.MethodGet, "/a", http.NoBody)
		reqA1.RemoteAddr = "192.0.2.81:1000"
		assert.Equal(t, http.StatusOK, testkit.Request(t, appScopes, reqA1).Code)

		reqA2 := httptest.NewRequest(http.MethodGet, "/a", http.NoBody)
		reqA2.RemoteAddr = "192.0.2.81:1000"
		assert.Equal(t, http.StatusTooManyRequests, testkit.Request(t, appScopes, reqA2).Code)

		// Sibling Scope scB still has its full Limit=2 for the same client IP.
		reqB1 := httptest.NewRequest(http.MethodGet, "/b", http.NoBody)
		reqB1.RemoteAddr = "192.0.2.81:1000"
		assert.Equal(t, http.StatusOK, testkit.Request(t, appScopes, reqB1).Code)

		reqB2 := httptest.NewRequest(http.MethodGet, "/b", http.NoBody)
		reqB2.RemoteAddr = "192.0.2.81:1000"
		assert.Equal(t, http.StatusOK, testkit.Request(t, appScopes, reqB2).Code)

		reqB3 := httptest.NewRequest(http.MethodGet, "/b", http.NoBody)
		reqB3.RemoteAddr = "192.0.2.81:1000"
		assert.Equal(t, http.StatusTooManyRequests, testkit.Request(t, appScopes, reqB3).Code)
	})
}

func TestRateLimitByUser_AuthFallbackAndRedisLifecycle(t *testing.T) {
	t.Run("RateLimitByUser isolates authenticated subjects and falls back to ip:", func(t *testing.T) {
		app := testkit.New(t)
		app.UseRateLimit(pi.RateLimitConfig{
			Limit:   1,
			Window:  time.Minute,
			KeyFunc: pi.RateLimitByUser,
		})
		app.GET("/me", func(c *pi.Context) (any, error) {
			return map[string]string{"key": pi.RateLimitByUser(c)}, nil
		})
		require.NoError(t, app.RouteError())

		// Nil context is safe.
		assert.Equal(t, "ip:unknown", pi.RateLimitByUser(nil))

		// Alice gets 1 request allowed, 2nd blocked.
		rAlice1 := httptest.NewRequest(http.MethodGet, "/me", http.NoBody)
		rAlice1 = rAlice1.WithContext(auth.WithPrincipal(rAlice1.Context(), auth.Principal{Subject: "alice"}))
		rAlice1.RemoteAddr = "192.0.2.50:1111"
		wAlice1 := testkit.Request(t, app, rAlice1)
		assert.Equal(t, http.StatusOK, wAlice1.Code)
		assert.Contains(t, wAlice1.Body.String(), "sub:alice")

		rAlice2 := httptest.NewRequest(http.MethodGet, "/me", http.NoBody)
		rAlice2 = rAlice2.WithContext(auth.WithPrincipal(rAlice2.Context(), auth.Principal{Subject: "alice"}))
		rAlice2.RemoteAddr = "192.0.2.51:2222" // Different IP, same user -> blocked.
		assert.Equal(t, http.StatusTooManyRequests, testkit.Request(t, app, rAlice2).Code)

		// Bob (same IP as Alice's 1st request) has separate user bucket.
		rBob := httptest.NewRequest(http.MethodGet, "/me", http.NoBody)
		rBob = rBob.WithContext(auth.WithPrincipal(rBob.Context(), auth.Principal{Subject: "bob"}))
		rBob.RemoteAddr = "192.0.2.50:3333"
		assert.Equal(t, http.StatusOK, testkit.Request(t, app, rBob).Code)

		// Unauthenticated request falls back to ip:<clientIP>.
		rAnon := httptest.NewRequest(http.MethodGet, "/me", http.NoBody)
		rAnon.RemoteAddr = "192.0.2.50:4444"
		wAnon := testkit.Request(t, app, rAnon)
		assert.Equal(t, http.StatusOK, wAnon.Code)
		assert.Contains(t, wAnon.Body.String(), "ip:192.0.2.50")

		rAnon2 := httptest.NewRequest(http.MethodGet, "/me", http.NoBody)
		rAnon2.RemoteAddr = "192.0.2.50:5555"
		assert.Equal(t, http.StatusTooManyRequests, testkit.Request(t, app, rAnon2).Code)
	})

	t.Run("Nil Store stays local in-memory even when app.Redis() is configured; explicit Redis store uses Redis and is not closed on App.Stop", func(t *testing.T) {
		mr, client := newTestPiRedisClient(t)
		app, err := pi.Build(pi.WithRedis(client, pi.Borrowed))
		require.NoError(t, err)

		// Default Store==nil must stay local in-memory and NOT touch app.Redis().
		app.GET("/local-rl", func(*pi.Context) (any, error) {
			return "ok", nil
		}, pi.WithEndpointRateLimit(pi.RateLimitConfig{Limit: 1, Window: time.Minute}))

		// Explicit Store: pi.NewRedisRateLimitStore(app.Redis()) uses Redis.
		app.GET("/redis-rl", func(*pi.Context) (any, error) {
			return "ok", nil
		}, pi.WithEndpointRateLimit(pi.RateLimitConfig{
			Limit:  1,
			Window: time.Minute,
			Store:  pi.NewRedisRateLimitStore(app.Redis()),
		}))
		require.NoError(t, app.RouteError())

		localReq1 := httptest.NewRequest(http.MethodGet, "/local-rl", http.NoBody)
		localReq1.RemoteAddr = "198.51.100.77:1234"
		assert.Equal(t, http.StatusOK, testkit.Request(t, app, localReq1).Code)
		assert.Empty(t, mr.Keys(), "default Store==nil must not silently switch to app.Redis()")

		localReq2 := httptest.NewRequest(http.MethodGet, "/local-rl", http.NoBody)
		localReq2.RemoteAddr = "198.51.100.77:1234"
		assert.Equal(t, http.StatusTooManyRequests, testkit.Request(t, app, localReq2).Code)

		req1 := httptest.NewRequest(http.MethodGet, "/redis-rl", http.NoBody)
		req1.RemoteAddr = "198.51.100.77:1234"
		assert.Equal(t, http.StatusOK, testkit.Request(t, app, req1).Code)
		assert.NotEmpty(t, mr.Keys())

		req2 := httptest.NewRequest(http.MethodGet, "/redis-rl", http.NoBody)
		req2.RemoteAddr = "198.51.100.77:1234"
		assert.Equal(t, http.StatusTooManyRequests, testkit.Request(t, app, req2).Code)
		require.NoError(t, app.Stop(context.Background()))

		// Explicit user-passed store is not closed on App.Stop; FailClosed returns 503 vs fail-open 200.
		spy := &spyRateLimitStore{RateLimitStore: pi.NewMemoryRateLimitStore(100)}
		appUser := testkit.New(t)
		appUser.UseRateLimit(pi.RateLimitConfig{Limit: 5, Store: spy, FailClosed: true})
		appUser.GET("/fc", func(*pi.Context) (any, error) { return "ok", nil })
		require.NoError(t, appUser.RouteError())

		spy.forceErr.Store(true)
		assert.Equal(t, http.StatusServiceUnavailable, testkit.Request(t, appUser, httptest.NewRequest(http.MethodGet, "/fc", http.NoBody)).Code)
		require.NoError(t, appUser.Stop(context.Background()))
		assert.False(t, spy.closed.Load(), "user-passed store must not be closed by App.Stop")
	})
}
