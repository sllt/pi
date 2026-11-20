package service

import (
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestJoinURL_DoubleSlashAndQueryPreservation(t *testing.T) {
	cases := []struct {
		name string
		base string
		path string
		want string
	}{
		{name: "leading slash on path", base: "http://example.com", path: "/v1/charge", want: "http://example.com/v1/charge"},
		{name: "trailing slash on base and leading slash on path", base: "http://example.com/", path: "/v1/charge", want: "http://example.com/v1/charge"},
		{name: "relative path without leading slash", base: "http://example.com", path: "v1/charge", want: "http://example.com/v1/charge"},
		{name: "empty path preserves base without trailing slash", base: "http://example.com/v1/", path: "", want: "http://example.com/v1"},
		{name: "root slash path preserves base", base: "http://example.com/v1/", path: "/", want: "http://example.com/v1"},
		{name: "path with query string", base: "http://example.com/", path: "/v1/charge?mode=test&a=1", want: "http://example.com/v1/charge?mode=test&a=1"},
		{name: "query-only path", base: "http://example.com/v1/", path: "?mode=test", want: "http://example.com/v1?mode=test"},
		{name: "empty base", base: "", path: "test-path", want: "/test-path"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, joinURL(tc.base, tc.path))
		})
	}
}

func TestRequest_DoubleSlashFixOverHTTP(t *testing.T) {
	var seenURI string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenURI = r.RequestURI
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	svc := NewHTTPService(server.URL+"/", nil, nil)
	defer func() { _ = CloseService(svc) }()

	for _, path := range []string{"/v1/charge", "v1/charge"} {
		resp, err := svc.R().Get(t.Context(), path)
		require.NoError(t, err)
		assert.True(t, resp.OK())
		assert.Equal(t, "/v1/charge", seenURI)
	}

	resp, err := svc.R().Get(t.Context(), "/v1/charge?dry_run=true")
	require.NoError(t, err)
	assert.True(t, resp.OK())
	assert.Equal(t, "/v1/charge?dry_run=true", seenURI)

	resp, err = svc.R().Get(t.Context(), "")
	require.NoError(t, err)
	assert.True(t, resp.OK())
	assert.Equal(t, "/", seenURI)
}

func TestRequest_VerbsHeadersQueryJSONBodyAndResponse(t *testing.T) {
	type payload struct {
		Amount int    `json:"amount"`
		User   string `json:"user"`
	}
	type result struct {
		ID     string `json:"id"`
		Method string `json:"method"`
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rawBody, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		w.Header().Set("X-Trace-Echo", r.Header.Get("X-Trace"))
		w.Header().Set("X-Auth-Echo", r.Header.Get("Authorization"))
		w.Header().Set("X-Content-Type-Echo", r.Header.Get("Content-Type"))
		w.Header().Set("X-Query-Echo", r.URL.RawQuery)
		w.Header().Set("Content-Type", "application/json")

		switch r.Method {
		case http.MethodGet:
			assert.Equal(t, "/v1/items", r.URL.Path)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"get-1","method":"GET"}`))
		case http.MethodPost:
			var got payload
			require.NoError(t, json.Unmarshal(rawBody, &got))
			assert.Equal(t, payload{Amount: 99, User: "alice"}, got)
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"id":"post-1","method":"POST"}`))
		case http.MethodPut:
			assert.Equal(t, "raw-put", string(rawBody))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"put-1","method":"PUT"}`))
		case http.MethodPatch:
			assert.Equal(t, `{"amount":10,"user":"bob"}`, string(rawBody))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"patch-1","method":"PATCH"}`))
		case http.MethodDelete:
			assert.Equal(t, "force=true&tag=a&tag=b", r.URL.RawQuery)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"del-1","method":"DELETE"}`))
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	svc := NewHTTPService(server.URL, nil, nil)
	defer func() { _ = CloseService(svc) }()

	t.Run("GET with headers, bearer and query", func(t *testing.T) {
		resp, err := svc.R().
			Header("X-Trace", "t-get").
			Headers(map[string]string{"X-Extra": "1"}).
			Bearer("tok-get").
			Query("page", 2).
			QueryParams(map[string]any{"tag": []string{"go", "pi"}}).
			Get(t.Context(), "/v1/items")
		require.NoError(t, err)
		assert.True(t, resp.OK())
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "t-get", resp.Header.Get("X-Trace-Echo"))
		assert.Equal(t, "t-get", resp.GetHeader("X-Trace-Echo"))
		assert.Equal(t, "Bearer tok-get", resp.Header.Get("X-Auth-Echo"))
		assert.Equal(t, "page=2&tag=go&tag=pi", resp.Header.Get("X-Query-Echo"))
		assert.Contains(t, string(resp.Bytes()), `"get-1"`)
		assert.Contains(t, resp.String(), `"GET"`)

		var out result
		require.NoError(t, resp.JSON(&out))
		assert.Equal(t, result{ID: "get-1", Method: "GET"}, out)
	})

	t.Run("POST JSON", func(t *testing.T) {
		resp, err := svc.R().
			Bearer("tok-post").
			JSON(payload{Amount: 99, User: "alice"}).
			Post(t.Context(), "/v1/items")
		require.NoError(t, err)
		assert.True(t, resp.OK())
		assert.Equal(t, http.StatusCreated, resp.StatusCode)
		assert.Equal(t, "application/json", resp.Header.Get("X-Content-Type-Echo"))

		var out result
		require.NoError(t, resp.JSON(&out))
		assert.Equal(t, result{ID: "post-1", Method: "POST"}, out)
	})

	t.Run("PUT raw Body with custom content type", func(t *testing.T) {
		resp, err := svc.R().
			Body([]byte("raw-put"), "text/plain; charset=utf-8").
			Put(t.Context(), "/v1/items/1")
		require.NoError(t, err)
		assert.True(t, resp.OK())
		assert.Equal(t, "text/plain; charset=utf-8", resp.Header.Get("X-Content-Type-Echo"))
	})

	t.Run("PATCH JSON with explicit Content-Type preserved", func(t *testing.T) {
		resp, err := svc.R().
			Header("content-type", "application/merge-patch+json").
			JSON(payload{Amount: 10, User: "bob"}).
			Patch(t.Context(), "/v1/items/1")
		require.NoError(t, err)
		assert.True(t, resp.OK())
		assert.Equal(t, "application/merge-patch+json", resp.Header.Get("X-Content-Type-Echo"))
	})

	t.Run("DELETE appends query params cleanly", func(t *testing.T) {
		resp, err := svc.R().
			Query("force", true).
			Query("tag", []string{"a", "b"}).
			Delete(t.Context(), "/v1/items/1")
		require.NoError(t, err)
		assert.True(t, resp.OK())

		var out result
		require.NoError(t, resp.JSON(&out))
		assert.Equal(t, result{ID: "del-1", Method: "DELETE"}, out)
	})
}

func TestRequest_MaxBytesAndResponseJSONErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/large":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(strings.Repeat("a", 64)))
		case "/empty":
			w.WriteHeader(http.StatusNoContent)
		case "/bad-json":
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{broken`))
		}
	}))
	defer server.Close()

	svc := NewHTTPService(server.URL, nil, nil)
	defer func() { _ = CloseService(svc) }()

	_, err := svc.R().MaxBytes(32).Get(t.Context(), "/large")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrResponseTooLarge)

	resp, err := svc.R().MaxBytes(64).Get(t.Context(), "/large")
	require.NoError(t, err)
	assert.Len(t, resp.Bytes(), 64)

	emptyResp, err := svc.R().Get(t.Context(), "/empty")
	require.NoError(t, err)
	assert.True(t, emptyResp.OK())
	var out map[string]any
	err = emptyResp.JSON(&out)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrEmptyResponseBody)

	badResp, err := svc.R().Get(t.Context(), "/bad-json")
	require.NoError(t, err)
	assert.False(t, badResp.OK())
	err = badResp.JSON(&out)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode response json")

	_, err = (&Request{}).Get(t.Context(), "/large")
	require.Error(t, err)

	_, err = svc.R().MaxBytes(0).Get(t.Context(), "/large")
	require.Error(t, err)

	maxResp, err := svc.R().MaxBytes(math.MaxInt64).Get(t.Context(), "/large")
	require.NoError(t, err)
	assert.Len(t, maxResp.Bytes(), 64)
}

func TestRequest_DoesNotBypassDecorators(t *testing.T) {
	t.Run("RetryConfig retries failed POST via R()", func(t *testing.T) {
		var attempts atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, http.MethodPost, r.Method)
			n := attempts.Add(1)
			if n < 3 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
		}))
		defer server.Close()

		svc := NewHTTPService(server.URL, nil, nil, &RetryConfig{MaxRetries: 2})
		defer func() { _ = CloseService(svc) }()

		resp, err := svc.R().JSON(map[string]string{"k": "v"}).Post(t.Context(), "/retry")
		require.NoError(t, err)
		assert.True(t, resp.OK())
		assert.Equal(t, int32(3), attempts.Load())
	})

	t.Run("CircuitBreakerConfig opens on repeated failures via R()", func(t *testing.T) {
		var hits atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer server.Close()

		svc := NewHTTPService(server.URL, nil, nil, &CircuitBreakerConfig{
			Threshold: 1,
			Interval:  time.Minute,
		})
		defer func() { _ = CloseService(svc) }()

		// failureCount=1 <= Threshold(1) returns response; failureCount=2 > Threshold(1) opens circuit.
		resp, err := svc.R().Post(t.Context(), "/cb")
		require.NoError(t, err)
		assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

		_, err = svc.R().Post(t.Context(), "/cb")
		require.ErrorIs(t, err, ErrCircuitOpen)

		before := hits.Load()
		_, err = svc.R().Post(t.Context(), "/cb")
		require.ErrorIs(t, err, ErrCircuitOpen)
		assert.Equal(t, before, hits.Load(), "open circuit must short-circuit without calling server")
	})

	t.Run("DefaultHeaders injects headers via R()", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "pi-client", r.Header.Get("X-Client"))
			assert.Equal(t, "req-1", r.Header.Get("X-Request-ID"))
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		svc := NewHTTPService(server.URL, nil, nil, &DefaultHeaders{
			Headers: map[string]string{"X-Client": "pi-client"},
		})
		defer func() { _ = CloseService(svc) }()

		resp, err := svc.R().Header("X-Request-ID", "req-1").Post(t.Context(), "/headers")
		require.NoError(t, err)
		assert.True(t, resp.OK())
	})

	t.Run("APIKeyConfig injects X-Api-Key via R()", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "secret-key", r.Header.Get("X-Api-Key"))
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		svc := NewHTTPService(server.URL, nil, nil, &APIKeyConfig{APIKey: "secret-key"})
		defer func() { _ = CloseService(svc) }()

		resp, err := svc.R().Post(t.Context(), "/apikey")
		require.NoError(t, err)
		assert.True(t, resp.OK())
	})

	t.Run("BasicAuthConfig injects Authorization header via R()", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			user, pass, ok := r.BasicAuth()
			assert.True(t, ok)
			assert.Equal(t, "alice", user)
			assert.Equal(t, "s3cr3t", pass)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		svc := NewHTTPService(server.URL, nil, nil, &BasicAuthConfig{UserName: "alice", Password: "s3cr3t"})
		defer func() { _ = CloseService(svc) }()

		resp, err := svc.R().Post(t.Context(), "/basic")
		require.NoError(t, err)
		assert.True(t, resp.OK())
	})

	t.Run("RateLimiterConfig enforces rate limit via R()", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		svc := NewHTTPService(server.URL, nil, nil, &RateLimiterConfig{
			Requests: 1,
			Window:   time.Minute,
			Burst:    1,
		})
		defer func() { _ = CloseService(svc) }()

		resp, err := svc.R().Post(t.Context(), "/rl")
		require.NoError(t, err)
		assert.True(t, resp.OK())

		_, err = svc.R().Post(t.Context(), "/rl")
		require.Error(t, err)
		var rlErr *RateLimitError
		assert.ErrorAs(t, err, &rlErr)
	})

	t.Run("Combined decorator stack with HealthConfig outer layer does not bypass", func(t *testing.T) {
		var attempts atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "v1", r.Header.Get("X-Service-Version"))
			assert.Equal(t, "k-123", r.Header.Get("X-Api-Key"))
			if attempts.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		svc := NewHTTPService(
			server.URL,
			nil,
			nil,
			&DefaultHeaders{Headers: map[string]string{"X-Service-Version": "v1"}},
			&APIKeyConfig{APIKey: "k-123"},
			&RetryConfig{MaxRetries: 1},
			&HealthConfig{HealthEndpoint: "/healthz", Timeout: 2},
		)
		defer func() { _ = CloseService(svc) }()

		resp, err := svc.R().Post(t.Context(), "/stacked")
		require.NoError(t, err)
		assert.True(t, resp.OK())
		assert.Equal(t, int32(2), attempts.Load())
	})

	t.Run("MockHTTP works with R() via PostWithHeaders expectation", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockSvc := NewMockHTTP(ctrl)
		mockSvc.EXPECT().PostWithHeaders(
			gomock.Any(),
			"/v1/pay",
			map[string]any{"env": "test"},
			[]byte(`{"amount":50}`),
			map[string]string{"Content-Type": "application/json", "Authorization": "Bearer secret"},
		).Return(&http.Response{
			StatusCode: http.StatusCreated,
			Header:     http.Header{"X-Mock": []string{"yes"}},
			Body:       io.NopCloser(strings.NewReader(`{"paid":true}`)),
		}, nil)

		resp, err := mockSvc.R().
			Bearer("secret").
			Query("env", "test").
			JSON(map[string]int{"amount": 50}).
			Post(t.Context(), "/v1/pay")
		require.NoError(t, err)
		assert.Equal(t, http.StatusCreated, resp.StatusCode)
		assert.Equal(t, "yes", resp.GetHeader("X-Mock"))

		var out map[string]bool
		require.NoError(t, resp.JSON(&out))
		assert.True(t, out["paid"])
	})

	t.Run("RetryConfig + APIKeyConfig + JSON + Header does not collide on retry", func(t *testing.T) {
		var attempts atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "k-retry", r.Header.Get("X-Api-Key"))
			assert.Equal(t, "1", r.Header.Get("X-Custom"))
			assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

			n := attempts.Add(1)
			if n == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"retried":true}`))
		}))
		defer server.Close()

		svc := NewHTTPService(
			server.URL,
			nil,
			nil,
			&APIKeyConfig{APIKey: "k-retry"},
			&RetryConfig{MaxRetries: 2},
		)
		defer func() { _ = CloseService(svc) }()

		resp, err := svc.R().
			JSON(map[string]string{"op": "charge"}).
			Header("X-Custom", "1").
			Post(t.Context(), "/retry-auth")
		require.NoError(t, err)
		assert.True(t, resp.OK())
		assert.Equal(t, int32(2), attempts.Load())

		var out map[string]bool
		require.NoError(t, resp.JSON(&out))
		assert.True(t, out["retried"])
	})

	t.Run("Header canonicalization keeps mixed-case overrides deterministic", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "Bearer y", r.Header.Get("Authorization"))
			assert.Equal(t, []string{"Bearer y"}, r.Header.Values("Authorization"))
			assert.Equal(t, "2", r.Header.Get("X-Custom"))
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		svc := NewHTTPService(server.URL, nil, nil)
		defer func() { _ = CloseService(svc) }()

		resp, err := svc.R().
			Header("authorization", "Basic x").
			Bearer("y").
			Header("x-custom", "1").
			Headers(map[string]string{"X-Custom": "2"}).
			Get(t.Context(), "/canonical")
		require.NoError(t, err)
		assert.True(t, resp.OK())
	})
}
