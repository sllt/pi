package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCORSBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, origin, method, requestMethod, headers string
		code                                         int
		allowed                                      bool
	}{
		{"ordinary", "", "GET", "", "", 200, false},
		{"allowed", "https://app.example", "GET", "", "", 200, true},
		{"denied", "https://evil.example", "GET", "", "", 403, false},
		{"preflight", "https://app.example", "OPTIONS", "POST", "authorization, content-type", 204, true},
		{"bad method", "https://app.example", "OPTIONS", "DELETE", "", 403, false},
		{"bad header", "https://app.example", "OPTIONS", "POST", "X-Secret", 403, false},
		{"normal options", "", "OPTIONS", "", "", 200, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := map[string]string{"Access-Control-Allow-Origin": "https://app.example", "Access-Control-Allow-Credentials": "true"}
			methods := []string{"GET", "POST"}
			next := false
			h := CORS(cfg, &methods)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { next = true; w.WriteHeader(200) }))
			r := httptest.NewRequest(tc.method, "/", nil)
			r.Header.Set("Origin", tc.origin)
			r.Header.Set("Access-Control-Request-Method", tc.requestMethod)
			r.Header.Set("Access-Control-Request-Headers", tc.headers)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			require.Equal(t, tc.code, w.Code)
			if tc.allowed {
				require.Equal(t, tc.origin, w.Header().Get("Access-Control-Allow-Origin"))
				require.Equal(t, "true", w.Header().Get("Access-Control-Allow-Credentials"))
			} else {
				require.Empty(t, w.Header().Get("Access-Control-Allow-Origin"))
			}
			require.Contains(t, w.Header().Values("Vary"), "Origin")
			require.Equal(t, tc.code == 200, next)
		})
	}
}

func TestCORSRejectsAmbiguousConfig(t *testing.T) {
	require.Error(t, ValidateCORS(map[string]string{"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Credentials": "true"}))
	require.Error(t, ValidateCORS(map[string]string{"Access-Control-Allow-Origin": "https://app.example/path"}))
	require.NoError(t, ValidateCORS(map[string]string{"Access-Control-Allow-Origin": "https://app.example, http://localhost:3000"}))
	h := CORS(nil, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("unexpected request") }))
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Origin", "https://app.example")
	h.ServeHTTP(w, r)
	require.Equal(t, 403, w.Code)
}
