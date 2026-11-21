package middleware

import (
	"context"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
)

type metrics interface {
	IncrementCounter(ctx context.Context, name string, labels ...string)
	DeltaUpDownCounter(ctx context.Context, name string, value float64, labels ...string)
	RecordHistogram(ctx context.Context, name string, value float64, labels ...string)
	SetGauge(name string, value float64, labels ...string)
}

type noopMetricsChecker interface {
	IsNoop() bool
}

// Metrics is a middleware that records request response time metrics using the provided metrics interface.
func Metrics(metrics metrics) func(inner http.Handler) http.Handler {
	if metrics == nil {
		return func(inner http.Handler) http.Handler { return inner }
	}
	if nm, ok := metrics.(noopMetricsChecker); ok && nm.IsNoop() {
		return func(inner http.Handler) http.Handler { return inner }
	}

	return func(inner http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			srw, ok := w.(*StatusResponseWriter)
			if !ok {
				srw = &StatusResponseWriter{ResponseWriter: w}
			}

			inner.ServeHTTP(srw, r)

			// Read route pattern AFTER handler execution — chi populates RouteContext during routing
			var path string

			rctx := chi.RouteContext(r.Context())
			if rctx != nil {
				path = rctx.RoutePattern()
			}

			if strings.IndexByte(r.URL.Path, '.') >= 0 {
				ext := strings.ToLower(filepath.Ext(r.URL.Path))
				switch ext {
				case ".css", ".js", ".png", ".jpg", ".jpeg", ".gif", ".ico", ".svg", ".txt", ".html", ".json", ".woff", ".woff2", ".ttf", ".eot", ".pdf":
					path = r.URL.Path
				}
			}

			if path == "" || path == "/" || strings.HasPrefix(path, "/static") {
				path = r.URL.Path
			}

			path = strings.TrimSuffix(path, "/")

			duration := time.Since(start)

			metrics.RecordHistogram(context.Background(), "app_http_response", duration.Seconds(),
				"path", path, "method", r.Method, "status", statusCodeString(srw.status))
		})
	}
}

func statusCodeString(code int) string {
	switch code {
	case 0:
		return "0"
	case http.StatusOK:
		return "200"
	case http.StatusCreated:
		return "201"
	case http.StatusAccepted:
		return "202"
	case http.StatusNoContent:
		return "204"
	case http.StatusPartialContent:
		return "206"
	case http.StatusMovedPermanently:
		return "301"
	case http.StatusFound:
		return "302"
	case http.StatusSeeOther:
		return "303"
	case http.StatusNotModified:
		return "304"
	case http.StatusTemporaryRedirect:
		return "307"
	case http.StatusPermanentRedirect:
		return "308"
	case http.StatusBadRequest:
		return "400"
	case http.StatusUnauthorized:
		return "401"
	case http.StatusForbidden:
		return "403"
	case http.StatusNotFound:
		return "404"
	case http.StatusMethodNotAllowed:
		return "405"
	case http.StatusConflict:
		return "409"
	case http.StatusUnprocessableEntity:
		return "422"
	case http.StatusTooManyRequests:
		return "429"
	case http.StatusInternalServerError:
		return "500"
	case http.StatusBadGateway:
		return "502"
	case http.StatusServiceUnavailable:
		return "503"
	case http.StatusGatewayTimeout:
		return "504"
	default:
		return strconv.Itoa(code)
	}
}
