package middleware

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
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

// Metrics is a middleware that records request response time metrics using the provided metrics interface.
func Metrics(metrics metrics) func(inner http.Handler) http.Handler {
	return func(inner http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			srw := &StatusResponseWriter{ResponseWriter: w}

			inner.ServeHTTP(srw, r)

			// Read route pattern AFTER handler execution — chi populates RouteContext during routing
			var path string

			rctx := chi.RouteContext(r.Context())
			if rctx != nil {
				path = rctx.RoutePattern()
			}

			ext := strings.ToLower(filepath.Ext(r.URL.Path))
			switch ext {
			case ".css", ".js", ".png", ".jpg", ".jpeg", ".gif", ".ico", ".svg", ".txt", ".html", ".json", ".woff", ".woff2", ".ttf", ".eot", ".pdf":
				path = r.URL.Path
			}

			if path == "" || path == "/" || strings.HasPrefix(path, "/static") {
				path = r.URL.Path
			}

			path = strings.TrimSuffix(path, "/")

			duration := time.Since(start)

			metrics.RecordHistogram(context.Background(), "app_http_response", duration.Seconds(),
				"path", path, "method", r.Method, "status", fmt.Sprintf("%d", srw.status))
		})
	}
}
