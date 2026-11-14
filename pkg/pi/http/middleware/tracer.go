package middleware

import (
	"fmt"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"github.com/sllt/pi/pkg/pi/version"
)

// ResolveTextMapPropagator returns the global OTel TextMapPropagator if configured,
// or falls back to W3C TraceContext + Baggage without mutating global OTel state.
func ResolveTextMapPropagator() propagation.TextMapPropagator {
	return piHTTP.ResolveTextMapPropagator()
}

// Tracer is a middleware that starts a new OpenTelemetry trace span for each request
// using the global TracerProvider.
func Tracer(inner http.Handler) http.Handler {
	return TracerWithProvider(otel.GetTracerProvider)(inner)
}

// TracerWithProvider starts a new OpenTelemetry trace span for each request using the
// supplied TracerProvider resolver, falling back to otel.GetTracerProvider when nil.
func TracerWithProvider(providerFn func() trace.TracerProvider) func(http.Handler) http.Handler {
	return func(inner http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			ctx = ResolveTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(r.Header))

			var tp trace.TracerProvider
			if providerFn != nil {
				tp = providerFn()
			}
			if tp == nil {
				tp = otel.GetTracerProvider()
			}

			tr := tp.Tracer("pi-" + version.Framework)
			ctx, span := tr.Start(ctx, fmt.Sprintf("%s %s", strings.ToUpper(r.Method), r.URL.Path))

			defer span.End()

			inner.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
