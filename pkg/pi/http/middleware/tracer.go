package middleware

import (
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	traceNoop "go.opentelemetry.io/otel/trace/noop"

	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"github.com/sllt/pi/pkg/pi/version"
)

const tracerName = "pi-" + version.Framework

type lightweightTracerProvider interface {
	IsLightweightTrace() bool
}

type propagatorCacheEntry struct {
	keepAlive propagation.TextMapPropagator // pins pointer/slice backing memory against GC address reuse; never compared
	typ       reflect.Type
	ptr       uintptr
	slen      int
	isCustom  bool
}

var globalPropagatorCache atomic.Pointer[propagatorCacheEntry]

func fieldsHaveCustomKey(fields []string) bool {
	for _, f := range fields {
		switch f {
		case "traceparent", "tracestate", "baggage":
		default:
			return true
		}
	}
	return false
}

func hasCustomGlobalPropagator() bool {
	p := otel.GetTextMapPropagator()
	if p == nil {
		return false
	}
	switch p.(type) {
	case propagation.TraceContext, *propagation.TraceContext,
		propagation.Baggage, *propagation.Baggage:
		return false
	}

	rv := reflect.ValueOf(p)
	typ := rv.Type()
	kind := rv.Kind()
	cached := globalPropagatorCache.Load()
	cacheable := false
	var ptr uintptr
	var slen int

	switch kind {
	case reflect.Pointer:
		cacheable = true
		ptr = rv.Pointer()
		if cached != nil && cached.typ == typ && cached.ptr == ptr {
			return cached.isCustom
		}
	case reflect.Slice:
		cacheable = true
		ptr = rv.Pointer()
		slen = rv.Len()
		if cached != nil && cached.typ == typ && cached.ptr == ptr && cached.slen == slen {
			return cached.isCustom
		}
	case reflect.Struct:
		if typ.Size() == 0 {
			cacheable = true
			if cached != nil && cached.typ == typ {
				return cached.isCustom
			}
		}
	}

	isCustom := fieldsHaveCustomKey(p.Fields())
	if cacheable {
		globalPropagatorCache.Store(&propagatorCacheEntry{
			keepAlive: p,
			typ:       typ,
			ptr:       ptr,
			slen:      slen,
			isCustom:  isCustom,
		})
	}
	return isCustom
}

func hasTraceHeaders(h http.Header) bool {
	return h.Get("Traceparent") != "" || h.Get("Tracestate") != "" || h.Get("Baggage") != ""
}

func isPlainNoopTracerProvider(tp trace.TracerProvider) bool {
	switch tp.(type) {
	case traceNoop.TracerProvider, *traceNoop.TracerProvider:
		return true
	default:
		return false
	}
}

func upperMethod(m string) string {
	for i := 0; i < len(m); i++ {
		if m[i] >= 'a' && m[i] <= 'z' {
			return strings.ToUpper(m)
		}
	}
	return m
}

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
			extracted := false
			if hasTraceHeaders(r.Header) || hasCustomGlobalPropagator() {
				ctx = ResolveTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(r.Header))
				extracted = true
			}

			var tp trace.TracerProvider
			if providerFn != nil {
				tp = providerFn()
			}
			if tp == nil {
				tp = otel.GetTracerProvider()
			}

			if isPlainNoopTracerProvider(tp) {
				if !extracted {
					inner.ServeHTTP(w, r)
					return
				}
				inner.ServeHTTP(w, r.WithContext(ctx))
				return
			}

			if lw, ok := tp.(lightweightTracerProvider); ok && lw.IsLightweightTrace() {
				ctx, span := tp.Tracer(tracerName).Start(ctx, "")
				defer span.End()
				inner.ServeHTTP(w, r.WithContext(ctx))
				return
			}

			spanName := upperMethod(r.Method) + " " + r.URL.Path
			ctx, span := tp.Tracer(tracerName).Start(ctx, spanName)
			defer span.End()

			inner.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
