package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	otelTrace "go.opentelemetry.io/otel/trace"
	traceNoop "go.opentelemetry.io/otel/trace/noop"
)

type MockHandlerForTracing struct{}

// ServeHTTP is used for testing if the request context has traceId.
func (*MockHandlerForTracing) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	traceID := otelTrace.SpanFromContext(req.Context()).SpanContext().TraceID().String()
	_, _ = w.Write([]byte(traceID))
}

func TestTrace(_ *testing.T) {
	tp := trace.NewTracerProvider()
	otel.SetTracerProvider(tp)

	handler := Tracer(&MockHandlerForTracing{})
	req := httptest.NewRequest(http.MethodGet, "/dummy", http.NoBody)

	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)
}

type testLightweightSpan struct {
	traceNoop.Span
	sc    otelTrace.SpanContext
	ended *bool
}

func (s testLightweightSpan) SpanContext() otelTrace.SpanContext { return s.sc }
func (s testLightweightSpan) End(...otelTrace.SpanEndOption) {
	if s.ended != nil {
		*s.ended = true
	}
}
func (testLightweightSpan) IsRecording() bool { return false }

type testLightweightTracer struct {
	traceNoop.Tracer
	gotSpanName *string
	ended       *bool
}

func (t testLightweightTracer) Start(ctx context.Context, spanName string, _ ...otelTrace.SpanStartOption) (context.Context, otelTrace.Span) {
	if t.gotSpanName != nil {
		*t.gotSpanName = spanName
	}
	tid, _ := otelTrace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	sid, _ := otelTrace.SpanIDFromHex("00f067aa0ba902b7")
	sc := otelTrace.NewSpanContext(otelTrace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: otelTrace.FlagsSampled,
	})
	return otelTrace.ContextWithSpanContext(ctx, sc), testLightweightSpan{sc: sc, ended: t.ended}
}

type testLightweightTP struct {
	traceNoop.TracerProvider
	gotSpanName *string
	ended       *bool
}

func (testLightweightTP) IsLightweightTrace() bool { return true }

func (p testLightweightTP) Tracer(string, ...otelTrace.TracerOption) otelTrace.Tracer {
	return testLightweightTracer{gotSpanName: p.gotSpanName, ended: p.ended}
}

type customCtxKey struct{}

type customHeaderPropagator struct{}

func (customHeaderPropagator) Inject(context.Context, propagation.TextMapCarrier) {}

func (customHeaderPropagator) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	if got := carrier.Get("X-Custom-Trace"); got != "" {
		return context.WithValue(ctx, customCtxKey{}, got)
	}
	return ctx
}

func (customHeaderPropagator) Fields() []string { return []string{"x-custom-trace"} }

type wrappedProp struct {
	propagation.TextMapPropagator
}

type tracestateOnlyPropagator struct{}

func (tracestateOnlyPropagator) Inject(context.Context, propagation.TextMapCarrier) {}

func (tracestateOnlyPropagator) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	if got := carrier.Get("Tracestate"); got != "" {
		return context.WithValue(ctx, customCtxKey{}, got)
	}
	return ctx
}

func (tracestateOnlyPropagator) Fields() []string { return []string{"tracestate"} }

func TestTracerWithProvider_NoopAndLightweightAndCustom(t *testing.T) {
	origProp := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTextMapPropagator(origProp)
	})
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	t.Run("noop provider without trace headers skips WithContext", func(t *testing.T) {
		var innerReq *http.Request
		mw := TracerWithProvider(func() otelTrace.TracerProvider {
			return traceNoop.NewTracerProvider()
		})
		h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			innerReq = r
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/ping", http.NoBody)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		assert.Same(t, req, innerReq)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("noop provider with traceparent extracts incoming span context", func(t *testing.T) {
		const wantTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
		var gotTraceID string
		mw := TracerWithProvider(func() otelTrace.TracerProvider {
			return traceNoop.NewTracerProvider()
		})
		h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotTraceID = otelTrace.SpanFromContext(r.Context()).SpanContext().TraceID().String()
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/ping", http.NoBody)
		req.Header.Set("Traceparent", "00-"+wantTraceID+"-00f067aa0ba902b7-01")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		assert.Equal(t, wantTraceID, gotTraceID)
	})

	t.Run("lightweight provider uses empty span name and ends span", func(t *testing.T) {
		var gotSpanName = "unset"
		var ended bool
		var gotTraceID string
		mw := TracerWithProvider(func() otelTrace.TracerProvider {
			return testLightweightTP{gotSpanName: &gotSpanName, ended: &ended}
		})
		h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotTraceID = otelTrace.SpanFromContext(r.Context()).SpanContext().TraceID().String()
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/ping", http.NoBody)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		assert.Empty(t, gotSpanName)
		assert.True(t, ended)
		assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", gotTraceID)
	})

	t.Run("SDK provider uppercases lowercase HTTP method", func(t *testing.T) {
		exporter := tracetest.NewInMemoryExporter()
		tp := trace.NewTracerProvider(trace.WithSyncer(exporter))
		t.Cleanup(func() { _ = tp.Shutdown(t.Context()) })

		mw := TracerWithProvider(func() otelTrace.TracerProvider { return tp })
		h := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest("get", "/items", http.NoBody)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		spans := exporter.GetSpans()
		assert.Len(t, spans, 1)
		assert.Equal(t, "GET /items", spans[0].Name)
	})

	t.Run("custom global propagator extracts without traceparent or baggage", func(t *testing.T) {
		otel.SetTextMapPropagator(customHeaderPropagator{})
		var gotVal any
		mw := TracerWithProvider(func() otelTrace.TracerProvider {
			return traceNoop.NewTracerProvider()
		})
		h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotVal = r.Context().Value(customCtxKey{})
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/custom", http.NoBody)
		req.Header.Set("X-Custom-Trace", "abc-123")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		assert.Equal(t, "abc-123", gotVal)
	})

	t.Run("wrapped comparable struct holding non-comparable propagator does not panic", func(t *testing.T) {
		otel.SetTextMapPropagator(wrappedProp{
			TextMapPropagator: propagation.NewCompositeTextMapPropagator(
				propagation.TraceContext{},
				propagation.Baggage{},
			),
		})

		mw := TracerWithProvider(func() otelTrace.TracerProvider {
			return traceNoop.NewTracerProvider()
		})
		h := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		assert.NotPanics(t, func() {
			for range 2 {
				req := httptest.NewRequest(http.MethodGet, "/wrapped", http.NoBody)
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				assert.Equal(t, http.StatusOK, rec.Code)
			}
		})
	})

	t.Run("tracestate header alone triggers extraction", func(t *testing.T) {
		otel.SetTextMapPropagator(tracestateOnlyPropagator{})
		var gotVal any
		mw := TracerWithProvider(func() otelTrace.TracerProvider {
			return traceNoop.NewTracerProvider()
		})
		h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotVal = r.Context().Value(customCtxKey{})
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/tracestate", http.NoBody)
		req.Header.Set("Tracestate", "vendor=opaque")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		assert.Equal(t, "vendor=opaque", gotVal)
	})
}
