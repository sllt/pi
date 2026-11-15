package dbresolver

import (
	"time"

	"go.opentelemetry.io/otel/trace"
)

// Option is a function type for configuring the resolver.
type Option func(*Resolver)

// WithStrategy sets the strategy for the resolver.
func WithStrategy(strategy Strategy) Option {
	return func(r *Resolver) {
		r.strategy = strategy
	}
}

// WithFallback sets whether to fallback to primary on replica failure.
func WithFallback(fallback bool) Option {
	return func(r *Resolver) {
		r.readFallback = fallback
	}
}

// WithPrimaryRoutes sets exact paths or prefix patterns ("/admin/*") that always route to primary.
func WithPrimaryRoutes(routes map[string]bool) Option {
	return func(r *Resolver) {
		r.primaryRoutes = routes
	}
}

// WithTracerProvider configures an instance-scoped OpenTelemetry TracerProvider for the resolver
// and any lazily opened replicas.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(r *Resolver) {
		if tp != nil {
			r.tracerProvider = tp
			r.tracer = tp.Tracer("pi-dbresolver")
		}
	}
}

// WithCircuitBreaker configures replica circuit breaker thresholds.
func WithCircuitBreaker(maxFailures uint32, timeout time.Duration) Option {
	return func(r *Resolver) {
		if maxFailures > 0 {
			r.maxFailures = int32(maxFailures)
		}
		if timeout > 0 {
			r.breakerTimeout = timeout
		}
	}
}
