package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"time"

	"go.opentelemetry.io/otel/trace"
)

var (
	errNegativeMaxIdleConns        = errors.New("MaxIdleConns cannot be negative")
	errNegativeMaxIdleConnsPerHost = errors.New("MaxIdleConnsPerHost cannot be negative")
	errNegativeIdleConnTimeout     = errors.New("IdleConnTimeout cannot be negative")
)

// ConnectionPoolConfig holds the configuration for HTTP connection pool settings.
// It customizes the HTTP transport layer to optimize connection reuse for high-frequency requests.
//
// Note: This configuration must be applied first when using multiple options with AddHTTPService,
// as it needs to access the underlying HTTP client transport. If applied after wrapper options
// (CircuitBreaker, Retry, OAuth), it will be silently ignored.
//
// Example:
//
//	app.AddHTTPService("api-service", "https://api.example.com",
//	    &service.ConnectionPoolConfig{
//	        MaxIdleConns:        100,
//	        MaxIdleConnsPerHost: 20,
//	        IdleConnTimeout:     90 * time.Second,
//	    },
//	    &service.CircuitBreakerConfig{...}, // Other options after ConnectionPoolConfig
//	)
type ConnectionPoolConfig struct {
	// MaxIdleConns controls the maximum number of idle (keep-alive) connections across all hosts.
	// If not explicitly set (0), a default of 100 will be used.
	// Negative values will cause validation error.
	MaxIdleConns int

	// MaxIdleConnsPerHost controls the maximum idle (keep-alive) connections to keep per-host.
	// This is the critical setting for microservices making frequent requests to the same host.
	// If set to 0, Go's DefaultMaxIdleConnsPerHost (2) will be used.
	// Negative values will cause validation error.
	// Default Go value: 2 (which is often insufficient for microservices)
	// Recommended: 10-20 for typical microservices, higher for high-traffic services
	MaxIdleConnsPerHost int

	// IdleConnTimeout is the maximum amount of time an idle (keep-alive) connection will remain
	// idle before closing itself.
	// If not explicitly set (0), a default of 90 seconds will be used.
	// Negative values will cause validation error.
	IdleConnTimeout time.Duration
}

// Validate checks if the connection pool configuration values are valid.
func (c *ConnectionPoolConfig) Validate() error {
	if c.MaxIdleConns < 0 {
		return fmt.Errorf("%w, got: %d", errNegativeMaxIdleConns, c.MaxIdleConns)
	}

	if c.MaxIdleConnsPerHost < 0 {
		return fmt.Errorf("%w, got: %d", errNegativeMaxIdleConnsPerHost, c.MaxIdleConnsPerHost)
	}

	if c.IdleConnTimeout < 0 {
		return fmt.Errorf("%w, got: %v", errNegativeIdleConnTimeout, c.IdleConnTimeout)
	}

	return nil
}

// AddOption implements the Options interface to apply connection pool configuration to HTTP service.
// It modifies the underlying HTTP client's transport to use optimized connection pool settings.
func (c *ConnectionPoolConfig) AddOption(h HTTP) HTTP {
	// Extract the base httpService from any wrapped service
	httpSvc := extractHTTPService(h)
	if httpSvc == nil {
		// If we can't find the base service, return unchanged
		// This maintains backward compatibility
		return h
	}

	// Validate configuration before applying
	if err := c.Validate(); err != nil {
		return h
	}

	// Clone the default transport to preserve important settings like TLS timeouts and proxy configuration
	transport := http.DefaultTransport.(*http.Transport).Clone()

	// Apply connection pool settings with defaults
	if c.MaxIdleConns > 0 {
		transport.MaxIdleConns = c.MaxIdleConns
	} else {
		// Set a reasonable default if not specified
		transport.MaxIdleConns = 100
	}

	if c.MaxIdleConnsPerHost > 0 {
		transport.MaxIdleConnsPerHost = c.MaxIdleConnsPerHost
	}
	// Note: If MaxIdleConnsPerHost is 0, Go uses DefaultMaxIdleConnsPerHost (2)

	if c.IdleConnTimeout > 0 {
		transport.IdleConnTimeout = c.IdleConnTimeout
	} else {
		// Set a reasonable default if not specified
		transport.IdleConnTimeout = 90 * time.Second
	}

	// Apply the custom transport to the HTTP client
	httpSvc.Client.Transport = transport

	return h
}

// CloseService stops background loops (circuit breaker probes, rate-limiter bucket
// cleanup, OAuth refreshers) across a decorated HTTP service chain and closes idle connections.
func CloseService(h HTTP) error {
	if h == nil {
		return nil
	}
	switch v := h.(type) {
	case *httpService:
		if v != nil && v.Client != nil {
			v.Client.CloseIdleConnections()
		}
		return nil
	case *circuitBreaker:
		if v == nil {
			return nil
		}
		return v.Close()
	case *rateLimiter:
		if v == nil {
			return nil
		}
		return v.Close()
	case *retryProvider:
		if v == nil {
			return nil
		}
		return CloseService(v.HTTP)
	case *authProvider:
		if v == nil {
			return nil
		}
		return CloseService(v.HTTP)
	case *customHealthService:
		if v == nil {
			return nil
		}
		return CloseService(v.HTTP)
	case *customHeader:
		if v == nil {
			return nil
		}
		return CloseService(v.HTTP)
	default:
		if closer, ok := h.(io.Closer); ok {
			return closer.Close()
		}
		return nil
	}
}

// extractHTTPService attempts to extract the base *httpService from a potentially wrapped HTTP service.
// It handles the common wrapper types used in the service package.
func extractHTTPService(h HTTP) *httpService {
	switch v := h.(type) {
	case *httpService:
		return v
	case *circuitBreaker:
		if v == nil {
			return nil
		}
		return extractHTTPService(v.HTTP)
	case *retryProvider:
		if v == nil {
			return nil
		}
		return extractHTTPService(v.HTTP)
	case *authProvider:
		if v == nil {
			return nil
		}
		return extractHTTPService(v.HTTP)
	case *customHealthService:
		if v == nil {
			return nil
		}
		return extractHTTPService(v.HTTP)
	case *rateLimiter:
		if v == nil {
			return nil
		}
		return extractHTTPService(v.HTTP)
	case *customHeader:
		if v == nil {
			return nil
		}
		return extractHTTPService(v.HTTP)
	default:
		return nil
	}
}

type endpointHealthChecker interface {
	getHealthResponseForEndpoint(ctx context.Context, endpoint string, timeout int) *Health
}

// checkEndpointHealth unwraps built-in service decorators to reach endpointHealthChecker
// (*httpService) so custom HealthConfig endpoints/timeouts work identically without
// sealing the public HTTP interface. External test fakes fall back to curr.HealthCheck(ctx).
func checkEndpointHealth(ctx context.Context, h HTTP, endpoint string, timeout int) *Health {
	if h == nil {
		return &Health{
			Status:  serviceDown,
			Details: map[string]any{"error": "http service is nil"},
		}
	}
	curr := h
	for curr != nil {
		switch v := curr.(type) {
		case *httpService:
			if v == nil {
				return &Health{
					Status:  serviceDown,
					Details: map[string]any{"error": "http service is nil"},
				}
			}
			return v.getHealthResponseForEndpoint(ctx, endpoint, timeout)
		case *circuitBreaker:
			if v == nil {
				curr = nil
			} else {
				curr = v.HTTP
			}
		case *retryProvider:
			if v == nil {
				curr = nil
			} else {
				curr = v.HTTP
			}
		case *authProvider:
			if v == nil {
				curr = nil
			} else {
				curr = v.HTTP
			}
		case *customHealthService:
			if v == nil {
				curr = nil
			} else {
				curr = v.HTTP
			}
		case *rateLimiter:
			if v == nil {
				curr = nil
			} else {
				curr = v.HTTP
			}
		case *customHeader:
			if v == nil {
				curr = nil
			} else {
				curr = v.HTTP
			}
		default:
			if checker, ok := curr.(endpointHealthChecker); ok {
				return checker.getHealthResponseForEndpoint(ctx, endpoint, timeout)
			}
			return curr.HealthCheck(ctx)
		}
	}
	return &Health{
		Status:  serviceDown,
		Details: map[string]any{"error": "http service is nil"},
	}
}

func isNilServiceAny(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Func, reflect.Map, reflect.Slice, reflect.Chan:
		return rv.IsNil()
	default:
		return false
	}
}

func propagateServiceName(h HTTP, name string) {
	if isNilServiceAny(h) || name == "" {
		return
	}
	if s, ok := h.(interface{ UseServiceName(string) }); ok {
		s.UseServiceName(name)
	}
}

func propagateLogger(h HTTP, logger any) {
	if isNilServiceAny(h) || isNilServiceAny(logger) {
		return
	}
	if s, ok := h.(interface{ UseLogger(any) }); ok {
		s.UseLogger(logger)
	}
}

func propagateMetrics(h HTTP, metrics any) {
	if isNilServiceAny(h) || isNilServiceAny(metrics) {
		return
	}
	if s, ok := h.(interface{ UseMetrics(any) }); ok {
		s.UseMetrics(metrics)
	}
}

func propagateTracerProvider(h HTTP, tp trace.TracerProvider) {
	if isNilServiceAny(h) || isNilServiceAny(tp) {
		return
	}
	if s, ok := h.(interface{ UseTracerProvider(trace.TracerProvider) }); ok {
		s.UseTracerProvider(tp)
	}
}
