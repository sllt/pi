package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
)

// circuitBreaker states.
const (
	ClosedState = iota
	OpenState
)

const minCircuitBreakerTickerInterval = 10 * time.Millisecond

var (
	// ErrCircuitOpen indicates that the circuit breaker is open.
	ErrCircuitOpen                        = errors.New("unable to connect to server at host")
	ErrUnexpectedCircuitBreakerResultType = errors.New("unexpected result type from circuit breaker")
)

// CircuitBreakerConfig holds the configuration for the circuitBreaker.
type CircuitBreakerConfig struct {
	Threshold int             // Threshold represents the max no of retry before switching the circuit breaker state.
	Interval  time.Duration   // Interval represents the time interval duration between hitting the HealthURL.
	Context   context.Context // Optional host context; canceling stops background health probes.
}

// circuitBreaker represents a circuit breaker implementation.
type circuitBreaker struct {
	mu           sync.RWMutex
	state        int // ClosedState or OpenState
	failureCount int
	threshold    int
	interval     time.Duration
	lastChecked  time.Time
	probing      bool
	metrics      Metrics
	serviceName  string

	stopOnce  sync.Once
	ctxCancel context.CancelFunc
	stopCh    chan struct{}
	doneCh    chan struct{}

	HTTP
}

// NewCircuitBreaker creates a new circuitBreaker instance based on the provided config.
//
//nolint:revive // Allow returning unexported types as intended.
func NewCircuitBreaker(config CircuitBreakerConfig, h HTTP) *circuitBreaker {
	return NewCircuitBreakerWithContext(config.Context, config, h)
}

// NewCircuitBreakerWithContext creates a circuitBreaker whose background probe loop
// terminates when ctx is canceled or Close() is invoked.
//
//nolint:revive // Allow returning unexported types as intended.
func NewCircuitBreakerWithContext(ctx context.Context, config CircuitBreakerConfig, h HTTP) *circuitBreaker {
	if ctx == nil {
		ctx = context.Background()
	}
	probeCtx, cancel := context.WithCancel(ctx)
	stopCh := make(chan struct{})
	doneCh := make(chan struct{})

	cb := &circuitBreaker{
		state:     ClosedState,
		threshold: config.Threshold,
		interval:  config.Interval,
		ctxCancel: cancel,
		stopCh:    stopCh,
		doneCh:    doneCh,
		HTTP:      h,
	}

	if httpSvc := extractHTTPService(h); httpSvc != nil {
		httpSvc.mu.RLock()
		cb.metrics = httpSvc.Metrics
		cb.serviceName = httpSvc.name
		httpSvc.mu.RUnlock()
	}

	if cb.interval <= 0 {
		close(doneCh)
		return cb
	}

	// Perform asynchronous health checks with a bounded ticker floor and clean stop channel.
	go cb.startHealthChecks(probeCtx)

	return cb
}

// Close stops the circuit breaker's background health check loop and waits for exit.
func (cb *circuitBreaker) Close() error {
	if cb == nil {
		return nil
	}
	cb.stopOnce.Do(func() {
		if cb.ctxCancel != nil {
			cb.ctxCancel()
		}
		if cb.stopCh != nil {
			close(cb.stopCh)
		}
	})
	if cb.doneCh != nil {
		<-cb.doneCh
	}
	return CloseService(cb.HTTP)
}

// executeWithCircuitBreaker executes the given function with circuit breaker protection.
func (cb *circuitBreaker) executeWithCircuitBreaker(ctx context.Context, f func(ctx context.Context) (*http.Response,
	error)) (*http.Response, error) {
	if cb.isOpen() && !cb.tryCircuitRecovery(ctx) {
		return nil, ErrCircuitOpen
	}

	result, err := f(ctx)

	cb.mu.Lock()
	if err != nil || (result != nil && result.StatusCode > 500) {
		cb.handleFailure()
	} else {
		cb.resetFailureCount()
	}

	trip := cb.failureCount > cb.threshold
	if trip {
		cb.openCircuit()
	}
	cb.mu.Unlock()

	if trip {
		if result != nil && result.Body != nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(result.Body, 4096))
			_ = result.Body.Close()
		}
		return nil, ErrCircuitOpen
	}

	return result, err
}

// isOpen returns true if the circuit breaker is in the open state.
func (cb *circuitBreaker) isOpen() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	return cb.state == OpenState
}

func (cb *circuitBreaker) healthCheck(ctx context.Context) bool {
	if ctx == nil {
		ctx = context.Background()
	}
	if httpSvc := extractHTTPService(cb.HTTP); httpSvc != nil {
		httpSvc.mu.RLock()
		endpoint := httpSvc.healthEndpoint
		timeout := httpSvc.healthTimeout
		httpSvc.mu.RUnlock()
		if endpoint != "" {
			resp := checkEndpointHealth(ctx, cb.HTTP, endpoint, timeout)

			return resp != nil && resp.Status == serviceUp
		}
	}

	resp := cb.HTTP.HealthCheck(ctx)

	return resp != nil && resp.Status == serviceUp
}

// startHealthChecks initiates periodic health checks in a single cancelable goroutine.
func (cb *circuitBreaker) startHealthChecks(probeCtx context.Context) {
	defer close(cb.doneCh)

	tickerPeriod := max(cb.interval, minCircuitBreakerTickerInterval)
	ticker := time.NewTicker(tickerPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-cb.stopCh:
			return
		case <-probeCtx.Done():
			return
		case <-ticker.C:
			if probeCtx.Err() != nil {
				return
			}
			if cb.isOpen() && cb.healthCheck(probeCtx) {
				if probeCtx.Err() != nil {
					return
				}
				cb.mu.Lock()
				if cb.state == OpenState {
					cb.resetCircuit()
				}
				cb.mu.Unlock()
			}
		}
	}
}

// openCircuit transitions the circuit breaker to the open state.
func (cb *circuitBreaker) openCircuit() {
	cb.state = OpenState
	cb.lastChecked = time.Now()

	if cb.metrics != nil {
		cb.metrics.SetGauge("app_http_circuit_breaker_state", 1, "service", cb.serviceName)
	}
}

// resetCircuit transitions the circuit breaker to the closed state.
func (cb *circuitBreaker) resetCircuit() {
	cb.state = ClosedState
	cb.failureCount = 0

	if cb.metrics != nil {
		cb.metrics.SetGauge("app_http_circuit_breaker_state", 0, "service", cb.serviceName)
	}
}

// handleFailure increments the failure count and opens the circuit if the threshold is reached.
func (cb *circuitBreaker) handleFailure() {
	cb.failureCount++
	if cb.failureCount > cb.threshold {
		cb.openCircuit()
	}
}

// resetFailureCount resets the failure count to zero.
func (cb *circuitBreaker) resetFailureCount() {
	cb.failureCount = 0
}

func (cb *CircuitBreakerConfig) AddOption(h HTTP) HTTP {
	breaker := NewCircuitBreaker(*cb, h)

	breaker.mu.Lock()
	if breaker.metrics != nil {
		// Initialize the gauge to 0 (Closed) - gauge is registered by runtime observability
		breaker.metrics.SetGauge("app_http_circuit_breaker_state", 0, "service", breaker.serviceName)
	}
	breaker.mu.Unlock()

	return breaker
}

func (cb *circuitBreaker) tryCircuitRecovery(ctx context.Context) bool {
	if ctx == nil {
		ctx = context.Background()
	}

	cb.mu.Lock()
	if cb.state == ClosedState {
		cb.mu.Unlock()
		return true
	}
	if cb.probing || time.Since(cb.lastChecked) <= cb.interval {
		cb.mu.Unlock()
		return false
	}
	// Update lastChecked and set probing to prevent concurrent health-check stampedes
	// while avoiding holding cb.mu across network I/O.
	cb.lastChecked = time.Now()
	cb.probing = true
	cb.mu.Unlock()

	healthy := cb.healthCheck(ctx)

	cb.mu.Lock()
	cb.probing = false
	if healthy {
		cb.resetCircuit()
		cb.mu.Unlock()
		return true
	}
	cb.mu.Unlock()
	return false
}

func (*circuitBreaker) handleCircuitBreakerResult(result any, err error) (*http.Response, error) {
	if err != nil {
		return nil, err
	}

	response, ok := result.(*http.Response)
	if !ok || response == nil {
		return nil, ErrUnexpectedCircuitBreakerResultType
	}

	return response, nil
}

func (cb *circuitBreaker) doRequest(ctx context.Context, method, path string, queryParams map[string]any,
	body []byte, headers map[string]string) (*http.Response, error) {
	if cb.isOpen() {
		if !cb.tryCircuitRecovery(ctx) {
			return nil, ErrCircuitOpen
		}
	}

	var result any

	var err error

	switch method {
	case http.MethodGet:
		result, err = cb.executeWithCircuitBreaker(ctx, func(ctx context.Context) (*http.Response, error) {
			return cb.HTTP.GetWithHeaders(ctx, path, queryParams, headers)
		})
	case http.MethodPost:
		result, err = cb.executeWithCircuitBreaker(ctx, func(ctx context.Context) (*http.Response, error) {
			return cb.HTTP.PostWithHeaders(ctx, path, queryParams, body, headers)
		})
	case http.MethodPatch:
		result, err = cb.executeWithCircuitBreaker(ctx, func(ctx context.Context) (*http.Response, error) {
			return cb.HTTP.PatchWithHeaders(ctx, path, queryParams, body, headers)
		})
	case http.MethodPut:
		result, err = cb.executeWithCircuitBreaker(ctx, func(ctx context.Context) (*http.Response, error) {
			return cb.HTTP.PutWithHeaders(ctx, path, queryParams, body, headers)
		})
	case http.MethodDelete:
		result, err = cb.executeWithCircuitBreaker(ctx, func(ctx context.Context) (*http.Response, error) {
			return cb.HTTP.DeleteWithHeaders(ctx, path, body, headers)
		})
	}

	resp, err := cb.handleCircuitBreakerResult(result, err)
	if err != nil {
		return nil, err
	}

	return resp, err
}

func (cb *circuitBreaker) R() *Request {
	return newRequest(cb)
}

func (cb *circuitBreaker) GetWithHeaders(ctx context.Context, path string, queryParams map[string]any,
	headers map[string]string) (*http.Response, error) {
	return cb.doRequest(ctx, http.MethodGet, path, queryParams, nil, headers)
}

// PostWithHeaders is a wrapper for doRequest with the POST method and headers.
func (cb *circuitBreaker) PostWithHeaders(ctx context.Context, path string, queryParams map[string]any,
	body []byte, headers map[string]string) (*http.Response, error) {
	return cb.doRequest(ctx, http.MethodPost, path, queryParams, body, headers)
}

// PatchWithHeaders is a wrapper for doRequest with the PATCH method and headers.
func (cb *circuitBreaker) PatchWithHeaders(ctx context.Context, path string, queryParams map[string]any,
	body []byte, headers map[string]string) (*http.Response, error) {
	return cb.doRequest(ctx, http.MethodPatch, path, queryParams, body, headers)
}

// PutWithHeaders is a wrapper for doRequest with the PUT method and headers.
func (cb *circuitBreaker) PutWithHeaders(ctx context.Context, path string, queryParams map[string]any,
	body []byte, headers map[string]string) (*http.Response, error) {
	return cb.doRequest(ctx, http.MethodPut, path, queryParams, body, headers)
}

// DeleteWithHeaders is a wrapper for doRequest with the DELETE method and headers.
func (cb *circuitBreaker) DeleteWithHeaders(ctx context.Context, path string, body []byte, headers map[string]string) (
	*http.Response, error) {
	return cb.doRequest(ctx, http.MethodDelete, path, nil, body, headers)
}

func (cb *circuitBreaker) Get(ctx context.Context, path string, queryParams map[string]any) (*http.Response, error) {
	return cb.doRequest(ctx, http.MethodGet, path, queryParams, nil, nil)
}

// Post is a wrapper for doRequest with the POST method and headers.
func (cb *circuitBreaker) Post(ctx context.Context, path string, queryParams map[string]any,
	body []byte) (*http.Response, error) {
	return cb.doRequest(ctx, http.MethodPost, path, queryParams, body, nil)
}

// Patch is a wrapper for doRequest with the PATCH method and headers.
func (cb *circuitBreaker) Patch(ctx context.Context, path string, queryParams map[string]any,
	body []byte) (*http.Response, error) {
	return cb.doRequest(ctx, http.MethodPatch, path, queryParams, body, nil)
}

// Put is a wrapper for doRequest with the PUT method and headers.
func (cb *circuitBreaker) Put(ctx context.Context, path string, queryParams map[string]any,
	body []byte) (*http.Response, error) {
	return cb.doRequest(ctx, http.MethodPut, path, queryParams, body, nil)
}

// Delete is a wrapper for doRequest with the DELETE method and headers.
func (cb *circuitBreaker) Delete(ctx context.Context, path string, body []byte) (
	*http.Response, error) {
	return cb.doRequest(ctx, http.MethodDelete, path, nil, body, nil)
}

func (cb *circuitBreaker) UseServiceName(name string) {
	if cb == nil || name == "" {
		return
	}
	cb.mu.Lock()
	if cb.serviceName == "" {
		cb.serviceName = name
	}
	cb.mu.Unlock()
	propagateServiceName(cb.HTTP, name)
}

func (cb *circuitBreaker) UseLogger(logger any) {
	if cb == nil {
		return
	}
	propagateLogger(cb.HTTP, logger)
}

func (cb *circuitBreaker) UseMetrics(metrics any) {
	if cb == nil {
		return
	}
	propagateMetrics(cb.HTTP, metrics)
	m, ok := metrics.(Metrics)
	if !ok || isNilServiceAny(m) {
		return
	}
	cb.mu.Lock()
	if isNilServiceAny(cb.metrics) {
		cb.metrics = m
		cb.metrics.SetGauge("app_http_circuit_breaker_state", float64(cb.state), "service", cb.serviceName)
	}
	cb.mu.Unlock()
}

func (cb *circuitBreaker) UseTracerProvider(tp trace.TracerProvider) {
	if cb == nil {
		return
	}
	propagateTracerProvider(cb.HTTP, tp)
}
