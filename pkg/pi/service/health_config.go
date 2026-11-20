package service

import (
	"context"

	"go.opentelemetry.io/otel/trace"
)

type HealthConfig struct {
	HealthEndpoint string
	Timeout        int
}

func (h *HealthConfig) AddOption(svc HTTP) HTTP {
	// if timeout is not provided we set a convenient default timeout.
	if h.Timeout == 0 {
		h.Timeout = defaultTimeout
	}

	// Set health config on the parent httpService so other options can access it
	if httpSvc := extractHTTPService(svc); httpSvc != nil {
		httpSvc.mu.Lock()
		httpSvc.healthEndpoint = h.HealthEndpoint
		httpSvc.healthTimeout = h.Timeout
		httpSvc.mu.Unlock()
	}

	return &customHealthService{
		healthEndpoint: h.HealthEndpoint,
		timeout:        h.Timeout,
		HTTP:           svc,
	}
}

type customHealthService struct {
	healthEndpoint string
	timeout        int
	HTTP
}

func (c *customHealthService) R() *Request {
	return newRequest(c)
}

func (c *customHealthService) HealthCheck(ctx context.Context) *Health {
	return checkEndpointHealth(ctx, c.HTTP, c.healthEndpoint, c.timeout)
}

func (c *customHealthService) UseServiceName(name string) {
	if c == nil {
		return
	}
	propagateServiceName(c.HTTP, name)
}

func (c *customHealthService) UseLogger(logger any) {
	if c == nil {
		return
	}
	propagateLogger(c.HTTP, logger)
}

func (c *customHealthService) UseMetrics(metrics any) {
	if c == nil {
		return
	}
	propagateMetrics(c.HTTP, metrics)
}

func (c *customHealthService) UseTracerProvider(tp trace.TracerProvider) {
	if c == nil {
		return
	}
	propagateTracerProvider(c.HTTP, tp)
}
