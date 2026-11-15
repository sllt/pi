package service

import (
	"context"
	"io"
	"net/http"
	"sync"

	"go.opentelemetry.io/otel/trace"
)

type RetryConfig struct {
	MaxRetries int
}

func (r *RetryConfig) AddOption(h HTTP) HTTP {
	rp := &retryProvider{
		maxRetries: r.MaxRetries,
		HTTP:       h,
	}

	if httpSvc := extractHTTPService(h); httpSvc != nil {
		httpSvc.mu.RLock()
		rp.metrics = httpSvc.Metrics
		rp.serviceName = httpSvc.name
		httpSvc.mu.RUnlock()
	}

	return rp
}

type retryProvider struct {
	mu          sync.RWMutex
	maxRetries  int
	metrics     Metrics
	serviceName string
	HTTP
}

func (rp *retryProvider) Get(ctx context.Context, path string, queryParams map[string]any) (*http.Response,
	error) {
	return rp.doWithRetry(func() (*http.Response, error) {
		return rp.HTTP.Get(ctx, path, queryParams)
	})
}

func (rp *retryProvider) GetWithHeaders(ctx context.Context, path string, queryParams map[string]any,
	headers map[string]string) (*http.Response, error) {
	return rp.doWithRetry(func() (*http.Response, error) {
		return rp.HTTP.GetWithHeaders(ctx, path, queryParams, headers)
	})
}

func (rp *retryProvider) Post(ctx context.Context, path string, queryParams map[string]any,
	body []byte) (*http.Response, error) {
	return rp.doWithRetry(func() (*http.Response, error) {
		return rp.HTTP.Post(ctx, path, queryParams, body)
	})
}

func (rp *retryProvider) PostWithHeaders(ctx context.Context, path string, queryParams map[string]any,
	body []byte,
	headers map[string]string) (*http.Response, error) {
	return rp.doWithRetry(func() (*http.Response, error) {
		return rp.HTTP.PostWithHeaders(ctx, path, queryParams, body, headers)
	})
}

func (rp *retryProvider) Put(ctx context.Context, api string, queryParams map[string]any, body []byte) (
	*http.Response, error) {
	return rp.doWithRetry(func() (*http.Response, error) {
		return rp.HTTP.Put(ctx, api, queryParams, body)
	})
}

func (rp *retryProvider) PutWithHeaders(ctx context.Context, path string, queryParams map[string]any, body []byte,
	headers map[string]string) (*http.Response, error) {
	return rp.doWithRetry(func() (*http.Response, error) {
		return rp.HTTP.PutWithHeaders(ctx, path, queryParams, body, headers)
	})
}

func (rp *retryProvider) Patch(ctx context.Context, path string, queryParams map[string]any, body []byte) (
	*http.Response, error) {
	return rp.doWithRetry(func() (*http.Response, error) {
		return rp.HTTP.Patch(ctx, path, queryParams, body)
	})
}

func (rp *retryProvider) PatchWithHeaders(ctx context.Context, path string, queryParams map[string]any, body []byte,
	headers map[string]string) (*http.Response, error) {
	return rp.doWithRetry(func() (*http.Response, error) {
		return rp.HTTP.PatchWithHeaders(ctx, path, queryParams, body, headers)
	})
}

func (rp *retryProvider) Delete(ctx context.Context, path string, body []byte) (*http.Response, error) {
	return rp.doWithRetry(func() (*http.Response, error) {
		return rp.HTTP.Delete(ctx, path, body)
	})
}

func (rp *retryProvider) DeleteWithHeaders(ctx context.Context, path string, body []byte, headers map[string]string) (
	*http.Response, error) {
	return rp.doWithRetry(func() (*http.Response, error) {
		return rp.HTTP.DeleteWithHeaders(ctx, path, body, headers)
	})
}

func (rp *retryProvider) doWithRetry(reqFunc func() (*http.Response, error)) (*http.Response, error) {
	var (
		resp *http.Response
		err  error
	)

	for i := 0; i <= rp.maxRetries; i++ {
		resp, err = reqFunc()
		if err == nil && resp != nil && resp.StatusCode <= 500 {
			return resp, nil
		}

		if i > 0 {
			rp.mu.RLock()
			m := rp.metrics
			svcName := rp.serviceName
			rp.mu.RUnlock()
			if !isNilServiceAny(m) {
				m.IncrementCounter(context.Background(), "app_http_retry_count", "service", svcName)
			}
		}

		if i < rp.maxRetries && resp != nil && resp.Body != nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
		}
	}

	return resp, err
}

func (rp *retryProvider) UseServiceName(name string) {
	if rp == nil || name == "" {
		return
	}
	rp.mu.Lock()
	if rp.serviceName == "" {
		rp.serviceName = name
	}
	rp.mu.Unlock()
	propagateServiceName(rp.HTTP, name)
}

func (rp *retryProvider) UseLogger(logger any) {
	if rp == nil {
		return
	}
	propagateLogger(rp.HTTP, logger)
}

func (rp *retryProvider) UseMetrics(metrics any) {
	if rp == nil {
		return
	}
	propagateMetrics(rp.HTTP, metrics)
	m, ok := metrics.(Metrics)
	if !ok || isNilServiceAny(m) {
		return
	}
	rp.mu.Lock()
	if isNilServiceAny(rp.metrics) {
		rp.metrics = m
	}
	rp.mu.Unlock()
}

func (rp *retryProvider) UseTracerProvider(tp trace.TracerProvider) {
	if rp == nil {
		return
	}
	propagateTracerProvider(rp.HTTP, tp)
}
