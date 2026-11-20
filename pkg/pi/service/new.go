package service

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/httptrace/otelhttptrace"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var defaultHTTPPropagator = propagation.NewCompositeTextMapPropagator(
	propagation.TraceContext{},
	propagation.Baggage{},
)

func resolveHTTPTextMapPropagator() propagation.TextMapPropagator {
	if p := otel.GetTextMapPropagator(); p != nil && len(p.Fields()) > 0 {
		return p
	}
	return defaultHTTPPropagator
}

type httpService struct {
	mu sync.RWMutex
	*http.Client
	trace.Tracer
	tracerProvider trace.TracerProvider
	explicitTracer bool
	url            string
	name           string
	Logger
	Metrics

	// healthEndpoint is the custom endpoint for health checks (shared across options)
	healthEndpoint string
	// healthTimeout is the timeout in seconds for health check requests
	healthTimeout int
}

type HTTP interface {
	// HTTP is embedded as HTTP would be able to access it's clients method
	httpClient

	// R starts a fluent outbound HTTP request builder through this service
	// (preserving all configured decorators).
	R() *Request

	// HealthCheck to get the service health and report it to the current application
	HealthCheck(ctx context.Context) *Health
}

type httpClient interface {
	// Get performs an HTTP GET request.
	Get(ctx context.Context, api string, queryParams map[string]any) (*http.Response, error)
	// GetWithHeaders performs an HTTP GET request with custom headers.
	GetWithHeaders(ctx context.Context, path string, queryParams map[string]any,
		headers map[string]string) (*http.Response, error)

	// Post performs an HTTP POST request.
	Post(ctx context.Context, path string, queryParams map[string]any, body []byte) (*http.Response, error)
	// PostWithHeaders performs an HTTP POST request with custom headers.
	PostWithHeaders(ctx context.Context, path string, queryParams map[string]any, body []byte,
		headers map[string]string) (*http.Response, error)

	// Put performs an HTTP PUT request.
	Put(ctx context.Context, api string, queryParams map[string]any, body []byte) (*http.Response, error)
	// PutWithHeaders performs an HTTP PUT request with custom headers.
	PutWithHeaders(ctx context.Context, api string, queryParams map[string]any, body []byte,
		headers map[string]string) (*http.Response, error)

	// Patch performs an HTTP PATCH request.
	Patch(ctx context.Context, api string, queryParams map[string]any, body []byte) (*http.Response, error)
	// PatchWithHeaders performs an HTTP PATCH request with custom headers.
	PatchWithHeaders(ctx context.Context, api string, queryParams map[string]any, body []byte,
		headers map[string]string) (*http.Response, error)

	// Delete performs an HTTP DELETE request.
	Delete(ctx context.Context, api string, body []byte) (*http.Response, error)
	// DeleteWithHeaders performs an HTTP DELETE request with custom headers.
	DeleteWithHeaders(ctx context.Context, api string, body []byte, headers map[string]string) (*http.Response, error)
}

// NewHTTPService function creates a new instance of the httpService struct, which implements the HTTP interface.
// It initializes the http.Client, url, Tracer, and Logger fields of the httpService struct with the provided values.
func NewHTTPService(serviceAddress string, logger Logger, metrics Metrics, options ...Options) HTTP {
	return newHTTPService(serviceAddress, logger, metrics, nil, nil, false, options...)
}

// NewHTTPServiceWithTracer creates an HTTP service bound to an explicit trace.Tracer.
func NewHTTPServiceWithTracer(
	serviceAddress string,
	logger Logger,
	metrics Metrics,
	tracer trace.Tracer,
	options ...Options,
) HTTP {
	return newHTTPService(serviceAddress, logger, metrics, tracer, nil, tracer != nil, options...)
}

// NewHTTPServiceWithTracerProvider creates an HTTP service bound to an explicit trace.TracerProvider.
func NewHTTPServiceWithTracerProvider(
	serviceAddress string,
	logger Logger,
	metrics Metrics,
	tracerProvider trace.TracerProvider,
	options ...Options,
) HTTP {
	var tracer trace.Tracer
	if tracerProvider != nil {
		tracer = tracerProvider.Tracer("pi-http-client")
	}
	return newHTTPService(serviceAddress, logger, metrics, tracer, tracerProvider, tracerProvider != nil, options...)
}

func newHTTPService(
	serviceAddress string,
	logger Logger,
	metrics Metrics,
	tracer trace.Tracer,
	tracerProvider trace.TracerProvider,
	explicitTracer bool,
	options ...Options,
) HTTP {
	if tracer == nil {
		if tracerProvider != nil {
			tracer = tracerProvider.Tracer("pi-http-client")
		} else {
			tracer = otel.Tracer("pi-http-client")
		}
	}
	h := &httpService{
		// using default HTTP client to do HTTP communication
		Client:         &http.Client{},
		url:            serviceAddress,
		Tracer:         tracer,
		tracerProvider: tracerProvider,
		explicitTracer: explicitTracer,
		Logger:         logger,
		Metrics:        metrics,
	}

	var svc HTTP

	svc = h

	// if options are given, then add them to the httpService struct
	for _, o := range options {
		if o != nil {
			svc = o.AddOption(svc)
		}
	}

	return svc
}

func (h *httpService) R() *Request {
	return newRequest(h)
}

func (h *httpService) Get(ctx context.Context, path string, queryParams map[string]any) (*http.Response, error) {
	return h.GetWithHeaders(ctx, path, queryParams, nil)
}

func (h *httpService) GetWithHeaders(ctx context.Context, path string, queryParams map[string]any,
	headers map[string]string) (*http.Response, error) {
	return h.createAndSendRequest(ctx, http.MethodGet, path, queryParams, nil, headers)
}

func (h *httpService) Post(ctx context.Context, path string, queryParams map[string]any,
	body []byte) (*http.Response, error) {
	return h.PostWithHeaders(ctx, path, queryParams, body, nil)
}

func (h *httpService) PostWithHeaders(ctx context.Context, path string, queryParams map[string]any,
	body []byte, headers map[string]string) (*http.Response, error) {
	return h.createAndSendRequest(ctx, http.MethodPost, path, queryParams, body, headers)
}

func (h *httpService) Patch(ctx context.Context, path string, queryParams map[string]any, body []byte) (*http.Response, error) {
	return h.PatchWithHeaders(ctx, path, queryParams, body, nil)
}

func (h *httpService) PatchWithHeaders(ctx context.Context, path string, queryParams map[string]any,
	body []byte, headers map[string]string) (*http.Response, error) {
	return h.createAndSendRequest(ctx, http.MethodPatch, path, queryParams, body, headers)
}

func (h *httpService) Put(ctx context.Context, path string, queryParams map[string]any,
	body []byte) (*http.Response, error) {
	return h.PutWithHeaders(ctx, path, queryParams, body, nil)
}

func (h *httpService) PutWithHeaders(ctx context.Context, path string, queryParams map[string]any,
	body []byte, headers map[string]string) (*http.Response, error) {
	return h.createAndSendRequest(ctx, http.MethodPut, path, queryParams, body, headers)
}

func (h *httpService) Delete(ctx context.Context, path string, body []byte) (*http.Response, error) {
	return h.DeleteWithHeaders(ctx, path, body, nil)
}

func (h *httpService) DeleteWithHeaders(ctx context.Context, path string, body []byte, headers map[string]string) (*http.Response, error) {
	return h.createAndSendRequest(ctx, http.MethodDelete, path, nil, body, headers)
}

func (h *httpService) createAndSendRequest(ctx context.Context, method string, path string,
	queryParams map[string]any, body []byte, headers map[string]string) (*http.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	h.mu.RLock()
	tracer := h.Tracer
	tracerProvider := h.tracerProvider
	logger := h.Logger
	metrics := h.Metrics
	svcName := h.name
	h.mu.RUnlock()

	uri := joinURL(h.url, path)

	if tracer != nil {
		var span trace.Span
		ctx, span = tracer.Start(ctx, uri)
		defer span.End()
	}

	// Attach client-side trace handling for HTTP request.
	var traceOpts []otelhttptrace.ClientTraceOption
	if tracerProvider != nil {
		traceOpts = append(traceOpts, otelhttptrace.WithTracerProvider(tracerProvider))
	}
	clientTraceCtx := httptrace.WithClientTrace(ctx, otelhttptrace.NewClientTrace(ctx, traceOpts...))

	// Create the HTTP request with the tracing context.
	req, err := http.NewRequestWithContext(clientTraceCtx, method, uri, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}

	var isContentTypeSet bool

	for k, v := range headers {
		if strings.EqualFold(k, "content-type") {
			isContentTypeSet = true
		}

		req.Header.Set(k, v)
	}

	if !isContentTypeSet {
		req.Header.Set("Content-Type", "application/json")
	}

	// Inject tracing information into the request headers.
	resolveHTTPTextMapPropagator().Inject(clientTraceCtx, propagation.HeaderCarrier(req.Header))

	// encode the query parameters on the request.
	encodeQueryParameters(req, queryParams)

	log := &Log{
		Timestamp:     time.Now(),
		CorrelationID: trace.SpanFromContext(clientTraceCtx).SpanContext().TraceID().String(),
		HTTPMethod:    method,
		URI:           uri,
	}

	requestStart := time.Now()

	resp, err := h.Do(req)

	respTime := time.Since(requestStart)

	log.ResponseTime = respTime.Microseconds()

	if err != nil {
		log.ResponseCode = http.StatusServiceUnavailable
		if !isNilServiceAny(logger) {
			logger.Log(&ErrorLog{Log: log, ErrorMessage: err.Error()})
		}

		h.recordMetrics(clientTraceCtx, metrics, svcName, method, respTime.Seconds(), http.StatusServiceUnavailable)

		return resp, err
	}

	h.recordMetrics(clientTraceCtx, metrics, svcName, method, respTime.Seconds(), resp.StatusCode)
	log.ResponseCode = resp.StatusCode

	if !isNilServiceAny(logger) {
		logger.Log(log)
	}

	return resp, nil
}

func (h *httpService) updateMetrics(ctx context.Context, method string, timeTaken float64, statusCode int) {
	h.mu.RLock()
	metrics := h.Metrics
	svcName := h.name
	h.mu.RUnlock()
	h.recordMetrics(ctx, metrics, svcName, method, timeTaken, statusCode)
}

func (h *httpService) recordMetrics(ctx context.Context, metrics Metrics, svcName, method string, timeTaken float64, statusCode int) {
	if !isNilServiceAny(metrics) {
		labels := []string{"path", h.url, "method", method, "status", fmt.Sprintf("%v", statusCode)}

		if svcName != "" {
			labels = append(labels, "service", svcName)
		}

		metrics.RecordHistogram(ctx, "app_http_service_response", timeTaken, labels...)
	}
}

func (h *httpService) UseServiceName(name string) {
	if h == nil || name == "" {
		return
	}
	h.mu.Lock()
	if h.name == "" {
		h.name = name
	}
	h.mu.Unlock()
}

func (h *httpService) UseLogger(logger any) {
	if h == nil {
		return
	}
	l, ok := logger.(Logger)
	if !ok || isNilServiceAny(l) {
		return
	}
	h.mu.Lock()
	if isNilServiceAny(h.Logger) {
		h.Logger = l
	}
	h.mu.Unlock()
}

func (h *httpService) UseMetrics(metrics any) {
	if h == nil {
		return
	}
	m, ok := metrics.(Metrics)
	if !ok || isNilServiceAny(m) {
		return
	}
	h.mu.Lock()
	if isNilServiceAny(h.Metrics) {
		h.Metrics = m
	}
	h.mu.Unlock()
}

func (h *httpService) UseTracerProvider(tp trace.TracerProvider) {
	if h == nil || isNilServiceAny(tp) {
		return
	}
	h.mu.Lock()
	if !h.explicitTracer {
		h.tracerProvider = tp
		h.Tracer = tp.Tracer("pi-http-client")
	}
	h.mu.Unlock()
}

func joinURL(base, path string) string {
	trimmedBase := strings.TrimRight(base, "/")
	trimmedPath := strings.TrimLeft(path, "/")
	if trimmedPath == "" {
		return trimmedBase
	}
	if strings.HasPrefix(trimmedPath, "?") {
		return trimmedBase + trimmedPath
	}
	if trimmedBase == "" {
		return "/" + trimmedPath
	}
	return trimmedBase + "/" + trimmedPath
}

func encodeQueryParameters(req *http.Request, queryParams map[string]any) {
	if len(queryParams) == 0 {
		return
	}
	q := req.URL.Query()

	for k, v := range queryParams {
		switch vt := v.(type) {
		case []string:
			for _, val := range vt {
				q.Add(k, val)
			}
		default:
			q.Set(k, fmt.Sprintf("%v", v))
		}
	}

	req.URL.RawQuery = q.Encode()
}

type attributesOption map[string]string

func (a attributesOption) AddOption(h HTTP) HTTP {
	if svc := extractHTTPService(h); svc != nil {
		if name, ok := a["name"]; ok {
			svc.mu.Lock()
			svc.name = name
			svc.mu.Unlock()
		}
	}

	return h
}

// WithAttributes returns an Option that sets the attributes of the HTTP service.
func WithAttributes(attributes map[string]string) Options {
	return attributesOption(attributes)
}
