package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
)

const (
	defaultMaxResponseBytes int64 = 10 << 20 // 10 MiB
	drainLimitBytes         int64 = 4096
)

// ErrResponseTooLarge is returned when an outbound HTTP response body exceeds MaxBytes.
var ErrResponseTooLarge = errors.New("service: response body exceeds maximum size")

// Request is a fluent outbound HTTP request builder obtained via HTTP.R().
// Requests always execute through the originating HTTP service (including all
// configured decorators such as retry, circuit breaker, auth, headers, and rate limiting).
type Request struct {
	svc         HTTP
	headers     map[string]string
	query       map[string]any
	jsonBody    any
	hasJSON     bool
	rawBody     []byte
	contentType string
	hasRawBody  bool
	maxBytes    int64
	hasMaxBytes bool
}

func newRequest(svc HTTP) *Request {
	return &Request{
		svc:      svc,
		maxBytes: defaultMaxResponseBytes,
	}
}

func (r *Request) ensureInit() *Request {
	if r == nil {
		return newRequest(nil)
	}
	return r
}

// Header sets a single request header (key is canonicalized).
func (r *Request) Header(k, v string) *Request {
	r = r.ensureInit()
	key := http.CanonicalHeaderKey(strings.TrimSpace(k))
	if key == "" {
		return r
	}
	if r.headers == nil {
		r.headers = make(map[string]string)
	}
	r.headers[key] = v
	return r
}

// Headers sets multiple request headers (keys are canonicalized).
func (r *Request) Headers(m map[string]string) *Request {
	r = r.ensureInit()
	if len(m) == 0 {
		return r
	}
	if r.headers == nil {
		r.headers = make(map[string]string, len(m))
	}
	for k, v := range m {
		key := http.CanonicalHeaderKey(strings.TrimSpace(k))
		if key != "" {
			r.headers[key] = v
		}
	}
	return r
}

// Bearer sets the Authorization header to "Bearer <token>".
func (r *Request) Bearer(token string) *Request {
	return r.Header(AuthHeader, "Bearer "+token)
}

// Query sets a single query parameter.
func (r *Request) Query(k string, v any) *Request {
	r = r.ensureInit()
	if k == "" {
		return r
	}
	if r.query == nil {
		r.query = make(map[string]any)
	}
	r.query[k] = v
	return r
}

// QueryParams sets multiple query parameters.
func (r *Request) QueryParams(m map[string]any) *Request {
	r = r.ensureInit()
	if len(m) == 0 {
		return r
	}
	if r.query == nil {
		r.query = make(map[string]any, len(m))
	}
	for k, v := range m {
		if k != "" {
			r.query[k] = v
		}
	}
	return r
}

// JSON configures a JSON request body to be marshaled on execute and sets
// Content-Type: application/json when no Content-Type header is explicitly set.
func (r *Request) JSON(body any) *Request {
	r = r.ensureInit()
	r.jsonBody = body
	r.hasJSON = true
	r.rawBody = nil
	r.contentType = ""
	r.hasRawBody = false
	return r
}

// Body configures raw request payload bytes and an optional Content-Type header.
func (r *Request) Body(data []byte, contentType string) *Request {
	r = r.ensureInit()
	r.rawBody = data
	r.contentType = contentType
	r.hasRawBody = true
	r.jsonBody = nil
	r.hasJSON = false
	return r
}

// MaxBytes overrides the maximum allowed response body size in bytes (default 10 MiB).
func (r *Request) MaxBytes(n int64) *Request {
	r = r.ensureInit()
	r.maxBytes = n
	r.hasMaxBytes = true
	return r
}

// Get executes an HTTP GET request and buffers the bounded response body.
func (r *Request) Get(ctx context.Context, path string) (*Response, error) {
	return r.execute(ctx, http.MethodGet, path)
}

// Post executes an HTTP POST request and buffers the bounded response body.
func (r *Request) Post(ctx context.Context, path string) (*Response, error) {
	return r.execute(ctx, http.MethodPost, path)
}

// Put executes an HTTP PUT request and buffers the bounded response body.
func (r *Request) Put(ctx context.Context, path string) (*Response, error) {
	return r.execute(ctx, http.MethodPut, path)
}

// Patch executes an HTTP PATCH request and buffers the bounded response body.
func (r *Request) Patch(ctx context.Context, path string) (*Response, error) {
	return r.execute(ctx, http.MethodPatch, path)
}

// Delete executes an HTTP DELETE request and buffers the bounded response body.
func (r *Request) Delete(ctx context.Context, path string) (*Response, error) {
	return r.execute(ctx, http.MethodDelete, path)
}

func (r *Request) execute(ctx context.Context, method, path string) (*Response, error) {
	if r == nil || isNilServiceAny(r.svc) {
		return nil, errors.New("service: HTTP client is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	maxBytes := r.maxBytes
	if r.hasMaxBytes {
		if maxBytes <= 0 {
			return nil, fmt.Errorf("service: MaxBytes must be positive, got %d", maxBytes)
		}
	} else {
		maxBytes = defaultMaxResponseBytes
	}

	headers := cloneStringMap(r.headers)

	var bodyBytes []byte
	switch {
	case r.hasJSON:
		encoded, err := json.Marshal(r.jsonBody)
		if err != nil {
			return nil, fmt.Errorf("service: marshal json body: %w", err)
		}
		bodyBytes = encoded
		if !hasHeaderKey(headers, "Content-Type") {
			if headers == nil {
				headers = make(map[string]string, 1)
			}
			headers["Content-Type"] = "application/json"
		}
	case r.hasRawBody:
		bodyBytes = r.rawBody
		if r.contentType != "" && !hasHeaderKey(headers, "Content-Type") {
			if headers == nil {
				headers = make(map[string]string, 1)
			}
			headers["Content-Type"] = r.contentType
		}
	}

	var (
		httpResp *http.Response
		err      error
	)

	switch method {
	case http.MethodGet:
		httpResp, err = r.svc.GetWithHeaders(ctx, path, r.query, headers)
	case http.MethodPost:
		httpResp, err = r.svc.PostWithHeaders(ctx, path, r.query, bodyBytes, headers)
	case http.MethodPut:
		httpResp, err = r.svc.PutWithHeaders(ctx, path, r.query, bodyBytes, headers)
	case http.MethodPatch:
		httpResp, err = r.svc.PatchWithHeaders(ctx, path, r.query, bodyBytes, headers)
	case http.MethodDelete:
		deletePath, pathErr := appendQueryToPath(path, r.query)
		if pathErr != nil {
			return nil, pathErr
		}
		httpResp, err = r.svc.DeleteWithHeaders(ctx, deletePath, bodyBytes, headers)
	default:
		return nil, fmt.Errorf("service: unsupported HTTP method %q", method)
	}

	if err != nil {
		if httpResp != nil && httpResp.Body != nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(httpResp.Body, drainLimitBytes))
			_ = httpResp.Body.Close()
		}
		return nil, err
	}
	if httpResp == nil {
		return nil, errors.New("service: nil http response")
	}

	var respBody []byte
	if httpResp.Body != nil {
		defer func() {
			_, _ = io.Copy(io.Discard, io.LimitReader(httpResp.Body, drainLimitBytes))
			_ = httpResp.Body.Close()
		}()

		limit := maxBytes + 1
		if maxBytes == math.MaxInt64 {
			limit = math.MaxInt64
		}
		data, readErr := io.ReadAll(io.LimitReader(httpResp.Body, limit))
		if readErr != nil {
			return nil, fmt.Errorf("service: read response body: %w", readErr)
		}
		if maxBytes < math.MaxInt64 && int64(len(data)) > maxBytes {
			return nil, fmt.Errorf("%w: limit %d bytes", ErrResponseTooLarge, maxBytes)
		}
		respBody = data
	}

	return &Response{
		StatusCode: httpResp.StatusCode,
		Header:     httpResp.Header,
		headers:    httpResp.Header,
		Body:       respBody,
	}, nil
}

func cloneStringMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func hasHeaderKey(headers map[string]string, key string) bool {
	for k := range headers {
		if strings.EqualFold(k, key) {
			return true
		}
	}
	return false
}

func appendQueryToPath(path string, queryParams map[string]any) (string, error) {
	if len(queryParams) == 0 {
		return path, nil
	}
	u, err := url.Parse(path)
	if err != nil {
		return "", fmt.Errorf("service: invalid request path %q: %w", path, err)
	}
	q := u.Query()
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
	u.RawQuery = q.Encode()
	return u.String(), nil
}
