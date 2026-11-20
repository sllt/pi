// Package http provides a set of utilities for handling HTTP requests and responses within the Pi framework.
package http

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"

	"github.com/sllt/pi/pkg/pi/apperror"
	resTypes "github.com/sllt/pi/pkg/pi/http/response"
)

var (
	errEmptyResponse = errors.New("internal server error")
)

// NewResponder creates a new Responder instance from the given http.ResponseWriter.
func NewResponder(w http.ResponseWriter, method string) *Responder {
	return &Responder{w: w, method: method}
}

// RawReplay writes a pre-captured HTTP response (status, headers, and body)
// without JSON envelope wrapping, used by middleware such as Idempotency.
type RawReplay struct {
	StatusCode int
	Headers    map[string][]string
	Body       []byte
}

// Responder encapsulates an http.ResponseWriter and is responsible for crafting structured responses.
type Responder struct {
	w             http.ResponseWriter
	method        string
	ctx           context.Context
	streamBaseCtx context.Context
	policy        StatusPolicy
	override      int
}

type StatusPolicy uint8

const (
	LegacyStatus StatusPolicy = iota
	ExplicitStatus
)

func NewResponderForRequest(w http.ResponseWriter, r *http.Request, policy StatusPolicy) *Responder {
	return &Responder{w: w, method: r.Method, ctx: r.Context(), policy: policy}
}

// SetHeader sets a response header on the underlying http.ResponseWriter.
func (r *Responder) SetHeader(key, value string) {
	if r != nil && r.w != nil && key != "" {
		r.w.Header().Set(key, value)
	}
}

// SetStreamBaseContext sets the base context passed to Stream.Run and SSE.Run.
func (r *Responder) SetStreamBaseContext(ctx context.Context) {
	if r != nil {
		r.streamBaseCtx = ctx
	}
}

// Respond sends a response with the given data and handles potential errors, setting appropriate
// status codes and formatting responses as JSON with {code, data, message, meta} format.
func (r Responder) Respond(data any, err error) {
	if err != nil {
		data = nil
		var hr HeaderResponder
		if errors.As(err, &hr) && !isNilAny(hr) {
			for k, v := range hr.ResponseHeaders() {
				r.SetHeader(k, v)
			}
		}
	}
	if result, ok := data.(resTypes.Result); ok {
		switch result.StatusCode {
		case 200, 201, 202, 204:
			r.override = result.StatusCode
			data = result.Data
			for k, v := range result.Headers {
				r.w.Header().Set(k, v)
			}
		default:
			data = nil
			err = errors.New("invalid explicit success status")
		}
	}
	if err == nil {
		if response, ok := data.(resTypes.Response); ok {
			response.SetCustomHeaders(r.w)
		}
		if _, ok := data.(resTypes.Handled); ok {
			return
		}
		if !isRawReplay(data) && r.getHTTPStatusCode(data, nil) == http.StatusNoContent {
			r.w.WriteHeader(http.StatusNoContent)
			return
		}
	}
	if r.handleSpecialResponseTypes(data, err) {
		return
	}

	var resp any

	switch v := data.(type) {
	case resTypes.Raw:
		resp = v.Data
	case resTypes.Response:
		resp = r.buildResponse(v.Data, v.Meta, err)
	default:
		if isNil(data) {
			data = nil
		}

		resp = r.buildResponse(data, nil, err)
	}

	if r.w.Header().Get("Content-Type") == "" {
		r.w.Header().Set("Content-Type", "application/json")
	}

	jsonData, encodeErr := json.Marshal(resp)
	if encodeErr != nil {
		r.w.WriteHeader(http.StatusInternalServerError)
		if r.method != http.MethodHead {
			_, _ = r.w.Write([]byte(`{"code":-1,"data":null,"message":"failed to encode response as JSON"}` + "\n"))
		}

		return
	}

	statusCode := r.getHTTPStatusCode(data, err)
	r.w.WriteHeader(statusCode)
	if r.method != http.MethodHead {
		_, _ = r.w.Write(jsonData)
		_, _ = r.w.Write([]byte("\n"))
	}
}

// buildResponse constructs the unified response structure.
func (r Responder) buildResponse(data any, meta map[string]any, err error) response {
	if err == nil {
		return response{Code: 0, Data: data, Message: "ok", Meta: meta}
	}

	// Handle empty struct as data
	if isEmptyStruct(data) {
		return response{Code: getErrorCode(errEmptyResponse), Data: nil, Message: errEmptyResponse.Error()}
	}

	_, code, message := ErrorResponse(err)
	return response{Code: code, Data: nil, Message: message, Meta: meta, Details: ErrorDetails(err)}
}

// getHTTPStatusCode returns the HTTP status code for the response.
func (r Responder) getHTTPStatusCode(data any, err error) int {
	if err == nil {
		if r.override != 0 {
			return r.override
		}
		if customCode, ok := getCustomStatusCode(data); ok {
			return customCode
		}

		if r.policy == ExplicitStatus {
			return http.StatusOK
		}
		return handleSuccessStatusCode(r.method, data)
	}

	status, _, _ := ErrorResponse(err)
	return status
}

// getErrorCode returns the business error code from the error.
// Priority: CodeResponder.Code() > StatusCodeResponder.StatusCode() > -1
func getErrorCode(err error) int {
	_, code, _ := ErrorResponse(err)
	return code
}

// handleSpecialResponseTypes handles special response types that bypass JSON encoding.
// Returns true if the response was handled, false otherwise.
func (r Responder) handleSpecialResponseTypes(data any, err error) bool {
	statusCode := r.getStatusCodeForSpecialResponse(data, err)

	switch v := data.(type) {
	case resTypes.Stream:
		r.writeStreamResponse(v, statusCode)
		return true
	case *resTypes.Stream:
		if v == nil {
			return false
		}
		r.writeStreamResponse(*v, statusCode)
		return true
	case resTypes.SSE:
		r.writeSSEResponse(v, statusCode)
		return true
	case *resTypes.SSE:
		if v == nil {
			return false
		}
		r.writeSSEResponse(*v, statusCode)
		return true
	case resTypes.File:
		r.writeFileResponse(v, statusCode)
		return true
	case *resTypes.File:
		if v == nil {
			return false
		}
		r.writeFileResponse(*v, statusCode)
		return true

	case resTypes.Template:
		r.w.Header().Set("Content-Type", "text/html")
		r.w.WriteHeader(statusCode)
		if r.method != http.MethodHead {
			v.Render(r.w)
		}

		return true

	case resTypes.XML:
		r.writeXMLResponse(v, statusCode)
		return true
	case *resTypes.XML:
		if v == nil {
			return false
		}
		r.writeXMLResponse(*v, statusCode)
		return true

	case resTypes.Redirect:
		redirectStatusCode := http.StatusFound

		if r.method == http.MethodPost || r.method == http.MethodPut || r.method == http.MethodPatch {
			redirectStatusCode = http.StatusSeeOther
		}

		r.w.Header().Set("Location", v.URL)
		r.w.WriteHeader(redirectStatusCode)

		return true

	case RawReplay:
		r.writeReplayResponse(v, statusCode)
		return true
	case *RawReplay:
		if v == nil {
			return false
		}
		r.writeReplayResponse(*v, statusCode)
		return true
	}

	return false
}

func isRawReplay(data any) bool {
	switch v := data.(type) {
	case RawReplay:
		return true
	case *RawReplay:
		return v != nil
	default:
		return false
	}
}

func (r Responder) writeReplayResponse(v RawReplay, statusCode int) {
	for k, vals := range v.Headers {
		if k == "" || len(vals) == 0 {
			continue
		}
		canonKey := http.CanonicalHeaderKey(k)
		if canonKey == "Vary" {
			mergeHeaderTokens(r.w.Header(), "Vary", vals)
			continue
		}
		r.w.Header().Del(canonKey)
		for _, val := range vals {
			r.w.Header().Add(canonKey, val)
		}
	}
	r.w.WriteHeader(statusCode)
	if r.method != http.MethodHead && statusCode != http.StatusNoContent && statusCode != http.StatusNotModified && len(v.Body) > 0 {
		_, _ = r.w.Write(v.Body)
	}
}

func mergeHeaderTokens(dst http.Header, key string, incoming []string) {
	seen := make(map[string]struct{})
	for _, existing := range dst.Values(key) {
		for _, part := range strings.Split(existing, ",") {
			token := strings.TrimSpace(part)
			if token == "" {
				continue
			}
			seen[strings.ToLower(token)] = struct{}{}
		}
	}
	for _, val := range incoming {
		for _, part := range strings.Split(val, ",") {
			token := strings.TrimSpace(part)
			if token == "" {
				continue
			}
			lower := strings.ToLower(token)
			if _, ok := seen[lower]; ok {
				continue
			}
			seen[lower] = struct{}{}
			dst.Add(key, token)
		}
	}
}

func (r Responder) writeStreamResponse(v resTypes.Stream, statusCode int) {
	if v.Run == nil {
		r.Respond(nil, errors.New("stream callback is nil"))
		return
	}
	r.writeStreamingResponse(v.Headers, v.ContentType, "application/octet-stream", statusCode, true, func(ctx context.Context, w io.Writer, _ http.Flusher) error {
		return v.Run(ctx, w)
	})
}

func (r Responder) writeSSEResponse(v resTypes.SSE, statusCode int) {
	if v.Run == nil {
		r.Respond(nil, errors.New("sse callback is nil"))
		return
	}
	r.writeStreamingResponse(v.Headers, "", "text/event-stream; charset=utf-8", statusCode, false, v.Run)
}

func (r Responder) writeStreamingResponse(
	headers map[string]string,
	explicitContentType string,
	fallbackContentType string,
	statusCode int,
	autoFlush bool,
	run func(context.Context, io.Writer, http.Flusher) error,
) {
	for k, val := range headers {
		if k != "" && val != "" {
			r.w.Header().Set(k, val)
		}
	}
	contentType := strings.TrimSpace(explicitContentType)
	if contentType == "" {
		contentType = strings.TrimSpace(r.w.Header().Get("Content-Type"))
	}
	if contentType == "" {
		contentType = fallbackContentType
	}
	r.w.Header().Set("Content-Type", contentType)
	r.w.WriteHeader(statusCode)
	rc := http.NewResponseController(r.w)
	_ = rc.Flush()
	if r.method != http.MethodHead {
		ctx := r.streamBaseCtx
		if ctx == nil {
			ctx = r.ctx
		}
		if ctx == nil {
			ctx = context.Background()
		}
		sw := streamResponseWriter{ResponseWriter: r.w, rc: rc, autoFlush: autoFlush}
		_ = run(ctx, sw, sw)
	}
}

type streamResponseWriter struct {
	http.ResponseWriter
	rc        *http.ResponseController
	autoFlush bool
}

func (sw streamResponseWriter) Write(p []byte) (int, error) {
	n, err := sw.ResponseWriter.Write(p)
	if sw.autoFlush && n > 0 && sw.rc != nil {
		_ = sw.rc.Flush()
	}
	return n, err
}

func (sw streamResponseWriter) Flush() {
	if sw.rc != nil {
		_ = sw.rc.Flush()
	}
}

func (sw streamResponseWriter) Unwrap() http.ResponseWriter {
	return sw.ResponseWriter
}

func (r Responder) writeFileResponse(v resTypes.File, statusCode int) {
	for k, val := range v.Headers {
		if k != "" && val != "" {
			r.w.Header().Set(k, val)
		}
	}
	if ct := strings.TrimSpace(v.ContentType); ct != "" {
		r.w.Header().Set("Content-Type", ct)
	} else if strings.TrimSpace(r.w.Header().Get("Content-Type")) == "" {
		r.w.Header().Set("Content-Type", "application/octet-stream")
	}
	r.w.WriteHeader(statusCode)
	if r.method != http.MethodHead {
		_, _ = r.w.Write(v.Content)
	}
}

func (r Responder) writeXMLResponse(v resTypes.XML, statusCode int) {
	contentType := strings.TrimSpace(v.ContentType)
	if contentType == "" {
		contentType = strings.TrimSpace(r.w.Header().Get("Content-Type"))
	}
	if contentType == "" {
		contentType = "application/xml"
	}

	r.w.Header().Set("Content-Type", contentType)
	r.w.WriteHeader(statusCode)

	if r.method != http.MethodHead && len(v.Content) > 0 {
		_, _ = r.w.Write(v.Content)
	}
}

// getStatusCodeForSpecialResponse returns the appropriate status code for special response types.
func (r Responder) getStatusCodeForSpecialResponse(data any, err error) int {
	return r.getHTTPStatusCode(data, err)
}

// getCustomStatusCode extracts optional HTTP status code overrides from supported response types.
func getCustomStatusCode(data any) (int, bool) {
	var statusCode int

	switch v := data.(type) {
	case resTypes.Raw:
		statusCode = v.StatusCode
	case resTypes.XML:
		statusCode = v.StatusCode
	case *resTypes.XML:
		if v == nil {
			return 0, false
		}
		statusCode = v.StatusCode
	case resTypes.File:
		statusCode = v.StatusCode
	case *resTypes.File:
		if v == nil {
			return 0, false
		}
		statusCode = v.StatusCode
	case resTypes.Stream:
		statusCode = v.StatusCode
	case *resTypes.Stream:
		if v == nil {
			return 0, false
		}
		statusCode = v.StatusCode
	case resTypes.SSE:
		return http.StatusOK, true
	case *resTypes.SSE:
		if v == nil {
			return 0, false
		}
		return http.StatusOK, true
	case RawReplay:
		if v.StatusCode != 0 {
			return v.StatusCode, true
		}
		return 0, false
	case *RawReplay:
		if v == nil || v.StatusCode == 0 {
			return 0, false
		}
		return v.StatusCode, true
	default:
		return 0, false
	}

	if statusCode < http.StatusContinue || statusCode > 999 {
		return 0, false
	}

	return statusCode, true
}

// handleSuccessStatusCode returns the status code for successful responses based on HTTP method.
func handleSuccessStatusCode(method string, data any) int {
	switch method {
	case http.MethodPost:
		if data != nil {
			return http.StatusCreated
		}

		return http.StatusAccepted
	case http.MethodDelete:
		return http.StatusNoContent
	default:
		return http.StatusOK
	}
}

// isEmptyStruct checks if a value is a struct with all zero/empty fields.
func isEmptyStruct(data any) bool {
	if data == nil {
		return false
	}

	v := reflect.ValueOf(data)

	// Handle pointers by dereferencing them
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return false // nil pointer isn't an empty struct
		}

		v = v.Elem()
	}

	// Only check actual struct types
	if v.Kind() != reflect.Struct {
		return false
	}

	// Compare against a zero value of the same type
	zero := reflect.Zero(v.Type()).Interface()

	return reflect.DeepEqual(data, zero)
}

// ResponseMarshaller defines an interface for errors that can provide custom fields.
// This enables errors to extend the error response with additional fields.
type ResponseMarshaller interface {
	Response() map[string]any
}

// response represents the unified HTTP JSON response format.
type response struct {
	Code    int               `json:"code"`
	Data    any               `json:"data"`
	Message string            `json:"message"`
	Meta    map[string]any    `json:"meta,omitempty"`
	Details []apperror.Detail `json:"details,omitempty"`
}

// StatusCodeResponder allows errors to specify the HTTP status code.
type StatusCodeResponder interface {
	StatusCode() int
}

// CodeResponder allows errors to specify a business error code.
// This is used in the JSON response "code" field.
// If not implemented, falls back to StatusCodeResponder.StatusCode(), or -1.
type CodeResponder interface {
	Code() int
}

// HeaderResponder allows errors to specify HTTP response headers applied when writing
// the error response on the ServeHTTP goroutine.
type HeaderResponder interface {
	ResponseHeaders() map[string]string
}

// isNil checks if the given any value is nil.
// It returns true if the value is nil or if it is a pointer that points to nil.
func isNil(i any) bool {
	if i == nil {
		return true
	}

	v := reflect.ValueOf(i)

	return v.Kind() == reflect.Ptr && v.IsNil()
}

func isNilAny(i any) bool {
	if i == nil {
		return true
	}
	v := reflect.ValueOf(i)
	switch v.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
		return v.IsNil()
	default:
		return false
	}
}
