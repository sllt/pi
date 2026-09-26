// Package http provides a set of utilities for handling HTTP requests and responses within the Pi framework.
package http

import (
	"encoding/json"
	"errors"
	"net/http"
	"reflect"

	resTypes "github.com/sllt/pi/pkg/pi/http/response"
)

var (
	errEmptyResponse = errors.New("internal server error")
)

// NewResponder creates a new Responder instance from the given http.ResponseWriter.
func NewResponder(w http.ResponseWriter, method string) *Responder {
	return &Responder{w: w, method: method}
}

// Responder encapsulates an http.ResponseWriter and is responsible for crafting structured responses.
type Responder struct {
	w      http.ResponseWriter
	method string
}

// Respond sends a response with the given data and handles potential errors, setting appropriate
// status codes and formatting responses as JSON with {code, data, message, meta} format.
func (r Responder) Respond(data any, err error) {
	if err != nil {
		data = nil
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

		_, _ = r.w.Write([]byte(`{"code":-1,"data":null,"message":"failed to encode response as JSON"}` + "\n"))

		return
	}

	statusCode := r.getHTTPStatusCode(data, err)
	r.w.WriteHeader(statusCode)
	_, _ = r.w.Write(jsonData)
	_, _ = r.w.Write([]byte("\n"))
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
	return response{Code: code, Data: nil, Message: message, Meta: meta}
}

// getHTTPStatusCode returns the HTTP status code for the response.
func (r Responder) getHTTPStatusCode(data any, err error) int {
	if err == nil {
		if customCode, ok := getCustomStatusCode(data); ok {
			return customCode
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
	case resTypes.File:
		r.w.Header().Set("Content-Type", v.ContentType)
		r.w.WriteHeader(statusCode)
		_, _ = r.w.Write(v.Content)

		return true

	case resTypes.Template:
		r.w.Header().Set("Content-Type", "text/html")
		r.w.WriteHeader(statusCode)
		v.Render(r.w)

		return true

	case resTypes.XML:
		contentType := v.ContentType

		if contentType == "" {
			contentType = "application/xml"
		}

		r.w.Header().Set("Content-Type", contentType)
		r.w.WriteHeader(statusCode)

		if len(v.Content) > 0 {
			_, _ = r.w.Write(v.Content)
		}

		return true

	case resTypes.Redirect:
		redirectStatusCode := http.StatusFound

		if r.method == http.MethodPost || r.method == http.MethodPut || r.method == http.MethodPatch {
			redirectStatusCode = http.StatusSeeOther
		}

		r.w.Header().Set("Location", v.URL)
		r.w.WriteHeader(redirectStatusCode)

		return true
	}

	return false
}

// getStatusCodeForSpecialResponse returns the appropriate status code for special response types.
func (r Responder) getStatusCodeForSpecialResponse(data any, err error) int {
	if err == nil {
		if customCode, ok := getCustomStatusCode(data); ok {
			return customCode
		}

		return handleSuccessStatusCode(r.method, data)
	}

	status, _, _ := ErrorResponse(err)
	return status
}

// getCustomStatusCode extracts optional HTTP status code overrides from supported response types.
func getCustomStatusCode(data any) (int, bool) {
	var statusCode int

	switch v := data.(type) {
	case resTypes.Raw:
		statusCode = v.StatusCode
	case resTypes.XML:
		statusCode = v.StatusCode
	case resTypes.File:
		statusCode = v.StatusCode
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
	Code    int            `json:"code"`
	Data    any            `json:"data"`
	Message string         `json:"message"`
	Meta    map[string]any `json:"meta,omitempty"`
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

// isNil checks if the given any value is nil.
// It returns true if the value is nil or if it is a pointer that points to nil.
func isNil(i any) bool {
	if i == nil {
		return true
	}

	v := reflect.ValueOf(i)

	return v.Kind() == reflect.Ptr && v.IsNil()
}
