package service

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

// ErrEmptyResponseBody is returned by (*Response).JSON when the response body is empty.
var ErrEmptyResponseBody = errors.New("service: response body is empty")

// Response holds the buffered result of an outbound HTTP request executed via Request.
type Response struct {
	Body       []byte
	StatusCode int
	Header     http.Header
	headers    http.Header
}

// OK reports whether StatusCode is in the 2xx success range.
func (r *Response) OK() bool {
	return r != nil && r.StatusCode >= http.StatusOK && r.StatusCode < http.StatusMultipleChoices
}

// Bytes returns the raw response body bytes.
func (r *Response) Bytes() []byte {
	if r == nil {
		return nil
	}
	return r.Body
}

// String returns the response body as a string.
func (r *Response) String() string {
	if r == nil {
		return ""
	}
	return string(r.Body)
}

// JSON unmarshals the response body into dst.
func (r *Response) JSON(dst any) error {
	if r == nil {
		return errors.New("service: response is nil")
	}
	if dst == nil {
		return errors.New("service: json destination is nil")
	}
	trimmed := bytes.TrimSpace(r.Body)
	if len(trimmed) == 0 {
		return ErrEmptyResponseBody
	}
	if err := json.Unmarshal(trimmed, dst); err != nil {
		return fmt.Errorf("service: decode response json: %w", err)
	}
	return nil
}

// GetHeader returns the first value associated with the given header key.
func (r *Response) GetHeader(key string) string {
	if r == nil {
		return ""
	}
	if r.Header != nil {
		return r.Header.Get(key)
	}
	if r.headers != nil {
		return r.headers.Get(key)
	}

	return ""
}
