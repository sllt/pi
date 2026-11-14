package http

import "net/http"

// NewRequestWithSnapshot creates a Request backed by an optional pre-read raw body snapshot.
func NewRequestWithSnapshot(r *http.Request, validate func(any) error, rawBody []byte, hasRawBody bool) *Request {
	return &Request{
		req:        r,
		validate:   validate,
		rawBody:    rawBody,
		hasRawBody: hasRawBody,
	}
}

// HTTPRequest returns the underlying *http.Request, or nil when r is nil.
func (r *Request) HTTPRequest() *http.Request {
	if r == nil {
		return nil
	}
	return r.req
}

// RawBodyBytes returns the raw request body bytes, returning the captured snapshot without extra copying when available.
func (r *Request) RawBodyBytes() ([]byte, error) {
	if r == nil || r.req == nil {
		return nil, nil
	}
	if r.hasRawBody {
		return r.rawBody, nil
	}
	return r.body()
}
