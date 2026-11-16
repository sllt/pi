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

// NewRequestWithSnapshotAndLimit creates a Request backed by an optional pre-read raw body snapshot
// and an explicit maximum body size limit (when maxBodyBytes > 0).
func NewRequestWithSnapshotAndLimit(r *http.Request, validate func(any) error, rawBody []byte, hasRawBody bool, maxBodyBytes int64) *Request {
	return &Request{
		req:          r,
		validate:     validate,
		maxBodyBytes: maxBodyBytes,
		rawBody:      rawBody,
		hasRawBody:   hasRawBody,
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

// RawBodyBytesWithLimit returns the raw request body bytes, using limit when no snapshot or explicit maxBodyBytes is set.
func (r *Request) RawBodyBytesWithLimit(limit int64) ([]byte, error) {
	if r == nil || r.req == nil {
		return nil, nil
	}
	if r.hasRawBody {
		return r.rawBody, nil
	}
	if limit > 0 && r.maxBodyBytes <= 0 {
		clone := *r
		clone.maxBodyBytes = limit
		body, err := clone.body()
		if err == nil {
			r.rawBody = clone.rawBody
			r.hasRawBody = clone.hasRawBody
		}
		return body, err
	}
	return r.body()
}

// ReleaseRawBody releases the captured raw body buffer and replaces req.Body with http.NoBody
// while keeping req.MultipartForm intact for handler execution and post-handler cleanup.
func (r *Request) ReleaseRawBody() {
	if r == nil {
		return
	}
	r.rawBody = nil
	r.hasRawBody = true
	if r.req != nil {
		if r.req.Body != nil && r.req.Body != http.NoBody {
			_ = r.req.Body.Close()
		}
		r.req.Body = http.NoBody
	}
}
