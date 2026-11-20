package testkit

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"

	"github.com/sllt/pi/pkg/pi"
	"github.com/sllt/pi/pkg/pi/auth"
)

// Envelope represents Pi's standard JSON HTTP response envelope.
type Envelope struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
	Details any             `json:"details,omitempty"`
	Meta    any             `json:"meta,omitempty"`
}

// Client exercises *pi.App HTTP routes in-process without opening network ports.
// All With* derivation methods return a shallow-isolated copy without mutating the receiver.
type Client struct {
	t         TB
	app       *pi.App
	headers   http.Header
	cookies   []*http.Cookie
	principal *auth.Principal
}

// NewClient creates a fluent in-process test client bound to app.
func NewClient(t TB, app *pi.App) *Client {
	if t != nil {
		t.Helper()
		if app == nil {
			t.Fatalf("testkit: app must not be nil")
			return nil
		}
	}
	return &Client{
		t:   t,
		app: app,
	}
}

func (c *Client) clone() *Client {
	if c == nil {
		return &Client{}
	}
	cp := *c
	if c.headers != nil {
		cp.headers = c.headers.Clone()
	}
	if len(c.cookies) > 0 {
		cp.cookies = make([]*http.Cookie, 0, len(c.cookies))
		for _, ck := range c.cookies {
			if ck == nil {
				continue
			}
			copied := *ck
			cp.cookies = append(cp.cookies, &copied)
		}
	}
	if c.principal != nil {
		p := *c.principal
		p.Permissions = append([]string(nil), c.principal.Permissions...)
		cp.principal = &p
	}
	return &cp
}

// WithHeader returns a derived Client with the given header set.
func (c *Client) WithHeader(k, v string) *Client {
	cp := c.clone()
	if k == "" {
		return cp
	}
	if cp.headers == nil {
		cp.headers = make(http.Header)
	}
	cp.headers.Set(k, v)
	return cp
}

// WithBearer returns a derived Client with Authorization: Bearer <token>.
func (c *Client) WithBearer(token string) *Client {
	cp := c.clone()
	if cp.headers == nil {
		cp.headers = make(http.Header)
	}
	if token == "" {
		cp.headers.Del("Authorization")
	} else {
		cp.headers.Set("Authorization", "Bearer "+token)
	}
	return cp
}

// WithPrincipal returns a derived Client that attaches p via auth.WithPrincipal on each request.
func (c *Client) WithPrincipal(p auth.Principal) *Client {
	cp := c.clone()
	copied := p
	copied.Permissions = append([]string(nil), p.Permissions...)
	cp.principal = &copied
	return cp
}

// WithCookie returns a derived Client that sends c on each request.
func (c *Client) WithCookie(cookie *http.Cookie) *Client {
	cp := c.clone()
	if cookie != nil {
		copied := *cookie
		cp.cookies = append(cp.cookies, &copied)
	}
	return cp
}

// Get sends a GET request to path.
func (c *Client) Get(path string) *Response {
	if c != nil && c.t != nil {
		c.t.Helper()
	}
	return c.R().Get(path)
}

// Delete sends a DELETE request to path.
func (c *Client) Delete(path string) *Response {
	if c != nil && c.t != nil {
		c.t.Helper()
	}
	return c.R().Delete(path)
}

// PostJSON marshals body as JSON and sends a POST request to path.
func (c *Client) PostJSON(path string, body any) *Response {
	if c != nil && c.t != nil {
		c.t.Helper()
	}
	return c.R().JSON(body).Post(path)
}

// PutJSON marshals body as JSON and sends a PUT request to path.
func (c *Client) PutJSON(path string, body any) *Response {
	if c != nil && c.t != nil {
		c.t.Helper()
	}
	return c.R().JSON(body).Put(path)
}

// PatchJSON marshals body as JSON and sends a PATCH request to path.
func (c *Client) PatchJSON(path string, body any) *Response {
	if c != nil && c.t != nil {
		c.t.Helper()
	}
	return c.R().JSON(body).Patch(path)
}

// R starts a mutable per-request builder inheriting defaults from the Client.
func (c *Client) R() *RequestBuilder {
	r := &RequestBuilder{
		client:  c,
		headers: make(http.Header),
		query:   make(url.Values),
	}
	if c != nil {
		if c.headers != nil {
			r.headers = c.headers.Clone()
		}
		for _, ck := range c.cookies {
			if ck != nil {
				copied := *ck
				r.cookies = append(r.cookies, &copied)
			}
		}
		if c.principal != nil {
			p := *c.principal
			p.Permissions = append([]string(nil), c.principal.Permissions...)
			r.principal = &p
		}
	}
	return r
}

// RequestBuilder builds and executes a single in-process HTTP request against a *pi.App.
type RequestBuilder struct {
	client      *Client
	headers     http.Header
	cookies     []*http.Cookie
	query       url.Values
	principal   *auth.Principal
	jsonBody    any
	hasJSON     bool
	formBody    url.Values
	hasForm     bool
	rawBody     io.Reader
	contentType string
	hasRawBody  bool
}

// Header sets a single request header.
func (r *RequestBuilder) Header(k, v string) *RequestBuilder {
	if k != "" {
		r.headers.Set(k, v)
	}
	return r
}

// Headers sets multiple request headers.
func (r *RequestBuilder) Headers(m map[string]string) *RequestBuilder {
	for k, v := range m {
		if k != "" {
			r.headers.Set(k, v)
		}
	}
	return r
}

// Bearer sets Authorization: Bearer <token>.
func (r *RequestBuilder) Bearer(token string) *RequestBuilder {
	if token == "" {
		r.headers.Del("Authorization")
	} else {
		r.headers.Set("Authorization", "Bearer "+token)
	}
	return r
}

// Principal attaches p via auth.WithPrincipal on the executed request context.
func (r *RequestBuilder) Principal(p auth.Principal) *RequestBuilder {
	copied := p
	copied.Permissions = append([]string(nil), p.Permissions...)
	r.principal = &copied
	return r
}

// Cookie appends a cookie to the request.
func (r *RequestBuilder) Cookie(c *http.Cookie) *RequestBuilder {
	if c != nil {
		copied := *c
		r.cookies = append(r.cookies, &copied)
	}
	return r
}

// Query appends a query parameter. []string values are added as repeated parameters.
func (r *RequestBuilder) Query(k string, v any) *RequestBuilder {
	if k == "" {
		return r
	}
	switch vt := v.(type) {
	case []string:
		for _, val := range vt {
			r.query.Add(k, val)
		}
	default:
		r.query.Add(k, fmt.Sprintf("%v", v))
	}
	return r
}

// JSON configures a JSON request body to be marshaled on execute.
func (r *RequestBuilder) JSON(body any) *RequestBuilder {
	r.jsonBody = body
	r.hasJSON = true
	r.formBody = nil
	r.hasForm = false
	r.rawBody = nil
	r.contentType = ""
	r.hasRawBody = false
	return r
}

// Form configures a URL-encoded form body.
func (r *RequestBuilder) Form(vals url.Values) *RequestBuilder {
	r.formBody = vals
	r.hasForm = true
	r.jsonBody = nil
	r.hasJSON = false
	r.rawBody = nil
	r.contentType = ""
	r.hasRawBody = false
	return r
}

// Body configures a raw request body reader and optional Content-Type header.
func (r *RequestBuilder) Body(reader io.Reader, contentType string) *RequestBuilder {
	r.rawBody = reader
	r.contentType = contentType
	r.hasRawBody = true
	r.jsonBody = nil
	r.hasJSON = false
	r.formBody = nil
	r.hasForm = false
	return r
}

// Get executes a GET request.
func (r *RequestBuilder) Get(path string) *Response {
	if r != nil && r.client != nil && r.client.t != nil {
		r.client.t.Helper()
	}
	return r.Do(http.MethodGet, path)
}

// Post executes a POST request.
func (r *RequestBuilder) Post(path string) *Response {
	if r != nil && r.client != nil && r.client.t != nil {
		r.client.t.Helper()
	}
	return r.Do(http.MethodPost, path)
}

// Put executes a PUT request.
func (r *RequestBuilder) Put(path string) *Response {
	if r != nil && r.client != nil && r.client.t != nil {
		r.client.t.Helper()
	}
	return r.Do(http.MethodPut, path)
}

// Patch executes a PATCH request.
func (r *RequestBuilder) Patch(path string) *Response {
	if r != nil && r.client != nil && r.client.t != nil {
		r.client.t.Helper()
	}
	return r.Do(http.MethodPatch, path)
}

// Delete executes a DELETE request.
func (r *RequestBuilder) Delete(path string) *Response {
	if r != nil && r.client != nil && r.client.t != nil {
		r.client.t.Helper()
	}
	return r.Do(http.MethodDelete, path)
}

// Do executes an HTTP request with the given method and path against the bound *pi.App.
func (r *RequestBuilder) Do(method, path string) *Response {
	if r == nil || r.client == nil || r.client.t == nil {
		panic("testkit: RequestBuilder has nil client or TB")
	}
	t := r.client.t
	t.Helper()

	if r.client.app == nil {
		t.Fatalf("testkit: app must not be nil")
		return nil
	}

	target := strings.TrimSpace(path)
	if target == "" {
		target = "/"
	} else if !strings.HasPrefix(target, "/") && !strings.HasPrefix(target, "http://") && !strings.HasPrefix(target, "https://") {
		target = "/" + target
	}
	if _, err := url.ParseRequestURI(target); err != nil {
		t.Fatalf("testkit: invalid request path %q: %v", path, err)
		return nil
	}
	if len(r.query) > 0 {
		u, err := url.Parse(target)
		if err != nil {
			t.Fatalf("testkit: invalid request path %q: %v", path, err)
			return nil
		}
		q := u.Query()
		for k, vals := range r.query {
			for _, v := range vals {
				q.Add(k, v)
			}
		}
		u.RawQuery = q.Encode()
		target = u.String()
	}

	var (
		bodyReader io.Reader = http.NoBody
		defaultCT  string
	)
	switch {
	case r.hasJSON:
		encoded, err := json.Marshal(r.jsonBody)
		if err != nil {
			t.Fatalf("testkit: marshal json request body: %v", err)
			return nil
		}
		bodyReader = bytes.NewReader(encoded)
		defaultCT = "application/json"
	case r.hasForm:
		bodyReader = strings.NewReader(r.formBody.Encode())
		defaultCT = "application/x-www-form-urlencoded"
	case r.hasRawBody:
		if r.rawBody != nil {
			bodyReader = r.rawBody
		}
		defaultCT = r.contentType
	}

	req := httptest.NewRequest(method, target, bodyReader)
	for k, vals := range r.headers {
		for _, v := range vals {
			req.Header.Add(k, v)
		}
	}
	if defaultCT != "" && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", defaultCT)
	}
	for _, ck := range r.cookies {
		if ck != nil {
			req.AddCookie(ck)
		}
	}
	if r.principal != nil {
		req = req.WithContext(auth.WithPrincipal(req.Context(), *r.principal))
	}

	rec := Request(t, r.client.app, req)
	return &Response{
		ResponseRecorder: rec,
		t:                t,
	}
}

// Response wraps *httptest.ResponseRecorder with fluent status and JSON/envelope assertions.
type Response struct {
	*httptest.ResponseRecorder
	t TB
}

// ExpectStatus fails the test immediately if the recorded status code differs from want.
func (r *Response) ExpectStatus(want int) *Response {
	if r != nil && r.t != nil {
		r.t.Helper()
	}
	if r == nil || r.ResponseRecorder == nil {
		if r != nil && r.t != nil {
			r.t.Fatalf("testkit: expected status %d, got nil response", want)
		}
		return r
	}
	if r.Code != want {
		if r.t != nil {
			r.t.Fatalf("testkit: expected HTTP status %d, got %d; body=%s", want, r.Code, r.Body.String())
		}
	}
	return r
}

// JSON unmarshals the raw response body into dst.
func (r *Response) JSON(dst any) *Response {
	if r != nil && r.t != nil {
		r.t.Helper()
	}
	if r == nil || r.ResponseRecorder == nil {
		if r != nil && r.t != nil {
			r.t.Fatalf("testkit: cannot decode JSON from nil response")
		}
		return r
	}
	if dst == nil {
		if r.t != nil {
			r.t.Fatalf("testkit: JSON destination must not be nil")
		}
		return r
	}
	if err := json.Unmarshal(r.Body.Bytes(), dst); err != nil {
		if r.t != nil {
			r.t.Fatalf("testkit: decode response JSON: %v (body=%s)", err, r.Body.String())
		}
	}
	return r
}

// Envelope decodes and returns Pi's standard JSON response envelope.
func (r *Response) Envelope() Envelope {
	if r != nil && r.t != nil {
		r.t.Helper()
	}
	if r == nil || r.ResponseRecorder == nil {
		if r != nil && r.t != nil {
			r.t.Fatalf("testkit: cannot decode envelope from nil response")
		}
		return Envelope{}
	}
	var env Envelope
	r.JSON(&env)
	return env
}

// Data decodes the `data` field of Pi's standard JSON response envelope into dst.
func (r *Response) Data(dst any) *Response {
	if r != nil && r.t != nil {
		r.t.Helper()
	}
	if r == nil || r.ResponseRecorder == nil {
		if r != nil && r.t != nil {
			r.t.Fatalf("testkit: cannot decode data from nil response")
		}
		return r
	}
	if dst == nil {
		if r.t != nil {
			r.t.Fatalf("testkit: Data destination must not be nil")
		}
		return r
	}
	env := r.Envelope()
	if env.Code != 0 {
		if r.t != nil {
			r.t.Fatalf("testkit: expected success envelope (code=0), got code=%d message=%q (body=%s)",
				env.Code, env.Message, r.Body.String())
		}
		return r
	}
	trimmed := bytes.TrimSpace(env.Data)
	if len(trimmed) == 0 || string(trimmed) == "null" {
		if r.t != nil {
			r.t.Fatalf("testkit: response envelope has empty or null data (code=%d, message=%q, body=%s)",
				env.Code, env.Message, r.Body.String())
		}
		return r
	}
	if err := json.Unmarshal(trimmed, dst); err != nil {
		if r.t != nil {
			r.t.Fatalf("testkit: decode envelope data: %v (data=%s)", err, string(trimmed))
		}
	}
	return r
}
