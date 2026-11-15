package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/sllt/pi/pkg/pi/apperror"
)

const (
	defaultMaxMemory = 32 << 20 // 32 MB

	// DefaultMaxBodyBytes bounds request body buffering when no explicit limit is configured.
	DefaultMaxBodyBytes int64 = 1 << 20 // 1 MiB
)

type restoredBody struct {
	io.Reader
	io.Closer
}

var (
	errNoFileFound    = errors.New("no files were bounded")
	errNonPointerBind = errors.New("bind error, cannot bind to a non pointer type")
	errNonSliceBind   = errors.New("bind error: input is not a pointer to a byte slice")
)

// Request is an abstraction over the underlying http.Request. This abstraction is useful because it allows us
// to create applications without being aware of the transport. cmd.Request is another such abstraction.
type Request struct {
	req          *http.Request
	validate     func(any) error
	maxBodyBytes int64
	rawBody      []byte
	hasRawBody   bool
}

// NewRequest creates a new Pi Request instance from the given http.Request.
func NewRequest(r *http.Request) *Request {
	return &Request{
		req: r,
	}
}

// NewRequestWithValidator uses an app-owned validator; nil preserves legacy behavior.
func NewRequestWithValidator(r *http.Request, validate func(any) error) *Request {
	return &Request{req: r, validate: validate}
}

// NewRequestWithLimit creates a Request with an optional maximum body size limit (when maxBodyBytes > 0)
// and an app-owned struct validator.
func NewRequestWithLimit(r *http.Request, maxBodyBytes int64, validate func(any) error) *Request {
	return &Request{
		req:          r,
		validate:     validate,
		maxBodyBytes: maxBodyBytes,
	}
}

// Param returns the query parameter with the given key.
func (r *Request) Param(key string) string {
	if r == nil || r.req == nil || r.req.URL == nil {
		return ""
	}
	return r.req.URL.Query().Get(key)
}

// Context returns the context of the request.
func (r *Request) Context() context.Context {
	if r == nil || r.req == nil {
		return context.Background()
	}
	return r.req.Context()
}

// PathParam retrieves a path parameter from the request.
func (r *Request) PathParam(key string) string {
	if r == nil || r.req == nil {
		return ""
	}
	return chi.URLParam(r.req, key)
}

// Bind parses the request body and binds it to the provided interface.
// It also validates the struct using "binding" or "validate" tags.
func (r *Request) Bind(i any) error {
	if r == nil || r.req == nil {
		return apperror.New(apperror.InvalidArgument, 400, "invalid request body")
	}
	v := r.req.Header.Get("Content-Type")
	contentType := strings.TrimSpace(strings.Split(v, ";")[0])
	value := reflect.ValueOf(i)
	if contentType != "binary/octet-stream" && (!value.IsValid() || value.Kind() != reflect.Ptr || value.IsNil()) {
		return errNonPointerBind
	}

	var err error

	switch contentType {
	case "application/json":
		body, bodyErr := r.body()
		if bodyErr != nil {
			return bodyErr
		}

		err = json.Unmarshal(body, &i)
	case "multipart/form-data":
		err = r.bindMultipart(i)
	case "application/x-www-form-urlencoded":
		err = r.bindFormURLEncoded(i)
	case "binary/octet-stream":
		err = r.bindBinary(i)
	}

	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			return maxBytesErr
		}
		return apperror.New(apperror.InvalidArgument, 400, "invalid request body").WithCause(err)
	}

	// Validate the struct after binding
	if r.validate != nil {
		return r.validate(i)
	}
	return validateStruct(i)
}

// HostName retrieves the hostname from the request.
func (r *Request) HostName() string {
	if r == nil || r.req == nil {
		return ""
	}
	proto := r.req.Header.Get("X-Forwarded-Proto")
	if proto == "" {
		proto = "http"
	}

	return fmt.Sprintf("%s://%s", proto, r.req.Host)
}

// Params returns a slice of strings containing the values associated with the given query parameter key.
// If the parameter is not present, an empty slice is returned.
func (r *Request) Params(key string) []string {
	if r == nil || r.req == nil || r.req.URL == nil {
		return nil
	}
	values := r.req.URL.Query()[key]

	var result []string

	for _, value := range values {
		result = append(result, strings.Split(value, ",")...)
	}

	return result
}

func (r *Request) body() ([]byte, error) {
	if r == nil || r.req == nil {
		return nil, nil
	}
	if r.hasRawBody {
		return r.rawBody, nil
	}
	if r.req.Body == nil {
		return nil, nil
	}

	limit := r.maxBodyBytes
	if limit <= 0 {
		limit = DefaultMaxBodyBytes
	}

	origBody := r.req.Body
	bodyBytes, err := io.ReadAll(io.LimitReader(origBody, limit+1))
	if err != nil {
		r.req.Body = restoredBody{
			Reader: io.MultiReader(bytes.NewReader(bodyBytes), origBody),
			Closer: origBody,
		}
		return nil, err
	}
	if int64(len(bodyBytes)) > limit {
		r.req.Body = restoredBody{
			Reader: io.MultiReader(bytes.NewReader(bodyBytes), origBody),
			Closer: origBody,
		}
		return nil, &http.MaxBytesError{Limit: limit}
	}

	r.rawBody = bodyBytes
	r.hasRawBody = true
	r.req.Body = io.NopCloser(bytes.NewReader(bodyBytes))

	return bodyBytes, nil
}

func (r *Request) bindMultipart(ptr any) error {
	return r.bindForm(ptr, true)
}

func (r *Request) bindFormURLEncoded(ptr any) error {
	return r.bindForm(ptr, false)
}

func (r *Request) bindForm(ptr any, isMultipart bool) error {
	if r == nil || r.req == nil {
		return errNonPointerBind
	}
	ptrVal := reflect.ValueOf(ptr)
	if !ptrVal.IsValid() || ptrVal.Kind() != reflect.Ptr || ptrVal.IsNil() {
		return errNonPointerBind
	}

	ptrVal = ptrVal.Elem()

	var fd formData

	if isMultipart {
		if r.req.Body != nil && r.req.MultipartForm == nil {
			limit := r.maxBodyBytes
			if limit <= 0 {
				limit = defaultMaxMemory + DefaultMaxBodyBytes
			}
			r.req.Body = http.MaxBytesReader(nil, r.req.Body, limit)
		}
		if err := r.req.ParseMultipartForm(defaultMaxMemory); err != nil {
			return err
		}

		fd = formData{files: r.req.MultipartForm.File, fields: r.req.MultipartForm.Value}
	} else {
		bodyBytes, err := r.body()
		if err != nil {
			return err
		}

		reqCopy := r.req.Clone(r.req.Context())
		reqCopy.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		if err := reqCopy.ParseForm(); err != nil {
			return err
		}

		fd = formData{fields: reqCopy.Form}
	}

	ok, err := fd.mapStruct(ptrVal, nil)
	if err != nil {
		return err
	}

	if !ok {
		if isMultipart {
			return errNoFileFound
		}

		return errFieldsNotSet
	}

	return nil
}

// bindBinary handles binding for binary/octet-stream content type.
func (r *Request) bindBinary(raw any) error {
	// Ensure raw is a pointer to a byte slice
	byteSlicePtr, ok := raw.(*[]byte)
	if !ok || byteSlicePtr == nil {
		return fmt.Errorf("%w: %v", errNonSliceBind, raw)
	}

	body, err := r.body()
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			return maxBytesErr
		}
		return fmt.Errorf("failed to read request body: %w", err)
	}

	// Assign the body to the provided slice
	*byteSlicePtr = body

	return nil
}
