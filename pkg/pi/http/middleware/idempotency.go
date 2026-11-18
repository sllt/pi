package middleware

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/golang-jwt/jwt/v5"

	"github.com/sllt/pi/pkg/pi/apperror"
	"github.com/sllt/pi/pkg/pi/auth"
	piHTTP "github.com/sllt/pi/pkg/pi/http"
)

const (
	defaultIdempotencyHeader           = "Idempotency-Key"
	defaultIdempotencyTTL              = 24 * time.Hour
	defaultIdempotencyLockTTL          = 60 * time.Second
	defaultIdempotencyMaxBodyBytes     = int64(1 << 20)
	defaultIdempotencyMaxResponseBytes = int64(1 << 20)
	maxIdempotencyKeyLength            = 256
	idempotencyReplayHeader            = "X-Idempotent-Replayed"
)

// IdempotencyConfig configures HTTP Idempotency-Key middleware behavior.
type IdempotencyConfig struct {
	HeaderName       string
	Methods          []string
	TTL              time.Duration
	LockTTL          time.Duration
	MaxBodyBytes     int64
	MaxResponseBytes int64
	Required         bool
	Store            IdempotencyStore
	ScopeFunc        func(*http.Request) string
}

type idempotencyError struct {
	status  int
	code    int
	message string
}

func (e idempotencyError) Error() string         { return e.message }
func (e idempotencyError) StatusCode() int       { return e.status }
func (e idempotencyError) Code() int             { return e.code }
func (e idempotencyError) PublicMessage() string { return e.message }

// Idempotency creates an HTTP middleware enforcing request idempotency for configured methods.
func Idempotency(cfg ...IdempotencyConfig) func(http.Handler) http.Handler {
	var c IdempotencyConfig
	if len(cfg) > 0 {
		c = cfg[0]
	}
	mw, _ := IdempotencyWithCloser(c)
	return mw
}

// IdempotencyWithCloser creates an Idempotency middleware along with a cleanup closer for its store.
func IdempotencyWithCloser(cfg IdempotencyConfig) (func(http.Handler) http.Handler, func() error) {
	headerName := strings.TrimSpace(cfg.HeaderName)
	if headerName == "" {
		headerName = defaultIdempotencyHeader
	}

	methodSet := make(map[string]struct{})
	if len(cfg.Methods) == 0 {
		methodSet[http.MethodPost] = struct{}{}
		methodSet[http.MethodPatch] = struct{}{}
	} else {
		for _, m := range cfg.Methods {
			upper := strings.ToUpper(strings.TrimSpace(m))
			if upper != "" {
				methodSet[upper] = struct{}{}
			}
		}
	}

	ttl := cfg.TTL
	if ttl <= 0 {
		ttl = defaultIdempotencyTTL
	}
	lockTTL := cfg.LockTTL
	if lockTTL <= 0 {
		lockTTL = defaultIdempotencyLockTTL
	}
	maxBodyBytes := cfg.MaxBodyBytes
	if maxBodyBytes <= 0 {
		maxBodyBytes = defaultIdempotencyMaxBodyBytes
	}
	maxResponseBytes := cfg.MaxResponseBytes
	if maxResponseBytes <= 0 {
		maxResponseBytes = defaultIdempotencyMaxResponseBytes
	}

	store := cfg.Store
	if isNilIdempotencyStore(store) {
		store = NewMemoryIdempotencyStore(defaultIdempotencyMaxKeys)
	}

	scopeFunc := cfg.ScopeFunc
	if scopeFunc == nil {
		scopeFunc = DefaultIdempotencyScope
	}

	mw := func(next http.Handler) http.Handler {
		if next == nil {
			return http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			method := strings.ToUpper(r.Method)
			if _, enabled := methodSet[method]; !enabled {
				next.ServeHTTP(w, r)
				return
			}

			rawKey := r.Header.Get(headerName)
			if rawKey == "" {
				if cfg.Required {
					piHTTP.NewResponderForRequest(w, r, piHTTP.ExplicitStatus).Respond(
						nil,
						apperror.New(apperror.InvalidArgument, http.StatusBadRequest, fmt.Sprintf("missing required %s header", headerName)),
					)
					return
				}
				next.ServeHTTP(w, r)
				return
			}

			if !IsValidIdempotencyKey(rawKey) {
				piHTTP.NewResponderForRequest(w, r, piHTTP.ExplicitStatus).Respond(
					nil,
					apperror.New(apperror.InvalidArgument, http.StatusBadRequest, fmt.Sprintf("invalid %s header", headerName)),
				)
				return
			}

			var bodyBytes []byte
			if r.Body != nil && r.Body != http.NoBody {
				var readErr error
				bodyBytes, readErr = io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
				_ = r.Body.Close()
				if readErr != nil {
					var maxErr *http.MaxBytesError
					var netErr net.Error
					if errors.As(readErr, &maxErr) {
						piHTTP.NewResponderForRequest(w, r, piHTTP.ExplicitStatus).Respond(nil, piHTTP.ErrorPayloadTooLarge{})
						return
					}
					if errors.As(readErr, &netErr) && netErr.Timeout() {
						piHTTP.NewResponderForRequest(w, r, piHTTP.ExplicitStatus).Respond(nil, piHTTP.ErrorRequestTimeout{})
						return
					}
					piHTTP.NewResponderForRequest(w, r, piHTTP.ExplicitStatus).Respond(nil, piHTTP.ErrorMalformedBody{})
					return
				}
				if int64(len(bodyBytes)) > maxBodyBytes {
					piHTTP.NewResponderForRequest(w, r, piHTTP.ExplicitStatus).Respond(nil, piHTTP.ErrorPayloadTooLarge{})
					return
				}
				r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
				r.GetBody = func() (io.ReadCloser, error) {
					return io.NopCloser(bytes.NewReader(bodyBytes)), nil
				}
			}

			scope := scopeFunc(r)
			reqURI := ""
			if r.URL != nil {
				reqURI = r.URL.RequestURI()
			}
			contentType := strings.TrimSpace(r.Header.Get("Content-Type"))
			fingerprint := IdempotencyFingerprint(method, reqURI, scope, contentType, bodyBytes)
			storeKey := IdempotencyStoreKey(scope, rawKey)
			ownerToken := NewIdempotencyOwnerToken()

			existing, acquired, err := store.TryBegin(r.Context(), storeKey, fingerprint, ownerToken, lockTTL)
			if err != nil {
				if ctxErr := r.Context().Err(); ctxErr != nil {
					piHTTP.NewResponderForRequest(w, r, piHTTP.ExplicitStatus).Respond(nil, ctxErr)
					return
				}
				piHTTP.NewResponderForRequest(w, r, piHTTP.ExplicitStatus).Respond(
					nil,
					apperror.New(apperror.Unavailable, http.StatusServiceUnavailable, "idempotency store unavailable"),
				)
				return
			}

			if !acquired {
				if existing == nil {
					piHTTP.NewResponderForRequest(w, r, piHTTP.ExplicitStatus).Respond(
						nil,
						apperror.New(apperror.Conflict, http.StatusConflict, "A request with this Idempotency-Key is currently in progress"),
					)
					return
				}
				if existing.Fingerprint != fingerprint {
					piHTTP.NewResponderForRequest(w, r, piHTTP.ExplicitStatus).Respond(
						nil,
						idempotencyError{
							status:  http.StatusUnprocessableEntity,
							code:    http.StatusUnprocessableEntity,
							message: "Idempotency-Key cannot be reused with a different request payload",
						},
					)
					return
				}
				if existing.InProgress {
					piHTTP.NewResponderForRequest(w, r, piHTTP.ExplicitStatus).Respond(
						nil,
						apperror.New(apperror.Conflict, http.StatusConflict, "A request with this Idempotency-Key is currently in progress"),
					)
					return
				}

				copyReplayHeaders(w.Header(), existing.Headers, true)
				w.Header().Set(idempotencyReplayHeader, "true")
				status := existing.StatusCode
				if status == 0 {
					status = http.StatusOK
				}
				w.WriteHeader(status)
				if canWriteBody(method, status) && len(existing.Body) > 0 {
					_, _ = w.Write(existing.Body)
				}
				return
			}

			completed := false
			defer func() {
				if !completed {
					_ = store.Abort(context.WithoutCancel(r.Context()), storeKey, ownerToken)
				}
			}()

			cw := newIdempotencyResponseWriter(w, maxResponseBytes)
			next.ServeHTTP(cw, r)

			if cw.streaming {
				return
			}
			if !cw.wroteHeader {
				cw.statusCode = http.StatusOK
			}

			if !IsCacheableIdempotencyStatus(cw.statusCode) {
				copyReplayHeaders(w.Header(), cw.header, false)
				w.WriteHeader(cw.statusCode)
				if canWriteBody(method, cw.statusCode) && cw.buf.Len() > 0 {
					_, _ = w.Write(cw.buf.Bytes())
				}
				return
			}

			rec := IdempotencyRecord{
				Fingerprint: fingerprint,
				StatusCode:  cw.statusCode,
				Headers:     CloneSafeReplayHeaders(cw.header),
				Body:        append([]byte(nil), cw.buf.Bytes()...),
			}
			cErr := store.Complete(context.WithoutCancel(r.Context()), storeKey, ownerToken, rec, ttl)
			if cErr == nil || IsIdempotencyOwnershipLost(cErr) {
				completed = true
			}

			copyReplayHeaders(w.Header(), cw.header, false)
			w.Header().Set(idempotencyReplayHeader, "false")
			w.WriteHeader(cw.statusCode)
			if canWriteBody(method, cw.statusCode) && cw.buf.Len() > 0 {
				_, _ = w.Write(cw.buf.Bytes())
			}
		})
	}

	return mw, store.Close
}

func isNilIdempotencyStore(s IdempotencyStore) bool {
	if s == nil {
		return true
	}
	rv := reflect.ValueOf(s)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
		return rv.IsNil()
	default:
		return false
	}
}

// DefaultIdempotencyScope derives an isolation scope for r from verified auth
// principals (auth.FromContext, JWTClaim, Username, APIKey) or falls back to a
// SHA-256 hash of all Authorization, X-API-Key, and Cookie header values.
func DefaultIdempotencyScope(r *http.Request) string {
	if r == nil {
		return "anon"
	}
	ctx := r.Context()
	if p, ok := auth.FromContext(ctx); ok && p.Subject != "" {
		return "sub:" + p.Subject
	}
	if claims, ok := ctx.Value(JWTClaim).(jwt.MapClaims); ok {
		if sub, err := claims.GetSubject(); err == nil && sub != "" {
			return "sub:" + sub
		}
	}
	if user, ok := ctx.Value(Username).(string); ok && user != "" {
		return "user:" + user
	}
	if apiKey, ok := ctx.Value(APIKey).(string); ok && apiKey != "" {
		return "apikey:" + sha256HexStrings(apiKey)
	}

	authz := r.Header.Values("Authorization")
	apiKeys := r.Header.Values("X-API-Key")
	cookies := r.Header.Values("Cookie")
	if len(authz) > 0 || len(apiKeys) > 0 || len(cookies) > 0 {
		h := sha256.New()
		writeScopedHeaderSlice(h, "Authorization", authz)
		writeScopedHeaderSlice(h, "X-API-Key", apiKeys)
		writeScopedHeaderSlice(h, "Cookie", cookies)
		return "hdr:" + hex.EncodeToString(h.Sum(nil))
	}
	return "anon"
}

func writeScopedHeaderSlice(h io.Writer, name string, vals []string) {
	_, _ = fmt.Fprintf(h, "%s:%d:", name, len(vals))
	for _, v := range vals {
		_, _ = fmt.Fprintf(h, "%d:%s|", len(v), v)
	}
}

// IsValidIdempotencyKey reports whether key is a non-empty, valid UTF-8 header
// value within length bounds and free of ASCII control characters.
func IsValidIdempotencyKey(key string) bool {
	if strings.TrimSpace(key) == "" || len(key) > maxIdempotencyKeyLength || !utf8.ValidString(key) {
		return false
	}
	for _, r := range key {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

// IdempotencyFingerprint computes the canonical request fingerprint used to
// detect Idempotency-Key reuse with mismatched payloads.
func IdempotencyFingerprint(method, reqURI, scope, contentType string, body []byte) string {
	return sha256HexStrings(method, reqURI, scope, contentType, sha256HexBytes(body))
}

// IdempotencyStoreKey computes the scoped storage key for an Idempotency-Key.
func IdempotencyStoreKey(scope, rawKey string) string {
	return sha256HexStrings(scope, rawKey)
}

func sha256HexBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func sha256HexStrings(parts ...string) string {
	h := sha256.New()
	for i, part := range parts {
		if i > 0 {
			_, _ = h.Write([]byte{0})
		}
		_, _ = h.Write([]byte(part))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// NewIdempotencyOwnerToken generates a random per-request lock ownership token.
func NewIdempotencyOwnerToken() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func canWriteBody(method string, status int) bool {
	return method != http.MethodHead &&
		status != http.StatusNoContent &&
		status != http.StatusNotModified
}

// IsCacheableIdempotencyStatus reports whether status should be persisted and
// replayed by Idempotency-Key middleware.
// 2xx and deterministic 4xx (excluding 401, 403, 408, 409, 425, 429, 499) are cacheable.
func IsCacheableIdempotencyStatus(status int) bool {
	if status >= 200 && status < 300 {
		return true
	}
	if status >= 400 && status < 500 {
		switch status {
		case http.StatusUnauthorized,
			http.StatusForbidden,
			http.StatusRequestTimeout,
			http.StatusConflict,
			http.StatusTooEarly,
			http.StatusTooManyRequests,
			piHTTP.StatusClientClosedRequest:
			return false
		default:
			return true
		}
	}
	return false
}

func isSafeReplayHeader(name string, extraHopByHop map[string]struct{}) bool {
	lower := strings.ToLower(name)
	if _, ok := extraHopByHop[lower]; ok {
		return false
	}
	switch lower {
	case "connection",
		"keep-alive",
		"proxy-authenticate",
		"proxy-authorization",
		"te",
		"trailers",
		"transfer-encoding",
		"upgrade",
		"content-length",
		"set-cookie",
		"x-request-id",
		"x-correlation-id",
		"date",
		"x-idempotent-replayed":
		return false
	default:
		return true
	}
}

func parseConnectionHeaders(src http.Header) map[string]struct{} {
	vals := src.Values("Connection")
	if len(vals) == 0 {
		return nil
	}
	out := make(map[string]struct{})
	for _, v := range vals {
		for _, tok := range strings.Split(v, ",") {
			if name := strings.ToLower(strings.TrimSpace(tok)); name != "" {
				out[name] = struct{}{}
			}
		}
	}
	return out
}

// CloneSafeReplayHeaders copies canonicalized, non-hop-by-hop headers safe for
// persisting and replaying across idempotent retries.
func CloneSafeReplayHeaders(src http.Header) map[string][]string {
	if len(src) == 0 {
		return nil
	}
	extraHopByHop := parseConnectionHeaders(src)
	out := make(map[string][]string)
	for k, vals := range src {
		if !isSafeReplayHeader(k, extraHopByHop) {
			continue
		}
		ck := http.CanonicalHeaderKey(k)
		out[ck] = append(out[ck], vals...)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func copyReplayHeaders(dst http.Header, src map[string][]string, safeOnly bool) {
	if len(src) == 0 {
		return
	}
	var extraHopByHop map[string]struct{}
	if safeOnly {
		extraHopByHop = parseConnectionHeaders(http.Header(src))
	}
	for k, vals := range src {
		if safeOnly && !isSafeReplayHeader(k, extraHopByHop) {
			continue
		}
		ck := http.CanonicalHeaderKey(k)
		if ck == "Vary" {
			mergeHeaderValues(dst, ck, vals)
			continue
		}
		if ck == "X-Request-Id" && dst.Get(ck) != "" {
			continue
		}
		copied := make([]string, len(vals))
		copy(copied, vals)
		dst[ck] = copied
	}
}

func mergeHeaderValues(dst http.Header, key string, vals []string) {
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
	for _, v := range vals {
		for _, part := range strings.Split(v, ",") {
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

type idempotencyResponseWriter struct {
	w           http.ResponseWriter
	header      http.Header
	statusCode  int
	wroteHeader bool
	streaming   bool
	maxBytes    int64
	buf         bytes.Buffer
	writeErr    error
}

func newIdempotencyResponseWriter(w http.ResponseWriter, maxBytes int64) *idempotencyResponseWriter {
	return &idempotencyResponseWriter{
		w:        w,
		header:   make(http.Header),
		maxBytes: maxBytes,
	}
}

func (cw *idempotencyResponseWriter) Header() http.Header {
	if cw.streaming {
		return cw.w.Header()
	}
	return cw.header
}

func (cw *idempotencyResponseWriter) WriteHeader(code int) {
	if code >= 100 && code < 200 {
		copyReplayHeaders(cw.w.Header(), cw.header, false)
		cw.w.WriteHeader(code)
		return
	}
	if cw.wroteHeader {
		return
	}
	cw.wroteHeader = true
	cw.statusCode = code

	if strings.Contains(strings.ToLower(cw.header.Get("Content-Type")), "text/event-stream") {
		cw.switchToStreaming()
	}
}

func (cw *idempotencyResponseWriter) Write(p []byte) (int, error) {
	if !cw.wroteHeader {
		cw.WriteHeader(http.StatusOK)
	}
	if cw.writeErr != nil {
		return 0, cw.writeErr
	}
	if cw.streaming {
		return cw.w.Write(p)
	}
	if cw.maxBytes > 0 && int64(cw.buf.Len())+int64(len(p)) > cw.maxBytes {
		cw.switchToStreaming()
		if cw.writeErr != nil {
			return 0, cw.writeErr
		}
		return cw.w.Write(p)
	}
	return cw.buf.Write(p)
}

func (cw *idempotencyResponseWriter) Flush() {
	if !cw.wroteHeader {
		cw.statusCode = http.StatusOK
		cw.wroteHeader = true
	}
	if !cw.streaming {
		cw.switchToStreaming()
	}
	_ = http.NewResponseController(cw.w).Flush()
}

func (cw *idempotencyResponseWriter) Unwrap() http.ResponseWriter {
	return cw.w
}

func (cw *idempotencyResponseWriter) switchToStreaming() {
	if cw.streaming {
		return
	}
	cw.streaming = true
	copyReplayHeaders(cw.w.Header(), cw.header, false)
	if cw.statusCode == 0 {
		cw.statusCode = http.StatusOK
	}
	cw.w.WriteHeader(cw.statusCode)
	if cw.buf.Len() > 0 {
		if _, err := cw.w.Write(cw.buf.Bytes()); err != nil {
			cw.writeErr = err
		}
		cw.buf.Reset()
	}
}
