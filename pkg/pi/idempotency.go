package pi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/sllt/pi/pkg/pi/apperror"
	piRedis "github.com/sllt/pi/pkg/pi/datasource/redis"
	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"github.com/sllt/pi/pkg/pi/http/middleware"
	"github.com/sllt/pi/pkg/pi/http/response"
)

const (
	defaultPiIdempotencyHeader           = "Idempotency-Key"
	defaultPiIdempotencyTTL              = 24 * time.Hour
	defaultPiIdempotencyLockTTL          = 60 * time.Second
	defaultPiIdempotencyMaxResponseBytes = int64(1 << 20)
)

// IdempotencyConfig configures HTTP Idempotency-Key middleware behavior.
type IdempotencyConfig = middleware.IdempotencyConfig

// IdempotencyStore abstracts atomic storage for idempotency locks and cached responses.
type IdempotencyStore = middleware.IdempotencyStore

// IdempotencyRecord represents the persisted state of an idempotent request.
type IdempotencyRecord = middleware.IdempotencyRecord

// NewMemoryIdempotencyStore creates a goroutine-free in-memory IdempotencyStore.
// If maxKeys is omitted or <= 0, it defaults to 10000.
func NewMemoryIdempotencyStore(maxKeys ...int) *middleware.MemoryIdempotencyStore {
	return middleware.NewMemoryIdempotencyStore(maxKeys...)
}

// NewRedisIdempotencyStore constructs a Redis-backed IdempotencyStore.
// If keyPrefix is omitted or empty, it defaults to "pi:idem:".
func NewRedisIdempotencyStore(client piRedis.Client, keyPrefix ...string) *middleware.RedisIdempotencyStore {
	return middleware.NewRedisIdempotencyStore(client, keyPrefix...)
}

type piIdempotencyError struct {
	status  int
	code    int
	message string
}

func (e piIdempotencyError) Error() string         { return e.message }
func (e piIdempotencyError) StatusCode() int       { return e.status }
func (e piIdempotencyError) Code() int             { return e.code }
func (e piIdempotencyError) PublicMessage() string { return e.message }

// UseIdempotency attaches application-wide Idempotency-Key PiMiddleware (running after
// authentication PiMiddlewares) and registers its store closer to be closed during App.Stop.
// When cfg is omitted or cfg.Store is nil, it uses app.Redis() if configured or a shared
// App-scoped MemoryIdempotencyStore.
func (a *App) UseIdempotency(cfg ...IdempotencyConfig) {
	if a == nil {
		return
	}
	if len(cfg) > 1 {
		a.recordRouteError(fmt.Errorf("UseIdempotency accepts at most 1 IdempotencyConfig, got %d", len(cfg)))
		return
	}
	var c IdempotencyConfig
	if len(cfg) == 1 {
		c = cfg[0]
	}
	mw, closer := a.newIdempotencyPiMiddleware(c)
	a.registerMiddlewareCloser(closer)
	if a.httpServer != nil {
		a.UseMiddleware(mw)
	}
}

// UseIdempotency attaches group-scoped Idempotency-Key PiMiddleware and registers
// its store closer with the owning App when available.
// When cfg is omitted or cfg.Store is nil, it uses app.Redis() if configured or a shared
// App-scoped MemoryIdempotencyStore.
func (g *RouteGroup) UseIdempotency(cfg ...IdempotencyConfig) *RouteGroup {
	if g == nil {
		return g
	}
	if len(cfg) > 1 {
		if g.app != nil {
			g.app.recordRouteError(fmt.Errorf("UseIdempotency accepts at most 1 IdempotencyConfig, got %d", len(cfg)))
		}
		return g
	}
	var c IdempotencyConfig
	if len(cfg) == 1 {
		c = cfg[0]
	}
	mw, closer := g.app.newIdempotencyPiMiddleware(c)
	if g.app != nil {
		g.app.registerMiddlewareCloser(closer)
	}
	return g.UseMiddleware(mw)
}

// WithEndpointIdempotency attaches per-endpoint Idempotency-Key PiMiddleware and
// registers its store closer with the owning App during route registration.
// When cfg is omitted or cfg.Store is nil, it uses app.Redis() if configured or a shared
// App-scoped MemoryIdempotencyStore.
func WithEndpointIdempotency(cfg ...IdempotencyConfig) EndpointOption {
	if len(cfg) > 1 {
		return func(*typedEndpointConfig) error {
			return fmt.Errorf("WithEndpointIdempotency accepts at most 1 IdempotencyConfig, got %d", len(cfg))
		}
	}
	var c IdempotencyConfig
	if len(cfg) == 1 {
		c = cfg[0]
	}
	return func(epCfg *typedEndpointConfig) error {
		if !epCfg.hasIdempotency {
			epCfg.idempotencyPos = len(epCfg.piMWs)
			epCfg.piMWs = append(epCfg.piMWs, nil)
		}
		epCfg.hasIdempotency = true
		epCfg.idempotencyCfg = c
		return nil
	}
}

func (a *App) recordRouteError(err error) {
	if err == nil || a == nil || a.httpServer == nil || a.httpServer.registry == nil {
		return
	}
	reg := a.httpServer.registry
	reg.mu.Lock()
	defer reg.mu.Unlock()
	a.recordRouteErrorLocked(err)
}

func (a *App) resolveDefaultIdempotencyStore() (middleware.IdempotencyStore, func() error) {
	if a != nil {
		if redisClient := a.Redis(); !isNilValue(redisClient) {
			return middleware.NewRedisIdempotencyStore(redisClient), nil
		}
		a.defaultIdemMu.Lock()
		if a.defaultIdemStore == nil {
			store := middleware.NewMemoryIdempotencyStore()
			a.defaultIdemStore = store
			a.defaultIdemMu.Unlock()
			return store, store.Close
		}
		store := a.defaultIdemStore
		a.defaultIdemMu.Unlock()
		return store, nil
	}
	store := middleware.NewMemoryIdempotencyStore()
	return store, store.Close
}

func (a *App) resolveEndpointIdempotency(epCfg *typedEndpointConfig) {
	if epCfg == nil || !epCfg.hasIdempotency {
		return
	}
	mw, closer := a.newIdempotencyPiMiddleware(epCfg.idempotencyCfg)
	if epCfg.idempotencyPos >= 0 && epCfg.idempotencyPos < len(epCfg.piMWs) {
		epCfg.piMWs[epCfg.idempotencyPos] = mw
	} else {
		epCfg.piMWs = append(epCfg.piMWs, mw)
	}
	if closer != nil {
		epCfg.closers = append(epCfg.closers, closer)
	}
}

func (a *App) newIdempotencyPiMiddleware(cfg IdempotencyConfig) (PiMiddleware, func() error) {
	headerName := strings.TrimSpace(cfg.HeaderName)
	if headerName == "" {
		headerName = defaultPiIdempotencyHeader
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
		ttl = defaultPiIdempotencyTTL
	}
	lockTTL := cfg.LockTTL
	if lockTTL <= 0 {
		lockTTL = defaultPiIdempotencyLockTTL
	}
	maxResponseBytes := cfg.MaxResponseBytes
	if maxResponseBytes <= 0 {
		maxResponseBytes = defaultPiIdempotencyMaxResponseBytes
	}

	store := cfg.Store
	var closer func() error
	if isNilPiIdempotencyStore(store) {
		store, closer = a.resolveDefaultIdempotencyStore()
	} else {
		closer = store.Close
	}

	scopeFunc := cfg.ScopeFunc
	if scopeFunc == nil {
		scopeFunc = middleware.DefaultIdempotencyScope
	}

	mw := func(next Handler) Handler {
		return func(c *Context) (any, error) {
			if c == nil {
				return next(c)
			}
			piReq, ok := c.Request.(*piHTTP.Request)
			if !ok || piReq == nil {
				return next(c)
			}
			httpReq := piReq.HTTPRequest()
			if httpReq == nil {
				return next(c)
			}

			method := strings.ToUpper(httpReq.Method)
			if _, enabled := methodSet[method]; !enabled {
				return next(c)
			}

			rawKey := httpReq.Header.Get(headerName)
			if rawKey == "" {
				if cfg.Required {
					return nil, apperror.New(apperror.InvalidArgument, http.StatusBadRequest, fmt.Sprintf("missing required %s header", headerName))
				}
				return next(c)
			}

			if !middleware.IsValidIdempotencyKey(rawKey) {
				return nil, apperror.New(apperror.InvalidArgument, http.StatusBadRequest, fmt.Sprintf("invalid %s header", headerName))
			}

			bodyBytes, bodyErr := piReq.RawBodyBytes()
			if bodyErr != nil {
				var maxErr *http.MaxBytesError
				if errors.As(bodyErr, &maxErr) {
					return nil, piHTTP.ErrorPayloadTooLarge{}
				}
				var cancelErr piHTTP.ErrorClientClosedRequest
				if errors.Is(bodyErr, context.Canceled) || errors.As(bodyErr, &cancelErr) {
					return nil, piHTTP.ErrorClientClosedRequest{}
				}
				var timeoutErr piHTTP.ErrorRequestTimeout
				var netErr net.Error
				if errors.Is(bodyErr, context.DeadlineExceeded) || errors.As(bodyErr, &timeoutErr) || (errors.As(bodyErr, &netErr) && netErr.Timeout()) {
					return nil, piHTTP.ErrorRequestTimeout{}
				}
				return nil, piHTTP.ErrorMalformedBody{}
			}
			if cfg.MaxBodyBytes > 0 && int64(len(bodyBytes)) > cfg.MaxBodyBytes {
				return nil, piHTTP.ErrorPayloadTooLarge{}
			}

			ctx := baseContext(c)
			scopeReq := httpReq
			if httpReq.Context() != ctx {
				scopeReq = httpReq.WithContext(ctx)
			}
			scope := scopeFunc(scopeReq)

			reqURI := ""
			if httpReq.URL != nil {
				reqURI = httpReq.URL.RequestURI()
			}
			contentType := strings.TrimSpace(httpReq.Header.Get("Content-Type"))
			fingerprint := middleware.IdempotencyFingerprint(method, reqURI, scope, contentType, bodyBytes)
			storeKey := middleware.IdempotencyStoreKey(scope, rawKey)
			ownerToken := middleware.NewIdempotencyOwnerToken()

			existing, acquired, err := store.TryBegin(ctx, storeKey, fingerprint, ownerToken, lockTTL)
			if err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return nil, apperror.New(apperror.Unavailable, http.StatusServiceUnavailable, "idempotency store unavailable")
			}

			if !acquired {
				if existing == nil {
					return nil, apperror.New(apperror.Conflict, http.StatusConflict, "A request with this Idempotency-Key is currently in progress")
				}
				if existing.Fingerprint != fingerprint {
					return nil, piIdempotencyError{
						status:  http.StatusUnprocessableEntity,
						code:    http.StatusUnprocessableEntity,
						message: "Idempotency-Key cannot be reused with a different request payload",
					}
				}
				if existing.InProgress {
					return nil, apperror.New(apperror.Conflict, http.StatusConflict, "A request with this Idempotency-Key is currently in progress")
				}

				headers := make(http.Header)
				for k, vals := range middleware.CloneSafeReplayHeaders(http.Header(existing.Headers)) {
					headers[k] = vals
				}
				headers.Set("X-Idempotent-Replayed", "true")
				status := existing.StatusCode
				if status == 0 {
					status = http.StatusOK
				}
				return piHTTP.RawReplay{
					StatusCode: status,
					Headers:    headers,
					Body:       append([]byte(nil), existing.Body...),
				}, nil
			}

			completed := false
			defer func() {
				if !completed {
					_ = store.Abort(context.WithoutCancel(ctx), storeKey, ownerToken)
				}
			}()

			res, handlerErr := next(c)
			if handlerErr != nil && ctx.Err() != nil {
				return res, handlerErr
			}
			if isNonCacheableSpecialResponse(res) {
				return res, handlerErr
			}

			policy := piHTTP.LegacyStatus
			if c.rt != nil && c.rt.explicitHTTPStatus {
				policy = piHTTP.ExplicitStatus
			}
			rec := newPiIdemCaptureWriter(maxResponseBytes)
			piHTTP.NewResponderForRequest(rec, httpReq, policy).Respond(res, handlerErr)

			if rec.overflow {
				return res, handlerErr
			}
			status := rec.statusCode
			if status == 0 {
				status = http.StatusOK
			}
			if !middleware.IsCacheableIdempotencyStatus(status) {
				return res, handlerErr
			}

			record := middleware.IdempotencyRecord{
				Fingerprint: fingerprint,
				StatusCode:  status,
				Headers:     middleware.CloneSafeReplayHeaders(rec.header),
				Body:        append([]byte(nil), rec.buf.Bytes()...),
			}
			cErr := store.Complete(context.WithoutCancel(ctx), storeKey, ownerToken, record, ttl)
			if cErr == nil || middleware.IsIdempotencyOwnershipLost(cErr) {
				completed = true
			}

			if handlerErr != nil {
				if hs, ok := c.responder.(interface{ SetHeader(string, string) }); ok {
					hs.SetHeader("X-Idempotent-Replayed", "false")
				}
				return res, handlerErr
			}

			outHeaders := rec.header.Clone()
			if outHeaders == nil {
				outHeaders = make(http.Header)
			}
			outHeaders.Set("X-Idempotent-Replayed", "false")
			return piHTTP.RawReplay{
				StatusCode: status,
				Headers:    outHeaders,
				Body:       append([]byte(nil), rec.buf.Bytes()...),
			}, nil
		}
	}

	return mw, closer
}

func isNilPiIdempotencyStore(s middleware.IdempotencyStore) bool {
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

func isNonCacheableSpecialResponse(res any) bool {
	switch v := res.(type) {
	case response.Result:
		return isNonCacheableSpecialResponse(v.Data)
	case response.Stream, response.SSE, response.File, response.Handled, response.Template, response.Redirect:
		return true
	case *response.Stream:
		return v != nil
	case *response.SSE:
		return v != nil
	case *response.File:
		return v != nil
	default:
		return false
	}
}

type piIdemCaptureWriter struct {
	header      http.Header
	statusCode  int
	wroteHeader bool
	overflow    bool
	maxBytes    int64
	buf         bytes.Buffer
}

func newPiIdemCaptureWriter(maxBytes int64) *piIdemCaptureWriter {
	return &piIdemCaptureWriter{
		header:   make(http.Header),
		maxBytes: maxBytes,
	}
}

func (w *piIdemCaptureWriter) Header() http.Header {
	return w.header
}

func (w *piIdemCaptureWriter) WriteHeader(code int) {
	if code >= 100 && code < 200 {
		return
	}
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.statusCode = code
}

func (w *piIdemCaptureWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if w.overflow {
		return len(p), nil
	}
	if w.maxBytes > 0 && int64(w.buf.Len())+int64(len(p)) > w.maxBytes {
		w.overflow = true
		return len(p), nil
	}
	return w.buf.Write(p)
}
