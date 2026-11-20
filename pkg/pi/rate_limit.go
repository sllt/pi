package pi

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"net/http"
	"net/netip"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/sllt/pi/pkg/pi/apperror"
	"github.com/sllt/pi/pkg/pi/auth"
	piRedis "github.com/sllt/pi/pkg/pi/datasource/redis"
	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"github.com/sllt/pi/pkg/pi/http/middleware"
	"github.com/sllt/pi/pkg/pi/logging"
)

const defaultRateLimitWindow = time.Minute

// RateLimitStore abstracts atomic rate limit checks across in-memory and distributed stores.
type RateLimitStore = middleware.RateLimitStore

// RateLimitDecision represents the result of a rate limit check.
type RateLimitDecision = middleware.RateLimitDecision

// RateLimitConfig configures PiMiddleware rate limiting on App, RouteGroup, or Endpoint.
type RateLimitConfig struct {
	Limit          int
	Window         time.Duration
	Burst          int
	KeyFunc        func(*Context) string
	TrustedProxies []string
	Store          RateLimitStore
	FailClosed     bool
}

// NewMemoryRateLimitStore creates a goroutine-free in-memory RateLimitStore.
// If maxKeys is omitted or <= 0, it defaults to 100000.
func NewMemoryRateLimitStore(maxKeys ...int) *middleware.MemoryRateLimitStore {
	return middleware.NewMemoryRateLimitStore(maxKeys...)
}

// NewRedisRateLimitStore constructs a Redis-backed RateLimitStore.
// If keyPrefix is omitted or empty, it defaults to "pi:rl:".
func NewRedisRateLimitStore(client piRedis.Client, keyPrefix ...string) *middleware.RedisRateLimitStore {
	return middleware.NewRedisRateLimitStore(client, keyPrefix...)
}

type piRateLimitError struct {
	headers map[string]string
}

func (piRateLimitError) Error() string           { return "rate limit exceeded" }
func (piRateLimitError) StatusCode() int         { return http.StatusTooManyRequests }
func (piRateLimitError) Code() int               { return http.StatusTooManyRequests }
func (piRateLimitError) PublicMessage() string   { return "rate limit exceeded" }
func (piRateLimitError) LogLevel() logging.Level { return logging.WARN }
func (e piRateLimitError) ResponseHeaders() map[string]string {
	return e.headers
}

var (
	_ piHTTP.StatusCodeResponder = piRateLimitError{}
	_ piHTTP.CodeResponder       = piRateLimitError{}
	_ piHTTP.HeaderResponder     = piRateLimitError{}
	_ logging.LogLevelResponder  = piRateLimitError{}
)

// RateLimitByUser derives a rate-limit key from the authenticated principal
// (auth.FromContext subject, basic-auth username, hashed API key, or JWT subject),
// falling back to "ip:" + c.ClientIP() when unauthenticated.
func RateLimitByUser(c *Context) string {
	if c == nil {
		return "ip:unknown"
	}
	if c.Context != nil {
		if p, ok := auth.FromContext(c.Context); ok && p.Subject != "" {
			return "sub:" + p.Subject
		}
	}
	if httpReq := c.httpRequest(); httpReq != nil && httpReq.Context() != nil {
		if p, ok := auth.FromContext(httpReq.Context()); ok && p.Subject != "" {
			return "sub:" + p.Subject
		}
	}
	info := c.GetAuthInfo()
	if u := info.GetUsername(); u != "" {
		return "user:" + u
	}
	if k := info.GetAPIKey(); k != "" {
		sum := sha256.Sum256([]byte(k))
		return "apikey:" + hex.EncodeToString(sum[:])
	}
	if claims := info.GetClaims(); claims != nil {
		if sub, err := claims.GetSubject(); err == nil && sub != "" {
			return "sub:" + sub
		}
	}
	ip := c.ClientIP()
	if ip == "" {
		ip = "unknown"
	}
	return "ip:" + ip
}

// UseRateLimit attaches application-wide rate-limit PiMiddleware and registers
// its default memory store closer (if created) to close during App.Stop.
func (a *App) UseRateLimit(cfg RateLimitConfig) {
	if a == nil {
		return
	}
	if err := validateRateLimitConfig(cfg); err != nil {
		a.recordRouteError(err)
		return
	}
	a.ensureRateLimitMetrics()
	mw, closer := a.newRateLimitPiMiddleware(cfg, a.nextRateLimitScope("app"))
	a.registerMiddlewareCloser(closer)
	if a.httpServer != nil {
		a.UseMiddleware(mw)
	}
}

// UseRateLimit attaches group-scoped rate-limit PiMiddleware and registers
// its default memory store closer with the owning App when available.
func (g *RouteGroup) UseRateLimit(cfg RateLimitConfig) *RouteGroup {
	if g == nil {
		return g
	}
	if err := validateRateLimitConfig(cfg); err != nil {
		if g.app != nil {
			g.app.recordRouteError(err)
		}
		return g
	}
	if g.app != nil {
		g.app.ensureRateLimitMetrics()
	}
	mw, closer := g.app.newRateLimitPiMiddleware(cfg, g.app.nextRateLimitScope("group:"+groupFullPrefix(g.node)))
	if g.app != nil {
		g.app.registerMiddlewareCloser(closer)
	}
	return g.UseMiddleware(mw)
}

// WithEndpointRateLimit attaches per-endpoint rate-limit PiMiddleware and registers
// its default memory store closer with the owning App during route registration.
func WithEndpointRateLimit(cfg RateLimitConfig) EndpointOption {
	return func(epCfg *typedEndpointConfig) error {
		if err := validateRateLimitConfig(cfg); err != nil {
			return err
		}
		if !epCfg.hasRateLimit {
			epCfg.rateLimitPos = len(epCfg.piMWs)
			epCfg.piMWs = append(epCfg.piMWs, nil)
		}
		epCfg.hasRateLimit = true
		epCfg.rateLimitCfg = cfg
		return nil
	}
}

func validateRateLimitConfig(cfg RateLimitConfig) error {
	if cfg.Limit <= 0 {
		return fmt.Errorf("rate limit Limit must be positive, got %d", cfg.Limit)
	}
	if len(cfg.TrustedProxies) > 0 {
		if _, err := middleware.ParseTrustedProxies(cfg.TrustedProxies); err != nil {
			return err
		}
	}
	return nil
}

func endpointRateLimitScope(node *GroupNode, method, pattern string, rawHandler any) string {
	prefix := groupFullPrefix(node)
	trimmedPattern := strings.TrimSpace(pattern)
	if th, ok := rawHandler.(TypedHandler); ok && trimmedPattern == "" {
		return prefix + ":" + th.handlerName
	}
	return prefix + ":" + strings.ToUpper(strings.TrimSpace(method)) + ":" + trimmedPattern
}

func (a *App) nextRateLimitScope(base string) string {
	if a == nil {
		return base
	}
	return fmt.Sprintf("%s#%d", base, a.rateLimitRuleSeq.Add(1))
}

func (a *App) resolveEndpointRateLimit(epCfg *typedEndpointConfig, scope string) {
	if epCfg == nil || !epCfg.hasRateLimit {
		return
	}
	if a != nil {
		a.ensureRateLimitMetrics()
	}
	mw, closer := a.newRateLimitPiMiddleware(epCfg.rateLimitCfg, a.nextRateLimitScope("ep:"+scope))
	if epCfg.rateLimitPos >= 0 && epCfg.rateLimitPos < len(epCfg.piMWs) {
		epCfg.piMWs[epCfg.rateLimitPos] = mw
	} else {
		epCfg.piMWs = append(epCfg.piMWs, mw)
	}
	if closer != nil {
		epCfg.closers = append(epCfg.closers, closer)
	}
}

func (a *App) resolveDefaultRateLimitStore() (middleware.RateLimitStore, func() error) {
	if a != nil {
		a.defaultRLMu.Lock()
		if a.defaultRLStore == nil {
			store := middleware.NewMemoryRateLimitStore()
			a.defaultRLStore = store
			a.defaultRLMu.Unlock()
			return store, store.Close
		}
		store := a.defaultRLStore
		a.defaultRLMu.Unlock()
		return store, nil
	}
	store := middleware.NewMemoryRateLimitStore()
	return store, store.Close
}

func (a *App) newRateLimitPiMiddleware(cfg RateLimitConfig, scope string) (PiMiddleware, func() error) {
	window := cfg.Window
	if window <= 0 {
		window = defaultRateLimitWindow
	}
	burst := cfg.Burst
	if burst <= 0 {
		burst = cfg.Limit
	}

	var (
		trustedPrefixes    []netip.Prefix
		hasExplicitTrusted = len(cfg.TrustedProxies) > 0
	)
	if hasExplicitTrusted {
		trustedPrefixes, _ = middleware.ParseTrustedProxies(cfg.TrustedProxies)
	}

	store := cfg.Store
	var closer func() error
	if isNilRateLimitStore(store) {
		store, closer = a.resolveDefaultRateLimitStore()
	}

	mw := func(next Handler) Handler {
		return func(c *Context) (any, error) {
			if c == nil {
				return next(c)
			}
			httpReq := c.httpRequest()
			if httpReq != nil && httpReq.URL != nil && strings.HasPrefix(httpReq.URL.Path, "/.well-known") {
				return next(c)
			}

			var rawKey string
			if cfg.KeyFunc != nil {
				rawKey = cfg.KeyFunc(c)
			} else if hasExplicitTrusted && httpReq != nil {
				rawKey = middleware.ExtractClientIP(httpReq, trustedPrefixes)
			} else {
				rawKey = c.ClientIP()
			}
			if rawKey == "" {
				rawKey = "unknown"
			}
			storeKey := scope + ":" + rawKey

			ctx := baseContext(c)
			decision, err := store.Allow(ctx, storeKey, cfg.Limit, window, burst)
			if err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if cfg.FailClosed {
					return nil, apperror.New(apperror.Unavailable, http.StatusServiceUnavailable, "rate limit store unavailable")
				}
				return next(c)
			}

			if !decision.Allowed {
				if m := c.Metrics(); m != nil {
					method := ""
					if httpReq != nil {
						method = httpReq.Method
					}
					m.IncrementCounter(ctx, "app_http_rate_limit_exceeded_total",
						"path", piRateLimitRouteLabel(httpReq), "method", method)
				}
				retryAfterSec := int64(math.Ceil(decision.RetryAfter.Seconds()))
				if retryAfterSec < 1 {
					retryAfterSec = 1
				}
				limitVal := decision.Limit
				if limitVal <= 0 {
					limitVal = cfg.Limit
				}
				remVal := decision.Remaining
				if remVal < 0 {
					remVal = 0
				}
				return nil, piRateLimitError{
					headers: map[string]string{
						"Retry-After":           strconv.FormatInt(retryAfterSec, 10),
						"X-RateLimit-Limit":     strconv.Itoa(limitVal),
						"X-RateLimit-Remaining": strconv.Itoa(remVal),
					},
				}
			}

			return next(c)
		}
	}

	return mw, closer
}

func isNilRateLimitStore(s middleware.RateLimitStore) bool {
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

func piRateLimitRouteLabel(r *http.Request) string {
	if r == nil {
		return "unmatched"
	}
	if rctx := chi.RouteContext(r.Context()); rctx != nil {
		if pattern := strings.TrimSuffix(rctx.RoutePattern(), "/"); pattern != "" {
			return pattern
		}
		if rctx.Routes != nil {
			probe := chi.NewRouteContext()
			if rctx.Routes.Match(probe, r.Method, r.URL.Path) {
				if pattern := strings.TrimSuffix(probe.RoutePattern(), "/"); pattern != "" {
					return pattern
				}
				return "/"
			}
		}
		return "unmatched"
	}
	return "unmatched"
}
