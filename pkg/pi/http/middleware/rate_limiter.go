package middleware

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/netip"
	"strings"

	"github.com/go-chi/chi/v5"

	piHttp "github.com/sllt/pi/pkg/pi/http"
)

var (
	// errInvalidRequestsPerSecond is returned when RequestsPerSecond is not positive.
	errInvalidRequestsPerSecond = errors.New("requestsPerSecond must be positive")

	// errInvalidBurst is returned when Burst is not positive.
	errInvalidBurst = errors.New("burst must be positive")
)

// RateLimiterConfig holds configuration for rate limiting.
//
// Note: The default implementation uses in-memory token buckets and is suitable
// for single-pod deployments. In multi-pod deployments, each pod will enforce
// limits independently. For distributed rate limiting across multiple pods,
// a Redis-backed store can be implemented in a future update.
//
// Security: When using PerIP=true, only enable TrustedProxies if your application
// is behind a trusted reverse proxy (nginx, ALB, etc.) that sets X-Forwarded-For.
// Without trusted proxies, clients can spoof IP addresses to bypass rate limits.
//
// Cleanup: The rate limiter starts a background goroutine that runs for the
// application lifetime. This is acceptable for long-running servers but consider
// calling Store.StopCleanup() in shutdown handlers if needed.
type RateLimiterConfig struct {
	RequestsPerSecond float64
	Burst             int
	PerIP             bool
	Store             RateLimiterStore // Optional: defaults to in-memory store
	TrustedProxies    bool             // If true, trust X-Forwarded-For and X-Real-IP headers
	TrustedProxyCIDRs []string         // Optional CIDR/IP allowlist for safe proxy header extraction
	MaxKeys           int64            // Maximum unique rate limit keys (0 = default 100000)
	Context           context.Context  // Optional lifecycle context for background bucket cleanup
}

// Validate checks if the configuration values are valid.
func (c RateLimiterConfig) Validate() error {
	if c.RequestsPerSecond <= 0 {
		return errInvalidRequestsPerSecond
	}

	if c.Burst <= 0 {
		return errInvalidBurst
	}

	if len(c.TrustedProxyCIDRs) > 0 {
		if _, err := ParseTrustedProxies(c.TrustedProxyCIDRs); err != nil {
			return err
		}
	}

	return nil
}

// getIP extracts the client IP address from the request.
// If trustProxies is false, only RemoteAddr is used to prevent IP spoofing.
func getIP(r *http.Request, trustProxies bool) string {
	if !trustProxies {
		return getRemoteAddr(r)
	}

	// Try X-Forwarded-For header first
	if ip := getForwardedIP(r); ip != "" {
		return ip
	}

	// Try X-Real-IP header
	if ip := getRealIP(r); ip != "" {
		return ip
	}

	// Fall back to RemoteAddr
	return getRemoteAddr(r)
}

// getForwardedIP extracts IP from X-Forwarded-For header.
func getForwardedIP(r *http.Request) string {
	forwarded := r.Header.Get("X-Forwarded-For")
	if forwarded == "" {
		return ""
	}

	// X-Forwarded-For can contain multiple IPs, take the first one
	ips := strings.Split(forwarded, ",")
	if len(ips) == 0 {
		return ""
	}

	return strings.TrimSpace(ips[0])
}

// getRealIP extracts IP from X-Real-IP header.
func getRealIP(r *http.Request) string {
	realIP := r.Header.Get("X-Real-IP")
	return strings.TrimSpace(realIP)
}

// getRemoteAddr extracts IP from RemoteAddr.
func getRemoteAddr(r *http.Request) string {
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}

	return ip
}

// RateLimiter creates a middleware that limits requests based on the configuration.
func RateLimiter(config RateLimiterConfig, m metrics) func(http.Handler) http.Handler {
	mw, _ := RateLimiterWithCloser(config, m)
	return mw
}

// RateLimiterWithCloser creates a rate-limiting middleware and returns a cleanup closer
// that stops the store's background bucket sweeper on application shutdown.
func RateLimiterWithCloser(config RateLimiterConfig, m metrics) (func(http.Handler) http.Handler, func() error) {
	mw, start, closer := RateLimiterDeferred(config, m)
	ctx := config.Context
	if ctx == nil {
		ctx = context.Background()
	}
	start(ctx)
	return mw, closer
}

// RateLimiterDeferred validates config and constructs a rate-limiting middleware without
// starting background cleanup until start is invoked.
func RateLimiterDeferred(
	config RateLimiterConfig,
	m metrics,
) (func(http.Handler) http.Handler, func(context.Context), func() error) {
	// Validate configuration
	if err := config.Validate(); err != nil {
		panic(fmt.Sprintf("invalid rate limiter config: %v", err))
	}

	var (
		trustedPrefixes      []netip.Prefix
		hasTrustedProxyCIDRs = len(config.TrustedProxyCIDRs) > 0
	)
	if hasTrustedProxyCIDRs {
		trustedPrefixes, _ = ParseTrustedProxies(config.TrustedProxyCIDRs)
	}

	// Use in-memory store if none provided
	if config.Store == nil {
		config.Store = NewMemoryRateLimiterStore(config)
	}

	store := config.Store
	start := func(ctx context.Context) {
		if config.Context != nil {
			ctx = config.Context
		}
		if ctx == nil {
			ctx = context.Background()
		}
		store.StartCleanup(ctx)
	}

	closer := func() error {
		store.StopCleanup()
		return nil
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Skip rate limiting for health check endpoints
			if isWellKnown(r.URL.Path) {
				next.ServeHTTP(w, r)
				return
			}

			// Determine the rate limit key (IP or global)
			key := "global"
			if config.PerIP {
				if hasTrustedProxyCIDRs {
					key = ExtractClientIP(r, trustedPrefixes)
				} else {
					key = getIP(r, config.TrustedProxies)
				}
				// Fallback to "unknown" if IP extraction returns empty string
				// This prevents all requests from sharing the same bucket
				if key == "" {
					key = "unknown"
				}
			}

			// Check rate limit
			allowed, retryAfter, err := store.Allow(r.Context(), key, config)
			if err != nil {
				// Fail open on errors
				next.ServeHTTP(w, r)
				return
			}

			if !allowed {
				// Set Retry-After header (RFC 6585)
				// Use math.Ceil to ensure at least 1 second for sub-second delays
				w.Header().Set("Retry-After", fmt.Sprintf("%.0f", math.Ceil(retryAfter.Seconds())))

				// Increment rate limit exceeded metric using low-cardinality route pattern
				if m != nil {
					m.IncrementCounter(r.Context(), "app_http_rate_limit_exceeded_total",
						"path", rateLimitRouteLabel(r), "method", r.Method)
				}

				// Return 429 Too Many Requests
				responder := piHttp.NewResponder(w, r.Method)
				responder.Respond(nil, piHttp.ErrorTooManyRequests{})

				return
			}

			next.ServeHTTP(w, r)
		})
	}, start, closer
}

func rateLimitRouteLabel(r *http.Request) string {
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
