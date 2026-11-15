# Middleware in Pi

Middleware allows you intercepting and manipulating HTTP requests and responses flowing through your application's
router. Middlewares can perform tasks such as authentication, authorization, caching etc. before
or after the request reaches your application's handler.

## CORS Middleware in Pi
Pi includes built-in CORS (Cross-Origin Resource Sharing) middleware to handle CORS-related headers.
This middleware allows you to control access to your API from different origins. It automatically adds the necessary
headers to responses, allowing or restricting cross-origin requests. User can also override the default response headers
sent by Pi by providing the suitable CORS configs.

The CORS middleware provides the following overridable configs:

- `ACCESS_CONTROL_ALLOW_ORIGIN`: Set the allowed origin(s) for cross-origin requests. By default, it allows all origins (*).
- `ACCESS_CONTROL_ALLOW_HEADERS`: Define the allowed request headers (e.g., Authorization, Content-Type).
- `ACCESS_CONTROL_ALLOW_CREDENTIALS`: Set to true to allow credentials (cookies, HTTP authentication) in requests.
- `ACCESS_CONTROL_EXPOSE_HEADERS`: Specify additional headers exposed to the client.
- `ACCESS_CONTROL_MAX_AGE`: Set the maximum time (in seconds) for preflight request caching.

> Note: Pi automatically interprets the registered route methods and based on that sets the value of `ACCESS_CONTROL_ALLOW_METHODS`


## Adding Custom Middleware in Pi

By adding custom middleware to your Pi application, you can extend its functionality and implement
cross-cutting concerns in a modular and reusable way.
Use `app.Use(...)` for standard `net/http` middleware, or `app.UseMiddleware(...)` for `pi.PiMiddleware` when you need access to `*pi.Context` (`ctx.SQL()`, `ctx.Redis()`, `pi.Use[T](ctx)`, etc.).

### Using `Use` for Standard `net/http` Middleware

```go
import (
	"net/http"

	"github.com/sllt/pi/pkg/pi"
)

func customHTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Custom pre/post request logic
		next.ServeHTTP(w, r)
	})
}

func main() {
	app := pi.New()
	app.Use(customHTTPMiddleware)
	app.Run()
}
```

### Using `UseMiddleware` with `*pi.Context`

```go
import (
	"net/http"

	"github.com/sllt/pi/pkg/pi"
)

func auditMiddleware(next pi.PiHandler) pi.PiHandler {
	return func(ctx *pi.Context, w http.ResponseWriter, r *http.Request) {
		ctx.Infof("incoming %s %s", r.Method, r.URL.Path)
		next(ctx, w, r)
	}
}

func main() {
	app := pi.New()
	app.UseMiddleware(auditMiddleware)
	app.Run()
}
```

## Rate Limiter Middleware in Pi

Pi provides a built-in rate limiter middleware to protect your API from abuse and ensure fair resource distribution.
It uses a token bucket algorithm for smooth rate limiting with configurable burst capacity.

### Features

- **Token Bucket Algorithm**: Allows smooth rate limiting with configurable burst capacity
- **Per-IP Rate Limiting**: Each client IP gets its own rate limit (configurable)
- **Health Check Exemption**: `/.well-known/alive` and `/.well-known/health` endpoints are automatically exempt
- **Prometheus Metrics**: Track rate limit violations via `app_http_rate_limit_exceeded_total` counter
- **429 Status Code**: Returns standard HTTP 429 (Too Many Requests) when limit is exceeded

### Configuration

```go
import (
	"github.com/sllt/pi/pkg/pi"
	"github.com/sllt/pi/pkg/pi/http/middleware"
)

func main() {
	app := pi.New()

	// Configure rate limiter
	rateLimiterConfig := middleware.RateLimiterConfig{
		RequestsPerSecond: 5,    // Average requests per second
		Burst:             10,   // Maximum burst size
		PerIP:             true, // Enable per-IP limiting
	}

	// Add rate limiter middleware
	app.UseMiddleware(middleware.RateLimiter(rateLimiterConfig, app.Metrics()))

	app.GET("/api/resource", handler)
	app.Run()
}
```

### Parameters

- `RequestsPerSecond`: Average number of requests allowed per second
- `Burst`: Maximum number of requests that can be made in a burst (allows temporary spikes)
- `PerIP`: Set to `true` for per-IP limiting (recommended) or `false` for global rate limit across all clients
- `TrustedProxies`: *(Optional)* Set to `true` to trust `X-Forwarded-For` and `X-Real-IP` headers for IP extraction. Only enable when behind a trusted reverse proxy.

> **Security Warning**: Only set `TrustedProxies: true` if your application is behind a trusted reverse proxy (nginx, ALB, etc.). 
> Without a trusted proxy, clients can spoof headers to bypass rate limits.

