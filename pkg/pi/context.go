package pi

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	gorillaWS "github.com/gorilla/websocket"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"

	"github.com/sllt/pi/pkg/pi/cmd/terminal"
	"github.com/sllt/pi/pkg/pi/datasource/file"
	"github.com/sllt/pi/pkg/pi/datasource/pubsub"
	piRedis "github.com/sllt/pi/pkg/pi/datasource/redis"
	piSQL "github.com/sllt/pi/pkg/pi/datasource/sql"
	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"github.com/sllt/pi/pkg/pi/http/middleware"
	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/sllt/pi/pkg/pi/metrics"
	"github.com/sllt/pi/pkg/pi/service"
	piWS "github.com/sllt/pi/pkg/pi/websocket"
)

type Context struct {
	context.Context

	// Request provides protocol-independent request binding and parameter access.
	Request

	rt *runtimeKernel

	// responder abstracts response serialization across HTTP, gRPC, and CLI handlers.
	responder Responder

	// Out provides terminal output helpers for CLI subcommands.
	Out terminal.Output

	logging.ContextLogger

	workerExec *workerExecHandle
}

type AuthInfo interface {
	GetClaims() jwt.MapClaims
	GetUsername() string
	GetAPIKey() string
}

func (c *Context) runtimeKernelRef() *runtimeKernel {
	if c == nil {
		return nil
	}
	return c.rt
}

// SQL returns the primary SQL client, or untyped nil when SQL is not configured.
func (c *Context) SQL() piSQL.Client {
	if c == nil || c.rt == nil {
		return nil
	}
	return c.rt.getSQL()
}

// Redis returns the primary Redis client, or untyped nil when Redis is not configured.
func (c *Context) Redis() piRedis.Client {
	if c == nil || c.rt == nil {
		return nil
	}
	return c.rt.getRedis()
}

// PubSub returns the primary Pub/Sub client, or untyped nil when Pub/Sub is not configured.
func (c *Context) PubSub() pubsub.Client {
	if c == nil || c.rt == nil {
		return nil
	}
	return c.rt.getPubSub()
}

// GetPublisher returns the primary Pub/Sub publisher, or untyped nil when unconfigured.
func (c *Context) GetPublisher() pubsub.Publisher {
	if c == nil || c.rt == nil {
		return nil
	}
	return c.rt.getPublisher()
}

// GetSubscriber returns the primary Pub/Sub subscriber, or untyped nil when unconfigured.
func (c *Context) GetSubscriber() pubsub.Subscriber {
	if c == nil || c.rt == nil {
		return nil
	}
	return c.rt.getSubscriber()
}

// File returns the configured FileSystem (defaulting to local filesystem).
func (c *Context) File() file.FileSystem {
	if c == nil || c.rt == nil {
		return nil
	}
	return c.rt.getFile()
}

// Logger returns the base application logger (never nil).
func (c *Context) Logger() logging.Logger {
	if c == nil || c.rt == nil {
		return nopFallbackLogger{}
	}
	return c.rt.getLogger()
}

// Metrics returns the application metrics manager, or nil on a zero-value Context.
func (c *Context) Metrics() metrics.Manager {
	if c == nil || c.rt == nil {
		return nil
	}
	return c.rt.getMetrics()
}

// TracerProvider returns the application OpenTelemetry TracerProvider.
func (c *Context) TracerProvider() trace.TracerProvider {
	if c == nil || c.rt == nil {
		return otel.GetTracerProvider()
	}
	return c.rt.getTracerProvider()
}

// HTTPService returns the registered downstream HTTP service client with the given name, or nil.
func (c *Context) HTTPService(name string) service.HTTP {
	if c == nil || c.rt == nil {
		return nil
	}
	return c.rt.getHTTPService(name)
}

// GetHTTPService returns the registered downstream HTTP service client with the given name, or nil.
func (c *Context) GetHTTPService(name string) service.HTTP {
	return c.HTTPService(name)
}

// Health executes bounded singleflight deep health checks across core datasources, HTTP services, and adapters.
func (c *Context) Health(ctx context.Context) map[string]any {
	if c == nil || c.rt == nil {
		return (*runtimeKernel)(nil).Health(ctx)
	}
	return c.rt.Health(ctx)
}

// AppName returns the configured application name.
func (c *Context) AppName() string {
	if c == nil || c.rt == nil {
		return ""
	}
	return c.rt.getAppName()
}

// AppVersion returns the configured application version.
func (c *Context) AppVersion() string {
	if c == nil || c.rt == nil {
		return ""
	}
	return c.rt.getAppVersion()
}

// Header reads a request header (HTTP) or incoming metadata value (gRPC) case-insensitively.
func (c *Context) Header(key string) string {
	trimmed := strings.TrimSpace(key)
	if c == nil || trimmed == "" {
		return ""
	}
	if c.Request != nil {
		if httpReq, ok := c.Request.(*piHTTP.Request); ok && httpReq != nil {
			if r := httpReq.HTTPRequest(); r != nil {
				return r.Header.Get(trimmed)
			}
		} else if rawProvider, ok := c.Request.(interface{ HTTPRequest() *http.Request }); ok {
			if r := rawProvider.HTTPRequest(); r != nil {
				return r.Header.Get(trimmed)
			}
		}
	}
	metaCtx := c.Context
	if metaCtx == nil && c.Request != nil {
		metaCtx = c.Request.Context()
	}
	if metaCtx != nil {
		if vals := metadata.ValueFromIncomingContext(metaCtx, strings.ToLower(trimmed)); len(vals) > 0 {
			return vals[0]
		}
	}
	return ""
}

/*
Trace returns an open telemetry span. We have to always close the span after corresponding work is done. Usages:

	span := c.Trace("Some Work")
	// Do some work here.
	defer span.End()

If an entire function has to traced as span, we can use a simpler format:

	defer c.Trace("ExampleHandler").End()

We can write this at the start of function and because of how defer works, trace will start at that line
but End will be called after function ends.

Developer Note: If you chain methods in a defer statement, everything except the last function will be evaluated at call time.
*/
func (c *Context) Trace(name string) trace.Span {
	if c == nil {
		_, span := otel.GetTracerProvider().Tracer("pi-context").Start(context.Background(), name)
		return span
	}
	baseCtx := c.Context
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	tr := c.TracerProvider().Tracer("pi-context")
	ctx, span := tr.Start(baseCtx, name)

	c.Context = ctx
	c.ContextLogger.UpdateContext(ctx)

	return span
}

func (c *Context) Bind(i any) error {
	if c == nil || c.Request == nil {
		return nil
	}
	return c.Request.Bind(i)
}

// GetWebsocketConnection returns the active WebSocket connection bound to this Context, or nil.
func (c *Context) GetWebsocketConnection() *piWS.Connection {
	if c == nil || c.rt == nil {
		return nil
	}
	return c.rt.getConnectionFromContext(c.Context)
}

// WriteMessageToSocket writes a message to the WebSocket connection associated with the context.
// The data parameter can be of type string, []byte, or any struct that can be marshaled to JSON.
// It retrieves the WebSocket connection from the context and sends the message as a TextMessage.
func (c *Context) WriteMessageToSocket(data any) error {
	if c == nil || c.rt == nil || c.rt.wsManager == nil {
		return ErrConnectionNotFound
	}
	conn := c.rt.getConnectionFromContext(c.Context)

	message, err := serializeMessage(data)
	if err != nil {
		return err
	}

	return conn.WriteMessage(gorillaWS.TextMessage, message)
}

// WriteMessageToService writes a message to the WebSocket connection associated with the given service name.
// The data parameter can be of type string, []byte, or any struct that can be marshaled to JSON.
func (c *Context) WriteMessageToService(serviceName string, data any) error {
	if c == nil || c.rt == nil || c.rt.wsManager == nil {
		return fmt.Errorf("%w: %s", ErrConnectionNotFound, serviceName)
	}
	conn := c.rt.getWSConnectionByServiceName(serviceName)
	if conn == nil {
		return fmt.Errorf("%w: %s", ErrConnectionNotFound, serviceName)
	}

	message, err := serializeMessage(data)
	if err != nil {
		return err
	}

	return conn.WriteMessage(gorillaWS.TextMessage, message)
}

type authInfo struct {
	claims   jwt.MapClaims
	username string
	apiKey   string
}

// GetAuthInfo is a method on context, to access different methods to retrieve authentication info.
//
// GetAuthInfo().GetClaims() : retrieves the jwt claims.
// GetAuthInfo().GetUsername() : retrieves the username while basic authentication.
// GetAuthInfo().GetAPIKey() : retrieves the APIKey being used for authentication.
func (c *Context) GetAuthInfo() AuthInfo {
	if c == nil {
		return &authInfo{}
	}
	var authCtx context.Context
	if c.Request != nil && c.Request.Context() != nil {
		authCtx = c.Request.Context()
	} else {
		authCtx = c.Context
	}
	if authCtx == nil {
		return &authInfo{}
	}

	claims, _ := authCtx.Value(middleware.JWTClaim).(jwt.MapClaims)
	apiKey, _ := authCtx.Value(middleware.APIKey).(string)
	username, _ := authCtx.Value(middleware.Username).(string)

	return &authInfo{
		claims:   claims,
		username: username,
		apiKey:   apiKey,
	}
}

// GetClaims returns a response of jwt.MapClaims type when OAuth is enabled.
// It returns nil if called, when OAuth is not enabled.
func (a *authInfo) GetClaims() jwt.MapClaims {
	return a.claims
}

// GetUsername returns the username when basic auth is enabled.
// It returns an empty string if called, when basic auth is not enabled.
func (a *authInfo) GetUsername() string {
	return a.username
}

// GetAPIKey returns the APIKey when APIKey auth is enabled.
// It returns an empty string if called, when APIKey auth is not enabled.
func (a *authInfo) GetAPIKey() string {
	return a.apiKey
}

func newContext(w Responder, r Request, rt *runtimeKernel) *Context {
	reqCtx := context.Background()
	if r != nil && r.Context() != nil {
		reqCtx = r.Context()
	} else {
		r = noopRequest{ctx: reqCtx}
	}
	return &Context{
		Context:       reqCtx,
		Request:       r,
		responder:     w,
		rt:            rt,
		ContextLogger: *logging.NewContextLogger(reqCtx, rt.getLogger()),
	}
}

func newCMDContext(w Responder, r Request, rt *runtimeKernel, out terminal.Output) *Context {
	reqCtx := context.Background()
	if r != nil && r.Context() != nil {
		reqCtx = r.Context()
	} else {
		r = noopRequest{ctx: reqCtx}
	}
	return &Context{
		Context:       reqCtx,
		responder:     w,
		Request:       r,
		rt:            rt,
		Out:           out,
		ContextLogger: *logging.NewContextLogger(reqCtx, rt.getLogger()),
	}
}

func (c *Context) GetCorrelationID() string {
	if c == nil || c.Context == nil {
		return ""
	}
	return trace.SpanFromContext(c.Context).SpanContext().TraceID().String()
}
