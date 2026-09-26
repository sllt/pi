package pi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/sllt/pi/pkg/pi/config"
	piSQL "github.com/sllt/pi/pkg/pi/datasource/sql"
	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"github.com/sllt/pi/pkg/pi/http/middleware"
	"github.com/sllt/pi/pkg/pi/infra"
	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/sllt/pi/pkg/pi/metrics"
)

type Ownership uint8

const (
	Owned Ownership = iota + 1
	Borrowed
)

// Resource starts in declaration order and stops in reverse order. A failing
// Start is also stopped, so Stop must tolerate partially initialized resources.
// Borrowed resources are used without calling either hook. Start=nil means an
// already active owned resource; it is closed even if the App is never started.
type Resource struct {
	Name      string
	Ownership Ownership
	Start     func(context.Context) error
	Stop      func(context.Context) error
}
type managedResource struct {
	Resource
	active bool
}
type buildOptions struct {
	values         map[string]string
	logger         logging.Logger
	metrics        metrics.Manager
	metricsHandler http.Handler
	validate       func(any) error
	db             infra.DB
	dbOwnership    Ownership
	managedSQL     bool
	resources      []Resource
}
type Option func(*buildOptions) error

func WithConfig(values map[string]string) Option {
	snapshot := config.NewSnapshot(values)
	return func(o *buildOptions) error { o.values = snapshot.Values(); return nil }
}
func WithLogger(l logging.Logger) Option {
	return func(o *buildOptions) error {
		if l == nil {
			return errors.New("logger must not be nil")
		}
		o.logger = l
		return nil
	}
}
func WithMetrics(m metrics.Manager) Option {
	return func(o *buildOptions) error {
		if m == nil {
			return errors.New("metrics must not be nil")
		}
		o.metrics = m
		return nil
	}
}
func WithMetricsHandler(h http.Handler) Option {
	return func(o *buildOptions) error {
		if h == nil {
			return errors.New("metrics handler must not be nil")
		}
		o.metricsHandler = h
		return nil
	}
}
func WithValidator(v func(any) error) Option {
	return func(o *buildOptions) error {
		if v == nil {
			return errors.New("validator must not be nil")
		}
		o.validate = v
		return nil
	}
}
func WithResource(r Resource) Option {
	return func(o *buildOptions) error { o.resources = append(o.resources, r); return nil }
}
func WithSQL(db infra.DB, ownership Ownership) Option {
	return func(o *buildOptions) error {
		if nilOptionValue(db) {
			return errors.New("SQL must not be nil")
		}
		o.db = db
		o.dbOwnership = ownership
		return nil
	}
}

func nilOptionValue(v any) bool {
	if v == nil {
		return true
	}
	x := reflect.ValueOf(v)
	switch x.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return x.IsNil()
	}
	return false
}
func WithManagedSQL() Option { return func(o *buildOptions) error { o.managedSQL = true; return nil } }

// Build is a pure runtime constructor. Configuration is explicit (no ambient
// env loading); clients, listeners and workers start only in Start. Its Start
// context bounds startup, and is never the parent of runtime workers.
func Build(options ...Option) (*App, error) {
	o := buildOptions{values: map[string]string{}}
	for _, opt := range options {
		if opt == nil {
			return nil, errors.New("nil Build option")
		}
		if err := opt(&o); err != nil {
			return nil, err
		}
	}
	if _, ok := o.values["METRICS_ENABLED"]; !ok {
		o.values["METRICS_ENABLED"] = "false"
	}
	cfg := config.NewSnapshot(o.values)
	if err := validateBuildConfig(cfg); err != nil {
		return nil, err
	}
	if o.managedSQL && o.db != nil {
		return nil, errors.New("WithManagedSQL and WithSQL are mutually exclusive")
	}
	if o.managedSQL {
		if cfg.Get("DB_NAME") == "" {
			return nil, errors.New("DB_NAME is required for managed SQL")
		}
		switch cfg.Get("DB_DIALECT") {
		case "sqlite", "mysql", "postgres", "supabase", "cockroachdb":
		default:
			return nil, fmt.Errorf("unsupported DB_DIALECT %q", cfg.Get("DB_DIALECT"))
		}
		if cfg.Get("DB_DIALECT") != "sqlite" && cfg.Get("DB_HOST") == "" && cfg.Get("SUPABASE_PROJECT_REF") == "" {
			return nil, errors.New("DB_HOST is required for managed SQL")
		}
	}
	if o.db != nil {
		o.resources = append([]Resource{{Name: "sql", Ownership: o.dbOwnership, Stop: func(context.Context) error { return o.db.Close() }}}, o.resources...)
	}
	names := map[string]bool{}
	for _, r := range o.resources {
		if r.Name == "" || names[r.Name] || (o.managedSQL && r.Name == "sql") {
			return nil, fmt.Errorf("empty or duplicate resource name %q", r.Name)
		}
		if r.Ownership != Owned && r.Ownership != Borrowed {
			return nil, fmt.Errorf("resource %s requires Owned or Borrowed", r.Name)
		}
		names[r.Name] = true
	}
	address, httpEnabled, err := endpointConfig(cfg, "HTTP", defaultHTTPPort)
	if err != nil {
		return nil, err
	}
	grpcAddr, grpcEnabled, err := endpointConfig(cfg, "GRPC", defaultGRPCPort)
	if err != nil {
		return nil, err
	}
	metricsAddr, metricsEnabled, err := endpointConfig(cfg, "METRICS", defaultMetricPort)
	if err != nil {
		return nil, err
	}
	if metricsEnabled && o.metricsHandler == nil {
		return nil, errors.New("Build metrics listener requires WithMetricsHandler for an instance-owned exporter")
	}
	for _, keys := range []struct {
		cert, key string
		enabled   bool
	}{{"CERT_FILE", "KEY_FILE", httpEnabled}, {"GRPC_CERT_FILE", "GRPC_KEY_FILE", grpcEnabled}} {
		if !keys.enabled {
			continue
		}
		if _, err := serverTLS(cfg.Get(keys.cert), cfg.Get(keys.key)); err != nil {
			return nil, fmt.Errorf("%s: %w", keys.cert, err)
		}
	}
	mw := middleware.GetConfigs(cfg)
	if err := middleware.ValidateCORS(mw.CorsHeaders); err != nil {
		return nil, err
	}
	if o.validate == nil {
		o.validate = piHTTP.NewValidator(cfg.GetOrDefault("VALIDATION_LOCALE", "en"))
	}
	c := infra.NewIsolated(cfg, o.logger, o.metrics)
	c.Validate = o.validate
	a := &App{Config: cfg, container: c, pureBuild: true, httpDisabled: !httpEnabled, grpcDisabled: !grpcEnabled, waitDone: make(chan struct{})}
	if o.managedSQL {
		h := piSQL.NewHandle(cfg, c.Logger, c.Metrics())
		c.SQL = h
		a.resources = append(a.resources, managedResource{Resource: Resource{Name: "sql", Ownership: Owned, Start: h.Start, Stop: func(context.Context) error { return h.Close() }}})
	} else {
		c.SQL = o.db
	}
	for _, r := range o.resources {
		a.resources = append(a.resources, managedResource{Resource: r, active: r.Start == nil})
	}
	a.httpServer = newHTTPServer(c, defaultHTTPPort, mw)
	a.httpServer.address = address
	a.httpServer.certFile = cfg.Get("CERT_FILE")
	a.httpServer.keyFile = cfg.Get("KEY_FILE")
	a.grpcServer, err = newGRPCServer(c, defaultGRPCPort, cfg)
	if err != nil {
		return nil, err
	}
	a.grpcServer.address = grpcAddr
	if metricsEnabled {
		a.metricServer = newMetricServer(defaultMetricPort)
		a.metricServer.address = metricsAddr
		a.metricServer.handler = o.metricsHandler
	}
	a.subscriptionManager = newSubscriptionManager(c)
	return a, nil
}

func validateBuildConfig(c *config.Snapshot) error {
	allowed := map[string]bool{}
	// These belong to the HTTP/gRPC libraries, not Pi's endpoint schema.
	for _, k := range []string{"HTTP_PROXY", "GRPC_GO_LOG_SEVERITY_LEVEL", "GRPC_GO_LOG_VERBOSITY_LEVEL"} {
		allowed[k] = true
	}
	for _, p := range []string{"HTTP", "GRPC", "METRICS"} {
		for _, s := range []string{"ADDR", "HOST", "PORT", "ENABLED"} {
			allowed[p+"_"+s] = true
		}
	}
	for _, k := range []string{"GRPC_CERT_FILE", "GRPC_KEY_FILE", "GRPC_ENABLE_REFLECTION"} {
		allowed[k] = true
	}
	for _, k := range []string{"CORS_ALLOWED_ORIGINS", "CORS_ALLOWED_METHODS", "CORS_ALLOWED_HEADERS", "CORS_ALLOW_CREDENTIALS", "CORS_EXPOSE_HEADERS", "CORS_MAX_AGE", "DB_DIALECT", "DB_NAME", "DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_SSL_MODE", "DB_CHARSET", "DB_MAX_IDLE_CONNECTION", "DB_MAX_OPEN_CONNECTION", "DB_TLS_CA_CERT", "DB_TLS_CLIENT_CERT", "DB_TLS_CLIENT_KEY"} {
		allowed[k] = true
	}
	for k := range c.Values() {
		if (strings.HasPrefix(k, "HTTP_") || strings.HasPrefix(k, "GRPC_") || strings.HasPrefix(k, "METRICS_") || strings.HasPrefix(k, "CORS_") || strings.HasPrefix(k, "DB_")) && !allowed[k] {
			return fmt.Errorf("unknown runtime configuration field %s", k)
		}
	}
	for _, k := range []string{"REQUEST_TIMEOUT", "DB_MAX_IDLE_CONNECTION", "DB_MAX_OPEN_CONNECTION"} {
		if v := c.Get(k); v != "" {
			n, e := strconv.Atoi(v)
			if e != nil || n < 0 {
				return fmt.Errorf("%s must be a nonnegative integer", k)
			}
		}
	}
	if v := c.Get("GRPC_ENABLE_REFLECTION"); v != "" {
		if _, err := strconv.ParseBool(v); err != nil {
			return errors.New("GRPC_ENABLE_REFLECTION must be a boolean")
		}
	}
	if v := c.Get("DB_PORT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 65535 {
			return errors.New("DB_PORT must be in 1..65535")
		}
	}
	if v := c.Get("SHUTDOWN_GRACE_PERIOD"); v != "" {
		d, e := time.ParseDuration(v)
		if e != nil || d <= 0 {
			return errors.New("SHUTDOWN_GRACE_PERIOD must be a positive duration")
		}
	}
	if v := c.Get("VALIDATION_LOCALE"); v != "" && v != "en" && v != "zh" {
		return fmt.Errorf("unsupported VALIDATION_LOCALE %q", v)
	}
	return nil
}

func (a *App) startResources(ctx context.Context) error {
	for i := range a.resources {
		r := &a.resources[i]
		if r.Ownership == Borrowed {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		r.active = true
		if r.Start != nil {
			if err := resourceCall(r.Name, r.Start, ctx); err != nil {
				return err
			}
		}
	}
	return nil
}
func (a *App) stopResources(ctx context.Context) error {
	var err error
	for i := len(a.resources) - 1; i >= 0; i-- {
		r := &a.resources[i]
		if r.Ownership == Owned && r.active && r.Stop != nil {
			err = errors.Join(err, resourceCall(r.Name, r.Stop, ctx))
		}
	}
	return err
}
func resourceCall(name string, fn func(context.Context) error, ctx context.Context) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("resource %s panicked: %v", name, v)
		}
	}()
	if e := fn(ctx); e != nil {
		return fmt.Errorf("resource %s: %w", name, e)
	}
	return nil
}
