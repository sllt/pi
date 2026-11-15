package pi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/sllt/pi/pkg/pi/config"
	"github.com/sllt/pi/pkg/pi/datasource/file"
	"github.com/sllt/pi/pkg/pi/datasource/pubsub"
	piRedis "github.com/sllt/pi/pkg/pi/datasource/redis"
	piSQL "github.com/sllt/pi/pkg/pi/datasource/sql"
	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"github.com/sllt/pi/pkg/pi/http/middleware"
	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/sllt/pi/pkg/pi/metrics"
	"github.com/sllt/pi/pkg/pi/service"
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

type httpServiceDecl struct {
	name      string
	svc       service.HTTP
	ownership Ownership
}

type buildDeclKind uint8

const (
	declResource buildDeclKind = iota + 1
	declAdapter
)

type buildDeclaration struct {
	kind        buildDeclKind
	resource    Resource
	adapterName string
	adapterInst any
	adapterOwn  Ownership
	adapterOpts []AdapterOption
}

type buildOptions struct {
	explicitHTTPStatus bool
	values             map[string]string
	logger             logging.Logger
	metrics            metrics.Manager
	metricsHandler     http.Handler
	validate           func(any) error
	customValidate     func(any) error
	typedBind          TypedBindConfig
	openAPI            OpenAPIConfig
	hasOpenAPI         bool

	db             piSQL.Client
	dbOwnership    Ownership
	hasExplicitSQL bool
	managedSQL     bool

	redisClient      piRedis.Client
	redisOwnership   Ownership
	hasExplicitRedis bool
	managedRedis     bool

	pubsubClient      pubsub.Client
	pubsubOwnership   Ownership
	hasExplicitPubSub bool
	managedPubSub     bool

	fileStore       file.FileSystem
	fileOwnership   Ownership
	hasExplicitFile bool

	managedDatasources bool

	tracerProvider    trace.TracerProvider
	tracerOwnership   Ownership
	hasExplicitTracer bool

	httpServices []httpServiceDecl
	declarations []buildDeclaration
}

type Option func(*buildOptions) error

// WithExplicitHTTPStatus defaults successful responses to 200. Use response.OK,
// Created, Accepted or NoContent to express each operation's contract.
func WithExplicitHTTPStatus() Option {
	return func(o *buildOptions) error { o.explicitHTTPStatus = true; return nil }
}

func WithConfig(values map[string]string) Option {
	snapshot := config.NewSnapshot(values)
	return func(o *buildOptions) error { o.values = snapshot.Values(); return nil }
}

func WithLogger(l logging.Logger) Option {
	return func(o *buildOptions) error {
		if isNilValue(l) {
			return fmt.Errorf("%w: logger", ErrNilResource)
		}
		o.logger = l
		return nil
	}
}

func WithMetrics(m metrics.Manager) Option {
	return func(o *buildOptions) error {
		if isNilValue(m) {
			return fmt.Errorf("%w: metrics", ErrNilResource)
		}
		o.metrics = m
		return nil
	}
}

func WithMetricsHandler(h http.Handler) Option {
	return func(o *buildOptions) error {
		if isNilValue(h) {
			return fmt.Errorf("%w: metrics handler", ErrNilResource)
		}
		o.metricsHandler = h
		return nil
	}
}

func WithValidator(v func(any) error) Option {
	return func(o *buildOptions) error {
		if v == nil {
			return fmt.Errorf("%w: validator", ErrNilResource)
		}
		o.validate = v
		o.customValidate = v
		return nil
	}
}

func WithResource(r Resource) Option {
	return func(o *buildOptions) error {
		o.declarations = append(o.declarations, buildDeclaration{
			kind:     declResource,
			resource: r,
		})
		return nil
	}
}

// WithSQL injects an explicit primary SQL client into the App.
func WithSQL(db piSQL.Client, ownership Ownership) Option {
	return func(o *buildOptions) error {
		if isNilValue(db) {
			return fmt.Errorf("%w: sql", ErrNilResource)
		}
		if err := validateOwnership(ownership); err != nil {
			return fmt.Errorf("sql: %w", err)
		}
		if o.hasExplicitSQL {
			return fmt.Errorf("%w: \"sql\"", ErrDuplicateResource)
		}
		o.db = db
		o.dbOwnership = ownership
		o.hasExplicitSQL = true
		return nil
	}
}

// WithRedis injects an explicit primary Redis client into the App.
func WithRedis(client piRedis.Client, ownership Ownership) Option {
	return func(o *buildOptions) error {
		if isNilValue(client) {
			return fmt.Errorf("%w: redis", ErrNilResource)
		}
		if err := validateOwnership(ownership); err != nil {
			return fmt.Errorf("redis: %w", err)
		}
		if o.hasExplicitRedis {
			return fmt.Errorf("%w: \"redis\"", ErrDuplicateResource)
		}
		o.redisClient = client
		o.redisOwnership = ownership
		o.hasExplicitRedis = true
		return nil
	}
}

// WithPubSub injects an explicit primary Pub/Sub client into the App.
func WithPubSub(client pubsub.Client, ownership Ownership) Option {
	return func(o *buildOptions) error {
		if isNilValue(client) {
			return fmt.Errorf("%w: pubsub", ErrNilResource)
		}
		if err := validateOwnership(ownership); err != nil {
			return fmt.Errorf("pubsub: %w", err)
		}
		if o.hasExplicitPubSub {
			return fmt.Errorf("%w: \"pubsub\"", ErrDuplicateResource)
		}
		o.pubsubClient = client
		o.pubsubOwnership = ownership
		o.hasExplicitPubSub = true
		return nil
	}
}

// WithFile injects an explicit primary FileSystem into the App.
func WithFile(fs file.FileSystem, ownership Ownership) Option {
	return func(o *buildOptions) error {
		if isNilValue(fs) {
			return fmt.Errorf("%w: file", ErrNilResource)
		}
		if err := validateOwnership(ownership); err != nil {
			return fmt.Errorf("file: %w", err)
		}
		if o.hasExplicitFile {
			return fmt.Errorf("%w: \"file\"", ErrDuplicateResource)
		}
		o.fileStore = fs
		o.fileOwnership = ownership
		o.hasExplicitFile = true
		return nil
	}
}

// WithHTTPService injects a pre-constructed downstream HTTP service client into the App.
func WithHTTPService(name string, svc service.HTTP, ownership Ownership) Option {
	return func(o *buildOptions) error {
		trimmed := strings.TrimSpace(name)
		if trimmed == "" {
			return fmt.Errorf("%w: HTTP service name must not be empty", ErrDuplicateResource)
		}
		if isReservedResourceName(trimmed) {
			return fmt.Errorf("%w: %q", ErrReservedResourceName, trimmed)
		}
		if isNilValue(svc) {
			return fmt.Errorf("%w: HTTP service %q", ErrNilResource, trimmed)
		}
		if err := validateOwnership(ownership); err != nil {
			return fmt.Errorf("HTTP service %q: %w", trimmed, err)
		}
		o.httpServices = append(o.httpServices, httpServiceDecl{
			name:      trimmed,
			svc:       svc,
			ownership: ownership,
		})
		return nil
	}
}

// WithAdapter mounts an external datasource or custom adapter onto the App.
func WithAdapter(name string, instance any, ownership Ownership, opts ...AdapterOption) Option {
	return func(o *buildOptions) error {
		trimmed := strings.TrimSpace(name)
		if trimmed == "" {
			return fmt.Errorf("%w: adapter name must not be empty", ErrDuplicateResource)
		}
		if isReservedResourceName(trimmed) {
			return fmt.Errorf("%w: %q", ErrReservedResourceName, trimmed)
		}
		if isNilValue(instance) {
			return fmt.Errorf("%w: adapter %q", ErrNilResource, trimmed)
		}
		if err := validateOwnership(ownership); err != nil {
			return fmt.Errorf("adapter %q: %w", trimmed, err)
		}
		o.declarations = append(o.declarations, buildDeclaration{
			kind:        declAdapter,
			adapterName: trimmed,
			adapterInst: instance,
			adapterOwn:  ownership,
			adapterOpts: append([]AdapterOption(nil), opts...),
		})
		return nil
	}
}

// WithTracerProvider configures an instance-scoped OpenTelemetry TracerProvider
// without mutating global otel.SetTracerProvider. When ownership is Owned and tp
// implements Shutdown(context.Context) error, App.Stop flushes and shuts it down.
func WithTracerProvider(tp trace.TracerProvider, ownership Ownership) Option {
	return func(o *buildOptions) error {
		if isNilValue(tp) {
			return fmt.Errorf("%w: tracer provider", ErrNilResource)
		}
		if err := validateOwnership(ownership); err != nil {
			return fmt.Errorf("tracer provider: %w", err)
		}
		if o.hasExplicitTracer {
			return fmt.Errorf("%w: \"tracer\"", ErrDuplicateResource)
		}
		o.tracerProvider = tp
		o.tracerOwnership = ownership
		o.hasExplicitTracer = true
		return nil
	}
}

// WithManagedSQL constructs a lazy *sql.Handle from WithConfig values that opens in Start and closes in Stop.
func WithManagedSQL() Option {
	return func(o *buildOptions) error { o.managedSQL = true; return nil }
}

// WithManagedRedis constructs a lazy *redis.Handle from WithConfig values that opens in Start and closes in Stop.
func WithManagedRedis() Option {
	return func(o *buildOptions) error { o.managedRedis = true; return nil }
}

// WithManagedPubSub constructs a lazy Pub/Sub handle from WithConfig values that opens in Start and closes in Stop.
func WithManagedPubSub() Option {
	return func(o *buildOptions) error { o.managedPubSub = true; return nil }
}

// WithManagedDatasources automatically enables managed SQL (when DB_DIALECT is non-empty),
// managed Redis (when REDIS_HOST is non-empty), and managed Pub/Sub (when PUBSUB_BACKEND is non-empty),
// unless an explicit WithSQL / WithRedis / WithPubSub injection was provided.
func WithManagedDatasources() Option {
	return func(o *buildOptions) error { o.managedDatasources = true; return nil }
}

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

	if o.managedSQL && o.hasExplicitSQL {
		return nil, fmt.Errorf("%w: WithManagedSQL and WithSQL are mutually exclusive", ErrDuplicateResource)
	}
	if o.managedRedis && o.hasExplicitRedis {
		return nil, fmt.Errorf("%w: WithManagedRedis and WithRedis are mutually exclusive", ErrDuplicateResource)
	}
	if o.managedPubSub && o.hasExplicitPubSub {
		return nil, fmt.Errorf("%w: WithManagedPubSub and WithPubSub are mutually exclusive", ErrDuplicateResource)
	}

	useManagedSQL := o.managedSQL || (o.managedDatasources && !o.hasExplicitSQL && strings.TrimSpace(cfg.Get("DB_DIALECT")) != "")
	useManagedRedis := o.managedRedis || (o.managedDatasources && !o.hasExplicitRedis && strings.TrimSpace(cfg.Get("REDIS_HOST")) != "")
	useManagedPubSub := o.managedPubSub || (o.managedDatasources && !o.hasExplicitPubSub && strings.TrimSpace(cfg.Get("PUBSUB_BACKEND")) != "")

	if useManagedSQL {
		dialect := strings.TrimSpace(cfg.Get("DB_DIALECT"))
		switch dialect {
		case "sqlite", "mysql", "postgres", "supabase", "cockroachdb":
		default:
			return nil, fmt.Errorf("unsupported DB_DIALECT %q", cfg.Get("DB_DIALECT"))
		}
		if dialect != "supabase" && strings.TrimSpace(cfg.Get("DB_NAME")) == "" {
			return nil, errors.New("DB_NAME is required for managed SQL")
		}
		switch dialect {
		case "sqlite":
		case "supabase":
			if strings.TrimSpace(cfg.Get("DB_HOST")) == "" && strings.TrimSpace(cfg.Get("SUPABASE_PROJECT_REF")) == "" {
				return nil, errors.New("DB_HOST or SUPABASE_PROJECT_REF is required for managed supabase SQL")
			}
		default:
			if strings.TrimSpace(cfg.Get("DB_HOST")) == "" {
				return nil, errors.New("DB_HOST is required for managed SQL")
			}
		}
	}
	if useManagedRedis {
		if err := piRedis.ValidateConfig(cfg); err != nil {
			return nil, err
		}
	}
	if useManagedPubSub {
		if err := validatePubSubBaseConfig(cfg); err != nil {
			return nil, err
		}
	}

	// Pre-validate global uniqueness & reserved names across all services, adapters, and custom resources.
	seenNames := make(map[string]bool)
	for _, hs := range o.httpServices {
		if seenNames[hs.name] {
			return nil, fmt.Errorf("%w: %q", ErrDuplicateResource, hs.name)
		}
		seenNames[hs.name] = true
	}
	for _, decl := range o.declarations {
		switch decl.kind {
		case declResource:
			name := strings.TrimSpace(decl.resource.Name)
			if name == "" {
				return nil, fmt.Errorf("%w: empty resource name", ErrDuplicateResource)
			}
			if isReservedResourceName(name) {
				return nil, fmt.Errorf("%w: %q", ErrReservedResourceName, name)
			}
			if err := validateOwnership(decl.resource.Ownership); err != nil {
				return nil, fmt.Errorf("resource %s: %w", name, err)
			}
			if seenNames[name] {
				return nil, fmt.Errorf("%w: %q", ErrDuplicateResource, name)
			}
			seenNames[name] = true
		case declAdapter:
			if seenNames[decl.adapterName] {
				return nil, fmt.Errorf("%w: %q", ErrDuplicateResource, decl.adapterName)
			}
			seenNames[decl.adapterName] = true
		}
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
	var normalizedOpenAPI OpenAPIConfig
	if o.hasOpenAPI {
		normalizedOpenAPI, err = validateAndNormalizeOpenAPIConfig(o.openAPI)
		if err != nil {
			return nil, err
		}
	}
	if o.validate == nil {
		o.validate = piHTTP.NewValidator(cfg.GetOrDefault("VALIDATION_LOCALE", "en"))
	}

	k := newIsolatedKernel(cfg, o.logger, o.metrics, o.tracerProvider)
	k.validate = o.validate
	k.explicitHTTPStatus = o.explicitHTTPStatus
	k.maxBodyBytes = 1 << 20
	if v := strings.TrimSpace(cfg.Get("HTTP_MAX_BODY_BYTES")); v != "" {
		k.maxBodyBytes, _ = strconv.ParseInt(v, 10, 64)
	}

	a := &App{
		Config:           cfg,
		kernel:           k,
		pureBuild:        true,
		httpDisabled:     !httpEnabled,
		grpcDisabled:     !grpcEnabled,
		waitDone:         make(chan struct{}),
		customValidate:   o.customValidate,
		typedBind:        o.typedBind,
		openAPI:          normalizedOpenAPI,
		hasOpenAPIConfig: o.hasOpenAPI,
	}

	// Deterministic lifecycle order: tracer -> sql -> redis -> pubsub -> file -> httpServices -> declarations.
	if o.hasExplicitTracer {
		tp := o.tracerProvider
		mr := &managedResource{
			name:      "tracer",
			kind:      "core",
			instance:  tp,
			ownership: o.tracerOwnership,
			stopFn: func(ctx context.Context) error {
				if sd, ok := tp.(interface{ Shutdown(context.Context) error }); ok {
					return sd.Shutdown(ctx)
				}
				return nil
			},
		}
		mr.state.Store(resRunning)
		mr.active.Store(o.tracerOwnership == Owned)
		k.resources = append(k.resources, mr)
		k.names["tracer"] = "core"
	} else if sd, ok := k.tracerProvider.(interface{ Shutdown(context.Context) error }); ok {
		a.ownedTracerShutdown = sd.Shutdown
	}

	if useManagedSQL {
		h := piSQL.NewHandleWithTracerProvider(cfg, k.getLogger(), k.getMetrics(), k.getTracerProvider())
		if err := a.setCoreSlot("sql", h, Owned, false, false); err != nil {
			return nil, err
		}
	} else if o.hasExplicitSQL {
		if err := a.setCoreSlot("sql", o.db, o.dbOwnership, false, false); err != nil {
			return nil, err
		}
	}

	if useManagedRedis {
		h := piRedis.NewHandleWithTracerProvider(cfg, k.getLogger(), k.getMetrics(), k.getTracerProvider())
		if err := a.setCoreSlot("redis", h, Owned, false, false); err != nil {
			return nil, err
		}
	} else if o.hasExplicitRedis {
		if err := a.setCoreSlot("redis", o.redisClient, o.redisOwnership, false, false); err != nil {
			return nil, err
		}
	}

	if useManagedPubSub {
		h, err := newManagedPubSubHandle(cfg, k.getLogger(), k.getMetrics(), k.getTracerProvider())
		if err != nil {
			return nil, err
		}
		h.kernel = k
		if err := a.setCoreSlot("pubsub", h, Owned, false, false); err != nil {
			return nil, err
		}
	} else if o.hasExplicitPubSub {
		if err := a.setCoreSlot("pubsub", o.pubsubClient, o.pubsubOwnership, false, false); err != nil {
			return nil, err
		}
	}

	if o.hasExplicitFile {
		if err := a.setCoreSlot("file", o.fileStore, o.fileOwnership, false, false); err != nil {
			return nil, err
		}
	}

	for _, hs := range o.httpServices {
		if err := a.setHTTPServiceInternal(hs.name, hs.svc, hs.ownership, true, false); err != nil {
			return nil, err
		}
	}

	for _, decl := range o.declarations {
		switch decl.kind {
		case declResource:
			if err := k.addCustomResource(decl.resource); err != nil {
				return nil, err
			}
		case declAdapter:
			if err := a.Mount(decl.adapterName, decl.adapterInst, decl.adapterOwn, decl.adapterOpts...); err != nil {
				return nil, err
			}
		}
	}

	a.httpServer = newHTTPServer(k, defaultHTTPPort, mw)
	a.httpServer.address = address
	a.httpServer.certFile = cfg.Get("CERT_FILE")
	if a.hasOpenAPIConfig && a.openAPI.ServeSpec {
		a.ensureHTTPAvailable()
	}
	a.httpServer.keyFile = cfg.Get("KEY_FILE")
	a.bindWSManagerHooks()
	a.grpcServer, err = newGRPCServer(k, defaultGRPCPort, cfg)
	if err != nil {
		return nil, err
	}
	a.grpcServer.address = grpcAddr
	if metricsEnabled {
		a.metricServer = newMetricServer(defaultMetricPort)
		a.metricServer.address = metricsAddr
		a.metricServer.handler = o.metricsHandler
	}
	a.subscriptionManager = newSubscriptionManager(k)
	return a, nil
}

func validateBuildConfig(c *config.Snapshot) error {
	allowed := map[string]bool{}
	allowed["HTTP_MAX_BODY_BYTES"] = true
	// These belong to the HTTP/gRPC libraries, not Pi's endpoint schema.
	for _, k := range []string{"HTTP_PROXY", "GRPC_GO_LOG_SEVERITY_LEVEL", "GRPC_GO_LOG_VERBOSITY_LEVEL"} {
		allowed[k] = true
	}
	for _, p := range []string{"HTTP", "GRPC", "METRICS"} {
		for _, s := range []string{"ADDR", "HOST", "PORT", "ENABLED"} {
			allowed[p+"_"+s] = true
		}
	}
	for _, k := range []string{"GRPC_SERVER_HOST", "GRPC_CERT_FILE", "GRPC_KEY_FILE", "GRPC_ENABLE_REFLECTION"} {
		allowed[k] = true
	}
	for _, k := range []string{
		"CORS_ALLOWED_ORIGINS", "CORS_ALLOWED_METHODS", "CORS_ALLOWED_HEADERS",
		"CORS_ALLOW_CREDENTIALS", "CORS_EXPOSE_HEADERS", "CORS_MAX_AGE",
		"DB_DIALECT", "DB_URL", "DB_NAME", "DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD",
		"DB_SSL_MODE", "DB_CHARSET", "DB_MAX_IDLE_CONNECTION", "DB_MAX_OPEN_CONNECTION",
		"DB_REPLICA_MAX_IDLE_CONNECTIONS", "DB_REPLICA_MAX_OPEN_CONNECTIONS",
		"DB_REPLICAS", "DB_RESOLVER_STRATEGY",
		"DB_TLS_CA_CERT", "DB_TLS_CLIENT_CERT", "DB_TLS_CLIENT_KEY",
	} {
		allowed[k] = true
	}
	for k := range c.Values() {
		if (strings.HasPrefix(k, "HTTP_") || strings.HasPrefix(k, "GRPC_") || strings.HasPrefix(k, "METRICS_") || strings.HasPrefix(k, "CORS_") || strings.HasPrefix(k, "DB_")) && !allowed[k] {
			return fmt.Errorf("unknown runtime configuration field %s", k)
		}
	}

	// mTLS client certificate/key XOR checks across SQL, Redis, and Kafka.
	for _, pair := range [][2]string{
		{"DB_TLS_CLIENT_CERT", "DB_TLS_CLIENT_KEY"},
		{"REDIS_TLS_CERT", "REDIS_TLS_KEY"},
		{"KAFKA_TLS_CERT_FILE", "KAFKA_TLS_KEY_FILE"},
	} {
		hasCert := strings.TrimSpace(c.Get(pair[0])) != ""
		hasKey := strings.TrimSpace(c.Get(pair[1])) != ""
		if hasCert != hasKey {
			return fmt.Errorf("%s and %s must both be set or both be empty", pair[0], pair[1])
		}
	}

	for _, k := range []string{
		"REQUEST_TIMEOUT",
		"DB_MAX_IDLE_CONNECTION", "DB_MAX_OPEN_CONNECTION",
		"DB_REPLICA_MAX_IDLE_CONNECTIONS", "DB_REPLICA_MAX_OPEN_CONNECTIONS",
		"REDIS_DB", "REDIS_PUBSUB_DB",
		"PARTITION_SIZE",
		"REDIS_STREAMS_MAXLEN", "REDIS_PUBSUB_QUERY_LIMIT", "REDIS_PUBSUB_SUBSCRIBE_LIMIT",
	} {
		if v := strings.TrimSpace(c.Get(k)); v != "" {
			n, e := strconv.Atoi(v)
			if e != nil || n < 0 {
				return fmt.Errorf("%s must be a nonnegative integer", k)
			}
		}
	}
	for _, k := range []string{
		"GRPC_ENABLE_REFLECTION",
		"REDIS_TLS_ENABLED", "REDIS_TLS_INSECURE_SKIP_VERIFY",
		"KAFKA_TLS_INSECURE_SKIP_VERIFY",
		"MQTT_MESSAGE_ORDER", "MQTT_RETRIEVE_RETAINED",
	} {
		if v := strings.TrimSpace(c.Get(k)); v != "" {
			if _, err := strconv.ParseBool(v); err != nil {
				return fmt.Errorf("%s must be a boolean", k)
			}
		}
	}
	for _, k := range []string{"DB_PORT", "REDIS_PORT", "MQTT_PORT"} {
		if v := strings.TrimSpace(c.Get(k)); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 || n > 65535 {
				return fmt.Errorf("%s must be in 1..65535", k)
			}
		}
	}
	if v := strings.TrimSpace(c.Get("HTTP_MAX_BODY_BYTES")); v != "" {
		n, e := strconv.ParseInt(v, 10, 64)
		if e != nil || n <= 0 {
			return errors.New("HTTP_MAX_BODY_BYTES must be a positive integer")
		}
	}
	for _, k := range []string{"KAFKA_BATCH_SIZE", "KAFKA_BATCH_BYTES", "KAFKA_BATCH_TIMEOUT", "REDIS_PUBSUB_BUFFER_SIZE"} {
		if v := strings.TrimSpace(c.Get(k)); v != "" {
			n, e := strconv.Atoi(v)
			if e != nil || n <= 0 {
				return fmt.Errorf("%s must be a positive integer", k)
			}
		}
	}
	for _, k := range []string{"SHUTDOWN_GRACE_PERIOD", "MQTT_KEEP_ALIVE"} {
		if v := strings.TrimSpace(c.Get(k)); v != "" {
			d, e := time.ParseDuration(v)
			if e != nil || d <= 0 {
				return fmt.Errorf("%s must be a positive duration", k)
			}
		}
	}
	if v := c.Get("VALIDATION_LOCALE"); v != "" && v != "en" && v != "zh" {
		return fmt.Errorf("unsupported VALIDATION_LOCALE %q", v)
	}
	return nil
}
