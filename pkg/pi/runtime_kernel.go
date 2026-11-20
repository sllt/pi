package pi

import (
	"context"
	"errors"
	"io"
	"net/netip"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql" // Register MySQL driver for legacy database/sql usage.
	"go.opentelemetry.io/otel"
	metricNoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	traceNoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/sllt/pi/pkg/pi/config"
	"github.com/sllt/pi/pkg/pi/datasource"
	"github.com/sllt/pi/pkg/pi/datasource/file"
	"github.com/sllt/pi/pkg/pi/datasource/pubsub"
	"github.com/sllt/pi/pkg/pi/datasource/pubsub/google"
	"github.com/sllt/pi/pkg/pi/datasource/pubsub/kafka"
	"github.com/sllt/pi/pkg/pi/datasource/pubsub/mqtt"
	piRedis "github.com/sllt/pi/pkg/pi/datasource/redis"
	piSQL "github.com/sllt/pi/pkg/pi/datasource/sql"
	"github.com/sllt/pi/pkg/pi/http/middleware"
	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/sllt/pi/pkg/pi/logging/remotelogger"
	"github.com/sllt/pi/pkg/pi/metrics"
	"github.com/sllt/pi/pkg/pi/metrics/exporters"
	"github.com/sllt/pi/pkg/pi/service"
	"github.com/sllt/pi/pkg/pi/version"
	"github.com/sllt/pi/pkg/pi/websocket"
)

const (
	redisPubSubModeStreams = "streams"
	redisPubSubModePubSub  = "pubsub"
)

var (
	// ErrAdapterNotFound reports that no registered adapter satisfies the requested type or name.
	ErrAdapterNotFound = datasource.ErrAdapterNotFound
	// ErrAmbiguousAdapter reports that multiple registered adapters satisfy an unnamed lookup.
	ErrAmbiguousAdapter = datasource.ErrAmbiguousAdapter
	// ErrInvalidAdapterType reports an invalid type parameter (such as any) or type mismatch.
	ErrInvalidAdapterType = datasource.ErrInvalidAdapterType
	// ErrNilResource reports a nil or typed-nil resource/adapter/service passed to a registration API.
	ErrNilResource = errors.New("resource instance must not be nil")
	// ErrDuplicateResource reports a duplicate resource, adapter, or HTTP service name.
	ErrDuplicateResource = errors.New("duplicate resource or adapter name")
	// ErrReservedResourceName reports use of a runtime-reserved slot or health key name.
	ErrReservedResourceName = errors.New("resource name is reserved by Pi runtime")
	// ErrAppAlreadyStarted reports an attempt to mutate runtime resources after Start or route compilation.
	ErrAppAlreadyStarted = errors.New("cannot mutate runtime resources after app has started or routes are compiled")
)

func isNilValue(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return rv.IsNil()
	default:
		return false
	}
}

// runtimeKernel is the unexported runtime microkernel replacing pkg/pi/infra.Container.
// Lock hierarchy invariant (strictly unidirectional, never reversed):
//
//	App.runtimeMu -> kernel.mu -> adapters.mu / kernel.servicesMu -> healthState.mu
type runtimeKernel struct {
	appName    string
	appVersion string
	config     config.Config

	logger          logging.Logger
	loggerOwned     bool
	metricsManager  metrics.Manager
	tracerProvider  trace.TracerProvider
	hasCustomTracer bool
	isolated        bool

	// HTTP transport parameters
	validate           func(any) error
	explicitHTTPStatus bool
	maxBodyBytes       int64
	hasExplicitMaxBody bool
	trustedProxies     []netip.Prefix
	trustedProxyErr    error

	// Core slots & managed resource table (guarded by mu until frozen; unconfigured slots are strictly untyped nil)
	mu           sync.RWMutex
	frozen       atomic.Bool
	sql          piSQL.Client
	redis        piRedis.Client
	pubsub       pubsub.Client
	file         file.FileSystem
	explicitFile bool

	// Protocol & downstream HTTP service registry
	wsManager    *websocket.Manager
	servicesMu   sync.RWMutex
	httpServices map[string]service.HTTP

	resources []*managedResource
	names     map[string]string
	adapters  *adapterRegistry

	healthState *kernelHealthState
}

func newLegacyKernel(conf config.Config, customLogger logging.Logger, loggerOwned bool) *runtimeKernel {
	appName := "pi-app"
	appVersion := "dev"
	if conf != nil {
		appName = conf.GetOrDefault("APP_NAME", "pi-app")
		appVersion = conf.GetOrDefault("APP_VERSION", "dev")
	}

	k := &runtimeKernel{
		appName:      appName,
		appVersion:   appVersion,
		config:       conf,
		maxBodyBytes: 1 << 20,
		wsManager:    websocket.New(),
		httpServices: make(map[string]service.HTTP),
		names:        make(map[string]string),
		adapters:     newAdapterRegistry(),
		healthState:  newKernelHealthState(),
	}
	if conf != nil {
		if v := strings.TrimSpace(conf.Get("HTTP_MAX_BODY_BYTES")); v != "" {
			if parsed, err := strconv.ParseInt(v, 10, 64); err == nil && parsed > 0 {
				k.maxBodyBytes = parsed
				k.hasExplicitMaxBody = true
			}
		}
	}

	if !isNilValue(customLogger) {
		k.logger = customLogger
		k.loggerOwned = loggerOwned
	} else if conf != nil {
		levelFetchConfig, err := strconv.Atoi(conf.GetOrDefault("REMOTE_LOG_FETCH_INTERVAL", "15"))
		if err != nil {
			levelFetchConfig = 15
		}
		k.logger = remotelogger.New(
			logging.GetLevelFromString(conf.Get("LOG_LEVEL")),
			conf.Get("REMOTE_LOG_URL"),
			time.Duration(levelFetchConfig)*time.Second,
		)
		k.loggerOwned = true
		if err != nil {
			k.logger.Error("invalid value for REMOTE_LOG_FETCH_INTERVAL. setting default of 15 sec.")
		}
	} else {
		k.logger = logging.NewLogger(logging.INFO)
	}

	if conf != nil {
		if cc, ok := k.logger.(logging.CallerConfigurer); ok {
			showCaller := conf.GetOrDefault("LOG_CALLER", "true")
			cc.SetShowCaller(showCaller != "false")
		}
		if rawProxies := config.Lookup(conf, "TRUSTED_PROXIES").Strings(); len(rawProxies) > 0 {
			prefixes, err := middleware.ParseTrustedProxies(rawProxies)
			if err != nil {
				k.trustedProxyErr = err
				k.logger.Errorf("invalid TRUSTED_PROXIES configuration: %v", err)
			} else {
				k.trustedProxies = prefixes
			}
		}
	}

	k.logger.Debug("Runtime kernel is being created")

	if conf != nil {
		k.metricsManager = metrics.NewMetricsManager(exporters.Prometheus(k.appName, k.appVersion), k.logger)
	} else {
		k.metricsManager = metrics.NewMetricsManager(metricNoop.NewMeterProvider().Meter("pi"), k.logger)
	}
	k.registerFrameworkMetrics()
	k.metricsManager.SetGauge("app_info", 1,
		"app_name", k.appName, "app_version", k.appVersion, "framework_version", version.Framework)

	if conf != nil {
		rawRedis := piRedis.NewClient(conf, k.logger, k.metricsManager)
		if !isNilValue(rawRedis) {
			k.redis = rawRedis
		}

		rawSQL := piSQL.NewSQL(conf, k.logger, k.metricsManager)
		if !isNilValue(rawSQL) {
			k.sql = rawSQL
		}

		rawPubSub := k.createLegacyPubSub(conf)

		// Register owned pre-connected core resources in deterministic order: sql -> redis -> pubsub.
		if k.sql != nil {
			mr := bindManagedResource(k, "sql", "core", k.sql, Owned, true)
			k.resources = append(k.resources, mr)
			k.names["sql"] = "core"
			k.adapters.upsertCore("sql", k.sql, false)
		}
		if k.redis != nil {
			mr := bindManagedResource(k, "redis", "core", k.redis, Owned, true)
			k.resources = append(k.resources, mr)
			k.names["redis"] = "core"
			k.adapters.upsertCore("redis", k.redis, false)
		}
		if !isNilValue(rawPubSub) {
			k.pubsub = rawPubSub
			mr := bindManagedResource(k, "pubsub", "core", k.pubsub, Owned, true)
			k.resources = append(k.resources, mr)
			k.names["pubsub"] = "core"
			k.adapters.upsertCore("pubsub", k.pubsub, false)
		}
	}

	localFS := file.NewLocalFileSystem(k.logger)
	k.file = localFS
	k.explicitFile = false
	k.adapters.upsertCore("file", localFS, true)

	return k
}

func newIsolatedKernel(
	cfg config.Config,
	logger logging.Logger,
	m metrics.Manager,
	tp trace.TracerProvider,
) *runtimeKernel {
	if isNilValue(logger) {
		var lvlStr string
		if cfg != nil {
			lvlStr = cfg.Get("LOG_LEVEL")
		}
		logger = logging.NewLogger(logging.GetLevelFromString(lvlStr))
	}
	if isNilValue(m) {
		m = metrics.NewMetricsManager(metricNoop.NewMeterProvider().Meter("pi"), logger)
	}
	appName := "pi-app"
	appVersion := "dev"
	traceRatio := 1.0
	if cfg != nil {
		appName = cfg.GetOrDefault("APP_NAME", "pi-app")
		appVersion = cfg.GetOrDefault("APP_VERSION", "dev")
		if raw := strings.TrimSpace(cfg.Get("TRACER_RATIO")); raw != "" {
			if parsed, err := strconv.ParseFloat(raw, 64); err == nil {
				traceRatio = parsed
			}
		}
	}

	hasCustomTP := !isNilValue(tp)
	if !hasCustomTP {
		if traceRatio > 0 {
			tp = sdktrace.NewTracerProvider(
				sdktrace.WithResource(resource.NewWithAttributes(
					semconv.SchemaURL,
					semconv.ServiceNameKey.String(appName),
				)),
				sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(traceRatio))),
			)
		} else {
			tp = traceNoop.NewTracerProvider()
		}
	}

	k := &runtimeKernel{
		appName:         appName,
		appVersion:      appVersion,
		config:          cfg,
		logger:          logger,
		metricsManager:  m,
		tracerProvider:  tp,
		hasCustomTracer: hasCustomTP,
		isolated:        true,
		maxBodyBytes:    1 << 20,
		wsManager:       websocket.New(),
		httpServices:    make(map[string]service.HTTP),
		names:           make(map[string]string),
		adapters:        newAdapterRegistry(),
		healthState:     newKernelHealthState(),
	}

	localFS := file.NewLocalFileSystem(logger)
	k.file = localFS
	k.explicitFile = false
	k.adapters.upsertCore("file", localFS, true)

	k.registerFrameworkMetrics()
	return k
}

func (k *runtimeKernel) freeze() {
	if k == nil {
		return
	}
	if k.frozen.Load() {
		return
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.frozen.Load() {
		return
	}
	if k.adapters != nil {
		k.adapters.freeze()
	}
	k.frozen.Store(true)
}

func (k *runtimeKernel) getAppName() string {
	if k == nil {
		return ""
	}
	return k.appName
}

func (k *runtimeKernel) getAppVersion() string {
	if k == nil {
		return ""
	}
	return k.appVersion
}

func (k *runtimeKernel) getEnv(key string) config.Value {
	if k == nil {
		return ""
	}
	return config.Lookup(k.config, key)
}

func (k *runtimeKernel) getTrustedProxies() []netip.Prefix {
	if k == nil {
		return nil
	}
	if k.frozen.Load() {
		return k.trustedProxies
	}
	k.mu.RLock()
	prefixes := k.trustedProxies
	k.mu.RUnlock()
	return prefixes
}

func (k *runtimeKernel) getLogger() logging.Logger {
	if k != nil && !isNilValue(k.logger) {
		return k.logger
	}
	return logging.Noop()
}

func (k *runtimeKernel) getMetrics() metrics.Manager {
	if k == nil {
		return nil
	}
	return k.metricsManager
}

func (k *runtimeKernel) getTracerProvider() trace.TracerProvider {
	if k == nil {
		return otel.GetTracerProvider()
	}
	var tp trace.TracerProvider
	if k.frozen.Load() {
		tp = k.tracerProvider
	} else {
		k.mu.RLock()
		tp = k.tracerProvider
		k.mu.RUnlock()
	}
	if !isNilValue(tp) {
		return tp
	}
	if k.isolated {
		return traceNoop.NewTracerProvider()
	}
	return otel.GetTracerProvider()
}

func (k *runtimeKernel) setTracerProvider(tp trace.TracerProvider) {
	if k == nil || k.frozen.Load() {
		return
	}
	k.mu.Lock()
	if !k.frozen.Load() {
		k.tracerProvider = tp
		k.hasCustomTracer = !isNilValue(tp)
	}
	k.mu.Unlock()
}

func (k *runtimeKernel) shouldIsolateMetrics() bool {
	if k == nil {
		return false
	}
	if k.isolated {
		return true
	}
	if k.frozen.Load() {
		return k.hasCustomTracer
	}
	k.mu.RLock()
	hasCustom := k.hasCustomTracer
	k.mu.RUnlock()
	return hasCustom
}

func (k *runtimeKernel) getSQL() piSQL.Client {
	if k == nil {
		return nil
	}
	if k.frozen.Load() {
		return k.sql
	}
	k.mu.RLock()
	v := k.sql
	k.mu.RUnlock()
	return v
}

func (k *runtimeKernel) getRedis() piRedis.Client {
	if k == nil {
		return nil
	}
	if k.frozen.Load() {
		return k.redis
	}
	k.mu.RLock()
	v := k.redis
	k.mu.RUnlock()
	return v
}

func (k *runtimeKernel) getPubSub() pubsub.Client {
	if k == nil {
		return nil
	}
	if k.frozen.Load() {
		return k.pubsub
	}
	k.mu.RLock()
	v := k.pubsub
	k.mu.RUnlock()
	return v
}

func (k *runtimeKernel) getPublisher() pubsub.Publisher {
	if ps := k.getPubSub(); ps != nil {
		return ps
	}
	return nil
}

func (k *runtimeKernel) getSubscriber() pubsub.Subscriber {
	if ps := k.getPubSub(); ps != nil {
		return ps
	}
	return nil
}

func (k *runtimeKernel) getFile() file.FileSystem {
	if k == nil {
		return nil
	}
	if k.frozen.Load() {
		return k.file
	}
	k.mu.RLock()
	v := k.file
	k.mu.RUnlock()
	return v
}

func (k *runtimeKernel) getHTTPService(name string) service.HTTP {
	if k == nil || name == "" {
		return nil
	}
	k.servicesMu.RLock()
	svc := k.httpServices[name]
	k.servicesMu.RUnlock()
	return svc
}

func (k *runtimeKernel) getConnectionFromContext(ctx context.Context) *websocket.Connection {
	if k == nil || k.wsManager == nil || ctx == nil {
		return nil
	}
	if conn, ok := ctx.Value(websocket.WSConnectionKey).(*websocket.Connection); ok {
		return conn
	}
	if connID, ok := ctx.Value(websocket.WSConnectionKey).(string); ok {
		return k.wsManager.GetWebsocketConnection(connID)
	}
	return nil
}

func (k *runtimeKernel) getWSConnectionByServiceName(serviceName string) *websocket.Connection {
	if k == nil || k.wsManager == nil {
		return nil
	}
	return k.wsManager.GetConnectionByServiceName(serviceName)
}

func (k *runtimeKernel) addWSConnection(connID string, conn *websocket.Connection) {
	if k == nil || k.wsManager == nil {
		return
	}
	k.wsManager.AddWebsocketConnection(connID, conn)
}

func (k *runtimeKernel) removeWSConnection(connID string) {
	if k == nil || k.wsManager == nil {
		return
	}
	k.wsManager.CloseConnection(connID)
}

func (k *runtimeKernel) closeAllWSConnections(ctx context.Context) {
	if k == nil || k.wsManager == nil {
		return
	}
	for _, connID := range k.wsManager.ListConnections() {
		conn := k.wsManager.GetConnectionByServiceName(connID)
		k.wsManager.CloseConnection(connID)
		if conn != nil {
			_ = conn.WaitAsyncWrites(ctx)
		}
	}
}

func (k *runtimeKernel) closeOwnedLogger() error {
	if k == nil {
		return nil
	}
	k.mu.Lock()
	owned := k.loggerOwned
	k.loggerOwned = false
	logger := k.logger
	k.mu.Unlock()
	if !owned || isNilValue(logger) {
		return nil
	}
	if closer, ok := logger.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (k *runtimeKernel) registerFrameworkMetrics() {
	m := k.metricsManager
	if m == nil {
		return
	}
	if sm, ok := m.(metrics.StrictMock); ok && sm.IsStrictMock() {
		return
	}

	m.NewGauge("app_info", "Info for app_name, app_version and framework_version.")
	m.NewGauge("app_go_routines", "Number of Go routines running.")
	m.NewGauge("app_sys_memory_alloc", "Number of bytes allocated for heap objects.")
	m.NewGauge("app_sys_total_alloc", "Number of cumulative bytes allocated for heap objects.")
	m.NewGauge("app_go_numGC", "Number of completed Garbage Collector cycles.")
	m.NewGauge("app_go_sys", "Number of total bytes of memory.")

	httpBuckets := []float64{.001, .003, .005, .01, .02, .03, .05, .1, .2, .3, .5, .75, 1, 2, 3, 5, 10, 30}
	m.NewHistogram("app_http_response", "Response time of HTTP requests in seconds.", httpBuckets...)
	m.NewHistogram("app_http_service_response", "Response time of HTTP service requests in seconds.", httpBuckets...)
	m.NewCounter("app_http_retry_count", "Total number of retry events")
	m.NewGauge("app_http_circuit_breaker_state", "Current state of the circuit breaker (0 for Closed, 1 for Open)")

	dsBuckets := defaultDatasourceBuckets()
	m.NewHistogram("app_redis_stats", "Response time of Redis commands in milliseconds.", dsBuckets...)

	m.NewHistogram("app_sql_stats", "Response time of SQL queries in milliseconds.", dsBuckets...)
	m.NewGauge("app_sql_open_connections", "Number of open SQL connections.")
	m.NewGauge("app_sql_inUse_connections", "Number of inUse SQL connections.")

	m.NewCounter("app_pubsub_publish_total_count", "Number of total publish operations.")
	m.NewCounter("app_pubsub_publish_success_count", "Number of successful publish operations.")
	m.NewCounter("app_pubsub_subscribe_total_count", "Number of total subscribe operations.")
	m.NewCounter("app_pubsub_subscribe_success_count", "Number of successful subscribe operations.")
}

func defaultDatasourceBuckets() []float64 {
	return []float64{
		.05, .075, .1, .125, .15, .2, .3, .5, .75, 1, 2, 3, 5, 7.5, 10,
		25, 50, 100, 250, 500, 1000, 5000, 10000, 30000,
	}
}

func (k *runtimeKernel) createLegacyPubSub(conf config.Config) pubsub.Client {
	switch strings.ToUpper(conf.Get("PUBSUB_BACKEND")) {
	case "KAFKA":
		return k.createLegacyKafkaPubSub(conf)
	case "GOOGLE":
		return google.New(google.Config{
			ProjectID:        conf.Get("GOOGLE_PROJECT_ID"),
			SubscriptionName: conf.Get("GOOGLE_SUBSCRIPTION_NAME"),
		}, k.logger, k.metricsManager)
	case "MQTT":
		return k.createLegacyMQTTPubSub(conf)
	case "REDIS":
		k.warnIfRedisPubSubSharesRedisDB(conf)
		return piRedis.NewPubSub(conf, k.logger, k.metricsManager)
	default:
		return nil
	}
}

func (k *runtimeKernel) createLegacyKafkaPubSub(conf config.Config) pubsub.Client {
	if conf.Get("PUBSUB_BROKER") == "" {
		return nil
	}
	partition, _ := strconv.Atoi(conf.GetOrDefault("PARTITION_SIZE", "0"))
	offSet, _ := strconv.Atoi(conf.GetOrDefault("PUBSUB_OFFSET", "-1"))
	batchSize, _ := strconv.Atoi(conf.GetOrDefault("KAFKA_BATCH_SIZE", strconv.Itoa(kafka.DefaultBatchSize)))
	batchBytes, _ := strconv.Atoi(conf.GetOrDefault("KAFKA_BATCH_BYTES", strconv.Itoa(kafka.DefaultBatchBytes)))
	batchTimeout, _ := strconv.Atoi(conf.GetOrDefault("KAFKA_BATCH_TIMEOUT", strconv.Itoa(kafka.DefaultBatchTimeout)))

	tlsConf := kafka.TLSConfig{
		CertFile:           conf.Get("KAFKA_TLS_CERT_FILE"),
		KeyFile:            conf.Get("KAFKA_TLS_KEY_FILE"),
		CACertFile:         conf.Get("KAFKA_TLS_CA_CERT_FILE"),
		InsecureSkipVerify: conf.Get("KAFKA_TLS_INSECURE_SKIP_VERIFY") == "true",
	}

	return kafka.New(&kafka.Config{
		Brokers:          strings.Split(conf.Get("PUBSUB_BROKER"), ","),
		Partition:        partition,
		ConsumerGroupID:  conf.Get("CONSUMER_ID"),
		OffSet:           offSet,
		BatchSize:        batchSize,
		BatchBytes:       batchBytes,
		BatchTimeout:     batchTimeout,
		SecurityProtocol: conf.Get("KAFKA_SECURITY_PROTOCOL"),
		SASLMechanism:    conf.Get("KAFKA_SASL_MECHANISM"),
		SASLUser:         conf.Get("KAFKA_SASL_USERNAME"),
		SASLPassword:     conf.Get("KAFKA_SASL_PASSWORD"),
		TLS:              tlsConf,
	}, k.logger, k.metricsManager)
}

func (k *runtimeKernel) createLegacyMQTTPubSub(conf config.Config) pubsub.Client {
	var qos byte
	port, _ := strconv.Atoi(conf.Get("MQTT_PORT"))
	order, _ := strconv.ParseBool(conf.GetOrDefault("MQTT_MESSAGE_ORDER", "false"))
	retrieveRetained, _ := strconv.ParseBool(conf.GetOrDefault("MQTT_RETRIEVE_RETAINED", "false"))

	keepAlive, err := time.ParseDuration(conf.Get("MQTT_KEEP_ALIVE"))
	if err != nil {
		keepAlive = 30 * time.Second
		k.logger.Debug("MQTT_KEEP_ALIVE is not set or invalid, setting it to 30 seconds")
	}

	switch conf.Get("MQTT_QOS") {
	case "1":
		qos = 1
	case "2":
		qos = 2
	default:
		qos = 0
	}

	return mqtt.New(&mqtt.Config{
		Protocol:         conf.GetOrDefault("MQTT_PROTOCOL", "tcp"),
		Hostname:         conf.Get("MQTT_HOST"),
		Port:             port,
		Username:         conf.Get("MQTT_USER"),
		Password:         conf.Get("MQTT_PASSWORD"),
		ClientID:         conf.Get("MQTT_CLIENT_ID_SUFFIX"),
		QoS:              qos,
		Order:            order,
		RetrieveRetained: retrieveRetained,
		KeepAlive:        keepAlive,
		CloseTimeout:     0 * time.Millisecond,
	}, k.logger, k.metricsManager)
}

func (k *runtimeKernel) warnIfRedisPubSubSharesRedisDB(conf config.Config) {
	if isNilValue(k.redis) || effectiveRedisPubSubMode(conf) != redisPubSubModeStreams {
		return
	}
	redisDBStr := conf.Get("REDIS_DB")
	if redisDBStr == "" {
		redisDBStr = "0"
	}
	redisDB, err := strconv.Atoi(redisDBStr)
	if err != nil {
		redisDB = 0
	}
	pubsubDBStr := conf.Get("REDIS_PUBSUB_DB")
	if pubsubDBStr == "" {
		return
	}
	pubsubDB, err := strconv.Atoi(pubsubDBStr)
	if err != nil {
		return
	}
	if pubsubDB == redisDB {
		k.logger.Warnf(
			"REDIS_PUBSUB_DB (%d) is the same as REDIS_DB (%d); migrations may fail (kite_migrations HASH/STREAM). "+
				"Set REDIS_PUBSUB_DB to a different DB.",
			pubsubDB, redisDB,
		)
	}
}

func effectiveRedisPubSubMode(conf config.Config) string {
	if strings.ToLower(conf.Get("REDIS_PUBSUB_MODE")) == redisPubSubModePubSub {
		return redisPubSubModePubSub
	}
	return redisPubSubModeStreams
}
