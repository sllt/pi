package pi

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/sllt/pi/pkg/pi/config"
	"github.com/sllt/pi/pkg/pi/datasource"
	"github.com/sllt/pi/pkg/pi/datasource/pubsub"
	"github.com/sllt/pi/pkg/pi/datasource/pubsub/google"
	"github.com/sllt/pi/pkg/pi/datasource/pubsub/kafka"
	"github.com/sllt/pi/pkg/pi/datasource/pubsub/mqtt"
	piRedis "github.com/sllt/pi/pkg/pi/datasource/redis"
	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/sllt/pi/pkg/pi/metrics"
)

var (
	errPubSubNotStarted = errors.New("pubsub: managed client has not started")
	errPubSubClosed     = errors.New("pubsub: managed client is closed")
)

type managedPubSubHandle struct {
	mu             sync.RWMutex
	real           pubsub.Client
	closed         bool
	backend        string
	ackSemantics   pubsub.AckSemantics
	config         config.Config
	logger         logging.Logger
	metrics        metrics.Manager
	tracerProvider trace.TracerProvider
	kernel         *runtimeKernel
}

func newManagedPubSubHandle(
	cfg config.Config,
	logger logging.Logger,
	m metrics.Manager,
	tp trace.TracerProvider,
) (*managedPubSubHandle, error) {
	if err := validatePubSubBaseConfig(cfg); err != nil {
		return nil, err
	}
	backend := strings.ToUpper(strings.TrimSpace(cfg.Get("PUBSUB_BACKEND")))
	return &managedPubSubHandle{
		backend:        backend,
		ackSemantics:   staticAckSemanticsForConfig(backend, cfg),
		config:         cfg,
		logger:         logger,
		metrics:        m,
		tracerProvider: tp,
	}, nil
}

func staticAckSemanticsForConfig(backend string, cfg config.Config) pubsub.AckSemantics {
	switch backend {
	case "KAFKA":
		return pubsub.AckSemantics{
			Mode:                  pubsub.AckModeCumulativeOffset,
			SupportsContextCommit: true,
		}
	case "GOOGLE":
		return pubsub.AckSemantics{
			Mode:         pubsub.AckModeIndependent,
			SupportsNack: true,
		}
	case "REDIS":
		if effectiveRedisPubSubMode(cfg) == redisPubSubModePubSub {
			return pubsub.AckSemantics{
				Mode: pubsub.AckModeAuto,
			}
		}
		return pubsub.AckSemantics{
			Mode:                  pubsub.AckModeIndependent,
			SupportsContextCommit: true,
		}
	case "MQTT":
		return pubsub.AckSemantics{
			Mode: pubsub.AckModeAuto,
		}
	default:
		return pubsub.AckSemantics{}
	}
}

func validatePubSubBaseConfig(cfg config.Config) error {
	if cfg == nil {
		return errors.New("PUBSUB_BACKEND is required for managed PubSub")
	}
	backend := strings.ToUpper(strings.TrimSpace(cfg.Get("PUBSUB_BACKEND")))
	if backend == "" {
		return errors.New("PUBSUB_BACKEND is required for managed PubSub")
	}

	switch backend {
	case "KAFKA":
		return validateKafkaPubSubBaseConfig(cfg)
	case "GOOGLE":
		if strings.TrimSpace(cfg.Get("GOOGLE_PROJECT_ID")) == "" {
			return errors.New("GOOGLE_PROJECT_ID is required for managed GOOGLE PubSub")
		}
		return nil
	case "MQTT":
		return validateMQTTPubSubBaseConfig(cfg)
	case "REDIS":
		return validateRedisPubSubBaseConfig(cfg)
	default:
		return fmt.Errorf("unsupported PUBSUB_BACKEND %q", backend)
	}
}

func validateKafkaPubSubBaseConfig(cfg config.Config) error {
	rawBroker := strings.TrimSpace(cfg.Get("PUBSUB_BROKER"))
	if rawBroker == "" {
		return errors.New("PUBSUB_BROKER is required for managed KAFKA PubSub")
	}
	hasBroker := false
	for _, b := range strings.Split(rawBroker, ",") {
		if strings.TrimSpace(b) != "" {
			hasBroker = true
			break
		}
	}
	if !hasBroker {
		return errors.New("PUBSUB_BROKER must contain at least one non-empty broker address")
	}

	if v := strings.TrimSpace(cfg.Get("PARTITION_SIZE")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return errors.New("PARTITION_SIZE must be a nonnegative integer")
		}
	}
	if v := strings.TrimSpace(cfg.Get("PUBSUB_OFFSET")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < -2 {
			return errors.New("PUBSUB_OFFSET must be an integer >= -2")
		}
	}
	for _, k := range []string{"KAFKA_BATCH_SIZE", "KAFKA_BATCH_BYTES", "KAFKA_BATCH_TIMEOUT"} {
		if v := strings.TrimSpace(cfg.Get(k)); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n <= 0 {
				return fmt.Errorf("%s must be a positive integer", k)
			}
		}
	}

	certFile := strings.TrimSpace(cfg.Get("KAFKA_TLS_CERT_FILE"))
	keyFile := strings.TrimSpace(cfg.Get("KAFKA_TLS_KEY_FILE"))
	if (certFile != "") != (keyFile != "") {
		return errors.New("KAFKA_TLS_CERT_FILE and KAFKA_TLS_KEY_FILE must both be set or both be empty")
	}
	if v := strings.TrimSpace(cfg.Get("KAFKA_TLS_INSECURE_SKIP_VERIFY")); v != "" {
		if _, err := strconv.ParseBool(v); err != nil {
			return errors.New("KAFKA_TLS_INSECURE_SKIP_VERIFY must be a boolean")
		}
	}

	secProto := strings.ToUpper(strings.TrimSpace(cfg.Get("KAFKA_SECURITY_PROTOCOL")))
	switch secProto {
	case "", "PLAINTEXT", "SSL":
	case "SASL_PLAINTEXT", "SASL_SSL":
		mech := strings.ToUpper(strings.TrimSpace(cfg.Get("KAFKA_SASL_MECHANISM")))
		switch mech {
		case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
		default:
			return fmt.Errorf("unsupported or missing KAFKA_SASL_MECHANISM %q for %s", cfg.Get("KAFKA_SASL_MECHANISM"), secProto)
		}
		if strings.TrimSpace(cfg.Get("KAFKA_SASL_USERNAME")) == "" || cfg.Get("KAFKA_SASL_PASSWORD") == "" {
			return fmt.Errorf("KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required for %s", secProto)
		}
	default:
		return fmt.Errorf("unsupported KAFKA_SECURITY_PROTOCOL %q", cfg.Get("KAFKA_SECURITY_PROTOCOL"))
	}

	return nil
}

func validateMQTTPubSubBaseConfig(cfg config.Config) error {
	if strings.TrimSpace(cfg.Get("MQTT_HOST")) == "" {
		return errors.New("MQTT_HOST is required for managed MQTT PubSub")
	}
	if v := strings.TrimSpace(cfg.Get("MQTT_PORT")); v != "" {
		port, err := strconv.Atoi(v)
		if err != nil || port < 1 || port > 65535 {
			return errors.New("MQTT_PORT must be in 1..65535")
		}
	}
	if v := strings.TrimSpace(cfg.Get("MQTT_QOS")); v != "" {
		switch v {
		case "0", "1", "2":
		default:
			return errors.New("MQTT_QOS must be 0, 1, or 2")
		}
	}
	for _, k := range []string{"MQTT_MESSAGE_ORDER", "MQTT_RETRIEVE_RETAINED"} {
		if v := strings.TrimSpace(cfg.Get(k)); v != "" {
			if _, err := strconv.ParseBool(v); err != nil {
				return fmt.Errorf("%s must be a boolean", k)
			}
		}
	}
	if v := strings.TrimSpace(cfg.Get("MQTT_KEEP_ALIVE")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return errors.New("MQTT_KEEP_ALIVE must be a positive duration")
		}
	}
	return nil
}

func validateRedisPubSubBaseConfig(cfg config.Config) error {
	if err := piRedis.ValidateConfig(cfg); err != nil {
		return err
	}
	if v := strings.TrimSpace(cfg.Get("REDIS_PUBSUB_DB")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return errors.New("REDIS_PUBSUB_DB must be a nonnegative integer")
		}
	}
	if mode := strings.ToLower(strings.TrimSpace(cfg.Get("REDIS_PUBSUB_MODE"))); mode != "" {
		if mode != redisPubSubModeStreams && mode != redisPubSubModePubSub {
			return fmt.Errorf("unsupported REDIS_PUBSUB_MODE %q (expected streams or pubsub)", cfg.Get("REDIS_PUBSUB_MODE"))
		}
	}
	for _, k := range []string{"REDIS_STREAMS_MAXLEN", "REDIS_PUBSUB_QUERY_LIMIT", "REDIS_PUBSUB_SUBSCRIBE_LIMIT"} {
		if v := strings.TrimSpace(cfg.Get(k)); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 {
				return fmt.Errorf("%s must be a nonnegative integer", k)
			}
		}
	}
	if v := strings.TrimSpace(cfg.Get("REDIS_PUBSUB_BUFFER_SIZE")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return errors.New("REDIS_PUBSUB_BUFFER_SIZE must be a positive integer")
		}
	}
	if v := strings.TrimSpace(cfg.Get("REDIS_STREAMS_PEL_RATIO")); v != "" {
		r, err := strconv.ParseFloat(v, 64)
		if err != nil || r < 0 || r > 1 {
			return errors.New("REDIS_STREAMS_PEL_RATIO must be a float in 0..1")
		}
	}
	return nil
}

func (h *managedPubSubHandle) validateSubscriberConfig() error {
	if h == nil || h.config == nil {
		return nil
	}
	switch h.backend {
	case "KAFKA":
		if strings.TrimSpace(h.config.Get("CONSUMER_ID")) == "" {
			return errors.New("CONSUMER_ID is required when registering subscribers with managed KAFKA PubSub")
		}
	case "GOOGLE":
		if strings.TrimSpace(h.config.Get("GOOGLE_SUBSCRIPTION_NAME")) == "" {
			return errors.New("GOOGLE_SUBSCRIPTION_NAME is required when registering subscribers with managed GOOGLE PubSub")
		}
	case "REDIS":
		if effectiveRedisPubSubMode(h.config) == redisPubSubModeStreams &&
			strings.TrimSpace(h.config.Get("REDIS_STREAMS_CONSUMER_GROUP")) == "" {
			return errors.New("REDIS_STREAMS_CONSUMER_GROUP is required when registering subscribers with managed REDIS Streams PubSub")
		}
	}
	return nil
}

func (h *managedPubSubHandle) AckSemantics() pubsub.AckSemantics {
	if h == nil {
		return pubsub.AckSemantics{}
	}
	h.mu.RLock()
	realClient := h.real
	sem := h.ackSemantics
	h.mu.RUnlock()

	if realClient != nil {
		if p, ok := realClient.(pubsub.AckSemanticsProvider); ok {
			return p.AckSemantics()
		}
	}
	return sem
}

func (h *managedPubSubHandle) IsStarted() bool {
	if h == nil {
		return false
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return !h.closed && h.real != nil
}

func (h *managedPubSubHandle) UseLogger(v any) {
	if h == nil {
		return
	}
	l, ok := v.(logging.Logger)
	if !ok || l == nil {
		return
	}
	h.mu.Lock()
	h.logger = l
	h.mu.Unlock()
}

func (h *managedPubSubHandle) UseMetrics(v any) {
	if h == nil {
		return
	}
	m, ok := v.(metrics.Manager)
	if !ok || m == nil {
		return
	}
	h.mu.Lock()
	h.metrics = m
	h.mu.Unlock()
}

func (h *managedPubSubHandle) UseTracerProvider(tp trace.TracerProvider) {
	if h == nil || tp == nil {
		return
	}
	h.mu.Lock()
	h.tracerProvider = tp
	h.mu.Unlock()
}

func (h *managedPubSubHandle) Start(ctx context.Context) error {
	if h == nil {
		return errPubSubClosed
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return errPubSubClosed
	}
	if h.real != nil {
		h.mu.Unlock()
		return nil
	}
	conf := h.config
	backend := h.backend
	logger := h.logger
	m := h.metrics
	tp := h.tracerProvider
	kernel := h.kernel
	h.mu.Unlock()

	client, err := openPubSubBackendContext(ctx, backend, conf, logger, m, tp, kernel)
	if err != nil {
		return err
	}

	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		_ = client.Close()
		return errPubSubClosed
	}
	if h.real != nil {
		h.mu.Unlock()
		_ = client.Close()
		return nil
	}
	h.real = client
	h.mu.Unlock()
	return nil
}

func openPubSubBackendContext(
	ctx context.Context,
	backend string,
	conf config.Config,
	logger logging.Logger,
	m metrics.Manager,
	tp trace.TracerProvider,
	kernel *runtimeKernel,
) (pubsub.Client, error) {
	switch backend {
	case "KAFKA":
		partition, _ := strconv.Atoi(conf.GetOrDefault("PARTITION_SIZE", "0"))
		offSet, _ := strconv.Atoi(conf.GetOrDefault("PUBSUB_OFFSET", "-1"))
		batchSize, _ := strconv.Atoi(conf.GetOrDefault("KAFKA_BATCH_SIZE", strconv.Itoa(kafka.DefaultBatchSize)))
		batchBytes, _ := strconv.Atoi(conf.GetOrDefault("KAFKA_BATCH_BYTES", strconv.Itoa(kafka.DefaultBatchBytes)))
		batchTimeout, _ := strconv.Atoi(conf.GetOrDefault("KAFKA_BATCH_TIMEOUT", strconv.Itoa(kafka.DefaultBatchTimeout)))

		var brokers []string
		for _, b := range strings.Split(conf.Get("PUBSUB_BROKER"), ",") {
			if trimmed := strings.TrimSpace(b); trimmed != "" {
				brokers = append(brokers, trimmed)
			}
		}

		kcfg := &kafka.Config{
			Brokers:          brokers,
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
			TLS: kafka.TLSConfig{
				CertFile:           conf.Get("KAFKA_TLS_CERT_FILE"),
				KeyFile:            conf.Get("KAFKA_TLS_KEY_FILE"),
				CACertFile:         conf.Get("KAFKA_TLS_CA_CERT_FILE"),
				InsecureSkipVerify: conf.Get("KAFKA_TLS_INSECURE_SKIP_VERIFY") == "true",
			},
		}
		return kafka.OpenContext(ctx, kcfg, logger, m, tp)

	case "GOOGLE":
		return google.OpenContext(ctx, google.Config{
			ProjectID:        conf.Get("GOOGLE_PROJECT_ID"),
			SubscriptionName: conf.Get("GOOGLE_SUBSCRIPTION_NAME"),
		}, logger, m, tp)

	case "MQTT":
		port, _ := strconv.Atoi(conf.GetOrDefault("MQTT_PORT", "1883"))
		order, _ := strconv.ParseBool(conf.GetOrDefault("MQTT_MESSAGE_ORDER", "false"))
		retrieveRetained, _ := strconv.ParseBool(conf.GetOrDefault("MQTT_RETRIEVE_RETAINED", "false"))
		keepAlive, err := time.ParseDuration(conf.Get("MQTT_KEEP_ALIVE"))
		if err != nil || keepAlive <= 0 {
			keepAlive = 30 * time.Second
		}
		var qos byte
		switch conf.Get("MQTT_QOS") {
		case "1":
			qos = 1
		case "2":
			qos = 2
		default:
			qos = 0
		}
		mcfg := &mqtt.Config{
			Protocol:         conf.GetOrDefault("MQTT_PROTOCOL", "tcp"),
			Hostname:         strings.TrimSpace(conf.Get("MQTT_HOST")),
			Port:             port,
			Username:         conf.Get("MQTT_USER"),
			Password:         conf.Get("MQTT_PASSWORD"),
			ClientID:         conf.Get("MQTT_CLIENT_ID_SUFFIX"),
			QoS:              qos,
			Order:            order,
			RetrieveRetained: retrieveRetained,
			KeepAlive:        keepAlive,
			CloseTimeout:     0,
		}
		return mqtt.OpenContext(ctx, mcfg, logger, m, tp)

	case "REDIS":
		if kernel != nil {
			kernel.warnIfRedisPubSubSharesRedisDB(conf)
		}
		return piRedis.OpenPubSubContext(ctx, conf, logger, m, tp)

	default:
		return nil, fmt.Errorf("unsupported PUBSUB_BACKEND %q", backend)
	}
}

func (h *managedPubSubHandle) ready() (pubsub.Client, error) {
	if h == nil {
		return nil, errPubSubClosed
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.closed {
		return nil, errPubSubClosed
	}
	if h.real == nil {
		return nil, errPubSubNotStarted
	}
	return h.real, nil
}

func (h *managedPubSubHandle) Publish(ctx context.Context, topic string, message []byte) error {
	c, err := h.ready()
	if err != nil {
		return err
	}
	return c.Publish(ctx, topic, message)
}

func (h *managedPubSubHandle) Subscribe(ctx context.Context, topic string) (*pubsub.Message, error) {
	c, err := h.ready()
	if err != nil {
		return nil, err
	}
	return c.Subscribe(ctx, topic)
}

func (h *managedPubSubHandle) CreateTopic(ctx context.Context, name string) error {
	c, err := h.ready()
	if err != nil {
		return err
	}
	return c.CreateTopic(ctx, name)
}

func (h *managedPubSubHandle) DeleteTopic(ctx context.Context, name string) error {
	c, err := h.ready()
	if err != nil {
		return err
	}
	return c.DeleteTopic(ctx, name)
}

func (h *managedPubSubHandle) Query(ctx context.Context, query string, args ...any) ([]byte, error) {
	c, err := h.ready()
	if err != nil {
		return nil, err
	}
	return c.Query(ctx, query, args...)
}

func (h *managedPubSubHandle) Health() datasource.Health {
	return h.HealthContext(context.Background())
}

func (h *managedPubSubHandle) HealthContext(ctx context.Context) datasource.Health {
	c, err := h.ready()
	if err != nil {
		return datasource.Health{
			Status:  datasource.StatusDown,
			Details: map[string]any{"error": err.Error()},
		}
	}
	if ctxChecker, ok := c.(interface {
		HealthContext(context.Context) datasource.Health
	}); ok {
		return ctxChecker.HealthContext(ctx)
	}
	if ctxChecker, ok := c.(interface {
		HealthCheckContext(context.Context) datasource.Health
	}); ok {
		return ctxChecker.HealthCheckContext(ctx)
	}
	return c.Health()
}

func (h *managedPubSubHandle) Close() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	realClient := h.real
	h.real = nil
	h.mu.Unlock()
	if realClient != nil {
		return realClient.Close()
	}
	return nil
}
