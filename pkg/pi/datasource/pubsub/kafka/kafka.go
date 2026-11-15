// Package kafka provides a client for interacting with Apache Kafka message queues.This package facilitates interaction
// with Apache Kafka, allowing publishing and subscribing to topics, managing consumer groups, and handling messages.
package kafka

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/sllt/pi/pkg/pi/datasource/pubsub"
)

const (
	DefaultBatchSize       = 100
	DefaultBatchBytes      = 1048576
	DefaultBatchTimeout    = 1000
	defaultMaxBytes        = 10e6 // 10MB
	defaultMinBytes        = 10e3
	defaultRetryTimeout    = 10 * time.Second
	defaultReadTimeout     = 30 * time.Second
	protocolPlainText      = "PLAINTEXT"
	protocolSASL           = "SASL_PLAINTEXT"
	protocolSSL            = "SSL"
	protocolSASLSSL        = "SASL_SSL"
	messageMultipleBrokers = "MULTIPLE_BROKERS"
	brokerStatusUp         = "UP"
)

var errEmptyTopicName = errors.New("topic name cannot be empty")

type Config struct {
	Brokers          []string
	Partition        int
	ConsumerGroupID  string
	OffSet           int
	BatchSize        int
	BatchBytes       int
	BatchTimeout     int
	RetryTimeout     time.Duration
	SASLMechanism    string
	SASLUser         string
	SASLPassword     string
	SecurityProtocol string
	TLS              TLSConfig
}

type kafkaClient struct {
	dialer *kafka.Dialer
	conn   *multiConn

	writer Writer
	reader map[string]Reader

	mu        *sync.RWMutex
	cancel    context.CancelFunc
	retryDone chan struct{}

	logger  pubsub.Logger
	config  Config
	metrics Metrics
	tracer  trace.Tracer
}

func (k *kafkaClient) UseTracer(tracer any) {
	if k == nil {
		return
	}
	if t, ok := tracer.(trace.Tracer); ok && t != nil {
		if k.mu != nil {
			k.mu.Lock()
			k.tracer = t
			k.mu.Unlock()
		} else {
			k.tracer = t
		}
	}
}

func (k *kafkaClient) getTracer() trace.Tracer {
	if k != nil {
		var t trace.Tracer
		if k.mu != nil {
			k.mu.RLock()
			t = k.tracer
			k.mu.RUnlock()
		} else {
			t = k.tracer
		}
		if t != nil {
			return t
		}
	}
	return otel.GetTracerProvider().Tracer("pi")
}

func New(conf *Config, logger pubsub.Logger, metrics Metrics) *kafkaClient { //nolint:revive // New allows
	// returning unexported types as intended.
	err := validateConfigs(conf)
	if err != nil {
		logger.Errorf("could not initialize kafka, error: %v", err)

		return nil
	}

	if len(conf.Brokers) == 1 {
		logger.Debugf("connecting to Kafka broker: '%s'", conf.Brokers[0])
	} else {
		logger.Debugf("connecting to Kafka brokers: %v", conf.Brokers)
	}

	client := &kafkaClient{
		logger:  logger,
		config:  *conf,
		metrics: metrics,
		mu:      &sync.RWMutex{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	client.cancel = cancel

	err = client.initialize(ctx)
	if err != nil {
		logger.Errorf("failed to connect to kafka at %v, error: %v", conf.Brokers, err)

		retryDone := make(chan struct{})
		client.retryDone = retryDone
		go func() {
			defer close(retryDone)
			client.retryConnect(ctx)
		}()

		return client
	}

	return client
}

// ValidateConfig validates Kafka configuration purely in memory without opening sockets or reading TLS files.
func ValidateConfig(conf *Config) error {
	if conf == nil {
		return errBrokerNotProvided
	}
	cfgCopy := *conf
	return validateConfigs(&cfgCopy)
}

// OpenContext opens and verifies a Kafka client bounded by ctx without spawning retryConnect goroutines.
func OpenContext(ctx context.Context, conf *Config, logger pubsub.Logger, metrics Metrics, tp trace.TracerProvider) (pubsub.Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if conf == nil {
		return nil, errBrokerNotProvided
	}
	cfgCopy := *conf
	if err := validateConfigs(&cfgCopy); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = nopKafkaLogger{}
	}
	if metrics == nil {
		metrics = nopKafkaMetrics{}
	}
	var tracer trace.Tracer
	if tp != nil {
		tracer = tp.Tracer("pi")
	} else {
		tracer = noop.NewTracerProvider().Tracer("pi")
	}

	client := &kafkaClient{
		logger:  logger,
		config:  cfgCopy,
		metrics: metrics,
		mu:      &sync.RWMutex{},
		tracer:  tracer,
	}
	if err := client.initialize(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}
	return client, nil
}

type nopKafkaLogger struct{}

func (nopKafkaLogger) Debugf(string, ...any) {}
func (nopKafkaLogger) Debug(...any)          {}
func (nopKafkaLogger) Logf(string, ...any)   {}
func (nopKafkaLogger) Log(...any)            {}
func (nopKafkaLogger) Errorf(string, ...any) {}
func (nopKafkaLogger) Error(...any)          {}

type nopKafkaMetrics struct{}

func (nopKafkaMetrics) IncrementCounter(context.Context, string, ...string) {}

func (k *kafkaClient) Publish(ctx context.Context, topic string, message []byte) error {
	ctx, span := k.getTracer().Start(ctx, "kafka-publish")
	defer span.End()

	k.metrics.IncrementCounter(ctx, "app_pubsub_publish_total_count", "topic", topic)

	var writer Writer
	if k.mu != nil {
		k.mu.RLock()
		writer = k.writer
		k.mu.RUnlock()
	} else {
		writer = k.writer
	}
	if writer == nil || topic == "" {
		return errPublisherNotConfigured
	}

	start := time.Now()
	err := writer.WriteMessages(ctx,
		kafka.Message{
			Topic: topic,
			Value: message,
			Time:  time.Now(),
		},
	)
	end := time.Since(start)

	if err != nil {
		k.logger.Errorf("failed to publish message to kafka broker, error: %v", err)
		return err
	}

	var hostName string

	if len(k.config.Brokers) > 1 {
		hostName = messageMultipleBrokers
	} else {
		hostName = k.config.Brokers[0]
	}

	k.logger.Debug(&pubsub.Log{
		Mode:          "PUB",
		CorrelationID: span.SpanContext().TraceID().String(),
		MessageValue:  string(message),
		Topic:         topic,
		Host:          hostName,
		PubSubBackend: "KAFKA",
		Time:          end.Microseconds(),
	})

	k.metrics.IncrementCounter(ctx, "app_pubsub_publish_success_count", "topic", topic)

	return nil
}

func (k *kafkaClient) Query(ctx context.Context, query string, args ...any) ([]byte, error) {
	if !k.isConnected() {
		return nil, errClientNotConnected
	}

	if query == "" {
		return nil, errEmptyTopicName
	}

	offset, limit := k.parseQueryArgs(args...)

	reader, err := k.createReader(query, offset)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	readCtx := k.getReadContext(ctx)

	return k.readMessages(readCtx, reader, limit)
}

func (k *kafkaClient) Subscribe(ctx context.Context, topic string) (*pubsub.Message, error) {
	if !k.isConnected() {
		timer := time.NewTimer(defaultRetryTimeout)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, errors.Join(errClientNotConnected, ctx.Err())
		case <-timer.C:
			return nil, errClientNotConnected
		}
	}

	if k.config.ConsumerGroupID == "" {
		k.logger.Error("cannot subscribe as consumer_id is not provided in configs")

		return &pubsub.Message{}, ErrConsumerGroupNotProvided
	}

	ctx, span := k.getTracer().Start(ctx, "kafka-subscribe")
	defer span.End()

	k.metrics.IncrementCounter(ctx, "app_pubsub_subscribe_total_count", "topic", topic, "consumer_group", k.config.ConsumerGroupID)

	var reader Reader
	if k.mu != nil {
		k.mu.Lock()
	}

	if k.reader == nil {
		k.reader = make(map[string]Reader)
	}

	if k.reader[topic] == nil {
		k.reader[topic] = k.getNewReader(topic)
	}
	reader = k.reader[topic]

	if k.mu != nil {
		k.mu.Unlock()
	}

	start := time.Now()

	// Read a single message from the topic
	msg, err := reader.FetchMessage(ctx)
	if err != nil {
		k.logger.Errorf("failed to read message from kafka topic %s: %v", topic, err)

		return nil, err
	}

	m := pubsub.NewMessage(ctx)
	m.Value = msg.Value
	m.Topic = topic
	m.Committer = newKafkaMessage(&msg, reader, k.logger)

	end := time.Since(start)

	var hostName string

	if len(k.config.Brokers) > 1 {
		hostName = "multiple brokers"
	} else {
		hostName = k.config.Brokers[0]
	}

	k.logger.Debug(&pubsub.Log{
		Mode:          "SUB",
		CorrelationID: span.SpanContext().TraceID().String(),
		MessageValue:  string(msg.Value),
		Topic:         topic,
		Host:          hostName,
		PubSubBackend: "KAFKA",
		Time:          end.Microseconds(),
	})

	k.metrics.IncrementCounter(ctx, "app_pubsub_subscribe_success_count", "topic", topic, "consumer_group", k.config.ConsumerGroupID)

	return m, err
}

func (k *kafkaClient) Close() (err error) {
	if k.cancel != nil {
		k.cancel()
	}
	if k.retryDone != nil {
		<-k.retryDone
	}

	var (
		readers []Reader
		writer  Writer
		conn    *multiConn
	)
	if k.mu != nil {
		k.mu.Lock()
		for _, r := range k.reader {
			readers = append(readers, r)
		}
		k.reader = nil
		writer = k.writer
		conn = k.conn
		k.mu.Unlock()
	} else {
		for _, r := range k.reader {
			readers = append(readers, r)
		}
		writer = k.writer
		conn = k.conn
	}

	for _, r := range readers {
		if r != nil {
			err = errors.Join(err, r.Close())
		}
	}

	if writer != nil {
		err = errors.Join(err, writer.Close())
	}

	if conn != nil {
		err = errors.Join(err, conn.Close())
	}

	return err
}

func (*kafkaClient) AckSemantics() pubsub.AckSemantics {
	return pubsub.AckSemantics{
		Mode:                  pubsub.AckModeCumulativeOffset,
		SupportsContextCommit: true,
		SupportsNack:          false,
		SupportsSeek:          false,
	}
}
