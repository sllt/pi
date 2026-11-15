package nats

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var (
	errStatusDown         = errors.New("status down")
	errKeyNotFound        = errors.New("key not found")
	errInvalidConfig      = errors.New("nats-kv: server and bucket are required")
	errClientNotConnected = errors.New("nats-kv: client not connected")
)

func redactServerURL(raw string) string {
	if !strings.Contains(raw, "@") {
		return raw
	}
	parts := strings.Split(raw, ",")
	for i, p := range parts {
		trimmed := strings.TrimSpace(p)
		if u, err := url.Parse(trimmed); err == nil && u.User != nil {
			u.User = nil
			parts[i] = u.String()
		}
	}
	return strings.Join(parts, ",")
}

type Configs struct {
	Server string
	Bucket string
}

type jetStream struct {
	nats.JetStreamContext
}

func (j jetStream) AccountInfo() (*nats.AccountInfo, error) {
	return j.JetStreamContext.AccountInfo()
}

func (j jetStream) AccountInfoContext(ctx context.Context) (*nats.AccountInfo, error) {
	return j.JetStreamContext.AccountInfo(nats.Context(ctx))
}

type Client struct {
	mu      sync.RWMutex
	conn    *nats.Conn
	js      JetStream
	kv      nats.KeyValue
	configs *Configs
	tracer  trace.Tracer
	metrics Metrics
	logger  Logger
}

// New creates a new NATS-KV client with the provided configuration.
func New(configs Configs) *Client {
	return &Client{configs: &configs}
}

// UseLogger sets the logger for the NATS-KV client which asserts the Logger interface.
func (c *Client) UseLogger(logger any) {
	if l, ok := logger.(Logger); ok {
		c.logger = l
	}
}

// UseMetrics sets the metrics for the NATS-KV client which asserts the Metrics interface.
func (c *Client) UseMetrics(metrics any) {
	if m, ok := metrics.(Metrics); ok {
		c.metrics = m
	}
}

// UseTracer sets the tracer for NATS-KV client.
func (c *Client) UseTracer(tracer any) {
	if t, ok := tracer.(trace.Tracer); ok {
		c.tracer = t
	}
}

// FromEnv constructs an unstarted NATS-KV client from an environment lookup function.
func FromEnv(get func(string) string) (*Client, error) {
	if get == nil {
		return nil, errInvalidConfig
	}

	firstNonEmpty := func(keys ...string) string {
		for _, k := range keys {
			if v := strings.TrimSpace(get(k)); v != "" {
				return v
			}
		}
		return ""
	}

	server := firstNonEmpty("NATS_KV_SERVER", "NATS_SERVER")
	bucket := firstNonEmpty("NATS_KV_BUCKET")
	if server == "" || bucket == "" {
		return nil, errInvalidConfig
	}

	return New(Configs{Server: server, Bucket: bucket}), nil
}

// ConnectContext dials NATS, initializes JetStream and KV bucket within ctx, and closes the connection on error.
func (c *Client) ConnectContext(ctx context.Context) error {
	if c == nil || c.configs == nil || strings.TrimSpace(c.configs.Server) == "" || strings.TrimSpace(c.configs.Bucket) == "" {
		return errInvalidConfig
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.RLock()
	alreadyConnected := c.conn != nil && c.js != nil && c.kv != nil
	c.mu.RUnlock()
	if alreadyConnected {
		return nil
	}

	safeServer := redactServerURL(c.configs.Server)
	if c.logger != nil {
		c.logger.Debugf("connecting to NATS-KV Store at %v with bucket %q", safeServer, c.configs.Bucket)
	}

	if c.metrics != nil {
		natsBuckets := []float64{.05, .075, .1, .125, .15, .2, .3, .5, .75, 1, 2, 3, 4, 5, 7.5, 10}
		c.metrics.NewHistogram("app_nats_kv_stats", "Response time of NATS KV operations in milliseconds.", natsBuckets...)
	}

	var opts []nats.Option
	if dl, ok := ctx.Deadline(); ok {
		rem := time.Until(dl)
		if rem <= 0 {
			return ctx.Err()
		}
		opts = append(opts, nats.Timeout(rem))
	}

	nc, err := nats.Connect(c.configs.Server, opts...)
	if err != nil {
		return err
	}

	setupJS, err := nc.JetStream(nats.Context(ctx))
	if err != nil {
		nc.Close()
		return err
	}

	if _, err := setupJS.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: c.configs.Bucket,
	}); err != nil {
		nc.Close()
		return err
	}

	runtimeJS, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return err
	}

	kv, err := runtimeJS.KeyValue(c.configs.Bucket)
	if err != nil {
		nc.Close()
		return err
	}

	c.mu.Lock()
	if c.conn != nil && c.js != nil && c.kv != nil {
		c.mu.Unlock()
		nc.Close()
		return nil
	}
	c.conn = nc
	c.js = jetStream{runtimeJS}
	c.kv = kv
	c.mu.Unlock()

	if c.logger != nil {
		c.logger.Infof("successfully connected to NATS-KV Store at %s:%s ", safeServer, c.configs.Bucket)
	}

	return nil
}

// Close closes the underlying NATS connection idempotently.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	conn := c.conn
	c.conn = nil
	c.js = nil
	c.kv = nil
	c.mu.Unlock()

	if conn != nil {
		conn.Close()
	}
	return nil
}

// Connect establishes a connection to NATS-KV and registers metrics using the provided configuration when the client is created.
func (c *Client) Connect() {
	if c == nil || c.configs == nil {
		return
	}
	safeServer := redactServerURL(c.configs.Server)
	if c.logger != nil {
		c.logger.Debugf("connecting to NATS-KV Store at %v with bucket %q", safeServer, c.configs.Bucket)
	}

	if c.metrics != nil {
		natsBuckets := []float64{.05, .075, .1, .125, .15, .2, .3, .5, .75, 1, 2, 3, 4, 5, 7.5, 10}
		c.metrics.NewHistogram("app_nats_kv_stats", "Response time of NATS KV operations in milliseconds.", natsBuckets...)
	}

	nc, err := nats.Connect(c.configs.Server)
	if err != nil {
		if c.logger != nil {
			c.logger.Errorf("error while connecting to NATS: %v", err)
		}
		return
	}

	if c.logger != nil {
		c.logger.Debug("connection to NATS successful")
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		if c.logger != nil {
			c.logger.Errorf("error while initializing JetStream: %v", err)
		}
		return
	}

	if c.logger != nil {
		c.logger.Debug("jetStream initialized successfully")
	}

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: c.configs.Bucket,
	})
	if err != nil {
		nc.Close()
		if c.logger != nil {
			c.logger.Errorf("error while creating/accessing KV bucket: %v", err)
		}
		return
	}

	c.mu.Lock()
	if c.conn != nil {
		c.conn.Close()
	}
	c.conn = nc
	c.js = jetStream{js}
	c.kv = kv
	c.mu.Unlock()

	if c.logger != nil {
		c.logger.Infof("successfully connected to NATS-KV Store at %s:%s ", safeServer, c.configs.Bucket)
	}
}

func (c *Client) Get(ctx context.Context, key string) (string, error) {
	if c == nil {
		return "", errClientNotConnected
	}
	c.mu.RLock()
	kv := c.kv
	c.mu.RUnlock()
	if kv == nil {
		return "", errClientNotConnected
	}

	span := c.addTrace(ctx, "get", key)
	defer c.sendOperationStats(time.Now(), "GET", "get", span, key)

	entry, err := kv.Get(key)
	if err != nil {
		if errors.Is(err, nats.ErrKeyNotFound) {
			return "", fmt.Errorf("%w: %s", errKeyNotFound, key)
		}

		return "", fmt.Errorf("failed to get key: %w", err)
	}

	return string(entry.Value()), nil
}

func (c *Client) Set(ctx context.Context, key, value string) error {
	if c == nil {
		return errClientNotConnected
	}
	c.mu.RLock()
	kv := c.kv
	c.mu.RUnlock()
	if kv == nil {
		return errClientNotConnected
	}

	span := c.addTrace(ctx, "set", key)
	defer c.sendOperationStats(time.Now(), "SET", "set", span, key, value)

	_, err := kv.Put(key, []byte(value))
	if err != nil {
		return fmt.Errorf("failed to set key-value pair: %w", err)
	}

	return nil
}

func (c *Client) Delete(ctx context.Context, key string) error {
	if c == nil {
		return errClientNotConnected
	}
	c.mu.RLock()
	kv := c.kv
	c.mu.RUnlock()
	if kv == nil {
		return errClientNotConnected
	}

	span := c.addTrace(ctx, "delete", key)
	defer c.sendOperationStats(time.Now(), "DELETE", "delete", span, key)

	err := kv.Delete(key)
	if err != nil {
		if errors.Is(err, nats.ErrKeyNotFound) {
			return fmt.Errorf("%w: %s", errKeyNotFound, key)
		}

		return fmt.Errorf("failed to delete key: %w", err)
	}

	return nil
}

type Health struct {
	Status  string         `json:"status,omitempty"`
	Details map[string]any `json:"details,omitempty"`
}

func (c *Client) HealthCheck(ctx context.Context) (any, error) {
	if c == nil || c.configs == nil {
		return &Health{
			Status:  "DOWN",
			Details: map[string]any{"message": "nats-kv not configured"},
		}, errStatusDown
	}
	if ctx == nil {
		ctx = context.Background()
	}

	start := time.Now()
	span := c.addTrace(ctx, "healthcheck", c.configs.Bucket)

	safeServer := redactServerURL(c.configs.Server)
	h := &Health{
		Details: make(map[string]any),
	}

	h.Details["url"] = safeServer
	h.Details["bucket"] = c.configs.Bucket

	c.mu.RLock()
	js := c.js
	c.mu.RUnlock()

	if js == nil {
		h.Status = "DOWN"
		h.Details["message"] = "nats-kv not connected"
		if span != nil {
			span.End()
		}
		return h, errStatusDown
	}

	var err error
	if infoCtx, ok := js.(interface {
		AccountInfoContext(context.Context) (*nats.AccountInfo, error)
	}); ok {
		_, err = infoCtx.AccountInfoContext(ctx)
	} else {
		_, err = js.AccountInfo()
	}
	if err != nil {
		h.Status = "DOWN"

		if c.logger != nil {
			c.logger.Debug(&Log{
				Type:     "HEALTH CHECK",
				Key:      "health",
				Value:    fmt.Sprintf("Connection failed for bucket '%s' at '%s'", c.configs.Bucket, safeServer),
				Duration: time.Since(start).Microseconds(),
			})
		}

		if span != nil {
			span.End()
		}

		return h, errStatusDown
	}

	h.Status = "UP"

	if c.logger != nil {
		c.logger.Debug(&Log{
			Type:     "HEALTH CHECK",
			Key:      "health",
			Value:    fmt.Sprintf("Checking connection status for bucket '%s' at '%s'", c.configs.Bucket, safeServer),
			Duration: time.Since(start).Microseconds(),
		})
	}

	if span != nil {
		span.End()
	}

	return h, nil
}

func (c *Client) sendOperationStats(start time.Time, methodType, method string, span trace.Span, kv ...string) {
	duration := time.Since(start)

	var key string
	if len(kv) > 0 {
		key = kv[0]
	}

	if c.logger != nil {
		c.logger.Debug(&Log{
			Type:     methodType,
			Duration: duration.Microseconds(),
			Key:      key,
			Value:    c.configs.Bucket,
		})
	}

	if span != nil {
		defer span.End()

		span.SetAttributes(attribute.Int64(fmt.Sprintf("natskv.%v.duration(μs)", method), duration.Microseconds()))
	}

	if c.metrics != nil {
		c.metrics.RecordHistogram(context.Background(), "app_nats_kv_stats", float64(duration.Milliseconds()),
			"bucket", c.configs.Bucket,
			"operation", methodType)
	}
}

func (c *Client) addTrace(ctx context.Context, method, key string) trace.Span {
	if c.tracer != nil {
		_, span := c.tracer.Start(ctx, fmt.Sprintf("natskv-%v", method))
		span.SetAttributes(attribute.String("natskv.key", key))
		span.SetAttributes(attribute.String("operation", method))

		return span
	}

	return nil
}
