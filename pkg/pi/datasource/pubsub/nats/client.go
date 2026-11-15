package nats

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/sllt/pi/pkg/pi/datasource/pubsub"
	"go.opentelemetry.io/otel/trace"
)

//go:generate mockgen -destination=mock_tracer.go -package=nats go.opentelemetry.io/otel/trace Tracer

const defaultRetryTimeout = 2 * time.Second

var (
	errClientNotConnected = errors.New("nats client not connected")
	errEmptySubject       = errors.New("subject name cannot be empty")
)

const (
	piNatsStreamName     = "kite_migrations"
	defaultDeleteTimeout = 5 * time.Second
	defaultQueryTimeout  = 30 * time.Second
	defaultMaxBytes      = 100 * 1024 * 1024
	defaultAckWait       = 30 * time.Second
)

// Client represents a Client for NATS jStream operations.
type Client struct {
	connManager      ConnectionManagerInterface
	subManager       SubscriptionManagerInterface
	subscriptions    map[string]context.CancelFunc
	subMutex         sync.Mutex
	subWG            sync.WaitGroup
	connMu           sync.RWMutex
	retryMu          sync.Mutex
	stopCh           chan struct{}
	retryDone        chan struct{}
	closed           bool
	streamManager    StreamManagerInterface
	Config           *Config
	logger           pubsub.Logger
	metrics          Metrics
	tracer           trace.Tracer
	natsConnector    Connector
	jetStreamCreator JetStreamCreator
}

type messageHandler func(context.Context, jetstream.Msg) error

// ConnectContext establishes a connection to NATS and sets up JetStream without spawning background retry loops on failure.
func (c *Client) ConnectContext(ctx context.Context) error {
	if c == nil {
		return errClientNotConnected
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	c.connMu.RLock()
	connMgr := c.connManager
	c.connMu.RUnlock()
	if connMgr != nil && connMgr.isConnected() {
		return nil
	}

	if c.Config == nil {
		return errServerNotProvided
	}
	if err := validateConfigs(c.Config); err != nil {
		if c.logger != nil {
			c.logger.Errorf("could not initialize NATS jStream: %v", err)
		}
		return err
	}

	safeServer := redactServerURL(c.Config.Server)
	if c.logger != nil {
		c.logger.Debugf("connecting to NATS server at %v", safeServer)
	}

	if err := c.establishConnectionContext(ctx); err != nil {
		if c.logger != nil {
			c.logger.Errorf("failed to connect to NATS server at %v: %v", safeServer, err)
		}
		return err
	}

	return nil
}

// Connect establishes a connection to NATS and sets up jStream.
func (c *Client) Connect() error {
	var safeServer string
	if c != nil && c.Config != nil {
		safeServer = redactServerURL(c.Config.Server)
	}
	if c != nil && c.logger != nil {
		c.logger.Debugf("connecting to NATS server at %v", safeServer)
	}

	if err := validateAndPrepare(c.Config, c.logger); err != nil {
		return err
	}

	if err := c.establishConnection(); err != nil {
		if c.logger != nil {
			c.logger.Errorf("failed to connect to NATS server at %v: %v", safeServer, err)
		}

		c.startRetryConnect()

		return err
	}

	return nil
}

// UseLogger sets the logger for the NATS client.
func (c *Client) UseLogger(logger any) {
	if l, ok := logger.(pubsub.Logger); ok {
		c.logger = l
	}
}

// UseTracer sets the tracer for the NATS client.
func (c *Client) UseTracer(tracer any) {
	if t, ok := tracer.(trace.Tracer); ok {
		c.tracer = t
	}
}

// UseMetrics sets the metrics for the NATS client.
func (c *Client) UseMetrics(metrics any) {
	if m, ok := metrics.(Metrics); ok {
		c.metrics = m
	}
}

// Publish publishes a message to a topic.
func (c *Client) Publish(ctx context.Context, subject string, message []byte) error {
	if err := checkClient(c); err != nil {
		return err
	}

	c.connMu.RLock()
	connMgr := c.connManager
	c.connMu.RUnlock()

	if connMgr == nil {
		return errClientNotConnected
	}

	return connMgr.Publish(ctx, subject, message, c.metrics)
}

// Subscribe subscribes to a topic and returns a single message.
func (c *Client) Subscribe(ctx context.Context, topic string) (*pubsub.Message, error) {
	if c == nil {
		return nil, errClientNotConnected
	}
	stopCh := c.ensureStopCh()

	for {
		if err := checkClient(c); err != nil {
			c.waitRetryOrCancel(ctx, stopCh)

			return nil, errClientNotConnected
		}

		c.connMu.RLock()
		connMgr := c.connManager
		subMgr := c.subManager
		c.connMu.RUnlock()

		if connMgr == nil || subMgr == nil {
			c.waitRetryOrCancel(ctx, stopCh)

			return nil, errClientNotConnected
		}

		js, err := connMgr.jetStream()
		if err == nil {
			return subMgr.Subscribe(ctx, topic, js, c.Config, c.logger, c.metrics)
		}

		if c.logger != nil {
			c.logger.Debugf("Waiting for NATS connection to be established for topic %s", topic)
		}

		if !c.waitRetryOrCancel(ctx, stopCh) {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}

			return nil, errClientNotConnected
		}
	}
}

func (c *Client) SubscribeWithHandler(ctx context.Context, subject string, handler messageHandler) error {
	if c == nil {
		return errClientNotConnected
	}
	c.subMutex.Lock()
	defer c.subMutex.Unlock()

	// Cancel any existing subscription for this subject
	c.cancelExistingSubscription(subject)

	c.connMu.RLock()
	connMgr := c.connManager
	c.connMu.RUnlock()

	if connMgr == nil {
		return errClientNotConnected
	}

	js, err := connMgr.jetStream()
	if err != nil {
		return err
	}

	consumerName := fmt.Sprintf("%s_%s", c.Config.Consumer, strings.ReplaceAll(subject, ".", "_"))

	cons, err := c.createOrUpdateConsumer(ctx, js, subject, consumerName)
	if err != nil {
		return err
	}

	if c.subscriptions == nil {
		c.subscriptions = make(map[string]context.CancelFunc)
	}

	// Create a new context for this subscription
	subCtx, cancel := context.WithCancel(ctx)
	c.subscriptions[subject] = cancel

	c.subWG.Add(1)
	go func() {
		defer c.subWG.Done()
		defer cancel() // Ensure the cancellation is handled properly

		c.processMessages(subCtx, cons, subject, handler)
	}()

	return nil
}

func (c *Client) cancelExistingSubscription(subject string) {
	if cancel, exists := c.subscriptions[subject]; exists {
		cancel()
		delete(c.subscriptions, subject)
	}
}

// Close closes the Client, stopping any background reconnect loops and active subscriptions.
func (c *Client) Close(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	c.retryMu.Lock()
	if !c.closed {
		c.closed = true
		if c.stopCh == nil {
			c.stopCh = make(chan struct{})
		}
		close(c.stopCh)
	}
	done := c.retryDone
	c.retryMu.Unlock()

	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
		}
	}

	c.subMutex.Lock()
	for subject, cancel := range c.subscriptions {
		cancel()
		delete(c.subscriptions, subject)
	}
	c.subMutex.Unlock()

	c.connMu.RLock()
	subMgr := c.subManager
	connMgr := c.connManager
	c.connMu.RUnlock()

	if subMgr != nil {
		subMgr.Close()
	}

	waitDone := make(chan struct{})
	go func() {
		c.subWG.Wait()
		close(waitDone)
	}()

	var waitErr error
	select {
	case <-waitDone:
	case <-ctx.Done():
		waitErr = ctx.Err()
	}

	if connMgr != nil {
		connMgr.Close(ctx)
	}

	return waitErr
}

// Query retrieves messages from a NATS stream/subject.
func (c *Client) Query(ctx context.Context, query string, args ...any) ([]byte, error) {
	if err := checkClient(c); err != nil {
		return nil, err
	}

	if query == "" {
		return nil, errEmptySubject
	}

	// Parse optional arguments
	timeout, limit := parseQueryArgs(args...)

	// Create a query context with timeout
	queryCtx, cancel := createQueryContext(ctx, timeout)
	defer cancel()

	c.connMu.RLock()
	connMgr := c.connManager
	c.connMu.RUnlock()

	if connMgr == nil {
		return nil, errClientNotConnected
	}

	js, err := connMgr.jetStream()
	if err != nil {
		return nil, err
	}

	streamName := c.getStreamName(query)
	consumerName := fmt.Sprintf("query_%s_%d", query, time.Now().UnixNano())

	// Create a consumer
	cons, err := c.createConsumer(queryCtx, js, streamName, query, consumerName)
	if err != nil {
		return nil, err
	}
	defer c.cleanupConsumer(js, streamName, cons)

	// Collect messages
	return collectMessages(queryCtx, cons, limit, c.Config, c.logger)
}

// CreateTopic creates a new topic (stream) in NATS jStream.
func (c *Client) CreateTopic(ctx context.Context, name string) error {
	if err := checkClient(c); err != nil {
		return err
	}

	c.connMu.RLock()
	streamMgr := c.streamManager
	c.connMu.RUnlock()

	if streamMgr == nil {
		return errClientNotConnected
	}

	// For migrations stream, use special configuration with max bytes
	if name == piNatsStreamName {
		return streamMgr.CreateStream(ctx, &StreamConfig{
			Stream:    name,
			Subjects:  []string{name},
			MaxBytes:  defaultMaxBytes,
			Storage:   "file",
			Retention: "limits",
			MaxAge:    365 * 24 * time.Hour,
		})
	}

	return streamMgr.CreateStream(ctx, &StreamConfig{
		Stream:   name,
		Subjects: []string{name},
	})
}

// DeleteTopic deletes a topic (stream) in NATS jStream.
func (c *Client) DeleteTopic(ctx context.Context, name string) error {
	if err := checkClient(c); err != nil {
		return err
	}

	c.connMu.RLock()
	streamMgr := c.streamManager
	c.connMu.RUnlock()

	if streamMgr == nil {
		return errClientNotConnected
	}

	return streamMgr.DeleteStream(ctx, name)
}

// CreateStream creates a new stream in NATS jStream.
func (c *Client) CreateStream(ctx context.Context, cfg *StreamConfig) error {
	if err := checkClient(c); err != nil {
		return err
	}

	c.connMu.RLock()
	streamMgr := c.streamManager
	c.connMu.RUnlock()

	if streamMgr == nil {
		return errClientNotConnected
	}

	return streamMgr.CreateStream(ctx, cfg)
}

// DeleteStream deletes a stream in NATS jStream.
func (c *Client) DeleteStream(ctx context.Context, name string) error {
	if err := checkClient(c); err != nil {
		return err
	}

	c.connMu.RLock()
	streamMgr := c.streamManager
	c.connMu.RUnlock()

	if streamMgr == nil {
		return errClientNotConnected
	}

	return streamMgr.DeleteStream(ctx, name)
}

// CreateOrUpdateStream creates or updates a stream in NATS jStream.
func (c *Client) CreateOrUpdateStream(ctx context.Context, cfg *jetstream.StreamConfig) (jetstream.Stream, error) {
	if err := checkClient(c); err != nil {
		return nil, err
	}

	c.connMu.RLock()
	streamMgr := c.streamManager
	c.connMu.RUnlock()

	if streamMgr == nil {
		return nil, errClientNotConnected
	}

	return streamMgr.CreateOrUpdateStream(ctx, cfg)
}

// GetJetStreamStatus returns the status of the jStream connection.
func GetJetStreamStatus(ctx context.Context, js jetstream.JetStream) (string, error) {
	_, err := js.AccountInfo(ctx)
	if err != nil {
		return jetStreamStatusError, err
	}

	return jetStreamStatusOK, nil
}
