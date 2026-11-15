package sqs

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/sllt/pi/pkg/pi/datasource/pubsub"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

var (
	errClientNotConnected = errors.New("sqs client not connected")
	errQueueNotFound      = errors.New("sqs queue not found")
	errEmptyQueueName     = errors.New("queue name cannot be empty")
	errRegionRequired     = errors.New("sqs region is required")
)

// Config holds the configuration for the SQS client.
type Config struct {
	Region string // Region is the AWS region where the SQS queue is located.Example: "us-east-1", "eu-west-1"

	Endpoint string // Endpoint is the custom endpoint URL for SQS.

	AccessKeyID string // AccessKeyID is the AWS access key ID for authentication.

	SecretAccessKey string // SecretAccessKey is the AWS secret access key for authentication.

	SessionToken string // SessionToken is the AWS session token for temporary credentials.
}

// Internal configuration with sensible defaults managed by the framework.
const (
	defaultMaxMessages       = int32(1)
	defaultWaitTimeSeconds   = int32(20)
	defaultVisibilityTimeout = int32(30)
	defaultRetryDuration     = 5 * time.Second
	defaultQueryTimeout      = 30 * time.Second
	defaultQueryMaxMessages  = int32(10)
)

// Client represents an SQS client that implements the pubsub.Client interface.
type Client struct {
	conn    sqsClient
	cfg     *Config
	logger  pubsub.Logger
	metrics Metrics
	tracer  trace.Tracer

	// queueURLCache caches queue URLs to avoid repeated GetQueueUrl API calls.
	queueURLCache map[string]string
	cacheMu       sync.RWMutex

	connMu     sync.Mutex
	isRetrying atomic.Bool

	retryMu   sync.Mutex
	stopCh    chan struct{}
	retryDone chan struct{}
	closed    bool
}

// New creates a new SQS client with the provided configuration.
// The client is not connected until Connect() is called.
func New(cfg *Config) *Client {
	if cfg == nil {
		cfg = &Config{}
	}

	return &Client{
		cfg:           cfg,
		queueURLCache: make(map[string]string),
	}
}

// FromEnv constructs a zero-I/O SQS *Client from an environment/config lookup function.
//
// Supported keys:
//   - SQS_REGION (fallback: AWS_REGION, required)
//   - SQS_ENDPOINT (fallback: AWS_ENDPOINT_URL)
//   - SQS_ACCESS_KEY_ID (fallback: AWS_ACCESS_KEY_ID)
//   - SQS_SECRET_ACCESS_KEY (fallback: AWS_SECRET_ACCESS_KEY)
//   - SQS_SESSION_TOKEN (fallback: AWS_SESSION_TOKEN)
func FromEnv(get func(string) string) (*Client, error) {
	if get == nil {
		return nil, errRegionRequired
	}

	region := strings.TrimSpace(get("SQS_REGION"))
	if region == "" {
		region = strings.TrimSpace(get("AWS_REGION"))
	}
	if region == "" {
		return nil, errRegionRequired
	}

	endpoint := strings.TrimSpace(get("SQS_ENDPOINT"))
	if endpoint == "" {
		endpoint = strings.TrimSpace(get("AWS_ENDPOINT_URL"))
	}
	ak := strings.TrimSpace(get("SQS_ACCESS_KEY_ID"))
	if ak == "" {
		ak = strings.TrimSpace(get("AWS_ACCESS_KEY_ID"))
	}
	sk := strings.TrimSpace(get("SQS_SECRET_ACCESS_KEY"))
	if sk == "" {
		sk = strings.TrimSpace(get("AWS_SECRET_ACCESS_KEY"))
	}
	token := strings.TrimSpace(get("SQS_SESSION_TOKEN"))
	if token == "" {
		token = strings.TrimSpace(get("AWS_SESSION_TOKEN"))
	}

	return New(&Config{
		Region:          region,
		Endpoint:        endpoint,
		AccessKeyID:     ak,
		SecretAccessKey: sk,
		SessionToken:    token,
	}), nil
}

// UseLogger sets the logger for the SQS client.
func (c *Client) UseLogger(logger any) {
	if l, ok := logger.(pubsub.Logger); ok {
		c.logger = l
	}
}

// UseMetrics sets the metrics for the SQS client.
func (c *Client) UseMetrics(metrics any) {
	if m, ok := metrics.(Metrics); ok {
		c.metrics = m
	}
}

// UseTracer sets the tracer for the SQS client.
func (c *Client) UseTracer(tracer any) {
	if t, ok := tracer.(trace.Tracer); ok {
		c.tracer = t
	}
}

// ConnectContext establishes a connection to AWS SQS bounded by ctx without spawning background retry loops on failure.
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

	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.conn != nil {
		return nil
	}
	if c.cfg == nil || c.cfg.Region == "" {
		if c.logger != nil {
			c.logger.Error("SQS region is required")
		}
		return errRegionRequired
	}

	if c.logger != nil {
		c.logger.Debugf("connecting to AWS SQS in region: %s", c.cfg.Region)
	}

	awsCfg, err := c.loadAWSConfig(ctx)
	if err != nil {
		if c.logger != nil {
			c.logger.Errorf("failed to load AWS config: %v", err)
		}
		return err
	}

	opts := func(o *sqs.Options) {
		if c.cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.cfg.Endpoint)
		}
	}

	conn := sqs.NewFromConfig(awsCfg, opts)
	if _, err = conn.ListQueues(ctx, &sqs.ListQueuesInput{MaxResults: aws.Int32(1)}); err != nil {
		if c.logger != nil {
			c.logger.Errorf("failed to connect to SQS: %v", err)
		}
		return err
	}

	c.conn = conn
	if c.logger != nil {
		c.logger.Logf("connected to AWS SQS in region: %s", c.cfg.Region)
	}

	return nil
}

// Connect establishes a connection to AWS SQS.
func (c *Client) Connect() {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.logger == nil {
		return
	}

	if c.cfg.Region == "" {
		c.logger.Error("SQS region is required")
		return
	}

	c.logger.Debugf("connecting to AWS SQS in region: %s", c.cfg.Region)

	ctx := context.Background()

	awsCfg, err := c.loadAWSConfig(ctx)
	if err != nil {
		c.logger.Errorf("failed to load AWS config: %v", err)
		return
	}

	// Create SQS client with custom endpoint if provided.
	opts := func(o *sqs.Options) {
		if c.cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.cfg.Endpoint)
		}
	}

	c.conn = sqs.NewFromConfig(awsCfg, opts)

	// Verify connection by listing queues
	_, err = c.conn.ListQueues(ctx, &sqs.ListQueuesInput{
		MaxResults: aws.Int32(1),
	})
	if err != nil {
		c.logger.Errorf("failed to connect to SQS: %v", err)
		c.conn = nil

		// Only start retry goroutine if not already retrying
		c.startRetryConnect()

		return
	}

	c.logger.Logf("connected to AWS SQS in region: %s", c.cfg.Region)
}

func (c *Client) ensureStopCh() <-chan struct{} {
	c.retryMu.Lock()
	defer c.retryMu.Unlock()

	if c.stopCh == nil {
		c.stopCh = make(chan struct{})
	}

	return c.stopCh
}

func (c *Client) isClosed() bool {
	c.retryMu.Lock()
	defer c.retryMu.Unlock()

	return c.closed
}

func (c *Client) startRetryConnect() {
	c.retryMu.Lock()
	if c.closed || c.retryDone != nil {
		c.retryMu.Unlock()
		return
	}

	if c.stopCh == nil {
		c.stopCh = make(chan struct{})
	}

	stopCh := c.stopCh
	done := make(chan struct{})
	c.retryDone = done
	c.isRetrying.Store(true)
	c.retryMu.Unlock()

	go func() {
		defer func() {
			c.isRetrying.Store(false)
			close(done)
			c.retryMu.Lock()
			if c.retryDone == done {
				c.retryDone = nil
			}
			c.retryMu.Unlock()
		}()

		c.retryConnectWithStop(stopCh)
	}()
}

// loadAWSConfig loads the AWS configuration with credentials.
func (c *Client) loadAWSConfig(ctx context.Context) (aws.Config, error) {
	var opts []func(*config.LoadOptions) error

	opts = append(opts, config.WithRegion(c.cfg.Region))

	// Use static credentials if provided
	if c.cfg.AccessKeyID != "" && c.cfg.SecretAccessKey != "" {
		staticCreds := credentials.NewStaticCredentialsProvider(
			c.cfg.AccessKeyID,
			c.cfg.SecretAccessKey,
			c.cfg.SessionToken,
		)
		opts = append(opts, config.WithCredentialsProvider(staticCreds))
	}

	return config.LoadDefaultConfig(ctx, opts...)
}

// retryConnect attempts to reconnect to SQS on failure.
func (c *Client) retryConnect() {
	defer c.isRetrying.Store(false)
	c.retryConnectWithStop(c.ensureStopCh())
}

func (c *Client) retryConnectWithStop(stopCh <-chan struct{}) {
	for {
		timer := time.NewTimer(defaultRetryDuration)
		select {
		case <-stopCh:
			timer.Stop()
			return
		case <-timer.C:
		}

		if c.isClosed() {
			return
		}

		if c.logger != nil {
			c.logger.Debugf("retrying connection to SQS...")
		}

		c.connectInternal()

		if c.isConnected() {
			if c.logger != nil {
				c.logger.Logf("successfully reconnected to SQS")
			}

			return
		}
	}
}

// connectInternal establishes connection without spawning retry goroutine.
func (c *Client) connectInternal() {
	if c.isClosed() {
		return
	}

	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.isClosed() {
		return
	}

	ctx := context.Background()

	awsCfg, err := c.loadAWSConfig(ctx)
	if err != nil {
		return
	}

	opts := func(o *sqs.Options) {
		if c.cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.cfg.Endpoint)
		}
	}

	c.conn = sqs.NewFromConfig(awsCfg, opts)

	_, err = c.conn.ListQueues(ctx, &sqs.ListQueuesInput{
		MaxResults: aws.Int32(1),
	})
	if err != nil {
		c.conn = nil
	}
}

// isConnected checks if the client is connected.
func (c *Client) isConnected() bool {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	return c.conn != nil
}

func (c *Client) getConn() sqsClient {
	if c == nil {
		return nil
	}
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	return conn
}

// Publish sends a message to the specified SQS queue.
// The topic parameter is the queue name (not the full URL).
func (c *Client) Publish(ctx context.Context, topic string, message []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}
	conn := c.getConn()
	if conn == nil {
		return errClientNotConnected
	}

	if topic == "" {
		return errEmptyQueueName
	}

	ctx, span := c.startTrace(ctx, "sqs-publish")
	defer span.End()

	if c.metrics != nil {
		c.metrics.IncrementCounter(ctx, "app_pubsub_publish_total_count", "topic", topic)
	}

	queueURL, err := c.getQueueURL(ctx, topic)
	if err != nil {
		if c.logger != nil {
			c.logger.Errorf("failed to get queue URL for %s: %v", topic, err)
		}
		return err
	}

	start := time.Now()

	input := &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(string(message)),
	}

	result, err := conn.SendMessage(ctx, input)
	if err != nil {
		if c.logger != nil {
			c.logger.Errorf("failed to publish message to queue %s: %v", topic, err)
		}
		return err
	}

	if c.logger != nil {
		c.logger.Debug(&pubsub.Log{
			Mode:          "PUB",
			CorrelationID: span.SpanContext().TraceID().String(),
			MessageValue:  string(message),
			Topic:         topic,
			Host:          c.cfg.Region,
			PubSubBackend: "SQS",
			Time:          time.Since(start).Microseconds(),
		})
		if result != nil && result.MessageId != nil {
			c.logger.Debugf("message published to queue %s with ID: %s", topic, *result.MessageId)
		}
	}

	if c.metrics != nil {
		c.metrics.IncrementCounter(ctx, "app_pubsub_publish_success_count", "topic", topic)
	}

	return nil
}

// Subscribe receives a single message from the specified SQS queue.
// The topic parameter is the queue name (not the full URL).
func (c *Client) Subscribe(ctx context.Context, topic string) (*pubsub.Message, error) {
	if c == nil {
		return nil, errClientNotConnected
	}
	if ctx == nil {
		ctx = context.Background()
	}
	conn := c.getConn()
	if conn == nil {
		timer := time.NewTimer(defaultRetryDuration)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-c.ensureStopCh():
			timer.Stop()
		case <-timer.C:
		}

		return nil, errClientNotConnected
	}

	if topic == "" {
		return nil, errEmptyQueueName
	}

	ctx, span := c.startTrace(ctx, "sqs-subscribe")
	defer span.End()

	if c.metrics != nil {
		c.metrics.IncrementCounter(ctx, "app_pubsub_subscribe_total_count", "topic", topic)
	}

	queueURL, err := c.getQueueURL(ctx, topic)
	if err != nil {
		// Don't log for context cancellation or if it's a transient error
		if !isContextCanceled(err) && !isConnectionError(err) && c.logger != nil {
			c.logger.Errorf("queue %s not found: %v", topic, err)
		}

		return nil, err
	}

	start := time.Now()

	input := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: defaultMaxMessages,
		WaitTimeSeconds:     defaultWaitTimeSeconds,
		VisibilityTimeout:   defaultVisibilityTimeout,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameAll,
		},
	}

	result, err := conn.ReceiveMessage(ctx, input)
	if err != nil {
		// Only log non-transient errors, avoid flooding logs
		if !isContextCanceled(err) && !isConnectionError(err) && c.logger != nil {
			c.logger.Errorf("failed to receive message from queue %s: %v", topic, err)
		}

		return nil, err
	}

	if len(result.Messages) == 0 {
		return nil, nil // No messages available
	}

	sqsMsg := result.Messages[0]
	duration := time.Since(start)

	// Create pubsub message
	msg := pubsub.NewMessage(ctx)
	msg.Topic = topic
	msg.Value = []byte(*sqsMsg.Body)
	msg.MetaData = sqsMsg.MessageAttributes
	msg.Committer = newMessage(
		*sqsMsg.ReceiptHandle,
		queueURL,
		conn,
		c.logger,
	)

	if c.logger != nil {
		c.logger.Debug(&pubsub.Log{
			Mode:          "SUB",
			CorrelationID: span.SpanContext().TraceID().String(),
			MessageValue:  string(msg.Value),
			Topic:         topic,
			Host:          c.cfg.Region,
			PubSubBackend: "SQS",
			Time:          duration.Microseconds(),
		})
	}

	if c.metrics != nil {
		c.metrics.IncrementCounter(ctx, "app_pubsub_subscribe_success_count", "topic", topic)
	}

	return msg, nil
}

// CreateTopic creates a new SQS queue with the specified name.
// In SQS terminology, this creates a queue, not a topic.
func (c *Client) CreateTopic(ctx context.Context, name string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	conn := c.getConn()
	if conn == nil {
		return errClientNotConnected
	}

	if name == "" {
		return errEmptyQueueName
	}

	if c.logger != nil {
		c.logger.Debugf("creating SQS queue: %s", name)
	}

	_, err := conn.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(name),
	})
	if err != nil {
		if c.logger != nil {
			c.logger.Errorf("failed to create queue %s: %v", name, err)
		}
		return err
	}

	if c.logger != nil {
		c.logger.Logf("SQS queue created: %s", name)
	}

	return nil
}

// DeleteTopic deletes an SQS queue with the specified name.
func (c *Client) DeleteTopic(ctx context.Context, name string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	conn := c.getConn()
	if conn == nil {
		return errClientNotConnected
	}

	if name == "" {
		return errEmptyQueueName
	}

	queueURL, err := c.getQueueURL(ctx, name)
	if err != nil {
		return err
	}

	if c.logger != nil {
		c.logger.Debugf("deleting SQS queue: %s", name)
	}

	_, err = conn.DeleteQueue(ctx, &sqs.DeleteQueueInput{
		QueueUrl: aws.String(queueURL),
	})
	if err != nil {
		if c.logger != nil {
			c.logger.Errorf("failed to delete queue %s: %v", name, err)
		}
		return err
	}

	// Remove from cache
	c.cacheMu.Lock()
	delete(c.queueURLCache, name)
	c.cacheMu.Unlock()

	if c.logger != nil {
		c.logger.Logf("SQS queue deleted: %s", name)
	}

	return nil
}

// Query retrieves multiple messages from an SQS queue.
// Args: [0] = limit (int32, max 10).
func (c *Client) Query(ctx context.Context, query string, args ...any) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	conn := c.getConn()
	if conn == nil {
		return nil, errClientNotConnected
	}

	if query == "" {
		return nil, errEmptyQueueName
	}

	queueURL, err := c.getQueueURL(ctx, query)
	if err != nil {
		return nil, err
	}

	limit := parseQueryArgs(args...)

	// Use provided context or add default timeout
	readCtx := ctx
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc

		readCtx, cancel = context.WithTimeout(ctx, defaultQueryTimeout)
		defer cancel()
	}

	result, err := conn.ReceiveMessage(readCtx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: limit,
		WaitTimeSeconds:     defaultWaitTimeSeconds,
	})
	if err != nil {
		return nil, err
	}

	if len(result.Messages) == 0 {
		return nil, nil
	}

	// Combine all message bodies
	messages := make([]string, 0, len(result.Messages))
	for _, msg := range result.Messages {
		messages = append(messages, *msg.Body)
	}

	return []byte("[" + strings.Join(messages, ",") + "]"), nil
}

// Close closes the SQS client connection and stops any background reconnect loop.
func (c *Client) Close() error {
	if c == nil {
		return nil
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
		<-done
	}

	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.logger != nil {
		c.logger.Debug("closing SQS client")
	}

	c.conn = nil

	return nil
}

// getQueueURL retrieves the queue URL for a given queue name.
// It caches the result to avoid repeated API calls.
func (c *Client) getQueueURL(ctx context.Context, queueName string) (string, error) {
	// Check cache first
	c.cacheMu.RLock()

	if url, ok := c.queueURLCache[queueName]; ok {
		c.cacheMu.RUnlock()

		return url, nil
	}

	c.cacheMu.RUnlock()

	conn := c.getConn()
	if conn == nil {
		return "", errClientNotConnected
	}

	// Get queue URL from SQS
	result, err := conn.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		return "", errQueueNotFound
	}

	// Cache the result
	c.cacheMu.Lock()
	c.queueURLCache[queueName] = *result.QueueUrl
	c.cacheMu.Unlock()

	return *result.QueueUrl, nil
}

// startTrace starts a new trace span using the configured tracer or global OpenTelemetry tracer provider.
// This ensures proper trace propagation from the incoming context.
func (c *Client) startTrace(ctx context.Context, name string) (context.Context, trace.Span) {
	if c != nil && c.tracer != nil {
		return c.tracer.Start(ctx, name)
	}

	return otel.GetTracerProvider().Tracer("pi").Start(ctx, name)
}

// parseQueryArgs parses the query arguments for Query method.
func parseQueryArgs(args ...any) int32 {
	if len(args) > 0 {
		if l, ok := args[0].(int32); ok && l > 0 && l <= 10 {
			return l
		}
	}

	return defaultQueryMaxMessages
}

// isContextCanceled checks if the error is due to context cancellation.
// This is used to suppress error logs during graceful shutdown.
func isContextCanceled(err error) bool {
	if err == nil {
		return false
	}

	// Check for standard context errors
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// Check error message for "canceled" (AWS SDK wraps context errors)
	errMsg := err.Error()

	return strings.Contains(errMsg, "context canceled") ||
		strings.Contains(errMsg, "context deadline exceeded") ||
		strings.Contains(errMsg, "canceled")
}

// isConnectionError checks if the error is a connection-related error.
// These errors are transient and don't need to be logged as errors during normal retry flow.
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()

	return strings.Contains(errMsg, "connection refused") ||
		strings.Contains(errMsg, "no such host") ||
		strings.Contains(errMsg, "network is unreachable") ||
		strings.Contains(errMsg, "exceeded maximum number of attempts")
}
