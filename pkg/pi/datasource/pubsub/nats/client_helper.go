package nats

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/sllt/pi/pkg/pi/datasource/pubsub"
)

type noopNATSLogger struct{}

func (noopNATSLogger) Debugf(string, ...any) {}
func (noopNATSLogger) Debug(...any)          {}
func (noopNATSLogger) Logf(string, ...any)   {}
func (noopNATSLogger) Log(...any)            {}
func (noopNATSLogger) Errorf(string, ...any) {}
func (noopNATSLogger) Error(...any)          {}

// establishConnection handles the actual connection process to NATS and sets up jStream.
func (c *Client) establishConnection() error {
	logger := c.logger
	if logger == nil {
		logger = noopNATSLogger{}
	}
	connManager := NewConnectionManager(c.Config, logger, c.natsConnector, c.jetStreamCreator)
	if err := connManager.Connect(); err != nil {
		return err
	}

	return c.finishEstablishConnection(connManager, logger)
}

func (c *Client) establishConnectionContext(ctx context.Context) error {
	logger := c.logger
	if logger == nil {
		logger = noopNATSLogger{}
	}
	connManager := NewConnectionManager(c.Config, logger, c.natsConnector, c.jetStreamCreator)
	if err := connManager.ConnectContext(ctx); err != nil {
		return err
	}

	return c.finishEstablishConnection(connManager, logger)
}

func (c *Client) finishEstablishConnection(connManager *ConnectionManager, logger pubsub.Logger) error {
	js, err := connManager.jetStream()
	if err != nil {
		connManager.Close(context.Background())
		return err
	}

	streamMgr := newStreamManager(js, logger)
	subMgr := newSubscriptionManager(batchSize)

	c.connMu.Lock()
	c.connManager = connManager
	c.streamManager = streamMgr
	c.subManager = subMgr
	c.connMu.Unlock()

	if c.logger != nil {
		c.logger.Logf("connected to NATS server '%s'", redactServerURL(c.Config.Server))
	}

	return nil
}

func (c *Client) ensureStopCh() <-chan struct{} {
	c.retryMu.Lock()
	defer c.retryMu.Unlock()

	if c.stopCh == nil {
		c.stopCh = make(chan struct{})
	}

	return c.stopCh
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
	c.retryMu.Unlock()

	go func() {
		defer func() {
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

func (c *Client) waitRetryOrCancel(ctx context.Context, stopCh <-chan struct{}) bool {
	timer := time.NewTimer(defaultRetryTimeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-stopCh:
		return false
	case <-timer.C:
		return true
	}
}

func (c *Client) retryConnect() {
	c.retryConnectWithStop(c.ensureStopCh())
}

func (c *Client) retryConnectWithStop(stopCh <-chan struct{}) {
	for {
		select {
		case <-stopCh:
			return
		default:
		}

		safeServer := redactServerURL(c.Config.Server)
		if c.logger != nil {
			c.logger.Debugf("connecting to NATS server at %v", safeServer)
		}

		if err := c.establishConnection(); err != nil {
			if c.logger != nil {
				c.logger.Errorf("failed to connect to NATS server at %v: %v", safeServer, err)
			}

			timer := time.NewTimer(defaultRetryTimeout)
			select {
			case <-stopCh:
				timer.Stop()
				return
			case <-timer.C:
			}

			continue
		}

		return
	}
}

func validateAndPrepare(config *Config, logger pubsub.Logger) error {
	if err := validateConfigs(config); err != nil {
		if logger != nil {
			logger.Errorf("could not initialize NATS jStream: %v", err)
		}

		return err
	}

	return nil
}

func (c *Client) createOrUpdateConsumer(
	ctx context.Context, js jetstream.JetStream, subject, consumerName string) (jetstream.Consumer, error) {
	cons, err := js.CreateOrUpdateConsumer(ctx, c.Config.Stream.Stream, jetstream.ConsumerConfig{
		Durable:       consumerName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: subject,
		MaxDeliver:    c.Config.Stream.MaxDeliver,
		DeliverPolicy: jetstream.DeliverNewPolicy,
	})
	if err != nil {
		if c.logger != nil {
			c.logger.Errorf("failed to create or update consumer: %v", err)
		}
		return nil, err
	}

	return cons, nil
}

func (c *Client) processMessages(ctx context.Context, cons jetstream.Consumer, subject string, handler messageHandler) {
	for ctx.Err() == nil {
		if err := c.fetchAndProcessMessages(ctx, cons, subject, handler); err != nil && c.logger != nil {
			c.logger.Errorf("Error in message processing loop for subject %s: %v", subject, err)
		}
	}
}

func (c *Client) fetchAndProcessMessages(ctx context.Context, cons jetstream.Consumer, subject string, handler messageHandler) error {
	maxWait := c.Config.MaxWait
	if maxWait <= 0 {
		maxWait = time.Second
	}
	msgs, err := cons.Fetch(1, jetstream.FetchMaxWait(maxWait))
	if err != nil {
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) && c.logger != nil {
			c.logger.Errorf("Error fetching messages for subject %s: %v", subject, err)
		}

		if errors.Is(err, context.Canceled) {
			return nil
		}

		return err
	}

	return c.processFetchedMessages(ctx, msgs, handler, subject)
}

func (c *Client) processFetchedMessages(ctx context.Context, msgs jetstream.MessageBatch, handler messageHandler, subject string) error {
	for msg := range msgs.Messages() {
		if err := c.handleMessage(ctx, msg, handler); err != nil && c.logger != nil {
			c.logger.Errorf("Error processing message: %v", err)
		}
	}

	if err := msgs.Error(); err != nil {
		if c.logger != nil {
			c.logger.Errorf("Error in message batch for subject %s: %v", subject, err)
		}
		return err
	}

	return nil
}

func (c *Client) handleMessage(ctx context.Context, msg jetstream.Msg, handler messageHandler) error {
	err := handler(ctx, msg)
	if err == nil {
		if ackErr := msg.Ack(); ackErr != nil {
			if c.logger != nil {
				c.logger.Errorf("Error sending ACK for message: %v", ackErr)
			}
			return ackErr
		}

		return nil
	}

	if c.logger != nil {
		c.logger.Errorf("Error handling message: %v", err)
	}

	if nakErr := msg.Nak(); nakErr != nil {
		if c.logger != nil {
			c.logger.Debugf("Error sending NAK for message: %v", nakErr)
		}

		return nakErr
	}

	return err
}

// parseQueryArgs parses the query arguments.
func parseQueryArgs(args ...any) (timeout time.Duration, limit int) {
	// Default values
	timeout = defaultQueryTimeout
	limit = 100

	if len(args) > 0 {
		// First argument can be a custom timeout
		if val, ok := args[0].(time.Duration); ok && val > 0 {
			timeout = val
		}
	}

	if len(args) > 1 {
		// Second argument is the message limit
		if val, ok := args[1].(int); ok && val > 0 {
			limit = val
		}
	}

	return timeout, limit
}

// collectMessages fetches messages from the consumer and combines them.
func collectMessages(ctx context.Context, cons jetstream.Consumer, limit int,
	config *Config, logger pubsub.Logger) ([]byte, error) {
	var result []byte

	messagesCollected := 0

	for messagesCollected < limit {
		// Fetch messages with a batch size based on remaining needed messages
		fetchSize := minInt(batchSize, limit-messagesCollected)
		if fetchSize <= 0 {
			break
		}

		msgs, err := fetchBatch(cons, fetchSize, config.MaxWait, logger)
		if err != nil {
			return result, err
		}

		collected := processBatch(ctx, msgs, &result, &messagesCollected, limit, logger)
		if !collected {
			break
		}
	}

	if strings.Contains(cons.CachedInfo().Config.FilterSubject, piNatsStreamName) && len(result) == 0 && logger != nil {
		logger.Debugf("No migration records found in stream %s", config.Stream.Stream)
	}

	return result, nil
}

func fetchBatch(cons jetstream.Consumer, fetchSize int,
	maxWait time.Duration, logger pubsub.Logger) (jetstream.MessageBatch, error) {
	msgs, err := cons.Fetch(fetchSize, jetstream.FetchMaxWait(maxWait))
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return nil, nil
		}

		if logger != nil {
			logger.Errorf("Error fetching messages: %v", err)
		}

		return nil, err
	}

	return msgs, nil
}

func processBatch(ctx context.Context, msgs jetstream.MessageBatch,
	result *[]byte, messagesCollected *int, limit int, logger pubsub.Logger) bool {
	receivedAny := false

	for msg := range msgs.Messages() {
		receivedAny = true

		// Add newline separator between messages
		if len(*result) > 0 {
			*result = append(*result, '\n')
		}

		// Append message data
		*result = append(*result, msg.Data()...)

		// Acknowledge the message
		if err := msg.Ack(); err != nil && logger != nil {
			logger.Debugf("Error acknowledging message: %v", err)
		}

		*messagesCollected++
		if *messagesCollected >= limit {
			break
		}
	}

	if !receivedAny || errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(ctx.Err(), context.Canceled) {
		return false
	}

	return true
}

// Helper function for Go versions < 1.21.
func minInt(a, b int) int {
	if a < b {
		return a
	}

	return b
}

func checkClient(c *Client) error {
	if c == nil {
		return errClientNotConnected
	}

	c.connMu.RLock()
	connMgr := c.connManager
	c.connMu.RUnlock()

	if connMgr == nil {
		return errClientNotConnected
	}

	if !connMgr.isConnected() {
		return errClientNotConnected
	}

	return nil
}

func createQueryContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return ctx, func() {}
	}

	return context.WithTimeout(ctx, timeout)
}

func (c *Client) getStreamName(query string) string {
	if query == piNatsStreamName {
		return piNatsStreamName
	}

	return c.Config.Stream.Stream
}

func (c *Client) createConsumer(ctx context.Context, js jetstream.JetStream, streamName,
	query, consumerName string) (jetstream.Consumer, error) {
	cons, err := js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Durable:       consumerName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: query,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       c.Config.MaxWait,
	})
	if err != nil {
		if c.logger != nil {
			c.logger.Errorf("failed to create consumer for query: %v", err)
		}

		return nil, err
	}

	return cons, nil
}

func (c *Client) cleanupConsumer(js jetstream.JetStream, streamName string, cons jetstream.Consumer) {
	deleteCtx, cancel := context.WithTimeout(context.Background(), defaultDeleteTimeout)
	defer cancel()

	info, err := cons.Info(deleteCtx)
	if err == nil {
		if err := js.DeleteConsumer(deleteCtx, streamName, info.Name); err != nil && c.logger != nil {
			c.logger.Debugf("failed to delete temporary consumer: %v", err)
		}
	}
}
