package eventhub

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/checkpoints"
	infra "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/sllt/pi/pkg/pi/datasource"
	"github.com/sllt/pi/pkg/pi/datasource/pubsub"
	"go.opentelemetry.io/otel/trace"
)

// code reference from https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-go-get-started-send
// metrics are being registered in the container, and we are using the same metrics so we have not re-registered the metrics here.
// It is different from other datasources.

var (
	ErrNoMsgReceived      = errors.New("no message received")
	ErrTopicMismatch      = errors.New("topic should be same as Event Hub name")
	errClientNotConnected = errors.New("eventhub client not connected")
	errEmptyTopic         = errors.New("topic name cannot be empty")
	errInvalidConfig      = errors.New("invalid eventhub configuration")
)

const (
	defaultQueryTimeout     = 30 * time.Second
	eventHubPropsTimeout    = 2 * time.Second
	basicTierMaxPartitions  = 2
	basicTierReceiveTimeout = 3 * time.Second
)

type Config struct {
	ConnectionString          string
	ContainerConnectionString string
	StorageServiceURL         string
	StorageContainerName      string
	EventhubName              string
	// if not provided, it will read from the $Default consumergroup.
	ConsumerGroup string
	// the following configs are for advance setup of the Event Hub.
	StorageOptions   *infra.ClientOptions
	BlobStoreOptions *checkpoints.BlobStoreOptions
	ConsumerOptions  *azeventhubs.ConsumerClientOptions
	ProducerOptions  *azeventhubs.ProducerClientOptions
}

type Client struct {
	mu       sync.RWMutex
	producer *azeventhubs.ProducerClient
	consumer *azeventhubs.ConsumerClient
	// we are using a processor such that to keep consuming the events from all the different partitions.
	processor *azeventhubs.Processor
	// a checkpoint is being called while committing the event received from the event.
	checkPoint *checkpoints.BlobStore
	// processorCtx is being stored such that to gracefully shutting down the application.
	processorCtx  context.CancelFunc
	processorDone chan struct{}
	cfg           Config
	logger        Logger
	metrics       Metrics
	tracer        trace.Tracer
}

// New Creates the client for Event Hub.
//
//nolint:gocritic // cfg is a configuration struct.
func New(cfg Config) *Client {
	return &Client{
		cfg: cfg,
	}
}

// FromEnv constructs a zero-I/O Event Hub *Client from an environment/config lookup function.
//
// Supported keys:
//   - EVENTHUB_CONNECTION_STRING (required)
//   - EVENTHUB_NAME (required)
//   - EVENTHUB_STORAGE_CONNECTION_STRING (fallback: EVENTHUB_CONTAINER_CONNECTION_STRING, required)
//   - EVENTHUB_STORAGE_SERVICE_URL (required)
//   - EVENTHUB_STORAGE_CONTAINER_NAME (required)
//   - EVENTHUB_CONSUMER_GROUP
func FromEnv(get func(string) string) (*Client, error) {
	if get == nil {
		return nil, errInvalidConfig
	}

	connStr := strings.TrimSpace(get("EVENTHUB_CONNECTION_STRING"))
	name := strings.TrimSpace(get("EVENTHUB_NAME"))
	containerConnStr := strings.TrimSpace(get("EVENTHUB_STORAGE_CONNECTION_STRING"))
	if containerConnStr == "" {
		containerConnStr = strings.TrimSpace(get("EVENTHUB_CONTAINER_CONNECTION_STRING"))
	}
	serviceURL := strings.TrimSpace(get("EVENTHUB_STORAGE_SERVICE_URL"))
	containerName := strings.TrimSpace(get("EVENTHUB_STORAGE_CONTAINER_NAME"))

	if connStr == "" || name == "" || containerConnStr == "" || serviceURL == "" || containerName == "" {
		return nil, errInvalidConfig
	}

	return New(Config{
		ConnectionString:          connStr,
		ContainerConnectionString: containerConnStr,
		StorageServiceURL:         serviceURL,
		StorageContainerName:      containerName,
		EventhubName:              name,
		ConsumerGroup:             strings.TrimSpace(get("EVENTHUB_CONSUMER_GROUP")),
	}), nil
}

//nolint:gocritic // cfg is a configuration struct.
func (c *Client) validConfigs(cfg Config) bool {
	ok := true

	if cfg.EventhubName == "" {
		ok = false
		if c.logger != nil {
			c.logger.Error("eventhubName cannot be an empty")
		}
	}

	if cfg.ConnectionString == "" {
		ok = false
		if c.logger != nil {
			c.logger.Error("connectionString cannot be an empty")
		}
	}

	if cfg.StorageServiceURL == "" {
		ok = false
		if c.logger != nil {
			c.logger.Error("storageServiceURL cannot be an empty")
		}
	}

	if cfg.StorageContainerName == "" {
		ok = false
		if c.logger != nil {
			c.logger.Error("storageContainerName cannot be an empty")
		}
	}

	if cfg.ContainerConnectionString == "" {
		ok = false
		if c.logger != nil {
			c.logger.Error("containerConnectionString cannot be an empty")
		}
	}

	return ok
}

// UseLogger sets the logger for the Event Hub client.
func (c *Client) UseLogger(logger any) {
	if l, ok := logger.(Logger); ok {
		c.logger = l
	}
}

// UseMetrics sets the metrics for the Event Hub client.
func (c *Client) UseMetrics(metrics any) {
	if m, ok := metrics.(Metrics); ok {
		c.metrics = m
	}
}

// UseTracer sets the tracer for the Event Hub client.
func (c *Client) UseTracer(tracer any) {
	if t, ok := tracer.(trace.Tracer); ok {
		c.tracer = t
	}
}

// ConnectContext initializes Event Hub producer, consumer, checkpoint store, and partition processor.
// Calling ConnectContext on an already-connected Client is an idempotent no-op.
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

	c.mu.RLock()
	if c.producer != nil && c.consumer != nil && c.processor != nil {
		c.mu.RUnlock()
		return nil
	}
	c.mu.RUnlock()

	if !c.validConfigs(c.cfg) {
		return errInvalidConfig
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.producer != nil && c.consumer != nil && c.processor != nil {
		return nil
	}

	if c.cfg.ConsumerGroup == "" {
		c.cfg.ConsumerGroup = azeventhubs.DefaultConsumerGroup
		if c.logger != nil {
			c.logger.Debugf("Using default consumer group: %s", c.cfg.ConsumerGroup)
		}
	} else if c.logger != nil {
		c.logger.Debugf("Using provided consumer group: %s", c.cfg.ConsumerGroup)
	}

	if c.logger != nil {
		c.logger.Debug("Event Hub connection started using connection string")
	}

	producerClient, err := azeventhubs.NewProducerClientFromConnectionString(c.cfg.ConnectionString,
		c.cfg.EventhubName, c.cfg.ProducerOptions)
	if err != nil {
		if c.logger != nil {
			c.logger.Errorf("error occurred while creating producer client %v", err)
		}
		return err
	}
	if c.logger != nil {
		c.logger.Debug("Event Hub producer client setup success")
	}

	containerClient, err := infra.NewClientFromConnectionString(c.cfg.ContainerConnectionString, c.cfg.StorageContainerName,
		c.cfg.StorageOptions)
	if err != nil {
		_ = producerClient.Close(ctx)
		if c.logger != nil {
			c.logger.Errorf("error occurred while creating container client %v", err)
		}
		return err
	}
	if c.logger != nil {
		c.logger.Debug("Event Hub container client setup success")
	}

	checkpointStore, err := checkpoints.NewBlobStore(containerClient, c.cfg.BlobStoreOptions)
	if err != nil {
		_ = producerClient.Close(ctx)
		if c.logger != nil {
			c.logger.Errorf("error occurred while creating blobstore %v", err)
		}
		return err
	}
	if c.logger != nil {
		c.logger.Debug("Event Hub blobstore client setup success")
	}

	consumerClient, err := azeventhubs.NewConsumerClientFromConnectionString(c.cfg.ConnectionString, c.cfg.EventhubName,
		c.cfg.ConsumerGroup, c.cfg.ConsumerOptions)
	if err != nil {
		_ = producerClient.Close(ctx)
		if c.logger != nil {
			c.logger.Errorf("error occurred while creating consumer client %v", err)
		}
		return err
	}
	if c.logger != nil {
		c.logger.Debug("Event Hub consumer client setup success")
	}

	processor, err := azeventhubs.NewProcessor(consumerClient, checkpointStore, nil)
	if err != nil {
		_ = consumerClient.Close(ctx)
		_ = producerClient.Close(ctx)
		if c.logger != nil {
			c.logger.Errorf("error occurred while creating processor %v", err)
		}
		return err
	}
	if c.logger != nil {
		c.logger.Debug("Event Hub processor setup success")
	}

	processorCtx, processorCancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	c.processorCtx = processorCancel
	c.processorDone = done
	logger := c.logger

	go func() {
		defer close(done)
		if runErr := processor.Run(processorCtx); runErr != nil && !errors.Is(runErr, context.Canceled) {
			if logger != nil {
				logger.Errorf("error occurred while running processor %v", runErr)
			}
			return
		}
		if logger != nil {
			logger.Debug("Event Hub processor running successfully")
		}
	}()

	c.processor = processor
	c.producer = producerClient
	c.consumer = consumerClient
	c.checkPoint = checkpointStore

	if c.logger != nil {
		c.logger.Debug("Event Hub client initialization complete")
	}

	return nil
}

// Connect establishes a connection to Event Hub and registers metrics using the provided configuration when the client was Created.
func (c *Client) Connect() {
	_ = c.ConnectContext(context.Background())
}

// Subscribe checks all partitions for the first available event and returns it.
func (c *Client) Subscribe(ctx context.Context, topic string) (*pubsub.Message, error) {
	if c == nil {
		return nil, errClientNotConnected
	}
	if ctx == nil {
		ctx = context.Background()
	}
	c.mu.RLock()
	producer := c.producer
	consumer := c.consumer
	processor := c.processor
	c.mu.RUnlock()

	if producer == nil || consumer == nil || processor == nil {
		return nil, errClientNotConnected
	}

	// Try processor approach first
	partitionClient := processor.NextPartitionClient(ctx)
	if partitionClient != nil {
		return c.processEventsFromPartitionClient(ctx, topic, partitionClient)
	}

	// Fallback to direct consumer approach if processor doesn't have partition clients ready
	return c.subscribeDirectFromConsumer(ctx, topic, consumer)
}

// processEventsFromPartitionClient processes events using the processor partition client.
func (c *Client) processEventsFromPartitionClient(ctx context.Context, topic string,
	partitionClient *azeventhubs.ProcessorPartitionClient) (*pubsub.Message, error) {
	defer closePartitionResources(ctx, partitionClient)

	timeout := c.getReceiveTimeout()

	receiveCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if c.metrics != nil {
		c.metrics.IncrementCounter(ctx, "app_pubsub_subscribe_total_count", "topic", topic, "subscription_name", partitionClient.PartitionID())
	}

	start := time.Now()

	// ReceiveEvents signature: ReceiveEvents(ctx context.Context, count int, options *ReceiveEventsOptions) ([]*ReceivedEventData, error)
	// Note: ReceiveEventsOptions is nil for default behavior.
	events, err := partitionClient.ReceiveEvents(receiveCtx, 1, nil)
	if err != nil {
		if !errors.Is(err, context.DeadlineExceeded) && c.logger != nil {
			c.logger.Debugf("Error receiving events from partition %s: %v", partitionClient.PartitionID(), err)
		}

		return nil, nil
	}

	if len(events) == 0 {
		return nil, nil
	}

	// Create message from the first event
	msg := pubsub.NewMessage(ctx)
	msg.Value = events[0].Body
	msg.Committer = &Message{
		event:     events[0],
		processor: partitionClient,
		logger:    c.logger,
	}
	msg.Topic = topic
	msg.MetaData = events[0].EventData

	end := time.Since(start)
	if c.logger != nil {
		c.logger.Debug(&Log{
			Mode:          "SUB",
			MessageValue:  strings.Join(strings.Fields(string(msg.Value)), " "),
			Topic:         topic,
			Host:          c.cfg.EventhubName + ":" + c.cfg.ConsumerGroup + ":" + partitionClient.PartitionID(),
			PubSubBackend: "EVHUB",
			Time:          end.Microseconds(),
		})
	}

	if c.metrics != nil {
		c.metrics.IncrementCounter(ctx, "app_pubsub_subscribe_success_count", "topic", topic, "subscription_name", partitionClient.PartitionID())
	}

	return msg, nil
}

// subscribeDirectFromConsumer uses consumer client directly as fallback.
func (c *Client) subscribeDirectFromConsumer(ctx context.Context, topic string, consumer *azeventhubs.ConsumerClient) (*pubsub.Message, error) {
	if consumer == nil {
		return nil, errClientNotConnected
	}
	// Get partition information
	props, err := consumer.GetEventHubProperties(ctx, nil)
	if err != nil {
		if c.logger != nil {
			c.logger.Errorf("Failed to get Event Hub properties: %v", err)
		}
		return nil, err
	}

	// Try each partition for available messages - use LATEST to avoid old messages
	for _, partitionID := range props.PartitionIDs {
		msg, err := c.tryReadFromPartition(ctx, partitionID, topic, consumer)
		if err != nil {
			if c.logger != nil {
				c.logger.Debugf("Error reading from partition %s: %v", partitionID, err)
			}
			continue
		}

		if msg != nil {
			return msg, nil
		}
	}

	return nil, nil
}

// tryReadFromPartition attempts to read a single message from specified partition.
func (c *Client) tryReadFromPartition(ctx context.Context, partitionID, topic string, consumer *azeventhubs.ConsumerClient) (*pubsub.Message, error) {
	// Create partition client for direct read with LATEST position to avoid old messages.
	partitionClient, err := consumer.NewPartitionClient(partitionID, &azeventhubs.PartitionClientOptions{
		StartPosition: azeventhubs.StartPosition{
			Latest: to.Ptr(true), // Use Latest to only get new messages
		},
	})
	if err != nil {
		return nil, err
	}

	defer partitionClient.Close(ctx)

	timeout := c.getReceiveTimeout()

	receiveCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if c.metrics != nil {
		c.metrics.IncrementCounter(ctx, "app_pubsub_subscribe_total_count", "topic", topic, "subscription_name", partitionID)
	}

	start := time.Now()

	events, err := partitionClient.ReceiveEvents(receiveCtx, 1, nil)

	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return nil, err
	}

	if len(events) == 0 {
		return nil, nil // No message available in this partition
	}

	// Create message from event
	msg := pubsub.NewMessage(ctx)

	msg.Value = events[0].Body
	msg.Committer = &Message{
		event:     events[0],
		processor: nil, // Not using processor for direct reads
		logger:    c.logger,
	}
	msg.Topic = topic
	msg.MetaData = events[0].EventData

	end := time.Since(start)
	if c.logger != nil {
		c.logger.Debug(&Log{
			Mode:          "SUB",
			MessageValue:  strings.Join(strings.Fields(string(msg.Value)), " "),
			Topic:         topic,
			Host:          c.cfg.EventhubName + ":" + c.cfg.ConsumerGroup + ":" + partitionID,
			PubSubBackend: "EVHUB",
			Time:          end.Microseconds(),
		})
	}

	if c.metrics != nil {
		c.metrics.IncrementCounter(ctx, "app_pubsub_subscribe_success_count", "topic", topic, "subscription_name", partitionID)
	}

	return msg, nil
}

// getReceiveTimeout returns appropriate timeout based on Event Hub characteristics.
func (c *Client) getReceiveTimeout() time.Duration {
	// Check if this might be basic tier by examining partition count
	if c.isLikelyBasicTier() {
		return basicTierReceiveTimeout
	}

	return time.Second
}

// isLikelyBasicTier detects basic tier characteristics.
func (c *Client) isLikelyBasicTier() bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	consumer := c.consumer
	c.mu.RUnlock()
	if consumer == nil {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), eventHubPropsTimeout)
	defer cancel()

	props, err := consumer.GetEventHubProperties(ctx, nil)
	if err != nil {
		return false // Default to standard behavior on error
	}

	// Basic tier typically has fewer partitions
	return len(props.PartitionIDs) <= basicTierMaxPartitions
}

func closePartitionResources(ctx context.Context, partitionClient *azeventhubs.ProcessorPartitionClient) {
	partitionClient.Close(ctx)
}

func (c *Client) Publish(ctx context.Context, topic string, message []byte) error {
	if c == nil {
		return errClientNotConnected
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if topic != c.cfg.EventhubName {
		return ErrTopicMismatch
	}

	c.mu.RLock()
	producer := c.producer
	c.mu.RUnlock()
	if producer == nil {
		return errClientNotConnected
	}

	if c.metrics != nil {
		c.metrics.IncrementCounter(ctx, "app_pubsub_publish_total_count", "topic", topic)
	}

	newBatchOptions := &azeventhubs.EventDataBatchOptions{}

	batch, err := producer.NewEventDataBatch(ctx, newBatchOptions)
	if err != nil {
		if c.logger != nil {
			c.logger.Errorf("failed to create event batch %v", err)
		}

		return err
	}

	data := []*azeventhubs.EventData{{
		Body: message,
	}}

	for i := 0; i < len(data); i++ {
		err = batch.AddEventData(data[i], nil)
		if err != nil && c.logger != nil {
			c.logger.Debugf("failed to add event data to batch %v", err)
		}
	}

	start := time.Now()

	// send the batch of events to the event hub
	if err := producer.SendEventDataBatch(ctx, batch, nil); err != nil {
		return err
	}

	end := time.Since(start)

	if c.logger != nil {
		c.logger.Debug(&Log{
			Mode:          "PUB",
			MessageValue:  strings.Join(strings.Fields(string(message)), " "),
			Topic:         topic,
			Host:          c.cfg.EventhubName,
			PubSubBackend: "EVHUB",
			Time:          end.Microseconds(),
		})
	}

	if c.metrics != nil {
		c.metrics.IncrementCounter(ctx, "app_pubsub_publish_success_count", "topic", topic)
	}

	return nil
}

// HealthCheckContext performs a context-aware deep health probe against Event Hub.
func (c *Client) HealthCheckContext(ctx context.Context) datasource.Health {
	if c == nil {
		return datasource.Health{Status: datasource.StatusDown}
	}
	if ctx == nil {
		ctx = context.Background()
	}

	c.mu.RLock()
	producer := c.producer
	consumer := c.consumer
	processor := c.processor
	name := c.cfg.EventhubName
	group := c.cfg.ConsumerGroup
	c.mu.RUnlock()

	details := map[string]any{
		"backend":  "EVHUB",
		"eventhub": name,
	}
	if producer == nil || consumer == nil || processor == nil {
		details["error"] = errClientNotConnected.Error()
		return datasource.Health{
			Status:  datasource.StatusDown,
			Details: details,
		}
	}
	if err := ctx.Err(); err != nil {
		details["error"] = err.Error()
		return datasource.Health{
			Status:  datasource.StatusDown,
			Details: details,
		}
	}
	if _, err := producer.GetEventHubProperties(ctx, nil); err != nil {
		details["error"] = err.Error()
		return datasource.Health{
			Status:  datasource.StatusDown,
			Details: details,
		}
	}

	details["consumer_group"] = group
	return datasource.Health{
		Status:  datasource.StatusUp,
		Details: details,
	}
}

func (c *Client) Health() datasource.Health {
	if c == nil {
		return datasource.Health{Status: datasource.StatusDown}
	}

	c.mu.RLock()
	connected := c.producer != nil && c.consumer != nil && c.processor != nil
	name := c.cfg.EventhubName
	group := c.cfg.ConsumerGroup
	c.mu.RUnlock()

	details := map[string]any{
		"backend":  "EVHUB",
		"eventhub": name,
	}
	if !connected {
		details["error"] = errClientNotConnected.Error()
		return datasource.Health{
			Status:  datasource.StatusDown,
			Details: details,
		}
	}

	details["consumer_group"] = group
	return datasource.Health{
		Status:  datasource.StatusUp,
		Details: details,
	}
}

func (c *Client) CreateTopic(_ context.Context, name string) error {
	// For Event Hub, creating topics is not supported, but we don't want to fail migrations
	if name == "kite_migrations" {
		return nil
	}

	if c != nil && c.logger != nil {
		c.logger.Error("topic creation is not supported in Event Hub")
	}

	return nil
}

func (c *Client) DeleteTopic(context.Context, string) error {
	if c != nil && c.logger != nil {
		c.logger.Error("topic deletion is not supported in Event Hub")
	}

	return nil
}

// Query retrieves messages from Azure Event Hub.
func (c *Client) Query(ctx context.Context, query string, args ...any) ([]byte, error) {
	if c == nil {
		return nil, errClientNotConnected
	}
	if ctx == nil {
		ctx = context.Background()
	}
	c.mu.RLock()
	consumer := c.consumer
	c.mu.RUnlock()

	if consumer == nil {
		return nil, errClientNotConnected
	}

	if query == "" {
		return nil, errEmptyTopic
	}

	if query != c.cfg.EventhubName {
		return nil, ErrTopicMismatch
	}

	startPosition, limit := c.parseQueryArgs(args...)

	// Use provided context or add default timeout
	readCtx := ctx

	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc

		readCtx, cancel = context.WithTimeout(ctx, defaultQueryTimeout)
		defer cancel()
	}

	return c.readMessages(readCtx, consumer, startPosition, limit)
}

func (c *Client) GetEventHubName() string {
	return c.cfg.EventhubName
}

// Shutdown gracefully stops the Event Hub processor and closes producer/consumer bounded by ctx.
func (c *Client) Shutdown(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	c.mu.Lock()
	producer := c.producer
	consumer := c.consumer
	cancel := c.processorCtx
	done := c.processorDone
	logger := c.logger
	c.producer = nil
	c.consumer = nil
	c.processor = nil
	c.checkPoint = nil
	c.processorCtx = nil
	c.processorDone = nil
	c.mu.Unlock()

	if cancel != nil {
		cancel()
		if logger != nil {
			logger.Debug("Event Hub processor context canceled")
		}
	}
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
		}
	}

	var lastErr error
	if producer != nil {
		if err := producer.Close(ctx); err != nil {
			if logger != nil {
				logger.Errorf("failed to close Event Hub producer: %v", err)
			}
			lastErr = err
		}
	}
	if consumer != nil {
		if err := consumer.Close(ctx); err != nil {
			if logger != nil {
				logger.Errorf("failed to close Event Hub consumer: %v", err)
			}
			lastErr = err
		}
	}

	return lastErr
}

// Close safely closes all Event Hub clients and resources.
func (c *Client) Close() error {
	return c.Shutdown(context.Background())
}
