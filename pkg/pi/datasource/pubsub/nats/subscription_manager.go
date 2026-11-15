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
)

const (
	consumeMessageDelay = 100 * time.Millisecond
)

type SubscriptionManager struct {
	subscriptions map[string]*subscription
	subMutex      sync.Mutex
	topicBuffers  map[string]chan *pubsub.Message
	bufferMutex   sync.RWMutex
	bufferSize    int
}

type subscription struct {
	cancel context.CancelFunc
	done   <-chan struct{}
}

func newSubscriptionManager(bufferSize int) *SubscriptionManager {
	return &SubscriptionManager{
		subscriptions: make(map[string]*subscription),
		topicBuffers:  make(map[string]chan *pubsub.Message),
		bufferSize:    bufferSize,
	}
}

func (sm *SubscriptionManager) Subscribe(
	ctx context.Context,
	topic string,
	js jetstream.JetStream,
	cfg *Config,
	logger pubsub.Logger,
	metrics Metrics) (*pubsub.Message, error) {
	if metrics != nil {
		metrics.IncrementCounter(ctx, "app_pubsub_subscribe_total_count", "topic", topic)
	}

	if err := sm.validateSubscribePrerequisites(js, cfg); err != nil {
		return nil, err
	}

	sm.subMutex.Lock()

	sub, exists := sm.subscriptions[topic]
	if !exists {
		cons, err := sm.createOrUpdateConsumer(ctx, js, topic, cfg)
		if err != nil {
			sm.subMutex.Unlock()
			return nil, err
		}

		subCtx, cancel := context.WithCancel(context.Background())
		sub = &subscription{cancel: cancel, done: subCtx.Done()}
		sm.subscriptions[topic] = sub

		buffer := sm.getOrCreateBuffer(topic)
		go sm.consumeMessages(subCtx, cons, topic, buffer, cfg, logger)
	}
	var subDone <-chan struct{}
	if sub != nil {
		subDone = sub.done
	}

	sm.subMutex.Unlock()

	buffer := sm.getOrCreateBuffer(topic)

	select {
	case msg := <-buffer:
		if metrics != nil {
			metrics.IncrementCounter(ctx, "app_pubsub_subscribe_success_count", "topic", topic)
		}
		return msg, nil
	case <-subDone:
		return nil, errClientNotConnected
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (*SubscriptionManager) validateSubscribePrerequisites(js jetstream.JetStream, cfg *Config) error {
	if js == nil {
		return errJetStreamNotConfigured
	}

	if cfg.Consumer == "" {
		return errConsumerNotProvided
	}

	return nil
}

func (sm *SubscriptionManager) getOrCreateBuffer(topic string) chan *pubsub.Message {
	sm.bufferMutex.Lock()
	defer sm.bufferMutex.Unlock()

	if buffer, exists := sm.topicBuffers[topic]; exists {
		return buffer
	}

	buffer := make(chan *pubsub.Message, sm.bufferSize)
	sm.topicBuffers[topic] = buffer

	return buffer
}

func (*SubscriptionManager) createOrUpdateConsumer(
	ctx context.Context, js jetstream.JetStream, topic string, cfg *Config) (jetstream.Consumer, error) {
	consumerName := fmt.Sprintf("%s_%s", cfg.Consumer, strings.ReplaceAll(topic, ".", "_"))
	cons, err := js.CreateOrUpdateConsumer(ctx, cfg.Stream.Stream, jetstream.ConsumerConfig{
		Durable:       consumerName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: topic,
		MaxDeliver:    cfg.Stream.MaxDeliver,
		DeliverPolicy: jetstream.DeliverNewPolicy,
		AckWait:       defaultAckWait,
	})

	return cons, err
}

func (sm *SubscriptionManager) consumeMessages(
	ctx context.Context,
	cons jetstream.Consumer,
	topic string,
	buffer chan *pubsub.Message,
	cfg *Config,
	logger pubsub.Logger) {
	// TODO: propagate errors to caller
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := sm.fetchAndProcessMessages(ctx, cons, topic, buffer, cfg, logger); err != nil && logger != nil {
				logger.Errorf("Error fetching messages for topic %s: %v", topic, err)
			}
		}
	}
}

func (sm *SubscriptionManager) fetchAndProcessMessages(
	ctx context.Context,
	cons jetstream.Consumer,
	topic string,
	buffer chan *pubsub.Message,
	cfg *Config,
	logger pubsub.Logger) error {
	maxWait := cfg.MaxWait
	if maxWait <= 0 {
		maxWait = time.Second
	}
	msgs, err := cons.Fetch(1, jetstream.FetchMaxWait(maxWait))
	if err != nil {
		return sm.handleFetchError(ctx, err, topic, logger)
	}

	return sm.processFetchedMessages(msgs, topic, buffer, logger)
}

func (*SubscriptionManager) handleFetchError(ctx context.Context, err error, topic string, logger pubsub.Logger) error {
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) && logger != nil {
		logger.Errorf("Error fetching messages for topic %s: %v", topic, err)
	}

	timer := time.NewTimer(consumeMessageDelay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-timer.C:
	}

	return nil
}

func (sm *SubscriptionManager) processFetchedMessages(
	msgs jetstream.MessageBatch,
	topic string,
	buffer chan *pubsub.Message,
	logger pubsub.Logger) error {
	for msg := range msgs.Messages() {
		pubsubMsg := sm.createPubSubMessage(msg, topic)

		if !sm.sendToBuffer(pubsubMsg, buffer) && logger != nil {
			logger.Logf("Message buffer is full for topic %s. Consider increasing buffer size or processing messages faster.", topic)
		}
	}

	return sm.checkBatchError(msgs, topic, logger)
}

func (*SubscriptionManager) createPubSubMessage(msg jetstream.Msg, topic string) *pubsub.Message {
	pubsubMsg := pubsub.NewMessage(context.Background()) // Pass a context if needed
	pubsubMsg.Topic = topic
	pubsubMsg.Value = msg.Data()
	pubsubMsg.MetaData = msg.Headers()
	pubsubMsg.Committer = &natsCommitter{msg: msg}

	return pubsubMsg
}

func (*SubscriptionManager) sendToBuffer(msg *pubsub.Message, buffer chan *pubsub.Message) bool {
	select {
	case buffer <- msg:
		return true
	default:
		return false
	}
}

func (*SubscriptionManager) checkBatchError(msgs jetstream.MessageBatch, topic string, logger pubsub.Logger) error {
	if err := msgs.Error(); err != nil {
		if logger != nil {
			logger.Errorf("Error in message batch for topic %s: %v", topic, err)
		}

		return err
	}

	return nil
}

func (sm *SubscriptionManager) Close() {
	sm.subMutex.Lock()

	for _, sub := range sm.subscriptions {
		if sub != nil && sub.cancel != nil {
			sub.cancel()
		}
	}

	sm.subscriptions = make(map[string]*subscription)
	sm.subMutex.Unlock()

	sm.bufferMutex.Lock()
	sm.topicBuffers = make(map[string]chan *pubsub.Message)
	sm.bufferMutex.Unlock()
}
