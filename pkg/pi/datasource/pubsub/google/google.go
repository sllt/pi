// Package google provides a client for interacting with Google Cloud Pub/Sub.This package facilitates interaction with
// Google Cloud Pub/Sub, allowing publishing and subscribing to topics, managing subscriptions, and handling messages.
package google

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	gcPubSub "cloud.google.com/go/pubsub"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/api/iterator"

	"github.com/sllt/pi/pkg/pi/datasource/pubsub"
)

var (
	errProjectIDNotProvided    = errors.New("google project id not provided")
	errSubscriptionNotProvided = errors.New("subscription name not provided")
	errClientNotConnected      = errors.New("google pubsub client is not connected")
	errTopicName               = errors.New("empty topic name")
)

const (
	defaultRetryInterval = 10 * time.Second
	messageBufferSize    = 100
)

type Config struct {
	ProjectID        string
	SubscriptionName string
}

type googleClient struct {
	Config

	client      Client
	logger      pubsub.Logger
	metrics     Metrics
	receiveChan map[string]chan *pubsub.Message
	subStarted  map[string]struct{}
	mu          sync.RWMutex
	subCtx      context.Context
	subCancel   context.CancelFunc
	subWG       sync.WaitGroup
	closed      bool
	stopCh      chan struct{}
	stopOnce    sync.Once
	retryDone   chan struct{}
	tracer      trace.Tracer
}

func (g *googleClient) UseTracer(tracer any) {
	if g == nil {
		return
	}
	if t, ok := tracer.(trace.Tracer); ok && t != nil {
		g.mu.Lock()
		g.tracer = t
		g.mu.Unlock()
	}
}

func (g *googleClient) getTracer() trace.Tracer {
	if g != nil {
		g.mu.RLock()
		t := g.tracer
		g.mu.RUnlock()
		if t != nil {
			return t
		}
	}
	return otel.GetTracerProvider().Tracer("pi")
}

const (
	defaultQueryTimeout = 30 * time.Second
	defaultMessageLimit = 10
)

//nolint:revive // We do not want anyone using the client without initialization steps.
func New(conf Config, logger pubsub.Logger, metrics Metrics) *googleClient {
	err := validateConfigs(&conf)
	if err != nil {
		logger.Errorf("could not configure google pubsub, error: %v", err)

		return nil
	}

	logger.Debugf("connecting to google pubsub client with projectID '%s' and subscriptionName '%s", conf.ProjectID, conf.SubscriptionName)

	var client googleClient

	client.Config = conf
	client.logger = logger
	client.metrics = metrics
	client.receiveChan = make(map[string]chan *pubsub.Message)
	client.subStarted = make(map[string]struct{})
	client.mu = sync.RWMutex{}
	client.subCtx, client.subCancel = context.WithCancel(context.Background())

	client.stopCh = make(chan struct{})

	gClient, err := connect(conf, logger)
	if err != nil {
		client.retryDone = make(chan struct{})
		go retryConnect(conf, logger, &client, client.stopCh, client.retryDone)

		return &client
	}

	client.client = gClient

	return &client
}

func connect(conf Config, logger pubsub.Logger) (*gcPubSub.Client, error) {
	return connectWithContext(context.Background(), conf, logger)
}

func connectWithContext(parent context.Context, conf Config, logger pubsub.Logger) (*gcPubSub.Client, error) {
	ctx, cancel := context.WithTimeout(parent, defaultRetryInterval)
	defer cancel()

	client, err := gcPubSub.NewClient(ctx, conf.ProjectID)
	if err != nil {
		logger.Errorf("could not create Google PubSub client, error: %v", err)
		return nil, err
	}

	it := client.Topics(ctx)

	_, err = it.Next()
	if err != nil {
		if errors.Is(err, iterator.Done) {
			logger.Debugf("no topics found in Google PubSub")
			return client, nil
		}

		_ = client.Close()
		logger.Errorf("google pubsub connection validation failed, error: %v", err)

		return nil, err
	}

	logger.Logf("connected to google pubsub client, projectID: %s", client.Project())

	return client, nil
}

func (g *googleClient) Publish(ctx context.Context, topic string, message []byte) error {
	ctx, span := g.getTracer().Start(ctx, "publish-gcp")
	defer span.End()

	g.metrics.IncrementCounter(ctx, "app_pubsub_publish_total_count", "topic", topic)

	t, err := g.getTopic(ctx, topic)
	if err != nil {
		g.logger.Errorf("could not create topic '%s', error: %v", topic, err)

		return err
	}

	start := time.Now()
	result := t.Publish(ctx, &gcPubSub.Message{
		Data:        message,
		PublishTime: time.Now(),
	})
	end := time.Since(start)

	_, err = result.Get(ctx)
	if err != nil {
		g.logger.Errorf("error publishing to google topic '%s', error: %v", topic, err)

		return err
	}

	g.logger.Debug(&pubsub.Log{
		Mode:          "PUB",
		CorrelationID: span.SpanContext().TraceID().String(),
		MessageValue:  string(message),
		Topic:         topic,
		Host:          g.ProjectID,
		PubSubBackend: "GCP",
		Time:          end.Microseconds(),
	})

	g.metrics.IncrementCounter(ctx, "app_pubsub_publish_success_count", "topic", topic)

	return nil
}

func (g *googleClient) ensureSubContextLocked() context.Context {
	if g.receiveChan == nil {
		g.receiveChan = make(map[string]chan *pubsub.Message)
	}
	if g.subStarted == nil {
		g.subStarted = make(map[string]struct{})
	}
	if g.subCtx == nil {
		g.subCtx, g.subCancel = context.WithCancel(context.Background())
	}
	return g.subCtx
}

func (g *googleClient) Subscribe(ctx context.Context, topic string) (*pubsub.Message, error) {
	g.mu.RLock()
	hasClient := g.client != nil
	isClosed := g.closed
	g.mu.RUnlock()

	if !hasClient {
		return nil, nil
	}
	if isClosed {
		return nil, errClientNotConnected
	}

	if !g.isConnected() {
		timer := time.NewTimer(defaultRetryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, errors.Join(errClientNotConnected, ctx.Err())
		case <-timer.C:
			return nil, errClientNotConnected
		}
	}

	spanCtx, span := g.getTracer().Start(ctx, "gcp-subscribe")
	defer span.End()

	g.metrics.IncrementCounter(spanCtx, "app_pubsub_subscribe_total_count", "topic", topic, "subscription_name", g.Config.SubscriptionName)

	g.mu.RLock()
	_, started := g.subStarted[topic]
	ch := g.receiveChan[topic]
	g.mu.RUnlock()

	if !started {
		t, err := g.getTopic(spanCtx, topic)
		if err != nil {
			return nil, err
		}

		subscription, err := g.getSubscription(spanCtx, t)
		if err != nil {
			return nil, err
		}

		g.mu.Lock()
		if g.closed {
			g.mu.Unlock()
			return nil, errClientNotConnected
		}
		if _, already := g.subStarted[topic]; !already {
			subCtx := g.ensureSubContextLocked()
			if existing, ok := g.receiveChan[topic]; ok && existing != nil {
				ch = existing
			} else {
				ch = make(chan *pubsub.Message)
				g.receiveChan[topic] = ch
			}
			g.subStarted[topic] = struct{}{}
			g.subWG.Add(1)

			msgChan := ch
			go func() {
				defer g.subWG.Done()
				recvErr := subscription.Receive(subCtx, func(cbCtx context.Context, msg *gcPubSub.Message) {
					m := pubsub.NewMessage(cbCtx)
					m.Topic = topic
					m.Value = msg.Data
					m.MetaData = msg.Attributes
					m.Committer = newGoogleMessage(msg)

					select {
					case msgChan <- m:
					case <-subCtx.Done():
						msg.Nack()
					}
				})
				if recvErr != nil && !errors.Is(recvErr, context.Canceled) {
					g.logger.Errorf("error getting a message from google: %s", recvErr.Error())
				}
			}()
		} else {
			ch = g.receiveChan[topic]
		}
		g.mu.Unlock()
	}

	if ch == nil {
		return nil, nil
	}

	fetchStart := time.Now()
	select {
	case m, ok := <-ch:
		if !ok || m == nil {
			return nil, nil
		}
		fetchDur := time.Since(fetchStart)
		g.metrics.IncrementCounter(spanCtx, "app_pubsub_subscribe_success_count", "topic", topic, "subscription_name",
			g.Config.SubscriptionName)

		g.logger.Debug(&pubsub.Log{
			Mode:          "SUB",
			CorrelationID: span.SpanContext().TraceID().String(),
			MessageValue:  string(m.Value),
			Topic:         topic,
			Host:          g.Config.ProjectID,
			PubSubBackend: "GCP",
			Time:          fetchDur.Microseconds(),
		})

		return m, nil
	case <-ctx.Done():
		return nil, nil
	}
}

func (g *googleClient) Query(ctx context.Context, query string, args ...any) ([]byte, error) {
	if !g.isConnected() {
		return nil, errClientNotConnected
	}

	if query == "" {
		return nil, errTopicName
	}

	timeout, limit := parseQueryArgs(args...)

	// Get topic and subscription
	topic, err := g.getTopic(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic: %w", err)
	}

	subscription, err := g.getQuerySubscription(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("failed to get subscription: %w", err)
	}

	msgChan := make(chan []byte, messageBufferSize)
	queryCtx, cancel := context.WithTimeout(ctx, timeout)

	defer cancel()

	// Start receiving messages
	go func() {
		defer close(msgChan)

		receiveCtx, receiveCancel := context.WithTimeout(queryCtx, timeout)
		defer receiveCancel()

		err := subscription.Receive(receiveCtx, func(_ context.Context, msg *gcPubSub.Message) {
			defer msg.Ack()

			select {
			case msgChan <- msg.Data:
			case <-receiveCtx.Done():
				return
			default:
				// Channel might be full, try non-blocking send
				g.logger.Debugf("Query: message channel full for topic %s", query)
			}
		})

		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			g.logger.Debugf("Query: receive ended for topic %s: %v", query, err)
		}
	}()

	// Collect messages
	return g.collectMessages(queryCtx, msgChan, limit), nil
}

func (g *googleClient) getTopic(ctx context.Context, topic string) (*gcPubSub.Topic, error) {
	if g.client == nil {
		return nil, errClientNotConnected
	}

	t := g.client.Topic(topic)

	// check if topic exists, if not create the topic
	if ok, err := t.Exists(ctx); !ok || err != nil {
		_, errTopicCreate := g.client.CreateTopic(ctx, topic)
		if errTopicCreate != nil {
			return nil, errTopicCreate
		}
	}

	return t, nil
}

func (g *googleClient) getSubscription(ctx context.Context, topic *gcPubSub.Topic) (*gcPubSub.Subscription, error) {
	if g.client == nil {
		return nil, errClientNotConnected
	}

	subscription := g.client.Subscription(g.SubscriptionName + "-" + topic.ID())

	// check if subscription already exists or not
	ok, err := subscription.Exists(context.Background())
	if err != nil {
		g.logger.Errorf("unable to check the existence of subscription, error: %v", err.Error())

		return nil, err
	}

	// if subscription is not present, create a new
	if !ok {
		subscription, err = g.client.CreateSubscription(ctx, g.SubscriptionName+"-"+topic.ID(), gcPubSub.SubscriptionConfig{
			Topic: topic,
		})
		if err != nil {
			return nil, err
		}
	}

	return subscription, nil
}

func (g *googleClient) DeleteTopic(ctx context.Context, name string) error {
	if g.client == nil {
		return errClientNotConnected
	}

	err := g.client.Topic(name).Delete(ctx)
	if err != nil && strings.Contains(err.Error(), "Topic not found") {
		return nil
	}

	return err
}

func (g *googleClient) CreateTopic(ctx context.Context, name string) error {
	if g.client == nil {
		return errClientNotConnected
	}

	_, err := g.client.CreateTopic(ctx, name)
	if err != nil && strings.Contains(err.Error(), "Topic already exists") {
		return nil
	}

	return err
}

func (g *googleClient) Close() error {
	if g.stopCh != nil {
		g.stopOnce.Do(func() {
			close(g.stopCh)
		})
	}
	if g.retryDone != nil {
		<-g.retryDone
	}

	g.mu.Lock()
	g.closed = true
	subCancel := g.subCancel
	c := g.client
	g.mu.Unlock()

	if subCancel != nil {
		subCancel()
	}
	g.subWG.Wait()

	g.mu.Lock()
	chans := g.receiveChan
	g.receiveChan = nil
	g.subStarted = nil
	g.mu.Unlock()

	var closeErr error
	if c != nil {
		closeErr = c.Close()
	}

	for _, ch := range chans {
		if ch != nil {
			close(ch)
		}
	}

	return closeErr
}

func (*googleClient) AckSemantics() pubsub.AckSemantics {
	return pubsub.AckSemantics{
		Mode:                  pubsub.AckModeIndependent,
		SupportsContextCommit: false,
		SupportsNack:          true,
	}
}

func retryConnect(conf Config, logger pubsub.Logger, g *googleClient, stopCh <-chan struct{}, done chan<- struct{}) {
	if done != nil {
		defer close(done)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if stopCh != nil {
		go func() {
			select {
			case <-stopCh:
				cancel()
			case <-ctx.Done():
			}
		}()
	}

	timer := time.NewTimer(defaultRetryInterval)
	defer timer.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-timer.C:
		}

		client, err := connectWithContext(ctx, conf, logger)
		if err == nil {
			g.mu.Lock()
			g.client = client
			g.mu.Unlock()

			logger.Logf("connected to google pubsub client, projectID: %s", conf.ProjectID)

			return
		}
		if ctx.Err() != nil {
			return
		}

		logger.Errorf("could not connect to Google PubSub, error: %v", err)

		timer.Reset(defaultRetryInterval)
	}
}
