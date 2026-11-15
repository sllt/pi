package nats

import (
	"context"

	"github.com/sllt/pi/pkg/pi/datasource"
	"github.com/sllt/pi/pkg/pi/datasource/pubsub"
)

// PubSubWrapper adapts Client to pubsub.JetStreamClient.
type PubSubWrapper struct {
	Client *Client
}

func (w *PubSubWrapper) Query(ctx context.Context, query string, args ...any) ([]byte, error) {
	if w == nil || w.Client == nil {
		return nil, errClientNotConnected
	}
	return w.Client.Query(ctx, query, args...)
}

// Publish publishes a message to a topic.
func (w *PubSubWrapper) Publish(ctx context.Context, topic string, message []byte) error {
	if w == nil || w.Client == nil {
		return errClientNotConnected
	}
	return w.Client.Publish(ctx, topic, message)
}

// Subscribe subscribes to a topic and returns a single message.
func (w *PubSubWrapper) Subscribe(ctx context.Context, topic string) (*pubsub.Message, error) {
	if w == nil || w.Client == nil {
		return nil, errClientNotConnected
	}
	return w.Client.Subscribe(ctx, topic)
}

// CreateTopic creates a new topic (stream) in NATS jStream.
func (w *PubSubWrapper) CreateTopic(ctx context.Context, name string) error {
	if w == nil || w.Client == nil {
		return errClientNotConnected
	}
	return w.Client.CreateTopic(ctx, name)
}

// DeleteTopic deletes a topic (stream) in NATS jStream.
func (w *PubSubWrapper) DeleteTopic(ctx context.Context, name string) error {
	if w == nil || w.Client == nil {
		return errClientNotConnected
	}
	return w.Client.DeleteTopic(ctx, name)
}

// Shutdown gracefully closes the NATS client bounded by ctx.
func (w *PubSubWrapper) Shutdown(ctx context.Context) error {
	if w == nil || w.Client == nil {
		return nil
	}
	return w.Client.Close(ctx)
}

// Close closes the Client.
func (w *PubSubWrapper) Close() error {
	return w.Shutdown(context.Background())
}

// Health returns the health status of the Client.
func (w *PubSubWrapper) Health() datasource.Health {
	if w == nil || w.Client == nil {
		return datasource.Health{Status: datasource.StatusDown}
	}
	return w.Client.Health()
}

// ConnectContext establishes a connection to NATS bounded by ctx without spawning background retry loops on failure.
func (w *PubSubWrapper) ConnectContext(ctx context.Context) error {
	if w == nil || w.Client == nil {
		return errClientNotConnected
	}
	return w.Client.ConnectContext(ctx)
}

// Connect establishes a connection to NATS.
func (w *PubSubWrapper) Connect() {
	if w == nil || w.Client == nil {
		return
	}
	if w.Client.connManager != nil && w.Client.connManager.Health().Status == datasource.StatusUp {
		if w.Client.logger != nil {
			w.Client.logger.Log("NATS connection already established")
		}
		return
	}

	err := w.Client.Connect()
	if err != nil && w.Client.logger != nil {
		w.Client.logger.Errorf("PubSubWrapper: Error connecting to NATS: %v", err)
	}
}

// UseLogger sets the logger for the NATS client.
func (w *PubSubWrapper) UseLogger(logger any) {
	if w != nil && w.Client != nil {
		w.Client.UseLogger(logger)
	}
}

// UseMetrics sets the metrics for the NATS client.
func (w *PubSubWrapper) UseMetrics(metrics any) {
	if w != nil && w.Client != nil {
		w.Client.UseMetrics(metrics)
	}
}

// UseTracer sets the tracer for the NATS client.
func (w *PubSubWrapper) UseTracer(tracer any) {
	if w != nil && w.Client != nil {
		w.Client.UseTracer(tracer)
	}
}
