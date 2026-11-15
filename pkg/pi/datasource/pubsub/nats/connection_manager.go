package nats

import (
	"context"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sllt/pi/pkg/pi/datasource"
	"github.com/sllt/pi/pkg/pi/datasource/pubsub"
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

type contextDialer struct {
	ctx context.Context
}

func (d contextDialer) Dial(network, address string) (net.Conn, error) {
	var nd net.Dialer
	return nd.DialContext(d.ctx, network, address)
}

//go:generate mockgen -destination=mock_jetstream.go -package=nats github.com/nats-io/nats.go/jetstream jStream,Stream,Consumer,Msg,MessageBatch

type ConnectionManager struct {
	conn             ConnInterface
	jStream          jetstream.JetStream
	config           *Config
	logger           pubsub.Logger
	natsConnector    Connector
	jetStreamCreator JetStreamCreator
}

func (cm *ConnectionManager) jetStream() (jetstream.JetStream, error) {
	if cm.jStream == nil {
		return nil, errJetStreamNotConfigured
	}

	return cm.jStream, nil
}

// natsConnWrapper wraps a nats.Conn to implement the ConnInterface.
type natsConnWrapper struct {
	conn *nats.Conn
}

func (w *natsConnWrapper) Status() nats.Status {
	return w.conn.Status()
}

func (w *natsConnWrapper) Close() {
	w.conn.Close()
}

func (w *natsConnWrapper) NATSConn() *nats.Conn {
	return w.conn
}

func (w *natsConnWrapper) JetStream() (jetstream.JetStream, error) {
	return jetstream.New(w.conn)
}

// NewConnectionManager creates a new ConnectionManager.
func NewConnectionManager(
	cfg *Config,
	logger pubsub.Logger,
	natsConnector Connector,
	jetStreamCreator JetStreamCreator) *ConnectionManager {
	// if logger is nil panic
	if logger == nil {
		panic("logger is required")
	}

	if natsConnector == nil {
		natsConnector = &defaultConnector{}
	}

	if jetStreamCreator == nil {
		jetStreamCreator = &DefaultJetStreamCreator{}
	}

	return &ConnectionManager{
		config:           cfg,
		logger:           logger,
		natsConnector:    natsConnector,
		jetStreamCreator: jetStreamCreator,
	}
}

// Connect establishes a connection to NATS and sets up JetStream.
func (cm *ConnectionManager) Connect() error {
	opts := []nats.Option{nats.Name("Pi NATS JetStreamClient")}

	if cm.config.CredsFile != "" {
		opts = append(opts, nats.UserCredentials(cm.config.CredsFile))
	}

	return cm.connectWithOpts(opts...)
}

// ConnectContext establishes a connection to NATS bounded by ctx.
func (cm *ConnectionManager) ConnectContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	opts := []nats.Option{
		nats.Name("Pi NATS JetStreamClient"),
		nats.SetCustomDialer(contextDialer{ctx: ctx}),
	}
	if dl, ok := ctx.Deadline(); ok {
		rem := time.Until(dl)
		if rem <= 0 {
			return context.DeadlineExceeded
		}
		opts = append(opts, nats.Timeout(rem))
	}
	if cm.config.CredsFile != "" {
		opts = append(opts, nats.UserCredentials(cm.config.CredsFile))
	}

	return cm.connectWithOpts(opts...)
}

func (cm *ConnectionManager) connectWithOpts(opts ...nats.Option) error {
	safeServer := redactServerURL(cm.config.Server)
	connInterface, err := cm.natsConnector.Connect(cm.config.Server, opts...)
	if err != nil {
		if cm.logger != nil {
			cm.logger.Debugf("Failed to connect to NATS server at %v: %v", safeServer, err)
		}
		return err
	}

	js, err := cm.jetStreamCreator.New(connInterface)
	if err != nil {
		connInterface.Close()
		if cm.logger != nil {
			cm.logger.Debugf("Failed to create jStream context: %v", err)
		}

		return err
	}

	cm.conn = connInterface
	cm.jStream = js
	if cm.logger != nil {
		cm.logger.Logf("Successfully connected to NATS server at %v", safeServer)
	}

	return nil
}

func (cm *ConnectionManager) Close(_ context.Context) {
	if cm.conn != nil {
		cm.conn.Close()
	}
}

func (cm *ConnectionManager) Publish(ctx context.Context, subject string, message []byte, metrics Metrics) error {
	if metrics != nil {
		metrics.IncrementCounter(ctx, "app_pubsub_publish_total_count", "subject", subject)
	}

	if !cm.isConnected() {
		return errClientNotConnected
	}

	if err := cm.validateJetStream(subject); err != nil {
		return err
	}

	_, err := cm.jStream.Publish(ctx, subject, message)
	if err != nil {
		if cm.logger != nil {
			cm.logger.Errorf("failed to publish message to NATS jStream: %v", err)
		}
		return err
	}

	if metrics != nil {
		metrics.IncrementCounter(ctx, "app_pubsub_publish_success_count", "subject", subject)
	}

	return nil
}

func (cm *ConnectionManager) validateJetStream(subject string) error {
	if cm.jStream == nil || subject == "" {
		err := errJetStreamNotConfigured
		if cm.logger != nil {
			cm.logger.Error(err.Error())
		}

		return err
	}

	return nil
}

func (cm *ConnectionManager) Health() datasource.Health {
	if cm.conn == nil {
		return datasource.Health{
			Status: datasource.StatusDown,
		}
	}

	safeServer := redactServerURL(cm.config.Server)
	status := cm.conn.Status()
	if status == nats.CONNECTED {
		return datasource.Health{
			Status: datasource.StatusUp,
			Details: map[string]any{
				"server": safeServer,
			},
		}
	}

	return datasource.Health{
		Status: datasource.StatusDown,
		Details: map[string]any{
			"server": safeServer,
		},
	}
}

func (cm *ConnectionManager) isConnected() bool {
	if cm.conn == nil {
		return false
	}

	return cm.conn.Status() == nats.CONNECTED
}
