// Package clickhouse provides a client for interacting with ClickHouse databases,
// supporting query execution, asynchronous inserts, and observability integration
// through logging, metrics, and tracing.
//
// It is designed to be used with the Pi framework and allows configuration
// of connection parameters, observability tools, and health checks.
package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Config struct {
	Hosts    string // Comma-separated list of ClickHouse server addresses.
	Username string // Username used for authentication.
	Password string // Password used for authentication.
	Database string // Name of the database to connect to.
}

type metricsLoop struct {
	mu      sync.Mutex
	stopCh  chan struct{}
	doneCh  chan struct{}
	stopped bool
}

// Client is a ClickHouse client implementation that wraps a Conn interface.
// It provides methods for executing queries, performing inserts, and collecting metrics.
type Client struct {
	mu      sync.RWMutex
	conn    Conn
	config  Config
	logger  Logger
	metrics Metrics
	tracer  trace.Tracer
	loop    *metricsLoop
}

var (
	errStatusDown         = errors.New("status down")
	errInvalidConfig      = errors.New("clickhouse: hosts are required")
	errClientNotConnected = errors.New("clickhouse: client not connected")
)

func (c *Client) getConn() Conn {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()
	return conn
}

// New initializes ClickHouse client with the provided configuration.
// Metrics, Logger has to be initialized before calling the Connect method.
// Usage:
//
//	client.UseLogger(Logger())
//	client.UseMetrics(Metrics())
//
//	client.Connect()
func New(config Config) *Client {
	return &Client{
		config: config,
		loop:   &metricsLoop{},
	}
}

// UseLogger sets the logger for the ClickHouse client.
func (c *Client) UseLogger(logger any) {
	if l, ok := logger.(Logger); ok {
		c.logger = l
	}
}

// UseMetrics sets the metrics for the ClickHouse client.
func (c *Client) UseMetrics(metrics any) {
	if m, ok := metrics.(Metrics); ok {
		c.metrics = m
	}
}

// UseTracer sets the tracer for ClickHouse client.
func (c *Client) UseTracer(tracer any) {
	if t, ok := tracer.(trace.Tracer); ok {
		c.tracer = t
	}
}

// FromEnv constructs an unstarted ClickHouse client from an environment lookup function.
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

	hosts := firstNonEmpty("CLICKHOUSE_HOSTS", "CLICKHOUSE_HOST")
	if hosts == "" {
		return nil, errInvalidConfig
	}

	return New(Config{
		Hosts:    hosts,
		Database: firstNonEmpty("CLICKHOUSE_DATABASE", "CLICKHOUSE_DB"),
		Username: firstNonEmpty("CLICKHOUSE_USERNAME", "CLICKHOUSE_USER"),
		Password: get("CLICKHOUSE_PASSWORD"),
	}), nil
}

// ConnectContext opens and pings ClickHouse within ctx, closing the connection and returning any error on failure.
func (c *Client) ConnectContext(ctx context.Context) error {
	if c == nil || strings.TrimSpace(c.config.Hosts) == "" {
		return errInvalidConfig
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.getConn() != nil {
		return nil
	}

	if c.logger != nil {
		c.logger.Debugf("connecting to Clickhouse db at %v to database %v", c.config.Hosts, c.config.Database)
	}

	if c.metrics != nil {
		clickHouseBuckets := []float64{.05, .075, .1, .125, .15, .2, .3, .5, .75, 1, 2, 3, 4, 5, 7.5, 10}
		c.metrics.NewHistogram("app_clickhouse_stats", "Response time of Clickhouse queries in microseconds.", clickHouseBuckets...)
		c.metrics.NewGauge("app_clickhouse_open_connections", "Number of open Clickhouse connections.")
		c.metrics.NewGauge("app_clickhouse_idle_connections", "Number of idle Clickhouse connections.")
	}

	var addresses []string
	for _, raw := range strings.Split(c.config.Hosts, ",") {
		if addr := strings.TrimSpace(raw); addr != "" {
			addresses = append(addresses, addr)
		}
	}
	if len(addresses) == 0 {
		return errInvalidConfig
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: addresses,
		Auth: clickhouse.Auth{
			Database: c.config.Database,
			Username: c.config.Username,
			Password: c.config.Password,
		},
	})
	if err != nil {
		return err
	}

	if err = conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return err
	}

	c.mu.Lock()
	if c.conn != nil {
		c.mu.Unlock()
		_ = conn.Close()
		return nil
	}
	c.conn = conn
	c.mu.Unlock()

	if c.logger != nil {
		c.logger.Logf("successfully connected to ClickhouseDB")
	}

	if c.metrics != nil {
		if c.loop == nil {
			c.loop = &metricsLoop{}
		}
		c.loop.mu.Lock()
		if !c.loop.stopped && c.loop.doneCh == nil {
			stopCh := make(chan struct{})
			doneCh := make(chan struct{})
			c.loop.stopCh = stopCh
			c.loop.doneCh = doneCh
			go pushDBMetrics(conn, c.metrics, stopCh, doneCh)
		}
		c.loop.mu.Unlock()
	}

	return nil
}

// Connect establishes a connection to ClickHouse and registers metrics using the provided configuration when the client was Created.
func (c *Client) Connect() {
	if c == nil {
		return
	}

	if c.logger != nil {
		c.logger.Debugf("connecting to Clickhouse db at %v to database %v", c.config.Hosts, c.config.Database)
	}

	if c.metrics != nil {
		clickHouseBuckets := []float64{.05, .075, .1, .125, .15, .2, .3, .5, .75, 1, 2, 3, 4, 5, 7.5, 10}
		c.metrics.NewHistogram("app_clickhouse_stats", "Response time of Clickhouse queries in microseconds.", clickHouseBuckets...)

		c.metrics.NewGauge("app_clickhouse_open_connections", "Number of open Clickhouse connections.")
		c.metrics.NewGauge("app_clickhouse_idle_connections", "Number of idle Clickhouse connections.")
	}

	addresses := strings.Split(c.config.Hosts, ",")

	ctx := context.Background()

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: addresses,
		Auth: clickhouse.Auth{
			Database: c.config.Database,
			Username: c.config.Username,
			Password: c.config.Password,
		},
	})
	if err != nil {
		if c.logger != nil {
			c.logger.Errorf("error while connecting to Clickhouse %v", err)
		}

		return
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	if err = conn.Ping(ctx); err != nil {
		if c.logger != nil {
			c.logger.Errorf("ping failed with error %v", err)
		}
	} else if c.logger != nil {
		c.logger.Logf("successfully connected to ClickhouseDB")
	}

	if c.loop == nil {
		c.loop = &metricsLoop{}
	}

	c.loop.mu.Lock()
	if !c.loop.stopped && c.loop.doneCh == nil {
		stopCh := make(chan struct{})
		doneCh := make(chan struct{})
		c.loop.stopCh = stopCh
		c.loop.doneCh = doneCh
		go pushDBMetrics(conn, c.metrics, stopCh, doneCh)
	}
	c.loop.mu.Unlock()
}

// Close stops the background ClickHouse metrics poller and closes the underlying driver connection.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}

	if c.loop != nil {
		c.loop.mu.Lock()
		if !c.loop.stopped {
			c.loop.stopped = true
			if c.loop.stopCh != nil {
				close(c.loop.stopCh)
			}
		}
		doneCh := c.loop.doneCh
		c.loop.mu.Unlock()

		if doneCh != nil {
			<-doneCh
		}
	}

	c.mu.Lock()
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()

	if closer, ok := conn.(io.Closer); ok {
		return closer.Close()
	}

	return nil
}

func pushDBMetrics(conn Conn, metrics Metrics, stopCh <-chan struct{}, doneCh chan struct{}) {
	defer close(doneCh)

	if conn == nil || metrics == nil {
		return
	}

	const frequency = 10

	stats := conn.Stats()
	metrics.SetGauge("app_clickhouse_open_connections", float64(stats.Open))
	metrics.SetGauge("app_clickhouse_idle_connections", float64(stats.Idle))

	ticker := time.NewTicker(frequency * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			stats := conn.Stats()
			metrics.SetGauge("app_clickhouse_open_connections", float64(stats.Open))
			metrics.SetGauge("app_clickhouse_idle_connections", float64(stats.Idle))
		}
	}
}

// Exec should be used for DDL and simple statements.
// It should not be used for larger inserts or query iterations.
func (c *Client) Exec(ctx context.Context, query string, args ...any) error {
	conn := c.getConn()
	if conn == nil {
		return errClientNotConnected
	}

	tracedCtx, span := c.addTrace(ctx, "exec", query)

	err := conn.Exec(tracedCtx, query, args...)

	defer c.sendOperationStats(time.Now(), "Exec", query, "exec", span, args...)

	return err
}

// Select method allows a set of response rows to be marshaled into a slice of structs with a single invocation..
// DB column names should be defined in the struct in `ch` tag.
// Example Usages:
//
//	type User struct {
//		Id   string `ch:"id"`
//		Name string `ch:"name"`
//		Age  string `ch:"age"`
//	}
//
// var user []User
// ch, err := pi.Use[*clickhouse.Client](ctx)
// err = ch.Select(ctx, &user, "SELECT * FROM users")
func (c *Client) Select(ctx context.Context, dest any, query string, args ...any) error {
	conn := c.getConn()
	if conn == nil {
		return errClientNotConnected
	}

	tracedCtx, span := c.addTrace(ctx, "select", query)

	err := conn.Select(tracedCtx, dest, query, args...)

	defer c.sendOperationStats(time.Now(), "Select", query, "select", span, args...)

	return err
}

// AsyncInsert allows the user to specify whether the client should wait for the server to complete the insert or
// respond once the data has been received.
func (c *Client) AsyncInsert(ctx context.Context, query string, wait bool, args ...any) error {
	conn := c.getConn()
	if conn == nil {
		return errClientNotConnected
	}

	tracedCtx, span := c.addTrace(ctx, "async-insert", query)

	err := conn.AsyncInsert(tracedCtx, query, wait, args...)

	defer c.sendOperationStats(time.Now(), "AsyncInsert", query, "async-insert", span, args...)

	return err
}

// sendOperationStats records the duration of a database operation and logs the query context.
// It also attaches metrics and trace attributes if enabled.
func (c *Client) sendOperationStats(start time.Time, methodType, query string, method string,
	span trace.Span, args ...any) {
	duration := time.Since(start).Microseconds()

	if c.logger != nil {
		c.logger.Debug(&Log{
			Type:     methodType,
			Query:    query,
			Duration: duration,
			Args:     args,
		})
	}

	if span != nil {
		defer span.End()

		span.SetAttributes(attribute.Int64(fmt.Sprintf("clickhouse.%v.duration", method), duration))
	}

	if c.metrics != nil {
		c.metrics.RecordHistogram(context.Background(), "app_clickhouse_stats", float64(duration), "hosts", c.config.Hosts,
			"database", c.config.Database, "type", getOperationType(query))
	}
}

// getOperationType extracts the operation type (e.g., SELECT, INSERT) from a query.
func getOperationType(query string) string {
	query = strings.TrimSpace(query)
	words := strings.Split(query, " ")

	return strings.ToUpper(words[0])
}

type Health struct {
	Status  string         `json:"status,omitempty"`
	Details map[string]any `json:"details,omitempty"`
}

// HealthCheck checks the health of the ClickHouse client by pinging the database.
func (c *Client) HealthCheck(ctx context.Context) (any, error) {
	h := Health{
		Details: make(map[string]any),
	}
	if c == nil {
		h.Status = "DOWN"
		h.Details["message"] = "clickhouse client is nil"
		return &h, errStatusDown
	}

	h.Details["host"] = c.config.Hosts
	h.Details["database"] = c.config.Database

	conn := c.getConn()
	if conn == nil {
		h.Status = "DOWN"
		h.Details["message"] = "clickhouse not connected"
		return &h, errStatusDown
	}

	if ctx == nil {
		ctx = context.Background()
	}

	err := conn.Ping(ctx)
	if err != nil {
		h.Status = "DOWN"

		return &h, errStatusDown
	}

	h.Status = "UP"

	return &h, nil
}

// addTrace starts a new trace span for the given operation and query.
func (c *Client) addTrace(ctx context.Context, method, query string) (context.Context, trace.Span) {
	if c.tracer != nil {
		contextWithTrace, span := c.tracer.Start(ctx, fmt.Sprintf("clickhouse-%v", method))

		span.SetAttributes(
			attribute.String("clickhouse.query", query),
		)

		return contextWithTrace, span
	}

	return ctx, nil
}
