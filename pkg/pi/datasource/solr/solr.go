package solr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/httptrace/otelhttptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var (
	errInvalidConfig    = errors.New("solr: host and port are required")
	errSolrNotConnected = errors.New("solr: client not connected")
)

type Config struct {
	Host string
	Port string
}

type Client struct {
	mu        sync.RWMutex
	config    Config
	url       string
	logger    Logger
	metrics   Metrics
	tracer    trace.Tracer
	client    *http.Client
	connected bool
}

// New initializes Solr driver with the provided configuration.
// The Connect method must be called to establish a connection to Solr.
// Usage:
// client := New(config)
// client.UseLogger(loggerInstance)
// client.UseMetrics(metricsInstance)
// client.Connect().
func New(conf Config) *Client {
	s := &Client{config: conf}
	s.url = "http://" + conf.Host + ":" + conf.Port + "/solr"
	s.client = &http.Client{}

	return s
}

// UseLogger sets the logger for the Solr client which asserts the Logger interface.
func (c *Client) UseLogger(logger any) {
	if l, ok := logger.(Logger); ok {
		c.logger = l
	}
}

// UseMetrics sets the metrics for the Solr client which asserts the Metrics interface.
func (c *Client) UseMetrics(metrics any) {
	if m, ok := metrics.(Metrics); ok {
		c.metrics = m
	}
}

// UseTracer sets the tracer for Solr client.
func (c *Client) UseTracer(tracer any) {
	if tracer, ok := tracer.(trace.Tracer); ok {
		c.tracer = tracer
	}
}

// FromEnv constructs an unstarted Solr client from an environment lookup function.
func FromEnv(get func(string) string) (*Client, error) {
	if get == nil {
		return nil, errInvalidConfig
	}

	host := strings.TrimSpace(get("SOLR_HOST"))
	if host == "" {
		return nil, errInvalidConfig
	}

	port := strings.TrimSpace(get("SOLR_PORT"))
	if port == "" {
		port = "8983"
	} else {
		parsed, err := strconv.Atoi(port)
		if err != nil || parsed <= 0 || parsed > 65535 {
			return nil, fmt.Errorf("solr: invalid SOLR_PORT %q", port)
		}
	}

	return &Client{
		config: Config{Host: host, Port: port},
		url:    "http://" + host + ":" + port + "/solr",
	}, nil
}

func (c *Client) httpClient() *http.Client {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	hc := c.client
	c.mu.RUnlock()
	return hc
}

// ConnectContext verifies connectivity to Solr within ctx and returns any error.
func (c *Client) ConnectContext(ctx context.Context) error {
	if c == nil || strings.TrimSpace(c.config.Host) == "" || strings.TrimSpace(c.config.Port) == "" {
		return errInvalidConfig
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	c.mu.Lock()
	if c.connected && c.client != nil {
		c.mu.Unlock()
		return nil
	}
	allocatedClient := false
	if c.client == nil {
		c.client = &http.Client{}
		allocatedClient = true
	}
	c.mu.Unlock()

	rollbackClient := func() {
		if !allocatedClient {
			return
		}
		c.mu.Lock()
		if !c.connected {
			if c.client != nil {
				c.client.CloseIdleConnections()
			}
			c.client = nil
		}
		c.mu.Unlock()
	}

	if c.logger != nil {
		c.logger.Debugf("connecting to Solr at %v", c.url)
	}

	if c.metrics != nil {
		solrBuckets := []float64{.05, .075, .1, .125, .15, .2, .3, .5, .75, 1, 2, 3, 4, 5, 7.5, 10}
		c.metrics.NewHistogram("app_solr_stats", "Response time of Solr operations in milliseconds.", solrBuckets...)
	}

	res, err := c.HealthCheck(ctx)
	if err != nil {
		rollbackClient()
		return err
	}
	if r, ok := res.(Response); ok && r.Code >= http.StatusBadRequest {
		rollbackClient()
		return fmt.Errorf("solr: health check returned HTTP %d", r.Code)
	}

	c.mu.Lock()
	c.connected = true
	c.mu.Unlock()

	if c.logger != nil {
		c.logger.Infof("connected to Solr at %v", c.url)
	}

	return nil
}

// Close closes idle HTTP connections and marks the Solr client disconnected idempotently.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	hc := c.client
	c.client = nil
	c.connected = false
	c.mu.Unlock()

	if hc != nil {
		hc.CloseIdleConnections()
	}
	return nil
}

// Connect establishes a connection to Solr and registers metrics using the provided configuration when the client was Created.
func (c *Client) Connect() {
	if c.logger != nil {
		c.logger.Debugf("connecting to Solr at %v", c.url)
	}

	c.mu.Lock()
	if c.client == nil {
		c.client = &http.Client{}
	}
	c.mu.Unlock()

	if c.metrics != nil {
		solrBuckets := []float64{.05, .075, .1, .125, .15, .2, .3, .5, .75, 1, 2, 3, 4, 5, 7.5, 10}
		c.metrics.NewHistogram("app_solr_stats", "Response time of Solr operations in milliseconds.", solrBuckets...)
	}

	_, err := c.HealthCheck(context.Background())
	if err != nil {
		if c.logger != nil {
			c.logger.Errorf("error while connecting to Solr: %v", err)
		}
		return
	}

	c.mu.Lock()
	c.connected = true
	c.mu.Unlock()

	if c.logger != nil {
		c.logger.Infof("connected to Solr at %v", c.url)
	}
}

func (c *Client) HealthCheck(ctx context.Context) (any, error) {
	if c == nil || c.httpClient() == nil || strings.TrimSpace(c.url) == "" {
		return nil, errSolrNotConnected
	}
	if ctx == nil {
		ctx = context.Background()
	}

	url := c.url + "/admin/info/system?wt=json"

	startTime := time.Now()

	resp, span, err := c.call(ctx, http.MethodGet, url, nil, nil)

	defer c.sendOperationStats(ctx, &QueryLog{Type: "HealthCheck", URL: url}, startTime, "healthcheck", span)

	if err != nil {
		return resp, err
	}
	if r, ok := resp.(Response); ok && r.Code >= http.StatusBadRequest {
		return resp, fmt.Errorf("solr: health check returned HTTP %d", r.Code)
	}

	return resp, nil
}

// Search searches documents in the given collections based on the parameters specified.
// This can be used for making any queries to Solr.
func (c *Client) Search(ctx context.Context, collection string, params map[string]any) (any, error) {
	url := c.url + "/" + collection + "/select"
	startTime := time.Now()

	resp, span, err := c.call(ctx, http.MethodGet, url, params, nil)

	c.sendOperationStats(ctx, &QueryLog{Type: "Search", URL: url}, startTime, "search", span)

	return resp, err
}

// Create makes documents in the specified collection. params can be used to send parameters like commit=true.
func (c *Client) Create(ctx context.Context, collection string, document *bytes.Buffer,
	params map[string]any) (any, error) {
	url := c.url + "/" + collection + "/update"
	startTime := time.Now()

	resp, span, err := c.call(ctx, http.MethodPost, url, params, document)

	c.sendOperationStats(ctx, &QueryLog{Type: "Create", URL: url}, startTime, "create", span)

	return resp, err
}

// Update updates documents in the specified collection. params can be used to send parameters like commit=true.
func (c *Client) Update(ctx context.Context, collection string, document *bytes.Buffer,
	params map[string]any) (any, error) {
	url := c.url + "/" + collection + "/update"
	startTime := time.Now()

	resp, span, err := c.call(ctx, http.MethodPost, url, params, document)

	c.sendOperationStats(ctx, &QueryLog{Type: "Update", URL: url}, startTime, "update", span)

	return resp, err
}

// Delete deletes documents in the specified collection. params can be used to send parameters like commit=true.
func (c *Client) Delete(ctx context.Context, collection string, document *bytes.Buffer,
	params map[string]any) (any, error) {
	url := c.url + "/" + collection + "/update"
	startTime := time.Now()

	resp, span, err := c.call(ctx, http.MethodPost, url, params, document)

	c.sendOperationStats(ctx, &QueryLog{Type: "Delete", URL: url}, startTime, "delete", span)

	return resp, err
}

// ListFields retrieves all the fields in the schema for the specified collection.
// params can be used to send query parameters like wt, fl, includeDynamic etc.
func (c *Client) ListFields(ctx context.Context, collection string, params map[string]any) (any, error) {
	url := c.url + "/" + collection + "/schema/fields"
	startTime := time.Now()

	resp, span, err := c.call(ctx, http.MethodGet, url, params, nil)

	c.sendOperationStats(ctx, &QueryLog{Type: "ListFields", URL: url}, startTime, "list-fields", span)

	return resp, err
}

// Retrieve retrieves the entire schema that includes all the fields,field types,dynamic rules and copy field rules.
// params can be used to specify the format of response.
func (c *Client) Retrieve(ctx context.Context, collection string, params map[string]any) (any, error) {
	url := c.url + "/" + collection + "/schema"
	startTime := time.Now()

	resp, span, err := c.call(ctx, http.MethodGet, url, params, nil)

	c.sendOperationStats(ctx, &QueryLog{Type: "Retrieve", URL: url}, startTime, "retrieve", span)

	return resp, err
}

// AddField adds Field in the schema for the specified collection.
func (c *Client) AddField(ctx context.Context, collection string, document *bytes.Buffer) (any, error) {
	url := c.url + "/" + collection + "/schema"
	startTime := time.Now()

	resp, span, err := c.call(ctx, http.MethodPost, url, nil, document)

	c.sendOperationStats(ctx, &QueryLog{Type: "AddField", URL: url}, startTime, "add-field", span)

	return resp, err
}

// UpdateField updates the field definitions in the schema for the specified collection.
func (c *Client) UpdateField(ctx context.Context, collection string, document *bytes.Buffer) (any, error) {
	url := c.url + "/" + collection + "/schema"
	startTime := time.Now()

	resp, span, err := c.call(ctx, http.MethodPost, url, nil, document)

	c.sendOperationStats(ctx, &QueryLog{Type: "UpdateField", URL: url}, startTime, "update-field", span)

	return resp, err
}

// DeleteField deletes the field definitions in the schema for the specified collection.
func (c *Client) DeleteField(ctx context.Context, collection string, document *bytes.Buffer) (any, error) {
	url := c.url + "/" + collection + "/schema"
	startTime := time.Now()

	resp, span, err := c.call(ctx, http.MethodPost, url, nil, document)

	c.sendOperationStats(ctx, &QueryLog{Type: "DeleteField", URL: url}, startTime, "delete-field", span)

	return resp, err
}

// Response stores the response from Solr.
type Response struct {
	Code int
	Data any
}

// call forms the http request and makes a call to solr and populates the solr response.
func (c *Client) call(ctx context.Context, method, url string, params map[string]any, body io.Reader) (any, trace.Span, error) {
	if c == nil {
		return nil, nil, errSolrNotConnected
	}

	var span trace.Span

	if c.tracer != nil {
		ctx, span = c.tracer.Start(ctx, fmt.Sprintf("Solr %s", method),
			trace.WithAttributes(
				attribute.String("solr.url", url),
			),
		)
	}

	ctx = httptrace.WithClientTrace(ctx, otelhttptrace.NewClientTrace(ctx))

	req, err := c.createRequest(ctx, method, url, params, body)
	if err != nil {
		return nil, span, err
	}

	hc := c.httpClient()
	if hc == nil {
		return nil, span, errSolrNotConnected
	}

	resp, err := hc.Do(req)
	if err != nil {
		return nil, span, err
	}
	defer resp.Body.Close()

	var respBody any

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, span, err
	}

	err = json.Unmarshal(b, &respBody)
	if err != nil {
		return nil, span, err
	}

	if span != nil {
		span.SetAttributes(
			attribute.Int("http.status_code", resp.StatusCode),
		)
	}

	return Response{resp.StatusCode, respBody}, span, nil
}

func (*Client) createRequest(ctx context.Context, method, url string, params map[string]any, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}

	if method != http.MethodGet {
		req.Header.Add("Content-Type", "application/json")
	}

	q := req.URL.Query()

	for k, val := range params {
		switch v := val.(type) {
		case []string:
			for _, val := range v {
				q.Add(k, val)
			}
		default:
			q.Add(k, fmt.Sprint(val))
		}
	}

	req.URL.RawQuery = q.Encode()

	return req, nil
}

func (c *Client) sendOperationStats(ctx context.Context, ql *QueryLog, startTime time.Time, method string, span trace.Span) {
	duration := time.Since(startTime).Microseconds()

	ql.Duration = duration

	if c.logger != nil {
		c.logger.Debug(ql)
	}

	if c.metrics != nil {
		c.metrics.RecordHistogram(ctx, "app_solr_stats", float64(duration),
			"type", ql.Type)
	}

	if span != nil {
		defer span.End()

		span.SetAttributes(
			attribute.String("solr.type", ql.Type),
			attribute.Int64(fmt.Sprintf("solr.%v.duration", method), duration))
	}
}
