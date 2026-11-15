package main

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sllt/pi/pkg/pi"
	"github.com/sllt/pi/pkg/pi/testkit"
	"github.com/sllt/pi/pkg/pi/testutil"
)

func TestMain(m *testing.M) {
	os.Setenv("PI_TELEMETRY", "false")

	m.Run()
}

func TestMainInitialization(t *testing.T) {
	conn, err := net.DialTimeout("tcp", "localhost:9092", 200*time.Millisecond)
	if err != nil {
		t.Skipf("skipping live Kafka broker test: localhost:9092 unavailable (%v)", err)
	}
	_ = conn.Close()

	_ = testutil.NewServerConfigs(t)

	log := testutil.StdoutOutputForFunc(func() {
		go main()

		time.Sleep(200 * time.Millisecond)
	})

	expectedLog := "connected to 1 Kafka brokers"
	if !strings.Contains(log, expectedLog) {
		t.Errorf("Expected log to contain %q, but got: %s", expectedLog, log)
	}
}

func newTestSubscriberContext(t *testing.T, req pi.Request) *pi.Context {
	t.Helper()

	app := testkit.New(t, pi.WithConfig(map[string]string{
		"HTTP_ENABLED": "false",
		"GRPC_ENABLED": "false",
	}))
	ctx := app.NewContext(context.Background())
	ctx.Request = req
	return ctx
}

type errorRequest struct{}

func (e *errorRequest) Context() context.Context {
	return context.Background()
}

func (e *errorRequest) Bind(v interface{}) error    { return errors.New("bind error") }
func (e *errorRequest) Param(key string) string     { return "" }
func (e *errorRequest) PathParam(key string) string { return "" }
func (e *errorRequest) HostName() string            { return "" }
func (e *errorRequest) Params(key string) []string  { return nil }

func TestProductSubscribe_BindError(t *testing.T) {
	ctx := newTestSubscriberContext(t, &errorRequest{})

	err := productHandler(ctx)

	if err != nil {
		t.Errorf("Expected nil error, got %v", err)
	}
}

func TestOrderSubscribe_BindError(t *testing.T) {
	ctx := newTestSubscriberContext(t, &errorRequest{})

	err := orderHandler(ctx)
	if err != nil {
		t.Errorf("Expected nil error, got %v", err)
	}
}

type successProductRequest struct{}

func (r *successProductRequest) Context() context.Context {
	return context.Background()
}

func (r *successProductRequest) Bind(v interface{}) error {
	p := v.(*struct {
		ProductId string `json:"productId"`
		Price     string `json:"price"`
	})
	p.ProductId = "123"
	p.Price = "599"
	return nil
}

func (r *successProductRequest) Param(string) string     { return "" }
func (r *successProductRequest) PathParam(string) string { return "" }
func (r *successProductRequest) HostName() string        { return "" }
func (r *successProductRequest) Params(string) []string  { return nil }

func TestProductHandler_Success(t *testing.T) {
	ctx := newTestSubscriberContext(t, &successProductRequest{})

	err := productHandler(ctx)
	if err != nil {
		t.Errorf("Expected nil error, got %v", err)
	}
}

type successOrderRequest struct{}

func (r *successOrderRequest) Context() context.Context {
	return context.Background()
}

func (r *successOrderRequest) Bind(v interface{}) error {
	o := v.(*struct {
		OrderId string `json:"orderId"`
		Status  string `json:"status"`
	})
	o.OrderId = "456"
	o.Status = "pending"
	return nil
}
func (r *successOrderRequest) Param(string) string     { return "" }
func (r *successOrderRequest) PathParam(string) string { return "" }
func (r *successOrderRequest) HostName() string        { return "" }
func (r *successOrderRequest) Params(string) []string  { return nil }

func TestOrderHandler_Success(t *testing.T) {
	ctx := newTestSubscriberContext(t, &successOrderRequest{})
	err := orderHandler(ctx)
	if err != nil {
		t.Errorf("Expected nil error, got %v", err)
	}
}
