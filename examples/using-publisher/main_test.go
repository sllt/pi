package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sllt/pi/pkg/pi"
	"github.com/sllt/pi/pkg/pi/datasource"
	"github.com/sllt/pi/pkg/pi/datasource/pubsub"
	"github.com/sllt/pi/pkg/pi/testkit"
	"github.com/sllt/pi/pkg/pi/testutil"
)

func TestMain(m *testing.M) {
	os.Setenv("PI_TELEMETRY", "false")
	m.Run()
}

func TestExamplePublisherError(t *testing.T) {
	t.Setenv("PUBSUB_BROKER", "localhost:1092")

	configs := testutil.NewServerConfigs(t)
	host := fmt.Sprint("http://localhost:", configs.HTTPPort)

	go main()
	time.Sleep(200 * time.Millisecond)

	testCases := []struct {
		desc               string
		path               string
		body               []byte
		expectedStatusCode int
	}{
		{"valid order", "/publish-order", []byte(`{"data":{"orderId":"123","status":"pending"}}`), http.StatusInternalServerError},
		{"valid product", "/publish-product", []byte(`{"data":{"productId":"123","price":"599"}}`), http.StatusInternalServerError},
	}

	client := http.Client{}

	for i, tc := range testCases {
		req, _ := http.NewRequest(http.MethodPost, host+tc.path, bytes.NewBuffer(tc.body))
		req.Header.Set("content-type", "application/json")

		resp, err := client.Do(req)
		require.NoError(t, err, "TEST[%d] %s failed", i, tc.desc)
		defer resp.Body.Close()

		assert.Equal(t, tc.expectedStatusCode, resp.StatusCode, "TEST[%d] %s failed", i, tc.desc)
	}
}

func TestOrderFunction(t *testing.T) {
	tests := []struct {
		name           string
		body           string
		expectError    bool
		publishError   error
		expectedResult interface{}
	}{
		{"valid order", `{"orderId":"123","status":"pending"}`, false, nil, "Published"},
		{"invalid JSON", `{"orderId":,"status":"pending"}`, true, nil, nil},
		{"publish error", `{"orderId":"123","status":"pending"}`, true, fmt.Errorf("publish failed"), nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			ps := &stubPubSub{
				publishFn: func(_ context.Context, topic string, _ []byte) error {
					called = true
					assert.Equal(t, "order-logs", topic)
					return tt.publishError
				},
			}

			testHandler(t, tt.name, order, ps, tt.body, tt.expectError, tt.expectedResult)
			if tt.publishError != nil || !tt.expectError {
				assert.True(t, called, "expected Publish to be invoked")
			}
		})
	}
}

func TestProductFunction(t *testing.T) {
	tests := []struct {
		name           string
		body           string
		expectError    bool
		publishError   error
		expectedResult interface{}
	}{
		{"valid product", `{"productId":"123","price":"599"}`, false, nil, "Published"},
		{"invalid JSON", `{"productId":,"price":"599"}`, true, nil, nil},
		{"publish error", `{"productId":"123","price":"599"}`, true, fmt.Errorf("publish failed"), nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			ps := &stubPubSub{
				publishFn: func(_ context.Context, topic string, _ []byte) error {
					called = true
					assert.Equal(t, "products", topic)
					return tt.publishError
				},
			}

			testHandler(t, tt.name, product, ps, tt.body, tt.expectError, tt.expectedResult)
			if tt.publishError != nil || !tt.expectError {
				assert.True(t, called, "expected Publish to be invoked")
			}
		})
	}
}

func testHandler(t *testing.T, name string, handler func(*pi.Context) (interface{}, error),
	ps pubsub.Client, body string, expectError bool, expectedResult interface{}) {

	t.Run(name, func(t *testing.T) {
		app := testkit.New(
			t,
			pi.WithConfig(map[string]string{"HTTP_ENABLED": "false", "GRPC_ENABLED": "false"}),
			pi.WithPubSub(ps, pi.Borrowed),
		)
		ctx := app.NewContext(context.Background())
		ctx.Request = &testRequest{
			Request: httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(body))),
			body:    body,
		}

		result, err := handler(ctx)

		assert.Equal(t, expectError, err != nil, "error presence mismatch")
		assert.Equal(t, expectedResult, result, "result mismatch")
	})
}

type stubPubSub struct {
	publishFn func(ctx context.Context, topic string, message []byte) error
}

func (s *stubPubSub) Publish(ctx context.Context, topic string, message []byte) error {
	if s.publishFn != nil {
		return s.publishFn(ctx, topic, message)
	}
	return nil
}

func (*stubPubSub) Subscribe(context.Context, string) (*pubsub.Message, error) { return nil, nil }
func (*stubPubSub) Health() datasource.Health                                  { return datasource.Health{Status: "UP"} }
func (*stubPubSub) CreateTopic(context.Context, string) error                  { return nil }
func (*stubPubSub) DeleteTopic(context.Context, string) error                  { return nil }
func (*stubPubSub) Query(context.Context, string, ...any) ([]byte, error)      { return nil, nil }
func (*stubPubSub) Close() error                                               { return nil }

type testRequest struct {
	*http.Request
	body string
}

func (r *testRequest) Bind(v interface{}) error {
	return json.Unmarshal([]byte(r.body), v)
}

func (r *testRequest) Param(key string) string     { return r.URL.Query().Get(key) }
func (r *testRequest) PathParam(key string) string { return "" }
func (r *testRequest) HostName() string            { return r.Host }
func (r *testRequest) Params(key string) []string  { return r.URL.Query()[key] }
