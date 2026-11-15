package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/sllt/pi/pkg/pi"
	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/sllt/pi/pkg/pi/service"
	"github.com/sllt/pi/pkg/pi/testkit"
	"github.com/sllt/pi/pkg/pi/testutil"
)

var port int

func TestMain(m *testing.M) {
	os.Setenv("PI_TELEMETRY", "false")
	m.Run()
}

func Test_main(t *testing.T) {
	configs := testutil.NewServerConfigs(t)

	c := &http.Client{}

	go main()
	time.Sleep(100 * time.Millisecond)

	testCases := []struct {
		desc        string
		path        string
		statusCode  int
		expectedRes string
	}{
		{
			desc:        "simple service handler",
			path:        "/fact",
			expectedRes: `{"data":{"fact":"Cats have 3 eyelids.","length":20}}` + "\n",
			statusCode:  200,
		},
		{
			desc: "health check",
			path: "/.well-known/health",
			expectedRes: `{"data":{"cat-facts":{"status":"UP","details":{"host":"catfact.ninja"}},` +
				`"fact-checker":{"status":"DOWN","details":{"error":"service down","host":"catfact.ninja"}},` +
				`"name":"using-http-service","status":"DEGRADED","version":"dev"}}` + "\n",
			statusCode: 200,
		},
	}

	for i, tc := range testCases {
		req, _ := http.NewRequest(http.MethodGet, configs.HTTPHost+tc.path, nil)
		resp, err := c.Do(req)

		require.NoError(t, err, "TEST[%d], Failed.\n%s", i, tc.desc)

		bodyBytes, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "TEST[%d], Failed.\n%s", i, tc.desc)

		assert.Equal(t, tc.expectedRes, string(bodyBytes), "TEST[%d], Failed.\n%s", i, tc.desc)
		assert.Equal(t, tc.statusCode, resp.StatusCode, "TEST[%d], Failed.\n%s", i, tc.desc)

		resp.Body.Close()
	}
}

func TestHTTPHandlerURLError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet,
		fmt.Sprint("http://localhost:", port, "/handle"), bytes.NewBuffer([]byte(`{"key":"value"}`)))
	piReq := piHTTP.NewRequest(req)

	logger := logging.NewLogger(logging.DEBUG)
	mockMetrics := service.NewMockMetrics(ctrl)
	mockMetrics.EXPECT().RecordHistogram(gomock.Any(), "app_http_service_response", gomock.Any(), gomock.Any(),
		"http://invalid", "method", "GET", "status", gomock.Any())

	svc := service.NewHTTPService("http://invalid", logger, mockMetrics)
	app := testkit.New(t,
		pi.WithConfig(map[string]string{"HTTP_ENABLED": "false", "GRPC_ENABLED": "false"}),
		pi.WithLogger(logger),
		pi.WithHTTPService("cat-facts", svc, pi.Borrowed),
	)

	ctx := app.NewContext(context.Background())
	ctx.Request = piReq

	resp, err := Handler(ctx)

	assert.Nil(t, resp)
	require.Error(t, err)
}

func TestHTTPHandlerResponseUnmarshalError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{invalid body}`))
	}))
	defer server.Close()

	logger := logging.NewLogger(logging.DEBUG)
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, fmt.Sprint("http://localhost:", port, "/handle"), bytes.NewBuffer([]byte(`{"key":"value"}`)))
	piReq := piHTTP.NewRequest(req)

	svc := service.NewHTTPService(server.URL, logger, nil)
	app := testkit.New(t,
		pi.WithConfig(map[string]string{"HTTP_ENABLED": "false", "GRPC_ENABLED": "false"}),
		pi.WithLogger(logger),
		pi.WithHTTPService("cat-facts", svc, pi.Borrowed),
	)

	ctx := app.NewContext(context.Background())
	ctx.Request = piReq

	resp, err := Handler(ctx)

	assert.Nil(t, resp)
	require.Error(t, err)
}
