package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-redis/redismock/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/sllt/pi/pkg/pi"
	"github.com/sllt/pi/pkg/pi/config"
	"github.com/sllt/pi/pkg/pi/datasource/redis"
	piSQL "github.com/sllt/pi/pkg/pi/datasource/sql"
	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/sllt/pi/pkg/pi/service"
	"github.com/sllt/pi/pkg/pi/testkit"
	"github.com/sllt/pi/pkg/pi/testutil"
)

func TestMain(m *testing.M) {
	os.Setenv("PI_TELEMETRY", "false")
	m.Run()
}

func TestIntegration_SimpleAPIServer(t *testing.T) {
	httpPort := testutil.GetFreePort(t)
	port := testutil.GetFreePort(t)

	t.Setenv("HTTP_PORT", strconv.Itoa(httpPort))
	t.Setenv("METRICS_PORT", strconv.Itoa(port))

	host := fmt.Sprintf("http://localhost:%d", httpPort)

	go main()
	time.Sleep(100 * time.Millisecond) // Giving some time to start the server

	tests := []struct {
		desc string
		path string
		body any
	}{
		{"hello handler", "/hello", "Hello World!"},
		{"hello handler with query parameter", "/hello?name=pi", "Hello pi!"},
		{"redis handler", "/redis", ""},
		{"mysql handler", "/mysql", float64(4)},
	}

	for i, tc := range tests {
		req, _ := http.NewRequest(http.MethodGet, host+tc.path, nil)
		req.Header.Set("content-type", "application/json")

		c := http.Client{}
		resp, err := c.Do(req)
		require.NoError(t, err, "TEST[%d], Failed.\n%s", i, tc.desc)

		var data = struct {
			Data any `json:"data"`
		}{}

		b, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "TEST[%d], Failed.\n%s", i, tc.desc)

		_ = json.Unmarshal(b, &data)

		assert.Equal(t, tc.body, data.Data, "TEST[%d], Failed.\n%s", i, tc.desc)
		assert.Equal(t, http.StatusOK, resp.StatusCode, "TEST[%d], Failed.\n%s", i, tc.desc)

		resp.Body.Close()
	}
}

func TestIntegration_SimpleAPIServer_Errors(t *testing.T) {
	httpPort := testutil.GetFreePort(t)
	port := testutil.GetFreePort(t)

	t.Setenv("HTTP_PORT", strconv.Itoa(httpPort))
	t.Setenv("METRICS_PORT", strconv.Itoa(port))

	host := fmt.Sprintf("http://localhost:%d", httpPort)

	go main()
	time.Sleep(100 * time.Millisecond) // Giving some time to start the server

	tests := []struct {
		desc       string
		path       string
		body       any
		statusCode int
	}{
		{
			desc:       "error handler called",
			path:       "/error",
			statusCode: http.StatusInternalServerError,
			body:       map[string]any{"message": "some error occurred"},
		},
		{
			desc:       "empty route",
			path:       "/",
			statusCode: http.StatusNotFound,
			body:       map[string]any{"message": "route not registered"},
		},
		{
			desc:       "route not registered with the server",
			path:       "/route",
			statusCode: http.StatusNotFound,
			body:       map[string]any{"message": "route not registered"},
		},
	}

	for i, tc := range tests {
		req, _ := http.NewRequest(http.MethodGet, host+tc.path, nil)
		req.Header.Set("content-type", "application/json")

		c := http.Client{}
		resp, err := c.Do(req)
		require.NoError(t, err, "TEST[%d], Failed.\n%s", i, tc.desc)

		var data = struct {
			Error any `json:"error"`
		}{}

		b, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "TEST[%d], Failed.\n%s", i, tc.desc)

		_ = json.Unmarshal(b, &data)

		assert.Equal(t, tc.body, data.Error, "TEST[%d], Failed.\n%s", i, tc.desc)
		assert.Equal(t, tc.statusCode, resp.StatusCode, "TEST[%d], Failed.\n%s", i, tc.desc)

		resp.Body.Close()
	}
}

func TestIntegration_SimpleAPIServer_Health(t *testing.T) {
	httpPort := testutil.GetFreePort(t)
	port := testutil.GetFreePort(t)

	t.Setenv("HTTP_PORT", strconv.Itoa(httpPort))
	t.Setenv("METRICS_PORT", strconv.Itoa(port))

	host := fmt.Sprintf("http://localhost:%d", httpPort)

	go main()
	time.Sleep(100 * time.Millisecond) // Giving some time to start the server

	tests := []struct {
		desc       string
		path       string
		statusCode int
	}{
		{"health handler", "/.well-known/health", http.StatusOK},
		{"favicon handler", "/favicon.ico", http.StatusOK},
	}

	for i, tc := range tests {
		req, _ := http.NewRequest(http.MethodGet, host+tc.path, nil)
		req.Header.Set("content-type", "application/json")

		c := http.Client{}
		resp, err := c.Do(req)

		require.NoError(t, err, "TEST[%d], Failed.\n%s", i, tc.desc)
		assert.Equal(t, tc.statusCode, resp.StatusCode, "TEST[%d], Failed.\n%s", i, tc.desc)
	}
}

func TestRedisHandler(t *testing.T) {
	logger := logging.NewLogger(logging.DEBUG)
	redisClient, mock := redismock.NewClientMock()

	rc := redis.NewClient(config.NewMockConfig(map[string]string{"REDIS_HOST": "localhost", "REDIS_PORT": "2001"}), logger, nil)
	rc.Client = redisClient

	mock.ExpectGet("test").SetErr(testutil.CustomError{ErrorMessage: "redis get error"})

	app := testkit.New(t,
		pi.WithConfig(map[string]string{"HTTP_ENABLED": "false", "GRPC_ENABLED": "false"}),
		pi.WithLogger(logger),
		pi.WithRedis(rc, pi.Borrowed),
	)

	ctx := app.NewContext(t.Context())
	resp, err := RedisHandler(ctx)

	assert.Nil(t, resp)
	require.Error(t, err)
}

func createTestContext(t *testing.T, method, url string, opts ...pi.Option) *pi.Context {
	t.Helper()
	req := httptest.NewRequest(method, url, nil)
	req.Header.Set("Content-Type", "application/json")

	baseOpts := []pi.Option{
		pi.WithConfig(map[string]string{"HTTP_ENABLED": "false", "GRPC_ENABLED": "false"}),
		pi.WithLogger(logging.NewLogger(logging.DEBUG)),
	}
	baseOpts = append(baseOpts, opts...)
	app := testkit.New(t, baseOpts...)

	ctx := app.NewContext(req.Context())
	ctx.Request = piHTTP.NewRequest(req)
	return ctx
}

func TestHelloHandler(t *testing.T) {
	ctx := createTestContext(t, http.MethodGet, "/hello?name=test")
	resp, err := HelloHandler(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "Hello test!", resp)

	ctx = createTestContext(t, http.MethodGet, "/hello")
	resp, err = HelloHandler(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "Hello World!", resp)
}

func TestErrorHandler(t *testing.T) {
	ctx := createTestContext(t, http.MethodGet, "/error")

	resp, err := ErrorHandler(ctx)
	assert.Nil(t, resp)
	assert.Error(t, err)
	assert.Equal(t, "some error occurred", err.Error())
}

func TestMysqlHandler(t *testing.T) {
	sqlDB, sqlMock, _ := piSQL.NewSQLMocks(t)
	defer sqlDB.Close()

	sqlMock.ExpectQuery("select 2\\+2").
		WillReturnRows(sqlMock.NewRows([]string{"value"}).AddRow(4))

	ctx := createTestContext(t, http.MethodGet, "/mysql", pi.WithSQL(sqlDB, pi.Borrowed))

	resp, err := MysqlHandler(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 4, resp)
}

func TestTraceHandler(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	redisClient, redisMock := redismock.NewClientMock()
	redisMock.MatchExpectationsInOrder(false)
	for range 5 {
		redisMock.ExpectPing().SetVal("PONG")
	}
	rc := &redis.Redis{Client: redisClient}

	mockHTTP := service.NewMockHTTP(ctrl)
	mockResp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(`{"data":"mock data"}`)),
	}
	mockHTTP.EXPECT().Get(gomock.Any(), "redis", nil).Return(mockResp, nil)

	ctx := createTestContext(
		t,
		http.MethodGet,
		"/trace",
		pi.WithRedis(rc, pi.Borrowed),
		pi.WithHTTPService("anotherService", mockHTTP, pi.Borrowed),
	)

	resp, err := TraceHandler(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "mock data", resp)
	assert.NoError(t, redisMock.ExpectationsWereMet())
}
