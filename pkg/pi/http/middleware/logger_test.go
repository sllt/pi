package middleware

import (
	"bufio"
	"bytes"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/sllt/pi/pkg/pi/testutil"
)

func Test_getIPAddress(t *testing.T) {
	{
		// When RemoteAddr is set
		addr := "0.0.0.0:8080"
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://dummy", http.NoBody)

		require.NoError(t, err, "TEST Failed.\n")

		req.RemoteAddr = addr
		ip := getIPAddress(req)

		assert.Equal(t, addr, ip, "TEST Failed.\n")
	}

	{
		// When `X-Forwarded-For` header is set
		addr := "192.168.0.1:8080"
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://dummy", http.NoBody)

		require.NoError(t, err, "TEST Failed.\n")

		req.Header.Set("X-Forwarded-For", addr)
		ip := getIPAddress(req)

		assert.Equal(t, addr, ip, "TEST Failed.\n")
	}

	{
		// When `X-Forwarded-For` has multiple comma-separated IPs
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://dummy", http.NoBody)
		require.NoError(t, err)

		req.RemoteAddr = "127.0.0.1:9000"
		req.Header.Set("X-Forwarded-For", " 10.0.0.1 , 10.0.0.2 ")
		assert.Equal(t, "10.0.0.1", getIPAddress(req))
	}

	{
		// When `X-Forwarded-For` has empty first segment, falls back to RemoteAddr
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://dummy", http.NoBody)
		require.NoError(t, err)

		req.RemoteAddr = "127.0.0.1:9000"
		req.Header.Set("X-Forwarded-For", " , 10.0.0.2")
		assert.Equal(t, "127.0.0.1:9000", getIPAddress(req))
	}
}

func Test_LoggingMiddleware(t *testing.T) {
	logs := testutil.StdoutOutputForFunc(func() {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://dummy", http.NoBody)

		rr := httptest.NewRecorder()
		probes := LogProbes{
			Disabled: false,
		}

		handler := Logging(probes, logging.NewMockLogger(logging.DEBUG))(http.HandlerFunc(testHandler))

		handler.ServeHTTP(rr, req)
	})

	assert.Contains(t, logs, "GET    200")
}

func Test_LoggingMiddlewareProbesEnable(t *testing.T) {
	logs := testutil.StdoutOutputForFunc(func() {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://dummy/.well-known/alive", http.NoBody)

		rr := httptest.NewRecorder()
		probes := LogProbes{
			Disabled: false,
			Paths:    []string{"/.well-known/alive", "/.well-known/health"},
		}

		handler := Logging(probes, logging.NewMockLogger(logging.DEBUG))(http.HandlerFunc(testHandler))

		handler.ServeHTTP(rr, req)
	})

	assert.Contains(t, logs, "GET    200")
}

func Test_LoggingMiddlewareProbesDisable(t *testing.T) {
	logs := testutil.StdoutOutputForFunc(func() {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://dummy/.well-known/alive", http.NoBody)

		rr := httptest.NewRecorder()
		probes := LogProbes{
			Disabled: true,
			Paths:    []string{"/.well-known/alive", "/.well-known/health"},
		}

		handler := Logging(probes, logging.NewMockLogger(logging.DEBUG))(http.HandlerFunc(testHandler))

		handler.ServeHTTP(rr, req)
	})

	assert.Empty(t, logs, "TEST Failed.\n")
}

func Test_LoggingMiddlewareError(t *testing.T) {
	logs := testutil.StderrOutputForFunc(func() {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://dummy", http.NoBody)

		rr := httptest.NewRecorder()
		probes := LogProbes{
			Disabled: false,
		}

		handler := Logging(probes, logging.NewMockLogger(logging.ERROR))(http.HandlerFunc(testHandlerError))

		handler.ServeHTTP(rr, req)
	})

	assert.Contains(t, logs, "GET    500")
}

// Test handler that uses the middleware.
func testHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("Test Handler"))
}

// Test handler for internalServerErrors that uses the middleware.
func testHandlerError(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusInternalServerError)
	_, _ = w.Write([]byte("error"))
}

func Test_LoggingMiddlewareStringPanicHandling(t *testing.T) {
	logs := testutil.StderrOutputForFunc(func() {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://dummy", http.NoBody)

		rr := httptest.NewRecorder()
		probes := LogProbes{
			Disabled: false,
		}

		handler := Logging(probes, logging.NewMockLogger(logging.DEBUG))(http.HandlerFunc(testStringPanicHandler))

		handler.ServeHTTP(rr, req)
	})

	assert.Contains(t, logs, "github.com/sllt/pi/pkg/pi/http/middleware.testStringPanicHandler")
}

// Test handler that uses the middleware.
func testStringPanicHandler(_ http.ResponseWriter, r *http.Request) {
	panic(r.URL.Path)
}

func Test_LoggingMiddlewareErrorPanicHandling(t *testing.T) {
	logs := testutil.StderrOutputForFunc(func() {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://dummy", http.NoBody)

		rr := httptest.NewRecorder()
		probes := LogProbes{
			Disabled: false,
		}

		handler := Logging(probes, logging.NewMockLogger(logging.DEBUG))(http.HandlerFunc(testErrorPanicHandler))

		handler.ServeHTTP(rr, req)
	})

	assert.Contains(t, logs, "github.com/sllt/pi/pkg/pi/http/middleware.testErrorPanicHandler")
}

// Test handler that uses the middleware.
func testErrorPanicHandler(http.ResponseWriter, *http.Request) {
	panic(testutil.CustomError{ErrorMessage: "panic"})
}

func Test_LoggingMiddlewareUnknownPanicHandling(t *testing.T) {
	logs := testutil.StderrOutputForFunc(func() {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://dummy", http.NoBody)

		rr := httptest.NewRecorder()
		probes := LogProbes{
			Disabled: false,
		}

		handler := Logging(probes, logging.NewMockLogger(logging.DEBUG))(http.HandlerFunc(testUnknownPanicHandler))

		handler.ServeHTTP(rr, req)
	})

	assert.Contains(t, logs, "github.com/sllt/pi/pkg/pi/http/middleware.testUnknownPanicHandler")
}

// Test handler that uses the middleware.
func testUnknownPanicHandler(w http.ResponseWriter, _ *http.Request) {
	panic(w)
}

func TestRequestLog_PrettyPrint(t *testing.T) {
	rl := &RequestLog{
		TraceID:      "7e5c0e9a58839071d4d006dd1d0f4f3a",
		SpanID:       "b19d9aa6323b29bb",
		StartTime:    "2024-04-16T13:34:35.761893+05:30",
		ResponseTime: 1432,
		Method:       "GET",
		UserAgent:    "",
		IP:           "[::1]:59614",
		URI:          "/test",
		Response:     200,
	}
	w := new(bytes.Buffer)
	rl.PrettyPrint(w)

	assert.Equal(t, "\u001B[38;5;8m7e5c0e9a58839071d4d006dd1d0f4f3a \u001B[38;5;34m200   \u001B[0m"+
		"     1432\u001B[38;5;8mµs\u001B[0m GET /test \n", w.String())
}

func TestRequestLog_PrettyPrint_OmitsEmptyOrZeroTraceID(t *testing.T) {
	for _, traceID := range []string{"", "00000000000000000000000000000000"} {
		rl := &RequestLog{
			TraceID:      traceID,
			ResponseTime: 1432,
			Method:       "GET",
			URI:          "/test",
			Response:     200,
		}
		w := new(bytes.Buffer)
		rl.PrettyPrint(w)

		assert.NotContains(t, w.String(), "00000000000000000000000000000000")
		assert.Equal(t, "\u001B[38;5;34m200   \u001B[0m     1432\u001B[38;5;8mµs\u001B[0m GET /test \n", w.String())
	}

	req := httptest.NewRequest(http.MethodGet, "http://dummy/no-span", http.NoBody)
	rr := httptest.NewRecorder()
	handler := Logging(LogProbes{}, logging.NewMockLogger(logging.INFO))(http.HandlerFunc(testHandler))
	handler.ServeHTTP(rr, req)
	assert.Empty(t, rr.Header().Get("X-Correlation-ID"))
}

func Test_ColorForStatusCode(t *testing.T) {
	testCases := []struct {
		desc   string
		code   int
		expOut int
	}{
		{desc: "200 OK", code: 200, expOut: 34},
		{desc: "201 Created", code: 201, expOut: 34},
		{desc: "400 Bad Request", code: 400, expOut: 220},
		{desc: "409 Conflict", code: 409, expOut: 220},
		{desc: "500 Internal Srv Error", code: 500, expOut: 202},
		{desc: "unknown status code", code: 0, expOut: 0},
	}

	for _, tc := range testCases {
		out := colorForStatusCode(tc.code)

		assert.Equal(t, tc.expOut, out)
	}
}

func Test_StatusResponseWriter_WriteHeader(t *testing.T) {
	tests := []struct {
		name           string
		status         int
		expectedStatus int
	}{
		{"WriteHeader 200", 200, 200},
		{"WriteHeader 404", 404, 404},
		{"WriteHeader 500", 500, 500},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			srw := &StatusResponseWriter{ResponseWriter: rr}

			srw.WriteHeader(tt.status)

			require.Equal(t, tt.expectedStatus, srw.status, "status mismatch")
			require.True(t, srw.wroteHeader, "expected wroteHeader to be true")
			require.Equal(t, tt.expectedStatus, rr.Code, "recorder status mismatch")
		})
	}
}

func Test_StatusResponseWriter_WriteHeader_DuplicateCalls(t *testing.T) {
	rr := httptest.NewRecorder()
	srw := &StatusResponseWriter{ResponseWriter: rr}

	srw.WriteHeader(http.StatusOK)
	srw.WriteHeader(http.StatusNotFound) // This should be ignored

	require.Equal(t, http.StatusOK, srw.status, "expected status 200")
	require.Equal(t, http.StatusOK, rr.Code, "expected recorder status 200")
}

func Test_StatusResponseWriter_Hijack_Supported(t *testing.T) {
	rr := httptest.NewRecorder()
	srw := &StatusResponseWriter{ResponseWriter: rr}

	// Wrap the recorder in a type that supports Hijack
	hijacker := &hijackableResponseRecorder{rr}
	srw.ResponseWriter = hijacker

	conn, rw, err := srw.Hijack()
	require.NoError(t, err, "expected no error during Hijack")
	require.NotNil(t, conn, "expected conn to be non-nil")
	require.NotNil(t, rw, "expected rw to be non-nil")
}

func Test_StatusResponseWriter_Hijack_NotSupported(t *testing.T) {
	rr := httptest.NewRecorder()
	srw := &StatusResponseWriter{ResponseWriter: rr}

	_, _, err := srw.Hijack()
	require.Error(t, err, "expected an error during Hijack")
	require.ErrorIs(t, err, errHijackNotSupported, "expected error to be errHijackNotSupported")
}

// hijackableResponseRecorder is a custom ResponseRecorder that supports the Hijack method.
type hijackableResponseRecorder struct {
	*httptest.ResponseRecorder
}

func (*hijackableResponseRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn := &mockConn{}
	rw := bufio.NewReadWriter(bufio.NewReader(bytes.NewReader(nil)), bufio.NewWriter(bytes.NewBuffer(nil)))

	return conn, rw, nil
}

// mockConn is a mock implementation of net.Conn for testing purposes.
type mockConn struct{}

func (*mockConn) Read([]byte) (n int, err error)   { return 0, nil }
func (*mockConn) Write([]byte) (n int, err error)  { return 0, nil }
func (*mockConn) Close() error                     { return nil }
func (*mockConn) LocalAddr() net.Addr              { return &mockAddr{} }
func (*mockConn) RemoteAddr() net.Addr             { return &mockAddr{} }
func (*mockConn) SetDeadline(time.Time) error      { return nil }
func (*mockConn) SetReadDeadline(time.Time) error  { return nil }
func (*mockConn) SetWriteDeadline(time.Time) error { return nil }

// mockAddr is a mock implementation of net.Addr for testing purposes.
type mockAddr struct{}

func (*mockAddr) Network() string { return "tcp" }
func (*mockAddr) String() string  { return "127.0.0.1:8080" }

func Test_LoggingMiddleware_LevelShortCircuit(t *testing.T) {
	var normalBuf, errBuf bytes.Buffer
	l := logging.NewWriterLogger(logging.WARN, &normalBuf, &errBuf)
	mw := Logging(LogProbes{}, l)

	okHandler := mw(http.HandlerFunc(testHandler))
	errHandler := mw(http.HandlerFunc(testHandlerError))

	// Below 500 when INFO is disabled -> skipped
	req := httptest.NewRequest(http.MethodGet, "http://dummy/ok", http.NoBody)
	rr := httptest.NewRecorder()
	okHandler.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Empty(t, normalBuf.String())
	assert.Empty(t, errBuf.String())

	// 500 error when INFO is disabled -> still logged to error writer
	reqErr := httptest.NewRequest(http.MethodGet, "http://dummy/fail", http.NoBody)
	rrErr := httptest.NewRecorder()
	errHandler.ServeHTTP(rrErr, reqErr)
	assert.Equal(t, http.StatusInternalServerError, rrErr.Code)
	assert.Empty(t, normalBuf.String())
	assert.Contains(t, errBuf.String(), `"response":500`)

	// Dynamic level change to INFO -> 200 OK is logged
	l.ChangeLevel(logging.INFO)
	req2 := httptest.NewRequest(http.MethodGet, "http://dummy/ok2", http.NoBody)
	rr2 := httptest.NewRecorder()
	okHandler.ServeHTTP(rr2, req2)
	assert.Equal(t, http.StatusOK, rr2.Code)
	assert.Contains(t, normalBuf.String(), `"response":200`)
}
