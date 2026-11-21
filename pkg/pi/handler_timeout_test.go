package pi

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	traceNoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/sllt/pi/pkg/pi/config"
	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"github.com/sllt/pi/pkg/pi/http/response"
	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/sllt/pi/pkg/pi/testutil"
)

type countedResponseWriter struct {
	*httptest.ResponseRecorder
	commits int
}

func (w *countedResponseWriter) WriteHeader(code int) {
	w.commits++
	w.ResponseRecorder.WriteHeader(code)
}

type lateResponsePayload struct {
	marshaled *atomic.Int32
}

func (p lateResponsePayload) MarshalJSON() ([]byte, error) {
	p.marshaled.Add(1)

	return []byte(`"late result"`), nil
}

type handlerResultLogger struct {
	logging.Logger
	errors chan any
}

func TestHandler_ServeHTTP_SynchronousExecutionAndFastFail(t *testing.T) {
	t.Run("executes_on_servehttp_goroutine_without_spawning_worker", func(t *testing.T) {
		for _, timeout := range []time.Duration{0, 100 * time.Millisecond} {
			var ranOnServeHTTPStack bool
			h := handler{
				rt:             newTestKernel(logging.NewLogger(logging.FATAL)),
				requestTimeout: timeout,
				function: func(c *Context) (any, error) {
					pcs := make([]uintptr, 32)
					n := runtime.Callers(1, pcs)
					frames := runtime.CallersFrames(pcs[:n])
					for {
						frame, more := frames.Next()
						if strings.HasSuffix(frame.Function, "handler.ServeHTTP") {
							ranOnServeHTTPStack = true
							break
						}
						if !more {
							break
						}
					}
					return "ok", nil
				},
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/sync", http.NoBody))
			require.Equal(t, http.StatusOK, w.Code)
			assert.True(t, ranOnServeHTTPStack, "timeout=%v must run directly on ServeHTTP goroutine", timeout)
		}
	})

	t.Run("fast_fails_before_handler_when_context_already_canceled_or_expired", func(t *testing.T) {
		canceledCtx, cancel := context.WithCancel(t.Context())
		cancel()

		var called atomic.Bool
		h := handler{
			rt: newTestKernel(logging.NewLogger(logging.FATAL)),
			function: func(*Context) (any, error) {
				called.Store(true)
				return "never", nil
			},
		}
		wCancel := httptest.NewRecorder()
		h.ServeHTTP(wCancel, httptest.NewRequest(http.MethodGet, "/", http.NoBody).WithContext(canceledCtx))
		assert.Equal(t, piHTTP.StatusClientClosedRequest, wCancel.Code)
		assert.False(t, called.Load())

		expiredCtx, expCancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
		defer expCancel()
		wTimeout := httptest.NewRecorder()
		h.ServeHTTP(wTimeout, httptest.NewRequest(http.MethodGet, "/", http.NoBody).WithContext(expiredCtx))
		assert.Equal(t, http.StatusRequestTimeout, wTimeout.Code)
		assert.False(t, called.Load())
	})

	t.Run("enforces_lazy_max_body_bytes_on_json_and_raw_body", func(t *testing.T) {
		h := handler{
			rt:           newTestKernel(logging.NewLogger(logging.FATAL)),
			maxBodyBytes: 16,
			function: func(c *Context) (any, error) {
				var body struct {
					Value string `json:"value"`
				}
				if err := c.Bind(&body); err != nil {
					return nil, err
				}
				return body.Value, nil
			},
		}
		overReq := httptest.NewRequest(http.MethodPost, "/items", strings.NewReader(`{"value":"0123456789abcdef"}`))
		overReq.Header.Set("Content-Type", "application/json")
		wOver := httptest.NewRecorder()
		h.ServeHTTP(wOver, overReq)
		assert.Equal(t, http.StatusRequestEntityTooLarge, wOver.Code)

		okReq := httptest.NewRequest(http.MethodPost, "/items", strings.NewReader(`{"value":"ok"}`))
		okReq.Header.Set("Content-Type", "application/json")
		wOK := httptest.NewRecorder()
		h.ServeHTTP(wOK, okReq)
		assert.Equal(t, http.StatusCreated, wOK.Code)
	})

	t.Run("skips_timeout_overwrite_for_response_handled", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			data any
		}{
			{name: "plain_handled", data: response.Handled{}},
			{name: "result_wrapped_handled", data: response.Result{StatusCode: http.StatusOK, Data: response.Handled{}}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				h := handler{
					rt:             newTestKernel(logging.NewLogger(logging.FATAL)),
					requestTimeout: 5 * time.Millisecond,
					function: func(c *Context) (any, error) {
						<-c.Done()
						return tc.data, nil
					},
				}
				w := &countedResponseWriter{ResponseRecorder: httptest.NewRecorder()}
				h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/handled", http.NoBody))
				assert.Equal(t, 0, w.commits)
				assert.Empty(t, w.Body.String())
			})
		}
	})

	t.Run("does_not_skip_timeout_for_pointer_nested_or_invalid_status_result_handled", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			data any
		}{
			{name: "pointer_result_handled", data: &response.Result{StatusCode: http.StatusOK, Data: response.Handled{}}},
			{name: "invalid_status_result_handled", data: response.Result{StatusCode: http.StatusInternalServerError, Data: response.Handled{}}},
			{name: "nested_result_handled", data: response.Result{StatusCode: http.StatusOK, Data: response.Result{StatusCode: http.StatusOK, Data: response.Handled{}}}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				h := handler{
					rt:             newTestKernel(logging.NewLogger(logging.FATAL)),
					requestTimeout: 5 * time.Millisecond,
					function: func(c *Context) (any, error) {
						<-c.Done()
						return tc.data, nil
					},
				}
				w := &countedResponseWriter{ResponseRecorder: httptest.NewRecorder()}
				h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/handled", http.NoBody))
				assert.Equal(t, 1, w.commits)
				assert.Equal(t, http.StatusRequestTimeout, w.Code)
			})
		}
	})
}

type handlerTimeoutNetErr struct{}

func (handlerTimeoutNetErr) Error() string   { return "i/o timeout" }
func (handlerTimeoutNetErr) Timeout() bool   { return true }
func (handlerTimeoutNetErr) Temporary() bool { return false }

type failingReadCloser struct {
	err error
}

func (f failingReadCloser) Read([]byte) (int, error) { return 0, f.err }
func (f failingReadCloser) Close() error             { return nil }

func TestHandler_POST_BodyMiddlewareWithSSEAndSlowHandler_AndSlowBody408(t *testing.T) {
	t.Run("post_sse_and_slow_handler_behind_body_reading_middleware_with_real_http_server", func(t *testing.T) {
		rt := newTestKernel(logging.NewLogger(logging.FATAL))

		mux := http.NewServeMux()
		bodyReadingMW := func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				_ = r.Body.Close()
				r.Body = io.NopCloser(bytes.NewReader(raw))
				next.ServeHTTP(w, r)
			})
		}

		mux.Handle("/sse", bodyReadingMW(handler{
			rt:             rt,
			requestTimeout: 25 * time.Millisecond,
			function: func(c *Context) (any, error) {
				var payload map[string]string
				if err := c.Bind(&payload); err != nil {
					return nil, err
				}
				return response.NewSSE(func(ctx context.Context, sse *response.SSEWriter) error {
					time.Sleep(60 * time.Millisecond)
					if ctx.Err() != nil {
						return ctx.Err()
					}
					return sse.SendEvent("finish", payload["msg"])
				}, response.WithSSEKeepAlive(0)), nil
			},
		}))

		mux.Handle("/slow", bodyReadingMW(handler{
			rt:             rt,
			requestTimeout: 25 * time.Millisecond,
			function: func(c *Context) (any, error) {
				var payload map[string]string
				if err := c.Bind(&payload); err != nil {
					return nil, err
				}
				time.Sleep(60 * time.Millisecond)
				return payload, nil
			},
		}))

		srv := httptest.NewServer(mux)
		defer srv.Close()

		// POST SSE must not abort at requestTimeout.
		sseResp, err := srv.Client().Post(srv.URL+"/sse", "application/json", strings.NewReader(`{"msg":"ok"}`))
		require.NoError(t, err)
		defer sseResp.Body.Close()
		sseBody, err := io.ReadAll(sseResp.Body)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, sseResp.StatusCode)
		assert.Contains(t, string(sseBody), "event: finish\ndata: ok\n\n")

		// Slow handler behind body-reading middleware must return 408, not 499.
		slowResp, err := srv.Client().Post(srv.URL+"/slow", "application/json", strings.NewReader(`{"msg":"ok"}`))
		require.NoError(t, err)
		defer slowResp.Body.Close()
		assert.Equal(t, http.StatusRequestTimeout, slowResp.StatusCode)
	})

	t.Run("slow_body_read_timeout_returns_408_in_legacy_and_typed_handler", func(t *testing.T) {
		legacyH := handler{
			rt: newTestKernel(logging.NewLogger(logging.FATAL)),
			function: func(c *Context) (any, error) {
				var payload struct {
					Name string `json:"name"`
				}
				if err := c.Bind(&payload); err != nil {
					return nil, err
				}
				return payload, nil
			},
		}
		legacyReq := httptest.NewRequest(http.MethodPost, "/legacy", failingReadCloser{err: handlerTimeoutNetErr{}})
		legacyReq.Header.Set("Content-Type", "application/json")
		legacyRec := httptest.NewRecorder()
		legacyH.ServeHTTP(legacyRec, legacyReq)
		assert.Equal(t, http.StatusRequestTimeout, legacyRec.Code)

		app, err := Build(
			WithConfig(map[string]string{
				"HTTP_ENABLED": "false",
				"GRPC_ENABLED": "false",
			}),
			WithExplicitHTTPStatus(),
		)
		require.NoError(t, err)
		defer func() { _ = app.Stop(context.Background()) }()
		type typedReq struct {
			Name string `json:"name"`
		}
		type typedRes struct {
			Name string `json:"name"`
		}
		app.POST("/typed", func(c *Context, req *typedReq) (*typedRes, error) {
			return &typedRes{Name: req.Name}, nil
		})
		appHTTP, err := app.HTTPHandler()
		require.NoError(t, err)

		tReq := httptest.NewRequest(http.MethodPost, "/typed", failingReadCloser{err: handlerTimeoutNetErr{}})
		tReq.Header.Set("Content-Type", "application/json")
		tRec := httptest.NewRecorder()
		appHTTP.ServeHTTP(tRec, tReq)
		assert.Equal(t, http.StatusRequestTimeout, tRec.Code)
	})
}

func TestHandlerOwnsPreparsedMultipartAndForm(t *testing.T) {
	t.Run("preparsed_multipart_binds_and_reads_file", func(t *testing.T) {
		var encoded bytes.Buffer
		writer := multipart.NewWriter(&encoded)
		part, err := writer.CreateFormFile("upload", "sample.txt")
		require.NoError(t, err)
		_, err = io.WriteString(part, "owned upload")
		require.NoError(t, err)
		require.NoError(t, writer.Close())

		r := httptest.NewRequest(http.MethodPost, "/upload", &encoded)
		r.Header.Set("Content-Type", writer.FormDataContentType())
		require.NoError(t, r.ParseMultipartForm(1))

		var gotData string
		h := handler{
			rt: newTestKernel(logging.NewLogger(logging.FATAL)),
			function: func(c *Context) (any, error) {
				var form struct {
					Upload *multipart.FileHeader `form:"upload"`
				}
				if err := c.Bind(&form); err != nil {
					return nil, err
				}
				if form.Upload == nil {
					return nil, errors.New("upload not bound")
				}
				file, err := form.Upload.Open()
				if err != nil {
					return nil, err
				}
				defer file.Close()
				data, err := io.ReadAll(file)
				if err != nil {
					return nil, err
				}
				gotData = string(data)
				return gotData, nil
			},
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		require.Equal(t, http.StatusCreated, w.Code)
		require.Equal(t, "owned upload", gotData)
	})

	t.Run("preparsed_post_form_reconstructs_raw_body", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/form", strings.NewReader("name=alice&role=admin"))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		require.NoError(t, r.ParseForm())

		var rawBody string
		var boundName string
		h := handler{
			rt: newTestKernel(logging.NewLogger(logging.FATAL)),
			function: func(c *Context) (any, error) {
				piReq := c.Request.(*piHTTP.Request)
				raw, err := piReq.RawBodyBytes()
				if err != nil {
					return nil, err
				}
				rawBody = string(raw)
				var form struct {
					Name string `form:"name"`
				}
				if err := c.Bind(&form); err != nil {
					return nil, err
				}
				boundName = form.Name
				return boundName, nil
			},
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		require.Equal(t, http.StatusCreated, w.Code)
		assert.Equal(t, "alice", boundName)
		assert.Contains(t, rawBody, "name=alice")
	})

	t.Run("prebuffered_in_memory_body_never_arms_socket_read_deadline", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/json", io.NopCloser(bytes.NewReader([]byte(`{"name":"alice"}`))))
		r.GetBody = nil
		r.Header.Set("Content-Type", "application/json")

		var deadlined bool
		h := handler{
			rt:             newTestKernel(logging.NewLogger(logging.FATAL)),
			requestTimeout: 50 * time.Millisecond,
			function: func(c *Context) (any, error) {
				var dst struct {
					Name string `json:"name"`
				}
				if err := c.Bind(&dst); err != nil {
					return nil, err
				}
				return dst.Name, nil
			},
		}
		w := &handlerDeadlineSpyWriter{
			ResponseRecorder: httptest.NewRecorder(),
			onSetReadDeadline: func(time.Time) {
				deadlined = true
			},
		}
		h.ServeHTTP(w, r)
		require.Equal(t, http.StatusCreated, w.Code)
		assert.False(t, deadlined, "in-memory io.NopCloser(bytes.NewReader) body must not arm socket SetReadDeadline")
	})
}

type handlerDeadlineSpyWriter struct {
	*httptest.ResponseRecorder
	onSetReadDeadline func(time.Time)
}

func (w *handlerDeadlineSpyWriter) SetReadDeadline(d time.Time) error {
	if w.onSetReadDeadline != nil {
		w.onSetReadDeadline(d)
	}
	return nil
}

func (l *handlerResultLogger) Error(args ...any) {
	for _, arg := range args {
		l.errors <- arg
	}
}

func waitForHandlerSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()

	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not reach the expected state")
	}
}

func TestHandler_ServeHTTP_CooperativeTimeoutAndCancel(t *testing.T) {
	for _, reason := range []string{"timeout", "cancel"} {
		for _, outcome := range []string{"success", "error", "panic"} {
			t.Run(reason+"/"+outcome, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()

				started, responded := make(chan struct{}), make(chan struct{})
				var marshaled atomic.Int32
				logger := &handlerResultLogger{
					Logger: logging.NewLogger(logging.FATAL), errors: make(chan any, 1),
				}
				h := handler{
					rt: newTestKernel(logger),
					function: func(c *Context) (any, error) {
						close(started)
						<-c.Done()
						data := response.Response{
							Data:    lateResponsePayload{marshaled: &marshaled},
							Headers: map[string]string{"X-Late-Result": "must-not-be-written"},
						}
						switch outcome {
						case "panic":
							panic("late panic")
						case "error":
							return data, errTest
						default:
							return data, nil
						}
					},
				}
				expectedStatus := piHTTP.StatusClientClosedRequest
				if reason == "timeout" {
					h.requestTimeout = time.Millisecond
					expectedStatus = http.StatusRequestTimeout
				}
				if outcome == "error" || outcome == "panic" {
					expectedStatus = http.StatusInternalServerError
				}
				w := &countedResponseWriter{ResponseRecorder: httptest.NewRecorder()}
				r := httptest.NewRequest(http.MethodGet, "/", http.NoBody).WithContext(ctx)
				go func() {
					h.ServeHTTP(w, r)
					close(responded)
				}()
				waitForHandlerSignal(t, started)
				if reason == "cancel" {
					cancel()
				}
				waitForHandlerSignal(t, responded)
				require.Equal(t, expectedStatus, w.Code)
				require.Equal(t, 1, w.commits)
				require.Empty(t, w.Header().Get("X-Late-Result"))

				if outcome != "success" {
					select {
					case entry := <-logger.errors:
						if outcome == "error" {
							require.IsType(t, &ErrorLogEntry{}, entry)
							assert.Equal(t, errTest.Error(), entry.(*ErrorLogEntry).Error)
						} else {
							require.IsType(t, panicLog{}, entry)
							assert.Equal(t, "late panic", entry.(panicLog).Error)
							assert.NotEmpty(t, entry.(panicLog).StackTrace)
						}
					case <-time.After(5 * time.Second):
						t.Fatal("failure was not logged")
					}
				}
				assert.Zero(t, marshaled.Load(), "discarded payload must not be encoded")
			})
		}
	}
}

func TestHandler_ServeHTTP_ContextReplacement(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started, release, responded := make(chan struct{}), make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	h := handler{
		rt: newTestKernel(logging.NewLogger(logging.FATAL)),
		function: func(c *Context) (any, error) {
			// Trace mutates the public Context field. Replacing it must not change
			// the cancellation source or classification used by ServeHTTP.
			c.Trace("handler-work").End()
			c.Context = context.Background()
			close(started)
			<-release

			return nil, nil
		},
	}
	w := httptest.NewRecorder()
	go func() {
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", http.NoBody).WithContext(ctx))
		close(responded)
	}()
	waitForHandlerSignal(t, started)
	cancel()
	unblock()
	waitForHandlerSignal(t, responded)
	assert.Equal(t, piHTTP.StatusClientClosedRequest, w.Code)
}

func TestHandler_ServeHTTP_CancelCompletionRace(t *testing.T) {
	for range 100 {
		ctx, cancel := context.WithCancel(t.Context())
		gate, responded := make(chan struct{}), make(chan struct{})
		h := handler{
			rt: newTestKernel(logging.NewLogger(logging.FATAL)),
			function: func(*Context) (any, error) {
				<-gate

				return response.Response{Data: "completed", Headers: map[string]string{"X-Completed": "true"}}, nil
			},
		}
		w := &countedResponseWriter{ResponseRecorder: httptest.NewRecorder()}
		go func() {
			h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", http.NoBody).WithContext(ctx))
			close(responded)
		}()
		canceled := make(chan struct{})
		go func() {
			<-gate
			cancel()
			close(canceled)
		}()
		close(gate)
		waitForHandlerSignal(t, responded)
		waitForHandlerSignal(t, canceled)
		require.Equal(t, 1, w.commits)
		switch w.Code {
		case http.StatusOK:
			assert.Equal(t, "true", w.Header().Get("X-Completed"))
			assert.JSONEq(t, `{"code":0,"data":"completed","message":"ok"}`, w.Body.String())
		case piHTTP.StatusClientClosedRequest:
			assert.Empty(t, w.Header().Get("X-Completed"))
			assert.Contains(t, w.Body.String(), "client closed request")
		default:
			t.Fatalf("unexpected status %d: %s", w.Code, w.Body.String())
		}
	}
}

func TestHandler_ServeHTTP_WebSocketTimeoutExemption(t *testing.T) {
	h := handler{
		requestTimeout: time.Nanosecond,
		rt:             newTestKernel(logging.NewLogger(logging.FATAL)),
		function: func(c *Context) (any, error) {
			if _, hasDeadline := c.Deadline(); hasDeadline {
				return nil, errors.New("WebSocket unexpectedly received an HTTP deadline")
			}

			return "ok", nil
		},
	}
	r := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	r.Header.Set("Connection", "Upgrade")
	r.Header.Set("Upgrade", "websocket")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestHandler_ExplicitStreamPanicKeepsCommittedResponse(t *testing.T) {
	var logs bytes.Buffer
	h := handler{
		rt: newTestKernel(logging.NewWriterLogger(logging.ERROR, &logs, &logs)),
		function: func(*Context) (any, error) {
			return response.Accepted(response.Stream{Run: func(_ context.Context, w io.Writer) error {
				_, _ = io.WriteString(w, "partial")
				panic("stream-private-cause")
			}}), nil
		},
	}
	w := &countedResponseWriter{ResponseRecorder: httptest.NewRecorder()}
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", http.NoBody))
	assert.Equal(t, http.StatusAccepted, w.Code)
	assert.Equal(t, 1, w.commits)
	assert.Equal(t, "partial", w.Body.String())
	assert.Contains(t, logs.String(), "stream-private-cause")
}

func TestHandler_SSEStreamsPastRequestTimeoutAndClonesPointer(t *testing.T) {
	var calls atomic.Int32
	shared := response.NewSSE(func(ctx context.Context, sse *response.SSEWriter) error {
		n := calls.Add(1)
		if err := sse.SendEvent("start", map[string]int32{"call": n}); err != nil {
			return err
		}
		// Exceed h.requestTimeout (15ms) while streaming.
		time.Sleep(45 * time.Millisecond)
		return sse.SendEvent("finish", "ok")
	}, response.WithSSEKeepAlive(0))
	origRun := shared.Run

	h := handler{
		rt:             newTestKernel(logging.NewLogger(logging.FATAL)),
		requestTimeout: 15 * time.Millisecond,
		function: func(*Context) (any, error) {
			return shared, nil
		},
	}

	for i := range 2 {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/events", http.NoBody))
		require.Equal(t, http.StatusOK, rec.Code, "iteration %d", i)
		assert.Equal(t, "text/event-stream; charset=utf-8", rec.Header().Get("Content-Type"))
		assert.Contains(t, rec.Body.String(), "event: start\n")
		assert.Contains(t, rec.Body.String(), "event: finish\ndata: ok\n\n")
	}
	// Shared *response.SSE Run function pointer must never be overwritten in place.
	assert.Equal(t, fmt.Sprintf("%p", origRun), fmt.Sprintf("%p", shared.Run))
}

func TestApp_GracefulStopCancelsActiveSSEBeforeClosingOwnedResources(t *testing.T) {
	port := testutil.GetFreePort(t)
	var resourceStopped atomic.Bool
	var sawResourceAlive atomic.Bool
	streamStarted := make(chan struct{})
	streamExited := make(chan struct{})

	app, err := Build(
		WithConfig(map[string]string{
			"HTTP_PORT":    strconv.Itoa(port),
			"GRPC_ENABLED": "false",
		}),
		WithResource(Resource{
			Name:      "stream-owned-dep",
			Ownership: Owned,
			Stop: func(context.Context) error {
				resourceStopped.Store(true)
				return nil
			},
		}),
	)
	require.NoError(t, err)

	app.GET("/events", func(*Context) (any, error) {
		return response.NewSSE(func(ctx context.Context, sse *response.SSEWriter) error {
			if err := sse.SendEvent("ready", "connected"); err != nil {
				return err
			}
			close(streamStarted)
			<-ctx.Done()
			sawResourceAlive.Store(!resourceStopped.Load())
			time.Sleep(15 * time.Millisecond)
			close(streamExited)
			return nil
		}, response.WithSSEKeepAlive(10*time.Millisecond)), nil
	}, WithEndpointTimeout(20*time.Millisecond))

	require.NoError(t, app.Start(t.Context()))

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/events", port), http.NoBody)
	require.NoError(t, err)
	client := &http.Client{}
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "text/event-stream; charset=utf-8", resp.Header.Get("Content-Type"))

	reader := bufio.NewReader(resp.Body)
	var gotReady bool
	for range 10 {
		line, readErr := reader.ReadString('\n')
		require.NoError(t, readErr)
		if strings.TrimSpace(line) == "data: connected" {
			gotReady = true
			break
		}
	}
	require.True(t, gotReady)

	waitForHandlerSignal(t, streamStarted)
	// Stay connected longer than the 20ms endpoint requestTimeout.
	time.Sleep(40 * time.Millisecond)

	stopCtx, stopCancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer stopCancel()
	startStop := time.Now()
	require.NoError(t, app.Stop(stopCtx))
	assert.Less(t, time.Since(startStop), time.Second)

	waitForHandlerSignal(t, streamExited)
	assert.True(t, sawResourceAlive.Load(), "Owned resource must stay alive until Stream.Run exits")
	assert.True(t, resourceStopped.Load(), "Owned resource must stop after Stream.Run exits")
}

func TestHandler_WrapStreamRun_PreStoppingAndErrorLogging(t *testing.T) {
	t.Run("cancels_synchronously_when_stopping_already_marked", func(t *testing.T) {
		var logs bytes.Buffer
		rt := newTestKernel(logging.NewWriterLogger(logging.ERROR, &logs, &logs))
		rt.MarkStopping()

		h := handler{rt: rt}
		var sawCanceledErr error
		wrapped := h.wrapStreamRun(func(ctx context.Context, _ io.Writer) error {
			sawCanceledErr = ctx.Err()
			return fmt.Errorf("aborted: %w", ctx.Err())
		}, "trace-pre-stop")

		err := wrapped(t.Context(), io.Discard)
		require.ErrorIs(t, err, context.Canceled)
		assert.ErrorIs(t, sawCanceledErr, context.Canceled)
		assert.Empty(t, logs.String(), "expected graceful shutdown cancel error log to be suppressed")
	})

	t.Run("suppresses_cancel_error_when_client_cancels_but_logs_real_errors", func(t *testing.T) {
		var logs bytes.Buffer
		rt := newTestKernel(logging.NewWriterLogger(logging.ERROR, &logs, &logs))
		h := handler{rt: rt}

		clientCtx, clientCancel := context.WithCancel(t.Context())
		clientCancel()
		wrappedCancel := h.wrapStreamRun(func(ctx context.Context, _ io.Writer) error {
			return fmt.Errorf("client gone: %w", ctx.Err())
		}, "trace-client-cancel")
		err := wrappedCancel(clientCtx, io.Discard)
		require.ErrorIs(t, err, context.Canceled)
		assert.Empty(t, logs.String())

		// Internal context.Canceled while streamCtx is still active must still log.
		wrappedInternalCancel := h.wrapStreamRun(func(context.Context, io.Writer) error {
			return context.Canceled
		}, "trace-internal-cancel")
		err = wrappedInternalCancel(t.Context(), io.Discard)
		require.ErrorIs(t, err, context.Canceled)
		assert.Contains(t, logs.String(), "trace-internal-cancel")

		// Non-cancel error even during stopping must still log.
		logs.Reset()
		rt.MarkStopping()
		wrappedRealErr := h.wrapStreamRun(func(context.Context, io.Writer) error {
			return errors.New("upstream write failed")
		}, "trace-real-err")
		err = wrappedRealErr(t.Context(), io.Discard)
		require.EqualError(t, err, "upstream write failed")
		assert.Contains(t, logs.String(), "upstream write failed")
	})

	t.Run("logs_panicLog_with_stack_trace_on_panic", func(t *testing.T) {
		logger := &handlerResultLogger{
			Logger: logging.NewLogger(logging.FATAL),
			errors: make(chan any, 1),
		}
		h := handler{rt: newTestKernel(logger)}
		wrapped := h.wrapStreamRun(func(context.Context, io.Writer) error {
			panic("stream-boom")
		}, "trace-panic")

		err := wrapped(t.Context(), io.Discard)
		require.EqualError(t, err, "stream panicked: stream-boom")

		select {
		case entry := <-logger.errors:
			pl, ok := entry.(panicLog)
			require.True(t, ok, "expected panicLog, got %T", entry)
			assert.Equal(t, "stream-boom", pl.Error)
			assert.NotEmpty(t, pl.StackTrace)
		case <-time.After(time.Second):
			t.Fatal("expected panicLog to be logged")
		}
	})
}

type streamHandoffResponseWriter struct {
	*httptest.ResponseRecorder
	onWriteHeader func()
}

func (w *streamHandoffResponseWriter) WriteHeader(code int) {
	if w.onWriteHeader != nil {
		w.onWriteHeader()
	}
	w.ResponseRecorder.WriteHeader(code)
}

func TestHandler_StreamAndSSEPreserveMiddlewareValuesAndSpans(t *testing.T) {
	type mwCtxKey string
	const (
		mwKey      mwCtxKey = "pi-mw-tenant"
		handlerKey mwCtxKey = "pi-handler-user"
	)

	prevTP := otel.GetTracerProvider()
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		_ = tp.Shutdown(context.Background())
	})

	for _, kind := range []string{"stream", "sse"} {
		t.Run(kind, func(t *testing.T) {
			port := testutil.GetFreePort(t)
			app, err := Build(WithConfig(map[string]string{
				"HTTP_PORT":    strconv.Itoa(port),
				"GRPC_ENABLED": "false",
			}))
			require.NoError(t, err)

			app.UseMiddleware(func(next Handler) Handler {
				return func(c *Context) (any, error) {
					c.Context = context.WithValue(c.Context, mwKey, "tenant-42")
					return next(c)
				}
			})

			var (
				expectedSpanID trace.SpanID
				gotTenant      any
				gotUser        any
				gotSpanID      trace.SpanID
				sawCanceled    atomic.Bool
			)
			streamDone := make(chan struct{})

			app.GET("/"+kind, func(c *Context) (any, error) {
				span := c.Trace("stream-setup")
				defer span.End()
				expectedSpanID = span.SpanContext().SpanID()
				c.Context = context.WithValue(c.Context, handlerKey, "alice")

				if kind == "stream" {
					return &response.Stream{
						ContentType: "text/plain",
						Run: func(ctx context.Context, w io.Writer) error {
							defer close(streamDone)
							gotTenant = ctx.Value(mwKey)
							gotUser = ctx.Value(handlerKey)
							gotSpanID = trace.SpanFromContext(ctx).SpanContext().SpanID()
							// Outlive the 15ms endpoint timeout without cancellation.
							time.Sleep(35 * time.Millisecond)
							if ctx.Err() != nil {
								return ctx.Err()
							}
							_, _ = io.WriteString(w, "chunk")
							<-ctx.Done()
							sawCanceled.Store(true)
							return ctx.Err()
						},
					}, nil
				}
				return response.NewSSE(func(ctx context.Context, sse *response.SSEWriter) error {
					defer close(streamDone)
					gotTenant = ctx.Value(mwKey)
					gotUser = ctx.Value(handlerKey)
					gotSpanID = trace.SpanFromContext(ctx).SpanContext().SpanID()
					// Outlive the 15ms endpoint timeout without cancellation.
					time.Sleep(35 * time.Millisecond)
					if ctx.Err() != nil {
						return ctx.Err()
					}
					if err := sse.SendData("hello"); err != nil {
						return err
					}
					<-ctx.Done()
					sawCanceled.Store(true)
					return ctx.Err()
				}, response.WithSSEKeepAlive(0)), nil
			}, WithEndpointTimeout(15*time.Millisecond))

			require.NoError(t, app.Start(t.Context()))
			defer func() { _ = app.Stop(t.Context()) }()

			reqCtx, reqCancel := context.WithCancel(t.Context())
			defer reqCancel()
			req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/%s", port, kind), http.NoBody)
			require.NoError(t, err)

			resp, err := (&http.Client{}).Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			buf := make([]byte, 64)
			n, readErr := resp.Body.Read(buf)
			require.NoError(t, readErr)
			assert.NotEmpty(t, string(buf[:n]))

			// Client disconnect cancels streamCtx promptly.
			reqCancel()
			resp.Body.Close()
			waitForHandlerSignal(t, streamDone)

			assert.Equal(t, "tenant-42", gotTenant)
			assert.Equal(t, "alice", gotUser)
			assert.True(t, expectedSpanID.IsValid())
			assert.Equal(t, expectedSpanID, gotSpanID)
			assert.True(t, sawCanceled.Load(), "client disconnect must cancel streamCtx")
		})
	}
}

func TestHandler_ActiveHandlersDoesNotDropToZeroBeforeStreamRun(t *testing.T) {
	activeCount := func(rt *runtimeKernel) int {
		rt.healthState.mu.Lock()
		defer rt.healthState.mu.Unlock()
		return rt.healthState.activeHandlers
	}

	t.Run("sse_and_stream_hold_active_lease_across_handoff", func(t *testing.T) {
		for _, kind := range []string{"stream", "sse"} {
			t.Run(kind, func(t *testing.T) {
				rt := newTestKernel(logging.NewLogger(logging.FATAL))
				var activeAtWriteHeader int
				var activeInsideRun int

				h := handler{
					rt: rt,
					function: func(*Context) (any, error) {
						if kind == "stream" {
							return &response.Stream{
								Run: func(context.Context, io.Writer) error {
									activeInsideRun = activeCount(rt)
									return nil
								},
							}, nil
						}
						return response.NewSSE(func(context.Context, *response.SSEWriter) error {
							activeInsideRun = activeCount(rt)
							return nil
						}, response.WithSSEKeepAlive(0)), nil
					},
				}

				rec := httptest.NewRecorder()
				w := &streamHandoffResponseWriter{
					ResponseRecorder: rec,
					onWriteHeader: func() {
						// Runs synchronously in ServeHTTP after worker doneHandler() and before Run().
						activeAtWriteHeader = activeCount(rt)
					},
				}

				h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/"+kind, http.NoBody))
				assert.GreaterOrEqual(t, activeAtWriteHeader, 1, "activeHandlers must not drop to 0 before stream Run starts")
				assert.GreaterOrEqual(t, activeInsideRun, 1)
				assert.Zero(t, activeCount(rt), "activeHandlers must return to 0 after ServeHTTP finishes")
			})
		}
	})

	t.Run("timed_out_stream_handler_does_not_leak_active_handlers", func(t *testing.T) {
		rt := newTestKernel(logging.NewLogger(logging.FATAL))
		var sseRan atomic.Bool

		h := handler{
			rt:             rt,
			requestTimeout: 5 * time.Millisecond,
			function: func(c *Context) (any, error) {
				<-c.Done()
				return response.NewSSE(func(context.Context, *response.SSEWriter) error {
					sseRan.Store(true)
					return nil
				}, response.WithSSEKeepAlive(0)), nil
			},
		}

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/sse", http.NoBody))
		assert.Equal(t, http.StatusRequestTimeout, rec.Code)
		assert.False(t, sseRan.Load())

		waitCtx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		require.NoError(t, rt.WaitHandlers(waitCtx))
		assert.Zero(t, activeCount(rt))
	})
}

func TestLightweightTracerProvider(t *testing.T) {
	t.Run("generates_valid_non_recording_span_and_preserves_parent_trace_id", func(t *testing.T) {
		tp := &lightweightTracerProvider{ratio: 1}
		assert.True(t, tp.IsLightweightTrace())
		assert.Equal(t, 1.0, tp.TraceRatio())

		tr := tp.Tracer("pi-test")
		ctx, span := tr.Start(t.Context(), "op")
		assert.False(t, span.IsRecording())
		sc := span.SpanContext()
		assert.True(t, sc.IsValid())
		assert.True(t, sc.HasTraceID())
		assert.True(t, sc.HasSpanID())
		assert.True(t, sc.IsSampled())
		assert.Equal(t, sc, trace.SpanContextFromContext(ctx))
		span.End()

		// Child span preserves TraceID and generates a fresh SpanID.
		childCtx, childSpan := tr.Start(ctx, "child")
		childSC := childSpan.SpanContext()
		assert.Equal(t, sc.TraceID(), childSC.TraceID())
		assert.NotEqual(t, sc.SpanID(), childSC.SpanID())
		assert.True(t, childSC.IsSampled())
		assert.Equal(t, childSC, trace.SpanContextFromContext(childCtx))
	})

	t.Run("unsampled_parent_stays_unsampled_and_preserves_tracestate", func(t *testing.T) {
		ts, err := trace.ParseTraceState("rojo=00f067aa0ba902b7")
		require.NoError(t, err)
		tid, err := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
		require.NoError(t, err)
		sid, err := trace.SpanIDFromHex("00f067aa0ba902b7")
		require.NoError(t, err)

		parentSC := trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    tid,
			SpanID:     sid,
			TraceFlags: 0,
			TraceState: ts,
			Remote:     true,
		})
		parentCtx := trace.ContextWithSpanContext(t.Context(), parentSC)

		tp := &lightweightTracerProvider{ratio: 1}
		tr := tp.Tracer("pi-test")
		childCtx, childSpan := tr.Start(parentCtx, "child")
		childSC := childSpan.SpanContext()
		assert.Equal(t, tid, childSC.TraceID())
		assert.NotEqual(t, sid, childSC.SpanID())
		assert.False(t, childSC.IsSampled())
		assert.Equal(t, trace.TraceFlags(0), childSC.TraceFlags())
		assert.Equal(t, "00f067aa0ba902b7", childSC.TraceState().Get("rojo"))
		assert.Equal(t, childSC, trace.SpanContextFromContext(childCtx))
	})

	t.Run("root_sampling_decision_inherited_consistently_by_children", func(t *testing.T) {
		tp := &lightweightTracerProvider{ratio: 1e-12}
		tr := tp.Tracer("pi-test")
		var sawUnsampledRoot bool
		for range 16 {
			rootCtx, rootSpan := tr.Start(t.Context(), "root")
			rootSC := rootSpan.SpanContext()
			assert.True(t, rootSC.IsValid())
			assert.True(t, rootSC.HasTraceID())
			assert.True(t, rootSC.HasSpanID())

			childCtx, childSpan := tr.Start(rootCtx, "child")
			childSC := childSpan.SpanContext()
			assert.Equal(t, rootSC.TraceID(), childSC.TraceID())
			assert.NotEqual(t, rootSC.SpanID(), childSC.SpanID())
			assert.Equal(t, rootSC.TraceFlags(), childSC.TraceFlags())
			assert.Equal(t, childSC, trace.SpanContextFromContext(childCtx))
			if !rootSC.IsSampled() {
				sawUnsampledRoot = true
			}
		}
		assert.True(t, sawUnsampledRoot)
	})

	t.Run("kernel_selects_lightweight_noop_or_sdk_tracer_provider", func(t *testing.T) {
		lwKernel := newIsolatedKernel(config.NewMockConfig(map[string]string{}), logging.NewLogger(logging.FATAL), nil, nil)
		_, isLW := lwKernel.getTracerProvider().(*lightweightTracerProvider)
		assert.True(t, isLW)

		noopKernel := newIsolatedKernel(config.NewMockConfig(map[string]string{"TRACER_RATIO": "0"}), logging.NewLogger(logging.FATAL), nil, nil)
		_, isNoop := noopKernel.getTracerProvider().(traceNoop.TracerProvider)
		assert.True(t, isNoop)

		nanKernel := newIsolatedKernel(config.NewMockConfig(map[string]string{"TRACER_RATIO": "NaN"}), logging.NewLogger(logging.FATAL), nil, nil)
		_, isNaNNoop := nanKernel.getTracerProvider().(traceNoop.TracerProvider)
		assert.True(t, isNaNNoop)

		sdkKernel := newIsolatedKernel(config.NewMockConfig(map[string]string{
			"TRACE_EXPORTER": "otlp",
			"TRACER_URL":     "127.0.0.1:4317",
		}), logging.NewLogger(logging.FATAL), nil, nil)
		sdkTP, isSDK := sdkKernel.getTracerProvider().(*sdktrace.TracerProvider)
		require.True(t, isSDK)
		_ = sdkTP.Shutdown(context.Background())
	})
}
