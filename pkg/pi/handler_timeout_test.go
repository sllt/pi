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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

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

type closedRequestBody struct {
	io.Reader
	closed atomic.Bool
}

func (b *closedRequestBody) Read(p []byte) (int, error) {
	if b.closed.Load() {
		return 0, errors.New("original body already closed")
	}
	return b.Reader.Read(p)
}
func (b *closedRequestBody) Close() error { b.closed.Store(true); return nil }

func TestHandlerSnapshotSurvivesTimeoutAndRouterReuse(t *testing.T) {
	entered, release, responded := make(chan struct{}), make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	type outcome struct {
		value, param string
		err          error
	}
	got := make(chan outcome, 1)
	h := handler{rt: newTestKernel(logging.NewLogger(logging.FATAL)), requestTimeout: 5 * time.Millisecond, function: func(c *Context) (any, error) {
		close(entered)
		<-release
		var body struct {
			Value string `json:"value"`
		}
		err := c.Bind(&body)
		got <- outcome{body.Value, c.PathParam("id"), err}
		return nil, err
	}}
	r := httptest.NewRequest("POST", "/items/original", nil)
	r.Header.Set("Content-Type", "application/json")
	body := &closedRequestBody{Reader: strings.NewReader(`{"value":"original"}`)}
	r.Body = body
	route := chi.NewRouteContext()
	route.URLParams.Add("id", "original")
	r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, route))
	w := httptest.NewRecorder()
	go func() { h.ServeHTTP(w, r); close(responded) }()
	waitForHandlerSignal(t, entered)
	waitForHandlerSignal(t, responded)
	require.Equal(t, 408, w.Code)
	require.True(t, body.closed.Load())
	route.URLParams.Values[0] = "reused"
	r.Header.Set("Content-Type", "wrong")
	unblock()
	result := <-got
	require.NoError(t, result.err)
	require.Equal(t, "original", result.value)
	require.Equal(t, "original", result.param)
}

func TestHandlerOwnsPreparsedMultipartAfterTimeout(t *testing.T) {
	var encoded bytes.Buffer
	writer := multipart.NewWriter(&encoded)
	part, err := writer.CreateFormFile("upload", "sample.txt")
	require.NoError(t, err)
	_, err = io.WriteString(part, "owned upload")
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	r := httptest.NewRequest("POST", "/upload", &encoded)
	r.Header.Set("Content-Type", writer.FormDataContentType())
	require.NoError(t, r.ParseMultipartForm(1))
	t.Cleanup(func() { _ = r.MultipartForm.RemoveAll() })
	entered, release, responded := make(chan struct{}), make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	type outcome struct {
		data string
		err  error
	}
	got := make(chan outcome, 1)
	h := handler{rt: newTestKernel(logging.NewLogger(logging.FATAL)), requestTimeout: 5 * time.Millisecond, function: func(c *Context) (any, error) {
		close(entered)
		<-release
		var form struct {
			Upload *multipart.FileHeader `form:"upload"`
		}
		err := c.Bind(&form)
		if err != nil {
			got <- outcome{err: err}
			return nil, err
		}
		if form.Upload == nil {
			err = errors.New("upload not bound")
			got <- outcome{err: err}
			return nil, err
		}
		file, err := form.Upload.Open()
		if err != nil {
			got <- outcome{err: err}
			return nil, err
		}
		defer file.Close()
		data, err := io.ReadAll(file)
		got <- outcome{string(data), err}
		return nil, err
	}}
	w := httptest.NewRecorder()
	go func() { h.ServeHTTP(w, r); close(responded) }()
	waitForHandlerSignal(t, entered)
	waitForHandlerSignal(t, responded)
	require.Equal(t, 408, w.Code)
	require.NoError(t, r.MultipartForm.RemoveAll())
	unblock()
	result := <-got
	require.NoError(t, result.err)
	require.Equal(t, "owned upload", result.data)
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

func TestHandler_ServeHTTP_LateCompletion(t *testing.T) {
	for _, reason := range []string{"timeout", "cancel"} {
		for _, outcome := range []string{"success", "error", "panic"} {
			t.Run(reason+"/"+outcome, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()

				started, returned, responded := make(chan struct{}), make(chan struct{}), make(chan struct{})
				release := make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				t.Cleanup(unblock)
				var marshaled atomic.Int32
				logger := &handlerResultLogger{
					Logger: logging.NewLogger(logging.FATAL), errors: make(chan any, 1),
				}
				h := handler{
					rt: newTestKernel(logger),
					function: func(*Context) (any, error) {
						defer close(returned)
						close(started)
						<-release // Deliberately ignores cancellation until the response is sent.
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
				status := piHTTP.StatusClientClosedRequest
				if reason == "timeout" {
					h.requestTimeout = time.Millisecond
					status = http.StatusRequestTimeout
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
				require.Equal(t, status, w.Code)
				require.Equal(t, 1, w.commits)
				require.Empty(t, w.Header().Get("X-Late-Result"))
				body := w.Body.String()

				unblock()
				waitForHandlerSignal(t, returned)
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
						t.Fatal("late failure was not logged")
					}
				}
				assert.Zero(t, marshaled.Load(), "late data must not be encoded")
				assert.Equal(t, body, w.Body.String())
				assert.Equal(t, 1, w.commits)
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

	t.Run("late_stream_after_timeout_does_not_leak_active_handlers", func(t *testing.T) {
		rt := newTestKernel(logging.NewLogger(logging.FATAL))
		entered, release, responded := make(chan struct{}), make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		t.Cleanup(unblock)

		h := handler{
			rt:             rt,
			requestTimeout: 5 * time.Millisecond,
			function: func(*Context) (any, error) {
				close(entered)
				<-release
				return response.NewSSE(func(context.Context, *response.SSEWriter) error {
					return nil
				}, response.WithSSEKeepAlive(0)), nil
			},
		}

		rec := httptest.NewRecorder()
		go func() {
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/sse", http.NoBody))
			close(responded)
		}()

		waitForHandlerSignal(t, entered)
		waitForHandlerSignal(t, responded)
		assert.Equal(t, http.StatusRequestTimeout, rec.Code)

		unblock()
		waitCtx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		require.NoError(t, rt.WaitHandlers(waitCtx))
		assert.Zero(t, activeCount(rt))
	})
}
