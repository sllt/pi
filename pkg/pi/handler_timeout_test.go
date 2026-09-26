package pi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"github.com/sllt/pi/pkg/pi/http/response"
	"github.com/sllt/pi/pkg/pi/infra"
	"github.com/sllt/pi/pkg/pi/logging"
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
					container: &infra.Container{Logger: logger},
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
		container: &infra.Container{Logger: logging.NewLogger(logging.FATAL)},
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
			container: &infra.Container{Logger: logging.NewLogger(logging.FATAL)},
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
		container:      &infra.Container{Logger: logging.NewLogger(logging.FATAL)},
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
