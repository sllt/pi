package pi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"go.opentelemetry.io/otel/trace"

	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"github.com/sllt/pi/pkg/pi/http/response"
	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/sllt/pi/pkg/pi/static"
	piWS "github.com/sllt/pi/pkg/pi/websocket"
)

const colorCodeError = 202 // 202 is red color code

type Handler func(c *Context) (any, error)

type handler struct {
	function       Handler
	rt             *runtimeKernel
	requestTimeout time.Duration
	maxBodyBytes   int64
}

type handlerResult struct {
	data       any
	err        error
	ctx        context.Context
	streamDone func()
}

type ErrorLogEntry struct {
	TraceID string `json:"trace_id,omitempty"`
	Error   string `json:"error,omitempty"`
}

func (el *ErrorLogEntry) PrettyPrint(writer io.Writer) {
	fmt.Fprintf(writer, "\u001B[38;5;8m%s \u001B[38;5;%dm%s \n", el.TraceID, colorCodeError, el.Error)
}

func (h handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	traceID := trace.SpanFromContext(r.Context()).SpanContext().TraceID().String()
	if !trace.SpanFromContext(r.Context()).SpanContext().HasTraceID() {
		traceID = uuid.NewString()
	}
	w.Header().Set("X-Request-ID", traceID)
	deadline := time.Now().Add(h.requestTimeout)
	policy := piHTTP.LegacyStatus
	if h.rt != nil && h.rt.explicitHTTPStatus {
		policy = piHTTP.ExplicitStatus
	}
	responder := piHTTP.NewResponderForRequest(w, r, policy)
	max := h.maxBodyBytes
	if max <= 0 && h.rt != nil {
		max = h.rt.maxBodyBytes
	}
	if max <= 0 {
		max = 1 << 20
	}
	// The asynchronous Handler owns a request snapshot. The net/http server may
	// close/reuse the original body and routing context after a timeout response.
	var (
		snapshotBytes []byte
		hasSnapshot   bool
	)
	if !websocket.IsWebSocketUpgrade(r) {
		owned := r.Clone(r.Context())
		if route := chi.RouteContext(r.Context()); route != nil {
			copy := *route
			copy.URLParams.Keys = append([]string(nil), route.URLParams.Keys...)
			copy.URLParams.Values = append([]string(nil), route.URLParams.Values...)
			copy.RoutePatterns = append([]string(nil), route.RoutePatterns...)
			owned = owned.WithContext(context.WithValue(owned.Context(), chi.RouteCtxKey, &copy))
		}
		if r.Body != nil || r.MultipartForm != nil || len(r.PostForm) > 0 {
			controller := http.NewResponseController(w)
			if h.requestTimeout > 0 {
				_ = controller.SetReadDeadline(deadline)
			}
			body, contentType, err := snapshotBody(w, r, max)
			if r.Body != nil {
				_ = r.Body.Close()
			}
			if h.requestTimeout > 0 {
				_ = controller.SetReadDeadline(time.Time{})
			}
			if err != nil {
				var limit *http.MaxBytesError
				var timeout net.Error
				if errors.As(err, &limit) {
					responder.Respond(nil, piHTTP.ErrorPayloadTooLarge{})
				} else if errors.As(err, &timeout) && timeout.Timeout() {
					responder.Respond(nil, piHTTP.ErrorRequestTimeout{})
				} else {
					responder.Respond(nil, piHTTP.ErrorMalformedBody{})
				}
				return
			}
			snapshotBytes = body
			hasSnapshot = true
			owned.Body = io.NopCloser(bytes.NewReader(body))
			if contentType != "" {
				owned.Header.Set("Content-Type", contentType)
				owned.ContentLength = int64(len(body))
				owned.Form = nil
				owned.PostForm = nil
				owned.MultipartForm = nil
			}
		} else {
			hasSnapshot = true
		}
		r = owned
	}
	var validate func(any) error
	if h.rt != nil {
		validate = h.rt.validate
	}
	piReq := piHTTP.NewRequestWithSnapshotAndLimit(r, validate, snapshotBytes, hasSnapshot, max)
	snapshotBytes = nil
	c := newContext(responder, piReq, h.rt)
	rawReqCtx := r.Context()

	if websocket.IsWebSocketUpgrade(r) {
		// If the request is a WebSocket upgrade, do not apply the timeout
		c.Context = r.Context()
		c.ContextLogger.UpdateContext(c.Context)
	} else if h.requestTimeout != 0 {
		ctx, cancel := context.WithDeadline(r.Context(), deadline)
		defer cancel()

		c.Context = ctx
		c.ContextLogger.UpdateContext(c.Context)
	}

	// The websocket middleware has already hijacked this connection. Execute
	// directly so ServeHTTP stays bound to the WebSocket loop lifetime and
	// WSHandlerUpgrade's deferred cleanup never fires while the loop is active.
	if r.Context().Value(piWS.WSConnectionKey) != nil {
		doneHandler := h.rt.BeginHandlerExecution()
		defer doneHandler()
		_ = h.execute(c, traceID)
		return
	}

	// Handler middleware and Context.Trace may replace c.Context. The response
	// goroutine owns this original cancellation context and never reads c again.
	requestCtx := c.Context
	results := make(chan handlerResult, 1)
	responseDone := make(chan struct{})
	defer close(responseDone)

	var (
		handoffMu sync.Mutex
		timedOut  bool
	)

	doneHandler := h.rt.BeginHandlerExecution()
	go func() {
		defer doneHandler()
		defer func() {
			if r.MultipartForm != nil {
				<-responseDone
				_ = r.MultipartForm.RemoveAll()
			}
		}()
		// Buffering lets a late Handler finish even after the response has timed out.
		execRes := h.execute(c, traceID)
		handoffMu.Lock()
		if !timedOut && execRes.err == nil && r.Method != http.MethodHead && hasActiveStreamResponse(execRes.data) && h.rt != nil {
			execRes.streamDone = h.rt.BeginHandlerExecution()
		}
		results <- execRes
		handoffMu.Unlock()
	}()

	var result handlerResult

	select {
	case <-requestCtx.Done():
		handoffMu.Lock()
		timedOut = true
		select {
		case late := <-results:
			if late.streamDone != nil {
				late.streamDone()
			}
		default:
		}
		handoffMu.Unlock()

		// Handle different context cancellation scenarios
		ctxErr := requestCtx.Err()

		// Server-side timeout occurred && fallback for other context errors
		result.err = piHTTP.ErrorRequestTimeout{}

		if errors.Is(ctxErr, context.Canceled) {
			// Client canceled the request (e.g., closed browser tab)
			result.err = piHTTP.ErrorClientClosedRequest{}
		}
	case result = <-results:
	}
	if result.streamDone != nil {
		defer result.streamDone()
	}
	if result.err == nil {
		if result.ctx != nil {
			responder.SetStreamBaseContext(context.WithoutCancel(result.ctx))
		}
		result.data = h.guardStream(result.data, traceID, rawReqCtx, result.streamDone)
	}

	// Handler function completed
	responder.Respond(result.data, result.err)
}

func hasActiveStreamResponse(data any) bool {
	switch value := data.(type) {
	case response.Result:
		if value.StatusCode == http.StatusNoContent {
			return false
		}
		return hasActiveStreamResponse(value.Data)
	case response.Stream:
		return value.Run != nil
	case *response.Stream:
		return value != nil && value.Run != nil
	case response.SSE:
		return value.Run != nil
	case *response.SSE:
		return value != nil && value.Run != nil
	default:
		return false
	}
}

// Explicit status envelopes retain the same stream ownership and error logging.
func (h handler) guardStream(data any, traceID string, cancelCtx context.Context, streamDone func()) any {
	switch value := data.(type) {
	case response.Result:
		value.Data = h.guardStream(value.Data, traceID, cancelCtx, streamDone)
		return value
	case response.Stream:
		if value.Run == nil {
			return value
		}
		value.Run = h.wrapStreamRunWithCancel(value.Run, traceID, cancelCtx, streamDone)
		return value
	case *response.Stream:
		if value == nil || value.Run == nil {
			return value
		}
		cp := *value
		cp.Run = h.wrapStreamRunWithCancel(cp.Run, traceID, cancelCtx, streamDone)
		return &cp
	case response.SSE:
		if value.Run == nil {
			return value
		}
		value.Run = h.wrapSSERunWithCancel(value.Run, traceID, cancelCtx, streamDone)
		return value
	case *response.SSE:
		if value == nil || value.Run == nil {
			return value
		}
		cp := *value
		cp.Run = h.wrapSSERunWithCancel(cp.Run, traceID, cancelCtx, streamDone)
		return &cp
	}
	return data
}

func (h handler) wrapStreamRun(run func(context.Context, io.Writer) error, traceID string) func(context.Context, io.Writer) error {
	return h.wrapStreamRunWithCancel(run, traceID, nil, nil)
}

func (h handler) wrapStreamRunWithCancel(
	run func(context.Context, io.Writer) error,
	traceID string,
	cancelCtx context.Context,
	streamDone func(),
) func(context.Context, io.Writer) error {
	return func(ctx context.Context, w io.Writer) error {
		return h.runGuardedStream(ctx, traceID, cancelCtx, streamDone, func(streamCtx context.Context) error {
			return run(streamCtx, w)
		})
	}
}

func (h handler) wrapSSERunWithCancel(
	run func(context.Context, io.Writer, http.Flusher) error,
	traceID string,
	cancelCtx context.Context,
	streamDone func(),
) func(context.Context, io.Writer, http.Flusher) error {
	return func(ctx context.Context, w io.Writer, flusher http.Flusher) error {
		return h.runGuardedStream(ctx, traceID, cancelCtx, streamDone, func(streamCtx context.Context) error {
			return run(streamCtx, w, flusher)
		})
	}
}

func (h handler) runGuardedStream(
	ctx context.Context,
	traceID string,
	cancelCtx context.Context,
	streamDone func(),
	fn func(context.Context) error,
) (err error) {
	if streamDone != nil {
		defer streamDone()
	} else if h.rt != nil {
		doneStream := h.rt.BeginHandlerExecution()
		defer doneStream()
	}
	if ctx == nil {
		ctx = context.Background()
	}
	streamCtx := ctx
	var stopCh <-chan struct{}
	if h.rt != nil {
		stopCh = h.rt.StoppingChan()
	}
	var cancel context.CancelFunc
	if stopCh != nil || (cancelCtx != nil && cancelCtx != ctx) {
		streamCtx, cancel = context.WithCancel(ctx)
		defer cancel()
	}

	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("stream panicked: %v", p)
			if h.rt != nil {
				h.rt.getLogger().Error(panicLog{
					Error:      fmt.Sprint(p),
					StackTrace: string(debug.Stack()),
				})
			}
			return
		}
		if err != nil && errors.Is(err, context.Canceled) && streamCtx.Err() != nil {
			return
		}
		h.logError(traceID, err)
	}()

	if cancel != nil {
		alreadyCanceled := false
		if stopCh != nil {
			select {
			case <-stopCh:
				cancel()
				alreadyCanceled = true
			default:
			}
		}
		if !alreadyCanceled && cancelCtx != nil && cancelCtx != ctx {
			select {
			case <-cancelCtx.Done():
				cancel()
				alreadyCanceled = true
			default:
			}
		}
		if !alreadyCanceled {
			stopWatch := make(chan struct{})
			var watchDone sync.WaitGroup
			watchDone.Add(1)
			var cancelDone <-chan struct{}
			if cancelCtx != nil && cancelCtx != ctx {
				cancelDone = cancelCtx.Done()
			}
			go func() {
				defer watchDone.Done()
				select {
				case <-stopCh:
					cancel()
				case <-cancelDone:
					cancel()
				case <-streamCtx.Done():
				case <-stopWatch:
				}
			}()
			defer func() {
				close(stopWatch)
				watchDone.Wait()
			}()
		}
	}

	return fn(streamCtx)
}

func (h handler) execute(c *Context, traceID string) (result handlerResult) {
	defer func() {
		if re := recover(); re != nil {
			result = handlerResult{err: piHTTP.ErrorPanicRecovery{}}
			if h.rt != nil {
				h.rt.getLogger().Error(panicLog{
					Error:      fmt.Sprint(re),
					StackTrace: string(debug.Stack()),
				})
			}
		}
	}()

	result.data, result.err = h.function(c)
	result.ctx = c.Context
	h.logError(traceID, result.err)

	return result
}

func healthHandler(c *Context) (any, error) {
	return c.Health(c), nil
}

func liveHandler(*Context) (any, error) {
	return struct {
		Status string `json:"status"`
	}{Status: "UP"}, nil
}

func faviconHandler(*Context) (any, error) {
	data, err := os.ReadFile("./static/favicon.ico")
	if err != nil {
		data, err = static.Files.ReadFile("favicon.ico")
	}

	return response.File{
		Content:     data,
		ContentType: "image/x-icon",
	}, err
}

func catchAllHandler(*Context) (any, error) {
	return nil, piHTTP.ErrorInvalidRoute{}
}

// Log the error(if any) with traceID and errorMessage.
func (h handler) logError(traceID string, err error) {
	if err != nil && h.rt != nil {
		l := h.rt.getLogger()
		errorLog := &ErrorLogEntry{TraceID: traceID, Error: err.Error()}

		// define the default log level for error
		loggerHelper := l.Error

		switch logging.GetLogLevelForError(err) {
		case logging.ERROR:
			// we use the default log level for error
		case logging.INFO:
			loggerHelper = l.Info
		case logging.NOTICE:
			loggerHelper = l.Notice
		case logging.DEBUG:
			loggerHelper = l.Debug
		case logging.WARN:
			loggerHelper = l.Warn
		case logging.FATAL:
			loggerHelper = l.Fatal
		}

		loggerHelper(errorLog)
	}
}

func handleWebSocketUpgrade(r *http.Request) {
	if websocket.IsWebSocketUpgrade(r) {
		// Do not respond with HTTP headers since this is a WebSocket request
		return
	}
}
