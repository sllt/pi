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
}

type handlerResult struct {
	data any
	err  error
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
		if r.Body != nil || r.MultipartForm != nil {
			controller := http.NewResponseController(w)
			if h.requestTimeout > 0 {
				_ = controller.SetReadDeadline(deadline)
			}
			var max int64
			if h.rt != nil {
				max = h.rt.maxBodyBytes
			}
			if max <= 0 {
				max = 32 << 20
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
	c := newContext(responder, piHTTP.NewRequestWithSnapshot(r, validate, snapshotBytes, hasSnapshot), h.rt)

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

	doneHandler := h.rt.BeginHandlerExecution()
	go func() {
		defer doneHandler()
		defer func() {
			<-responseDone
			if r.MultipartForm != nil {
				_ = r.MultipartForm.RemoveAll()
			}
		}()
		// Buffering lets a late Handler finish even after the response has timed out.
		results <- h.execute(c, traceID)
	}()

	var result handlerResult

	select {
	case <-requestCtx.Done():
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
	if result.err == nil {
		result.data = h.guardStream(result.data, traceID)
	}

	// Handler function completed
	responder.Respond(result.data, result.err)
}

// Explicit status envelopes retain the same stream ownership and error logging.
func (h handler) guardStream(data any, traceID string) any {
	switch value := data.(type) {
	case response.Result:
		value.Data = h.guardStream(value.Data, traceID)
		return value
	case response.Stream:
		if value.Run == nil {
			return value
		}
		run := value.Run
		value.Run = func(ctx context.Context, w io.Writer) (err error) {
			defer func() {
				if p := recover(); p != nil {
					err = fmt.Errorf("stream panicked: %v", p)
				}
				h.logError(traceID, err)
			}()
			return run(ctx, w)
		}
		return value
	}
	return data
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
