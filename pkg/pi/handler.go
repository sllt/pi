package pi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"go.opentelemetry.io/otel/trace"

	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"github.com/sllt/pi/pkg/pi/http/response"
	"github.com/sllt/pi/pkg/pi/infra"
	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/sllt/pi/pkg/pi/static"
)

const colorCodeError = 202 // 202 is red color code

type Handler func(c *Context) (any, error)

/*
Developer Note: There is an implementation where we do not need this internal handler struct
and directly use Handler. However, in that case the container dependency is not injected and
has to be created inside ServeHTTP method, which will result in multiple unnecessary calls.
This is what we implemented first.

There is another possibility where we write our own Router implementation and let httpServer
use that router which will return a Handler and httpServer will then create the context with
injecting container and call that Handler with the new context. A similar implementation is
done in CMD. Since this will require us to write our own router - we are not taking that path
for now. In the future, this can be considered as well if we are writing our own HTTP router.
*/

type handler struct {
	function       Handler
	container      *infra.Container
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
	responder := piHTTP.NewResponder(w, r.Method)
	c := newContext(responder, piHTTP.NewRequestWithValidator(r, h.container.Validate), h.container)

	traceID := trace.SpanFromContext(r.Context()).SpanContext().TraceID().String()
	if !trace.SpanFromContext(r.Context()).SpanContext().HasTraceID() {
		traceID = uuid.NewString()
	}
	w.Header().Set("X-Request-ID", traceID)

	if websocket.IsWebSocketUpgrade(r) {
		// If the request is a WebSocket upgrade, do not apply the timeout
		c.Context = r.Context()
	} else if h.requestTimeout != 0 {
		ctx, cancel := context.WithTimeout(r.Context(), h.requestTimeout)
		defer cancel()

		c.Context = ctx
	}

	// Handler middleware and Context.Trace may replace c.Context. The response
	// goroutine owns this original cancellation context and never reads c again.
	requestCtx := c.Context
	results := make(chan handlerResult, 1)

	go func() {
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
		handleWebSocketUpgrade(r)
	}

	// Handle custom headers if 'result' is a 'Response'.
	if resp, ok := result.data.(response.Response); ok && result.err == nil {
		resp.SetCustomHeaders(w)
	}

	// Handler function completed
	responder.Respond(result.data, result.err)
}

func (h handler) execute(c *Context, traceID string) (result handlerResult) {
	defer func() {
		if re := recover(); re != nil {
			result = handlerResult{err: piHTTP.ErrorPanicRecovery{}}
			h.container.Logger.Error(panicLog{
				Error:      fmt.Sprint(re),
				StackTrace: string(debug.Stack()),
			})
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
	if err != nil {
		errorLog := &ErrorLogEntry{TraceID: traceID, Error: err.Error()}

		// define the default log level for error
		loggerHelper := h.container.Logger.Error

		switch logging.GetLogLevelForError(err) {
		case logging.ERROR:
			// we use the default log level for error
		case logging.INFO:
			loggerHelper = h.container.Logger.Info
		case logging.NOTICE:
			loggerHelper = h.container.Logger.Notice
		case logging.DEBUG:
			loggerHelper = h.container.Logger.Debug
		case logging.WARN:
			loggerHelper = h.container.Logger.Warn
		case logging.FATAL:
			loggerHelper = h.container.Logger.Fatal
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
