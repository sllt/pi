package pi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	gWebsocket "github.com/gorilla/websocket"

	"github.com/sllt/pi/pkg/pi/websocket"
)

var (
	ErrMarshalingResponse = errors.New("error marshaling response")
	ErrConnectionNotFound = errors.New("connection not found for service")
)

func (a *App) OverrideWebsocketUpgrader(wsUpgrader websocket.Upgrader) {
	a.httpServer.ws.WebSocketUpgrader.Upgrader = wsUpgrader
}

// WebSocket registers a handler function for a WebSocket route. This method allows you to define a route handler for
// WebSocket connections. It internally handles the WebSocket handshake and provides a `websocket.Connection` object
// within the handler context. User can access the underlying WebSocket connection using `ctx.GetWebsocketConnection()`.
func (a *App) WebSocket(route string, handler Handler) {
	a.markWSServerRegistered()

	a.GET(route, func(ctx *Context) (any, error) {
		if !a.tryAdmitRuntimeTask() {
			return nil, errAppAlreadyStopped
		}
		defer a.runtimeTasks.Done()

		reqCtx := ctx.Request.Context()
		connID, ok := reqCtx.Value(websocket.WSConnectionKey).(string)
		if !ok || connID == "" {
			return nil, websocket.ErrorConnection
		}

		conn := a.httpServer.ws.GetWebsocketConnection(connID)
		if conn == nil || conn.Conn == nil {
			return nil, websocket.ErrorConnection
		}

		sess, regErr := a.registerServerWSSession(reqCtx, connID, conn)
		if regErr != nil {
			a.httpServer.ws.CloseConnectionIfMatch(connID, conn)
			return nil, regErr
		}
		defer a.unregisterServerWSSession(sess)

		// Preserve request values (auth Principal, trace span, WSConnectionKey) while binding
		// cancellation to App's Phase C drainExecCtx so mid-turn work can finish in Phase B.
		baseValuesCtx := context.WithValue(context.WithoutCancel(ctx.Context), websocket.WSConnectionKey, conn)
		execCtx, execCancel := context.WithCancelCause(baseValuesCtx)
		stopOnDrain := context.AfterFunc(a.drainExecutionContext(), func() {
			execCancel(context.Cause(a.drainExecutionContext()))
		})
		defer func() {
			stopOnDrain()
			execCancel(nil)
		}()

		ctx.Request = conn
		ctx.Context = execCtx
		ctx.ContextLogger.UpdateContext(execCtx)

		a.serveWebSocketLoop(ctx, conn, handler)

		if a.isIntakeClosed() && !conn.IsClosed() {
			closeFrame := gWebsocket.FormatCloseMessage(gWebsocket.CloseGoingAway, "server shutting down")
			_ = conn.WriteControl(gWebsocket.CloseMessage, closeFrame, time.Now().Add(200*time.Millisecond))
		}

		return nil, nil
	})
}

// AddWSService registers a WebSocket service, establishes a persistent connection, and optionally handles reconnection.
// It preserves legacy synchronous dialing semantics while managing background retries within App's lifecycle.
// New applications using pi.Build should prefer the declarative AddWSClient API.
func (a *App) AddWSService(serviceName, url string, headers http.Header, enableReconnection bool, retryInterval time.Duration) error {
	if a.isIntakeClosed() {
		return errAppAlreadyStopped
	}
	a.runtimeMu.Lock()
	if a.runtimeState == lifecycleStopping || a.runtimeState == lifecycleStopped {
		a.runtimeMu.Unlock()
		return errAppAlreadyStopped
	}
	var prevCompat *compatWSService
	filtered := a.compatWSServices[:0]
	for _, existing := range a.compatWSServices {
		if existing.serviceName == serviceName {
			prevCompat = existing
			continue
		}
		filtered = append(filtered, existing)
	}
	a.compatWSServices = filtered
	a.runtimeMu.Unlock()

	if prevCompat != nil {
		prevCompat.stopAndWait()
	}

	clonedHeaders := headers.Clone()
	conn, resp, err := gWebsocket.DefaultDialer.Dial(url, clonedHeaders)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}

	if err != nil {
		cleanErr := sanitizeWSDialError(err, url)
		a.Logger().Errorf("Failed to establish WebSocket connection to %s: %v", redactEndpointURL(url), cleanErr)

		if enableReconnection {
			return a.registerCompatWSReconnect(serviceName, url, clonedHeaders, retryInterval, nil)
		}

		return cleanErr
	}

	if enableReconnection {
		if regErr := a.registerCompatWSReconnect(serviceName, url, clonedHeaders, retryInterval, conn); regErr != nil {
			return regErr
		}
		a.Logger().Infof("Successfully connected to WebSocket service: %s", serviceName)
		return nil
	}

	if a.isIntakeClosed() {
		_ = conn.Close()
		return errAppAlreadyStopped
	}

	wsConn := &websocket.Connection{Conn: conn}
	if a.kernel != nil {
		a.kernel.addWSConnection(serviceName, wsConn)
	}
	if a.isIntakeClosed() {
		if hub := a.WebSocketHub(); hub != nil {
			hub.CloseConnectionIfMatch(serviceName, wsConn)
		} else {
			_ = wsConn.Close()
		}
		return errAppAlreadyStopped
	}
	a.Logger().Infof("Successfully connected to WebSocket service: %s", serviceName)

	return nil
}

func (a *App) registerCompatWSReconnect(
	serviceName, url string,
	headers http.Header,
	retryInterval time.Duration,
	initialConn *gWebsocket.Conn,
) error {
	compat := newCompatWSService(a, serviceName, url, headers, retryInterval)

	a.runtimeMu.Lock()
	if a.isIntakeClosed() || a.runtimeState == lifecycleStopping || a.runtimeState == lifecycleStopped || a.stopDone != nil {
		a.runtimeMu.Unlock()
		compat.cancelStop()
		if initialConn != nil {
			_ = initialConn.Close()
		}
		return errAppAlreadyStopped
	}
	a.compatWSServices = append(a.compatWSServices, compat)
	a.runtimeMu.Unlock()

	if initialConn != nil {
		if compat.attachConnected(initialConn) == nil {
			return errAppAlreadyStopped
		}
		return nil
	}

	compat.ensureRetryLoop()
	return nil
}

func (a *App) handleReconnection(serviceName, url string, headers http.Header, retryInterval time.Duration) {
	_ = a.registerCompatWSReconnect(serviceName, url, headers.Clone(), retryInterval, nil)
}

func (a *App) serveWebSocketLoop(ctx *Context, conn *websocket.Connection, handler Handler) {
	for {
		if (a != nil && a.isIntakeClosed()) || conn.IsClosed() {
			break
		}

		response, err := handler(ctx)
		if err != nil && a != nil && a.isIntakeClosed() && isExpectedWSShutdownErr(err) {
			break
		}
		if handleWebSocketError(ctx, "error handling message", err) {
			break
		}
		if a != nil && a.isIntakeClosed() && response == nil {
			break
		}

		message, err := serializeMessage(response)
		if handleWebSocketError(ctx, "failed to serialize message", err) {
			continue
		}

		err = conn.WriteMessage(websocket.TextMessage, message)
		if handleWebSocketError(ctx, "failed to write response to websocket", err) {
			break
		}

		if a != nil && a.isIntakeClosed() {
			break
		}
	}
}

func handleWebSocketConnection(ctx *Context, conn *websocket.Connection, handler Handler) {
	for {
		if conn.IsClosed() {
			break
		}

		response, err := handler(ctx)
		if handleWebSocketError(ctx, "error handling message", err) {
			break
		}

		message, err := serializeMessage(response)
		if handleWebSocketError(ctx, "failed to serialize message", err) {
			continue
		}

		err = conn.WriteMessage(websocket.TextMessage, message)
		if handleWebSocketError(ctx, "failed to write response to websocket", err) {
			break
		}
	}
}

func isExpectedWSShutdownErr(err error) bool {
	if err == nil {
		return false
	}
	var closeErr *gWebsocket.CloseError
	if errors.As(err, &closeErr) {
		return true
	}
	if errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, websocket.ErrConnectionClosed) ||
		errors.Is(err, gWebsocket.ErrCloseSent) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func handleWebSocketError(ctx *Context, msg string, err error) bool {
	if err == nil {
		return false
	}

	ctx.Errorf("%s: %v", msg, err)

	var closeErr *gWebsocket.CloseError
	if errors.As(err, &closeErr) {
		return true
	}
	if errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, websocket.ErrConnectionClosed) ||
		errors.Is(err, websocket.ErrWriteLockTimeout) ||
		errors.Is(err, gWebsocket.ErrCloseSent) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	errText := err.Error()
	return strings.Contains(errText, "broken pipe") ||
		strings.Contains(errText, "connection reset by peer") ||
		strings.Contains(errText, "use of closed network connection")
}

func serializeMessage(response any) ([]byte, error) {
	var (
		message []byte
		err     error
	)

	switch v := response.(type) {
	case string:
		message = []byte(v)
	case []byte:
		message = v
	default:
		message, err = json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrMarshalingResponse, err)
		}
	}

	return message, nil
}
