package middleware

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"

	gorillaWebsocket "github.com/gorilla/websocket"

	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/sllt/pi/pkg/pi/websocket"
)

var anonWSConnSeq atomic.Uint64

// WSHandlerUpgrade middleware upgrades the incoming http request to a websocket connection using websocket upgrader.
func WSHandlerUpgrade(logger logging.Logger, wsManager *websocket.Manager) func(inner http.Handler) http.Handler {
	return func(inner http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if gorillaWebsocket.IsWebSocketUpgrade(r) {
				if wsManager == nil || wsManager.WebSocketUpgrader == nil {
					http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
					return
				}
				if wsManager.IsAdmissionClosed() {
					http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
					return
				}

				conn, err := wsManager.WebSocketUpgrader.Upgrade(w, r, nil)
				if err != nil {
					if logger != nil {
						logger.Errorf("Failed to upgrade to WebSocket: %v", err)
					}
					http.Error(w, "Could not open WebSocket connection", http.StatusBadRequest)

					return
				}

				connID := r.Header.Get("Sec-WebSocket-Key")
				if connID == "" {
					connID = fmt.Sprintf("ws-anon-%d", anonWSConnSeq.Add(1))
				}
				wsConn := &websocket.Connection{Conn: conn}
				if policy := wsManager.HubPolicy(); policy.WriteTimeout > 0 {
					wsConn.SetWriteTimeout(policy.WriteTimeout)
				}

				// Add the connection to the hub and ensure cleanup if downstream middlewares reject before App.WebSocket.
				wsManager.AddWebsocketConnection(connID, wsConn)
				defer wsManager.CloseConnectionIfMatch(connID, wsConn)

				// Store the websocket connection key in the context
				ctx := context.WithValue(r.Context(), websocket.WSConnectionKey, connID)
				r = r.WithContext(ctx)
			}

			inner.ServeHTTP(w, r)
		})
	}
}
