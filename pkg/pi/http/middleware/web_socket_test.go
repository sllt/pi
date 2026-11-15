package middleware

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/sllt/pi/pkg/pi/logging"
	piWebSocket "github.com/sllt/pi/pkg/pi/websocket"
)

var errConnection = errors.New("can't create connection")

func initializeWebSocketMocks(t *testing.T) (*piWebSocket.MockUpgrader, *piWebSocket.Manager) {
	t.Helper()

	mockCtrl := gomock.NewController(t)
	mockUpgrader := piWebSocket.NewMockUpgrader(mockCtrl)

	wsManager := piWebSocket.New()
	wsManager.WebSocketUpgrader = &piWebSocket.WSUpgrader{Upgrader: mockUpgrader}

	return mockUpgrader, wsManager
}

func TestWSConnectionCreate_Error(t *testing.T) {
	mockUpgrader, wsManager := initializeWebSocketMocks(t)
	logger := logging.NewMockLogger(logging.DEBUG)

	mockUpgrader.EXPECT().Upgrade(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil,
		errConnection).Times(1)

	handler := WSHandlerUpgrade(logger, wsManager)(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
	}))

	req := httptest.NewRequest(http.MethodGet, "/ws", http.NoBody)
	req.Header.Set("Connection", "upgrade")
	req.Header.Set("Upgrade", "websocket")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if status := recorder.Code; status != http.StatusBadRequest {
		t.Errorf("Unexpected status code: %d", status)
	}
}

func Test_WSConnectionCreate_Success(t *testing.T) {
	mockUpgrader, wsManager := initializeWebSocketMocks(t)
	logger := logging.NewMockLogger(logging.DEBUG)

	mockConn := &piWebSocket.Connection{
		Conn: &websocket.Conn{},
	}

	mockUpgrader.EXPECT().Upgrade(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockConn.Conn, nil).Times(1)

	middleware := WSHandlerUpgrade(logger, wsManager)

	innerHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/ws", http.NoBody)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")

	rec := httptest.NewRecorder()

	handler := middleware(innerHandler)
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}
