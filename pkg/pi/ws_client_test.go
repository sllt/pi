package pi

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gWebsocket "github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sllt/pi/pkg/pi/config"
	"github.com/sllt/pi/pkg/pi/logging"
)

func httpToWSURL(raw string) string {
	return "ws" + strings.TrimPrefix(raw, "http")
}

type closableTestLogger struct {
	logging.Logger
	closed atomic.Int32
}

func (l *closableTestLogger) Close() error {
	l.closed.Add(1)
	return nil
}

func TestWSClient_ValidationAndPureConstruction(t *testing.T) {
	var handshakes atomic.Int32
	var gotHeader atomic.Value
	var gotProto atomic.Value

	upgrader := gWebsocket.Upgrader{
		Subprotocols: []string{"v1.pi", "v2.pi"},
		CheckOrigin:  func(*http.Request) bool { return true },
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handshakes.Add(1)
		gotHeader.Store(r.Header.Get("X-Client-Token"))
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		gotProto.Store(conn.Subprotocol())
		defer conn.Close()
		for {
			if _, _, readErr := conn.ReadMessage(); readErr != nil {
				return
			}
		}
	}))
	defer srv.Close()

	app, err := Build(
		WithConfig(map[string]string{
			"HTTP_ENABLED": "false",
			"GRPC_ENABLED": "false",
		}),
	)
	require.NoError(t, err)

	require.ErrorIs(t, app.AddWSClient("   ", httpToWSURL(srv.URL), WSClientPolicy{}), errWSServiceNameEmpty)
	require.ErrorIs(t, app.AddWSClient("bad-url", "http://example.com/ws", WSClientPolicy{}), errWSClientInvalidURL)
	require.ErrorIs(t, app.AddWSClient("bad-host", "ws:///missing-host", WSClientPolicy{}), errWSClientInvalidURL)
	require.ErrorIs(t, app.AddWSClient("neg-dur", httpToWSURL(srv.URL), WSClientPolicy{WriteTimeout: -time.Second}), errWSClientNegativeDuration)
	require.ErrorIs(t, app.AddWSClient("bad-backoff", httpToWSURL(srv.URL), WSClientPolicy{
		BackoffInitial: 5 * time.Second,
		BackoffMax:     time.Second,
	}), errWSClientBackoffRange)
	require.ErrorIs(t, app.AddWSClient("neg-retries", httpToWSURL(srv.URL), WSClientPolicy{MaxRetries: -1}), errWSClientNegativeRetries)

	headers := http.Header{"X-Client-Token": []string{"immutable-token"}}
	tlsCfg := &tls.Config{ServerName: "initial.example"}
	protos := []string{"v1.pi"}

	require.NoError(t, app.AddWSClient("orders-ws", httpToWSURL(srv.URL), WSClientPolicy{
		Headers:          headers,
		TLSConfig:        tlsCfg,
		Subprotocols:     protos,
		Required:         true,
		HandshakeTimeout: 2 * time.Second,
		WriteTimeout:     2 * time.Second,
	}))
	require.ErrorIs(t, app.AddWSClient("orders-ws", httpToWSURL(srv.URL), WSClientPolicy{}), errWSServiceNameDuplicate)

	// Mutate caller inputs after registration to verify defensive cloning.
	headers.Set("X-Client-Token", "mutated-token")
	tlsCfg.ServerName = "mutated.example"
	protos[0] = "v2.pi"

	// Pure construction guarantee: zero network dials before Start.
	assert.Equal(t, int32(0), handshakes.Load())
	assert.Nil(t, app.WebSocketHub().GetConnectionByServiceName("orders-ws"))

	startCtx, startCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer startCancel()
	require.NoError(t, app.Start(startCtx))
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer stopCancel()
		require.NoError(t, app.Stop(stopCtx))
	}()

	require.ErrorIs(t, app.AddWSClient("late-ws", httpToWSURL(srv.URL), WSClientPolicy{}), errWSClientRegisterTooLate)
	assert.Equal(t, int32(1), handshakes.Load())
	assert.Equal(t, "immutable-token", gotHeader.Load())
	assert.Equal(t, "v1.pi", gotProto.Load())
	assert.NotNil(t, app.WebSocketHub().GetConnectionByServiceName("orders-ws"))
}

func TestWSClient_RequiredGatesStartAndReadinessAcrossDisconnect(t *testing.T) {
	var attempts atomic.Int32
	var serverConnsMu sync.Mutex
	var serverConns []*gWebsocket.Conn

	upgrader := gWebsocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n <= 2 {
			http.Error(w, "warming up", http.StatusServiceUnavailable)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		serverConnsMu.Lock()
		serverConns = append(serverConns, conn)
		serverConnsMu.Unlock()

		for {
			mt, msg, readErr := conn.ReadMessage()
			if readErr != nil {
				_ = conn.Close()
				return
			}
			_ = conn.WriteMessage(mt, append([]byte("ack:"), msg...))
		}
	}))
	defer func() {
		serverConnsMu.Lock()
		conns := append([]*gWebsocket.Conn(nil), serverConns...)
		serverConnsMu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
		srv.Close()
	}()

	app, err := Build(
		WithConfig(map[string]string{
			"HTTP_ENABLED": "false",
			"GRPC_ENABLED": "false",
		}),
	)
	require.NoError(t, err)
	app.workerJitter = func(maxDelay time.Duration) time.Duration { return maxDelay }

	var gateGate atomic.Bool
	gateReconnect := make(chan struct{})
	var gateOnce sync.Once

	// Pause server upgrades on attempt 4 when gateGate is set so we can observe disconnected readiness.
	origHandler := srv.Config.Handler
	srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if gateGate.Load() {
			<-gateReconnect
		}
		origHandler.ServeHTTP(w, r)
	})
	defer gateOnce.Do(func() { close(gateReconnect) })

	require.NoError(t, app.AddWSClient("realtime", httpToWSURL(srv.URL), WSClientPolicy{
		Required:           true,
		EnableReconnection: true,
		BackoffInitial:     15 * time.Millisecond,
		BackoffMax:         40 * time.Millisecond,
		HandshakeTimeout:   time.Second,
		WriteTimeout:       time.Second,
	}))

	startCtx, startCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer startCancel()
	require.NoError(t, app.Start(startCtx))
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer stopCancel()
		_ = app.Stop(stopCtx)
	}()

	assert.GreaterOrEqual(t, attempts.Load(), int32(3))
	assert.True(t, app.isReady())

	conn1 := app.WebSocketHub().GetConnectionByServiceName("realtime")
	require.NotNil(t, conn1)
	require.NoError(t, conn1.WriteMessage(gWebsocket.TextMessage, []byte("ping1")))
	_, reply, err := conn1.ReadMessage()
	require.NoError(t, err)
	assert.Equal(t, "ack:ping1", string(reply))

	// Hold reconnection while closing the first server socket so Readiness deterministically drops to false.
	gateGate.Store(true)
	serverConnsMu.Lock()
	firstServerConn := serverConns[0]
	serverConnsMu.Unlock()
	_ = firstServerConn.Close()

	_, _, err = conn1.ReadMessage()
	require.Error(t, err)

	require.Eventually(t, func() bool {
		return !app.isReady()
	}, time.Second, 5*time.Millisecond)

	// Release reconnect gate and verify automatic recovery + readiness restoration.
	gateGate.Store(false)
	gateOnce.Do(func() { close(gateReconnect) })

	require.Eventually(t, func() bool {
		return app.isReady()
	}, 2*time.Second, 10*time.Millisecond)

	conn2 := app.WebSocketHub().GetConnectionByServiceName("realtime")
	require.NotNil(t, conn2)
	assert.NotSame(t, conn1, conn2)
	require.NoError(t, conn2.WriteMessage(gWebsocket.TextMessage, []byte("ping2")))
	_, reply2, err := conn2.ReadMessage()
	require.NoError(t, err)
	assert.Equal(t, "ack:ping2", string(reply2))
}

func TestWSClient_RequiredStartupTimeoutRollback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	var resourceStopped atomic.Bool
	app, err := Build(
		WithConfig(map[string]string{
			"HTTP_ENABLED": "false",
			"GRPC_ENABLED": "false",
		}),
		WithResource(Resource{
			Name:      "owned-dep",
			Ownership: Owned,
			Start:     func(context.Context) error { return nil },
			Stop: func(context.Context) error {
				resourceStopped.Store(true)
				return nil
			},
		}),
	)
	require.NoError(t, err)

	require.NoError(t, app.AddWSClient("must-connect", httpToWSURL(srv.URL), WSClientPolicy{
		Required:           true,
		EnableReconnection: true,
		BackoffInitial:     20 * time.Millisecond,
		BackoffMax:         50 * time.Millisecond,
		HandshakeTimeout:   200 * time.Millisecond,
	}))

	startCtx, cancel := context.WithTimeout(context.Background(), 90*time.Millisecond)
	defer cancel()

	err = app.Start(startCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.True(t, resourceStopped.Load(), "Owned resource must be rolled back when required WS client times out")
	assert.False(t, app.isReady())
}

func TestWSClient_OptionalDegradeAndFatalOnRetriesExhausted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "always down", http.StatusBadGateway)
	}))
	defer srv.Close()

	t.Run("optional client degrades without stopping app", func(t *testing.T) {
		app, err := Build(
			WithConfig(map[string]string{
				"HTTP_ENABLED": "false",
				"GRPC_ENABLED": "false",
			}),
		)
		require.NoError(t, err)
		app.workerJitter = func(maxDelay time.Duration) time.Duration { return maxDelay }

		require.NoError(t, app.AddWSClient("optional-feed", httpToWSURL(srv.URL), WSClientPolicy{
			Required:           false,
			EnableReconnection: true,
			FatalOnFailure:     false,
			BackoffInitial:     10 * time.Millisecond,
			BackoffMax:         20 * time.Millisecond,
			MaxRetries:         2,
			RetryWindow:        5 * time.Second,
			HandshakeTimeout:   200 * time.Millisecond,
		}))

		require.NoError(t, app.Start(context.Background()))
		defer func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			require.NoError(t, app.Stop(stopCtx))
		}()

		slot := app.wsClientSlots()[0]
		require.Eventually(t, func() bool {
			slot.mu.Lock()
			defer slot.mu.Unlock()
			return slot.state == workerDegraded && slot.clientRetries == 2
		}, 2*time.Second, 10*time.Millisecond)

		assert.True(t, app.isReady(), "Non-required degraded WS client must not break app readiness")
	})

	t.Run("FatalOnFailure escalates exhausted retries to app shutdown", func(t *testing.T) {
		app, err := Build(
			WithConfig(map[string]string{
				"HTTP_ENABLED": "false",
				"GRPC_ENABLED": "false",
			}),
		)
		require.NoError(t, err)
		app.workerJitter = func(maxDelay time.Duration) time.Duration { return maxDelay }

		require.NoError(t, app.AddWSClient("critical-feed", httpToWSURL(srv.URL), WSClientPolicy{
			Required:           false,
			EnableReconnection: true,
			FatalOnFailure:     true,
			BackoffInitial:     10 * time.Millisecond,
			BackoffMax:         20 * time.Millisecond,
			MaxRetries:         1,
			RetryWindow:        5 * time.Second,
			HandshakeTimeout:   200 * time.Millisecond,
		}))

		require.NoError(t, app.Start(context.Background()))

		waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		waitErr := app.Wait(waitCtx)
		require.ErrorIs(t, waitErr, errWSClientRetriesExhausted)
	})
}

func TestWSClient_PingIntervalDetectsClosedPeer(t *testing.T) {
	var connects atomic.Int32
	var mu sync.Mutex
	var serverConns []*gWebsocket.Conn

	upgrader := gWebsocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		connects.Add(1)
		mu.Lock()
		serverConns = append(serverConns, conn)
		mu.Unlock()
		for {
			if _, _, readErr := conn.ReadMessage(); readErr != nil {
				_ = conn.Close()
				return
			}
		}
	}))
	defer func() {
		mu.Lock()
		conns := append([]*gWebsocket.Conn(nil), serverConns...)
		mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
		srv.Close()
	}()

	app, err := Build(
		WithConfig(map[string]string{
			"HTTP_ENABLED": "false",
			"GRPC_ENABLED": "false",
		}),
	)
	require.NoError(t, err)
	app.workerJitter = func(maxDelay time.Duration) time.Duration { return maxDelay }

	require.NoError(t, app.AddWSClient("heartbeat-ws", httpToWSURL(srv.URL), WSClientPolicy{
		Required:           true,
		EnableReconnection: true,
		PingInterval:       25 * time.Millisecond,
		WriteTimeout:       100 * time.Millisecond,
		BackoffInitial:     15 * time.Millisecond,
		BackoffMax:         30 * time.Millisecond,
	}))

	require.NoError(t, app.Start(context.Background()))
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, app.Stop(stopCtx))
	}()

	require.Equal(t, int32(1), connects.Load())

	// Abruptly close server-side TCP connection without the client calling ReadMessage/WriteMessage.
	mu.Lock()
	firstConn := serverConns[0]
	mu.Unlock()
	_ = firstConn.UnderlyingConn().Close()

	require.Eventually(t, func() bool {
		return connects.Load() >= 2 && app.isReady()
	}, 3*time.Second, 15*time.Millisecond)
}

func TestAddWSService_CompatSyncDial_StopBeforeStart_AndSingleReconnector(t *testing.T) {
	t.Run("sync dial error when enableReconnection is false", func(t *testing.T) {
		app, err := Build(
			WithConfig(map[string]string{
				"HTTP_ENABLED": "false",
				"GRPC_ENABLED": "false",
			}),
		)
		require.NoError(t, err)

		err = app.AddWSService("strict-legacy", "ws://127.0.0.1:1/ws", nil, false, 50*time.Millisecond)
		require.Error(t, err)

		stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, app.Stop(stopCtx))
	})

	t.Run("Stop before Start cancels background retry promptly", func(t *testing.T) {
		app, err := Build(
			WithConfig(map[string]string{
				"HTTP_ENABLED": "false",
				"GRPC_ENABLED": "false",
			}),
		)
		require.NoError(t, err)

		// Initial dial fails; compat retry enters a 10s timer that Stop-before-Start must cancel immediately.
		err = app.AddWSService("prestart-cancel", "ws://127.0.0.1:1/ws", nil, true, 10*time.Second)
		require.NoError(t, err)

		startStop := time.Now()
		stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, app.Stop(stopCtx))
		assert.Less(t, time.Since(startStop), 750*time.Millisecond)
	})

	t.Run("pre-Start retry connects, Start does not duplicate loop, runtime disconnect reconnects", func(t *testing.T) {
		var allowUpgrade atomic.Bool
		var upgradeCount atomic.Int32
		var mu sync.Mutex
		var serverConns []*gWebsocket.Conn

		upgrader := gWebsocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !allowUpgrade.Load() {
				http.Error(w, "not ready", http.StatusServiceUnavailable)
				return
			}
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			upgradeCount.Add(1)
			mu.Lock()
			serverConns = append(serverConns, conn)
			mu.Unlock()
			for {
				if _, _, readErr := conn.ReadMessage(); readErr != nil {
					_ = conn.Close()
					return
				}
			}
		}))
		defer func() {
			mu.Lock()
			conns := append([]*gWebsocket.Conn(nil), serverConns...)
			mu.Unlock()
			for _, c := range conns {
				_ = c.Close()
			}
			srv.Close()
		}()

		app, err := Build(
			WithConfig(map[string]string{
				"HTTP_ENABLED": "false",
				"GRPC_ENABLED": "false",
			}),
		)
		require.NoError(t, err)

		// Initial call fails synchronously, returns nil, and starts background retry before Start.
		err = app.AddWSService("legacy-feed", httpToWSURL(srv.URL), http.Header{"X-Legacy": []string{"1"}}, true, 25*time.Millisecond)
		require.NoError(t, err)
		assert.Nil(t, app.WebSocketHub().GetConnectionByServiceName("legacy-feed"))

		allowUpgrade.Store(true)
		require.Eventually(t, func() bool {
			return app.WebSocketHub().GetConnectionByServiceName("legacy-feed") != nil
		}, 2*time.Second, 10*time.Millisecond)
		assert.Equal(t, int32(1), upgradeCount.Load())

		// Starting App afterward must NOT spawn a second reconnector or reconnect an already live socket.
		require.NoError(t, app.Start(context.Background()))
		time.Sleep(80 * time.Millisecond)
		assert.Equal(t, int32(1), upgradeCount.Load())

		// Drop active connection during runtime and verify compat reconnector establishes a replacement.
		firstClientConn := app.WebSocketHub().GetConnectionByServiceName("legacy-feed")
		require.NotNil(t, firstClientConn)
		mu.Lock()
		firstSrv := serverConns[0]
		mu.Unlock()
		_ = firstSrv.Close()
		_, _, _ = firstClientConn.ReadMessage()

		require.Eventually(t, func() bool {
			curr := app.WebSocketHub().GetConnectionByServiceName("legacy-feed")
			return curr != nil && curr != firstClientConn && upgradeCount.Load() == 2
		}, 2*time.Second, 10*time.Millisecond)

		stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, app.Stop(stopCtx))

		// Once stopped, closing server socket must not trigger further reconnections.
		mu.Lock()
		secondSrv := serverConns[1]
		mu.Unlock()
		_ = secondSrv.Close()
		time.Sleep(80 * time.Millisecond)
		assert.Equal(t, int32(2), upgradeCount.Load())
	})
}

func TestStage1B_TelemetryDefaultAndBorrowedLoggerIsolation(t *testing.T) {
	borrowed := &closableTestLogger{Logger: logging.NewMockLogger(logging.ERROR)}
	app, err := Build(
		WithConfig(map[string]string{
			"HTTP_ENABLED": "false",
			"GRPC_ENABLED": "false",
		}),
		WithLogger(borrowed),
	)
	require.NoError(t, err)
	assert.False(t, app.hasTelemetry(), "Pure Build must default telemetry to false unless explicitly enabled")

	require.NoError(t, app.Start(context.Background()))
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, app.Stop(stopCtx))
	assert.Equal(t, int32(0), borrowed.closed.Load(), "Borrowed logger injected via WithLogger must not be closed")

	// Pre-injected logger on legacy kernel must also not be closed on closeOwnedLogger().
	preSet := &closableTestLogger{Logger: logging.NewMockLogger(logging.ERROR)}
	c := newLegacyKernel(config.NewMockConfig(map[string]string{
		"APP_NAME":    "borrowed-logger-test",
		"APP_VERSION": "0.5.0-test",
	}), preSet, false)
	require.NoError(t, c.closeOwnedLogger())
	assert.Equal(t, int32(0), preSet.closed.Load(), "Pre-set logger on kernel must not be closed")

	// Kernel-owned remotelogger created by newLegacyKernel MUST be closed cleanly on closeOwnedLogger().
	pollHits := atomic.Int32{}
	logSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		pollHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"serviceName":"owned-log-app","logLevel":"INFO"}]}`))
	}))
	defer logSrv.Close()

	ownedKernel := newLegacyKernel(config.NewMockConfig(map[string]string{
		"APP_NAME":                  "owned-log-app",
		"APP_VERSION":               "0.5.0-test",
		"REMOTE_LOG_URL":            logSrv.URL,
		"REMOTE_LOG_FETCH_INTERVAL": "1",
	}), nil, false)
	require.NoError(t, ownedKernel.closeOwnedLogger())
	hitsAtClose := pollHits.Load()
	time.Sleep(1200 * time.Millisecond)
	assert.Equal(t, hitsAtClose, pollHits.Load(), "Owned remotelogger polling must stop after closeOwnedLogger()")
}
