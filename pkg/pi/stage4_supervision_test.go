package pi

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gWebsocket "github.com/gorilla/websocket"
	"github.com/sllt/tao/actor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	metricSdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"

	"github.com/sllt/pi/pkg/pi/datasource"
	"github.com/sllt/pi/pkg/pi/datasource/pubsub"
	piRedis "github.com/sllt/pi/pkg/pi/datasource/redis"
	"github.com/sllt/pi/pkg/pi/http/middleware"
	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/sllt/pi/pkg/pi/metrics"
	"github.com/sllt/pi/pkg/pi/service"
	"github.com/sllt/pi/pkg/pi/testutil"
	"github.com/sllt/pi/pkg/pi/websocket"
)

type stage4DummyGRPCSvc interface{}
type stage4DummyGRPCImpl struct{}

var stage4DummyGRPCDesc = grpc.ServiceDesc{
	ServiceName: "stage4.v1.OrderService",
	HandlerType: (*stage4DummyGRPCSvc)(nil),
}

// --- 1. Server WebSocket Actor Hub, Bounded Queue & Phase B Drain before SQL Close ---

func TestStage4_WSServer_ActorHubSlowConsumerAndDrainBeforeSQLClose(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "ws_drain.db")
	httpPort := testutil.GetFreePort(t)

	app, err := Build(
		WithConfig(map[string]string{
			"APP_NAME":              "ws-stage4-app",
			"HTTP_HOST":             "127.0.0.1",
			"HTTP_PORT":             strconv.Itoa(httpPort),
			"DB_DIALECT":            "sqlite",
			"DB_NAME":               dbPath,
			"SHUTDOWN_GRACE_PERIOD": "3s",
		}),
		WithManagedSQL(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = app.Stop(context.Background()) })

	app.ConfigureWebSocketHub(websocket.HubPolicy{
		MailboxSize:      4,
		MaxMessageBytes:  128,
		WriteTimeout:     2 * time.Second,
		DisconnectOnSlow: false,
	})

	midTurnEntered := make(chan struct{}, 1)
	releaseMidTurn := make(chan struct{})
	var sqlInsertErr atomic.Value

	app.WebSocket("/ws", func(c *Context) (any, error) {
		var msg string
		if bErr := c.Bind(&msg); bErr != nil {
			return nil, bErr
		}
		if msg == "hold-in-drain" {
			select {
			case midTurnEntered <- struct{}{}:
			default:
			}
			<-releaseMidTurn
			// Must succeed during Phase B drain BEFORE ManagedSQL closes!
			_, execErr := c.SQL().ExecContext(c.Context, "INSERT INTO ws_events(note) VALUES (?)", "completed-before-sql-close")
			if execErr != nil {
				sqlInsertErr.Store(execErr)
				return nil, execErr
			}
			return "ack-drain", nil
		}
		return "echo:" + msg, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, app.Start(ctx))

	_, err = app.SQL().ExecContext(ctx, "CREATE TABLE ws_events (id INTEGER PRIMARY KEY AUTOINCREMENT, note TEXT)")
	require.NoError(t, err)

	wsURL := fmt.Sprintf("ws://127.0.0.1:%d/ws", httpPort)
	cli1, resp1, err := gWebsocket.DefaultDialer.Dial(wsURL, nil)
	if resp1 != nil && resp1.Body != nil {
		_ = resp1.Body.Close()
	}
	require.NoError(t, err)
	defer cli1.Close()

	// Second idle client blocked in server ReadMessage to verify Stop wakes idle reads promptly.
	cliIdle, respIdle, err := gWebsocket.DefaultDialer.Dial(wsURL, nil)
	if respIdle != nil && respIdle.Body != nil {
		_ = respIdle.Body.Close()
	}
	require.NoError(t, err)
	defer cliIdle.Close()

	// Wait until both sessions are registered in Hub.
	require.Eventually(t, func() bool {
		return len(app.httpServer.ws.ListConnections()) == 2 && app.RuntimeSnapshot().WSServer.ActiveSessions == 2
	}, 2*time.Second, 10*time.Millisecond)

	var connID1 string
	for _, cid := range app.httpServer.ws.ListConnections() {
		connID1 = cid
		break
	}
	require.NotEmpty(t, connID1)

	// Verify Hub.Send copies caller payload and dispatches via wsSessionActor.
	buf := []byte("actor-pushed-hello")
	require.NoError(t, app.httpServer.ws.Send(connID1, websocket.TextMessage, buf))
	buf[0] = 'Z'

	// Oversized message rejected with ErrMessageTooLarge.
	require.ErrorIs(t, app.httpServer.ws.Send(connID1, websocket.BinaryMessage, make([]byte, 200)), websocket.ErrMessageTooLarge)

	// Read the pushed message from whichever client corresponds to connID1 (or send echo first).
	require.NoError(t, cli1.WriteMessage(gWebsocket.TextMessage, []byte("ping")))
	var sawEcho bool
	for range 2 {
		_ = cli1.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, got, rErr := cli1.ReadMessage()
		if rErr != nil {
			break
		}
		if string(got) == "echo:ping" {
			sawEcho = true
			break
		}
	}
	require.True(t, sawEcho)

	// Trigger mid-turn handler on cli1, then initiate App.Stop while cli1 is mid-turn and cliIdle is blocked in ReadMessage.
	require.NoError(t, cli1.WriteMessage(gWebsocket.TextMessage, []byte("hold-in-drain")))
	<-midTurnEntered

	stopErrCh := make(chan error, 1)
	go func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		stopErrCh <- app.Stop(stopCtx)
	}()

	// Once Stop begins, new WebSocket upgrade requests must be rejected immediately (503 or refused).
	require.Eventually(t, func() bool {
		return app.isIntakeClosed()
	}, time.Second, 5*time.Millisecond)

	lateCli, lateResp, lateErr := gWebsocket.DefaultDialer.Dial(wsURL, nil)
	if lateCli != nil {
		_ = lateCli.Close()
	}
	if lateResp != nil && lateResp.Body != nil {
		_ = lateResp.Body.Close()
	}
	require.Error(t, lateErr, "upgrade after Stop begins must be rejected")

	// Allow mid-turn wsHandler to execute its SQL write during Phase B drain!
	time.Sleep(40 * time.Millisecond)
	close(releaseMidTurn)

	require.NoError(t, <-stopErrCh)
	require.Nil(t, sqlInsertErr.Load(), "in-flight wsHandler SQL write must succeed before ManagedSQL closes")

	// Verify row persisted in SQLite file.
	verifyDB, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	defer verifyDB.Close()
	var count int
	require.NoError(t, verifyDB.QueryRow("SELECT COUNT(*) FROM ws_events WHERE note = ?", "completed-before-sql-close").Scan(&count))
	assert.Equal(t, 1, count)
}

func TestStage4_WSServer_ActorSlowConsumerDisconnect(t *testing.T) {
	httpPort := testutil.GetFreePort(t)
	app, err := Build(WithConfig(map[string]string{
		"HTTP_HOST": "127.0.0.1",
		"HTTP_PORT": strconv.Itoa(httpPort),
	}))
	require.NoError(t, err)

	app.ConfigureWebSocketHub(websocket.HubPolicy{
		MailboxSize:      1,
		MaxMessageBytes:  256,
		WriteTimeout:     5 * time.Second,
		DisconnectOnSlow: true,
	})

	app.WebSocket("/ws", func(c *Context) (any, error) {
		var s string
		if err := c.Bind(&s); err != nil {
			return nil, err
		}
		return s, nil
	})

	require.NoError(t, app.Start(context.Background()))
	defer func() { _ = app.Stop(context.Background()) }()

	wsURL := fmt.Sprintf("ws://127.0.0.1:%d/ws", httpPort)
	cli, resp, err := gWebsocket.DefaultDialer.Dial(wsURL, nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	require.NoError(t, err)
	defer cli.Close()

	var connID string
	require.Eventually(t, func() bool {
		ids := app.httpServer.ws.ListConnections()
		if len(ids) != 1 || app.RuntimeSnapshot().WSServer.ActiveSessions != 1 {
			return false
		}
		cid := ids[0]
		app.wsServerSessionsMu.RLock()
		sess := app.wsServerSessions[cid]
		app.wsServerSessionsMu.RUnlock()
		if sess == nil || sess.getActorPID() == (actor.PID{}) {
			return false
		}
		connID = cid
		return true
	}, 2*time.Second, 5*time.Millisecond)

	// Acquire write lock via NextWriter without closing so wsSessionActor blocks on its first frame,
	// deterministically saturating the session semaphore/mailbox!
	srvConn := app.httpServer.ws.GetWebsocketConnection(connID)
	require.NotNil(t, srvConn)
	holdWriter, err := srvConn.NextWriter(websocket.TextMessage)
	require.NoError(t, err)
	defer func() { _ = holdWriter.Close() }()

	var slowErr error
	for range 16 {
		sErr := app.httpServer.ws.Send(connID, websocket.TextMessage, []byte("burst-frame"))
		if sErr != nil {
			slowErr = sErr
			break
		}
	}
	require.ErrorIs(t, slowErr, websocket.ErrSlowConsumer)
	require.ErrorIs(t, slowErr, actor.ErrMailboxFull)
	assert.Nil(t, app.httpServer.ws.GetWebsocketConnection(connID), "DisconnectOnSlow must evict saturated session")
	assert.GreaterOrEqual(t, app.RuntimeSnapshot().WSServer.DroppedTotal, uint64(1))
}

// --- 2. Remaining Middleware & Service Background Loops (V050-04 Part 2) ---

type mockJWKSProvider struct {
	calls atomic.Int32
	body  string
}

func (m *mockJWKSProvider) GetWithHeaders(ctx context.Context, _ string, _ map[string]any, _ map[string]string) (*http.Response, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	m.calls.Add(1)
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(m.body)),
	}, nil
}

func generateTestJWKSJSON(t *testing.T, kid string) string {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	nB64 := base64.RawURLEncoding.EncodeToString(priv.PublicKey.N.Bytes())
	eB64 := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(priv.PublicKey.E)).Bytes())
	payload, err := json.Marshal(map[string]any{
		"keys": []map[string]string{
			{"kid": kid, "kty": "RSA", "n": nB64, "e": eB64},
		},
	})
	require.NoError(t, err)
	return string(payload)
}

func TestStage4_OAuthRateLimiterCircuitBreaker_LifecycleCleanup(t *testing.T) {
	jwksJSON := generateTestJWKSJSON(t, "k1")

	t.Run("NewOAuthContext periodic and non-positive interval stop cleanly", func(t *testing.T) {
		prov := &mockJWKSProvider{body: jwksJSON}
		ctx, cancel := context.WithCancel(context.Background())
		pk := middleware.NewOAuthContext(ctx, middleware.OauthConfigs{
			Provider:        prov,
			RefreshInterval: 15 * time.Millisecond,
			Path:            "/.well-known/jwks.json",
		})
		require.Eventually(t, func() bool {
			return pk.Get("k1") != nil && prov.calls.Load() >= 2
		}, time.Second, 5*time.Millisecond)

		cancel()
		closer, ok := pk.(io.Closer)
		require.True(t, ok)
		require.NoError(t, closer.Close())
		afterClose := prov.calls.Load()
		time.Sleep(45 * time.Millisecond)
		assert.Equal(t, afterClose, prov.calls.Load(), "JWKS refresh loop must stop after Close/cancel")

		// RefreshInterval <= 0 must NOT panic in time.NewTicker and must fetch once.
		provOnce := &mockJWKSProvider{body: jwksJSON}
		pkOnce := middleware.NewOAuthContext(context.Background(), middleware.OauthConfigs{
			Provider:        provOnce,
			RefreshInterval: 0,
			Path:            "/jwks",
		})
		require.Eventually(t, func() bool { return pkOnce.Get("k1") != nil }, time.Second, 5*time.Millisecond)
		require.NoError(t, pkOnce.(io.Closer).Close())
		assert.Equal(t, int32(1), provOnce.calls.Load())
	})

	t.Run("RateLimiter stores and CircuitBreaker close cleanly with App.Stop", func(t *testing.T) {
		var healthHits atomic.Int32
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/.well-known/alive" || r.URL.Path == "/health" {
				healthHits.Add(1)
				w.WriteHeader(http.StatusOK)
				return
			}
			if r.URL.Path == "/jwks" {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(jwksJSON))
				return
			}
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer upstream.Close()

		app, err := Build(WithConfig(map[string]string{"HTTP_ENABLED": "false"}))
		require.NoError(t, err)
		defer func() { _ = app.Stop(context.Background()) }()

		// Register HTTP service with CircuitBreaker + RateLimiter + OAuth middleware on App.
		app.AddHTTPService("downstream", upstream.URL,
			&service.CircuitBreakerConfig{Threshold: 1, Interval: 15 * time.Millisecond},
			&service.RateLimiterConfig{
				Requests: 100,
				Window:   time.Second,
				Burst:    200,
				Store:    service.NewLocalRateLimiterStore(),
			},
		)
		app.EnableOAuth(upstream.URL+"/jwks", 2)

		mwStore := middleware.NewMemoryRateLimiterStore(middleware.RateLimiterConfig{
			RequestsPerSecond: 10,
			Burst:             10,
		})
		mwStore.StartCleanup(context.Background())
		mwStore.StartCleanup(context.Background()) // idempotent
		mwStore.StopCleanup()
		mwStore.StopCleanup() // idempotent

		// CircuitBreaker with Interval <= 0 must not panic (use standalone service so closing zeroCB does not close "downstream").
		standaloneBase := service.NewHTTPService(upstream.URL, app.Logger(), app.Metrics())
		zeroCB := service.NewCircuitBreakerWithContext(context.Background(), service.CircuitBreakerConfig{
			Threshold: 2,
			Interval:  0,
		}, standaloneBase)
		require.NoError(t, service.CloseService(zeroCB))

		require.NoError(t, app.Start(context.Background()))

		// Trip downstream circuit breaker so periodic probe ticks hit upstream.
		svc := app.HTTPService("downstream")
		for range 3 {
			resp, _ := svc.Get(context.Background(), "/fail", nil)
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
		}
		require.Eventually(t, func() bool { return healthHits.Load() >= 1 }, time.Second, 10*time.Millisecond)

		// App.Stop closes Container.Services (CircuitBreaker + RateLimiter) and OAuth middleware closer!
		require.NoError(t, app.Stop(context.Background()))
		settled := healthHits.Load()
		time.Sleep(50 * time.Millisecond)
		assert.Equal(t, settled, healthHits.Load(), "CircuitBreaker background probe must stop on App.Stop")
	})
}

// --- 3. Tri-State Health Probes, gRPC Health Sync & Hung Adapter Isolation (V050-05) ---

func TestStage4_TriStateHealthProbesAndGRPCHealthSync(t *testing.T) {
	t.Run("startup failure keeps Startup false", func(t *testing.T) {
		app, err := Build(WithConfig(map[string]string{"HTTP_ENABLED": "false"}))
		require.NoError(t, err)
		require.NoError(t, app.AddWorker("never-ready", WorkerPolicy{
			Mode:      SuperviseRestart,
			Required:  true,
			WaitReady: true,
		}, func(c *Context) error {
			<-c.Done()
			return c.Err()
		}))

		startCtx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
		defer cancel()
		require.Error(t, app.Start(startCtx))
		assert.False(t, app.isStartupComplete(), "failed Start must never latch Startup=true")
		assert.False(t, app.isReady())
		_ = app.Stop(context.Background())
	})

	t.Run("HTTP probes and gRPC Health synchronize across Startup, BackingOff, Recovery, HeartbeatStall, and Stop", func(t *testing.T) {
		httpPort := testutil.GetFreePort(t)
		grpcPort := testutil.GetFreePort(t)

		app, err := Build(WithConfig(map[string]string{
			"APP_NAME":     "probe-sync-app",
			"APP_VERSION":  "0.5.0-test",
			"HTTP_HOST":    "127.0.0.1",
			"HTTP_PORT":    strconv.Itoa(httpPort),
			"GRPC_HOST":    "127.0.0.1",
			"GRPC_PORT":    strconv.Itoa(grpcPort),
			"GRPC_ENABLED": "true",
		}))
		require.NoError(t, err)

		app.GET("/ping", func(*Context) (any, error) {
			return "pong", nil
		})
		app.RegisterService(&stage4DummyGRPCDesc, &stage4DummyGRPCImpl{})

		// Pre-Start state check via test HTTP requests against router and state helpers.
		assert.False(t, app.isStartupComplete())
		assert.True(t, app.isAlive())
		assert.False(t, app.isReady())

		failGen1Ch := make(chan struct{})
		allowGen2ReadyCh := make(chan struct{})
		var genCount atomic.Int32

		require.NoError(t, app.AddWorker("critical-dep", WorkerPolicy{
			Mode:           SuperviseRestart,
			Required:       true,
			WaitReady:      true,
			MaxRestarts:    5,
			RestartWindow:  10 * time.Second,
			BackoffInitial: 60 * time.Millisecond,
			BackoffMax:     80 * time.Millisecond,
		}, func(c *Context) error {
			g := genCount.Add(1)
			if g == 1 {
				require.NoError(t, c.MarkReady())
				select {
				case <-failGen1Ch:
					return errors.New("transient upstream disconnect")
				case <-c.Done():
					return nil
				}
			}
			select {
			case <-allowGen2ReadyCh:
			case <-c.Done():
				return nil
			}
			require.NoError(t, c.MarkReady())
			<-c.Done()
			return nil
		}))

		require.NoError(t, app.Start(context.Background()))
		defer func() { _ = app.Stop(context.Background()) }()

		baseURL := fmt.Sprintf("http://127.0.0.1:%d", httpPort)
		getStatus := func(path string) (int, map[string]any) {
			resp, reqErr := http.Get(baseURL + path)
			require.NoError(t, reqErr)
			defer resp.Body.Close()
			var body map[string]any
			_ = json.NewDecoder(resp.Body).Decode(&body)
			return resp.StatusCode, body
		}

		grpcConn, err := grpc.NewClient(
			fmt.Sprintf("127.0.0.1:%d", grpcPort),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		require.NoError(t, err)
		defer grpcConn.Close()
		healthCli := healthpb.NewHealthClient(grpcConn)

		checkGRPC := func(svc string) healthpb.HealthCheckResponse_ServingStatus {
			cCtx, cCancel := context.WithTimeout(context.Background(), time.Second)
			defer cCancel()
			res, gErr := healthCli.Check(cCtx, &healthpb.HealthCheckRequest{Service: svc})
			require.NoError(t, gErr)
			return res.GetStatus()
		}

		// 1. Running & Ready: all 3 probes 200 UP, gRPC "" and service SERVING.
		code, _ := getStatus("/.well-known/startup")
		assert.Equal(t, http.StatusOK, code)
		code, _ = getStatus("/.well-known/alive")
		assert.Equal(t, http.StatusOK, code)
		code, _ = getStatus("/.well-known/ready")
		assert.Equal(t, http.StatusOK, code)
		code, healthBody := getStatus("/.well-known/health")
		assert.Equal(t, http.StatusOK, code)
		if dataMap, ok := healthBody["data"].(map[string]any); ok {
			assert.Equal(t, "UP", dataMap["status"])
		}
		assert.Equal(t, healthpb.HealthCheckResponse_SERVING, checkGRPC(""))
		assert.Equal(t, healthpb.HealthCheckResponse_SERVING, checkGRPC("stage4.v1.OrderService"))

		// 2. Trigger Required worker failure -> BackingOff/Starting before gen2 MarkReady:
		// Startup MUST stay locked 200, Alive MUST stay 200, Ready MUST flip to 503, gRPC -> NOT_SERVING!
		close(failGen1Ch)
		require.Eventually(t, func() bool {
			rCode, _ := getStatus("/.well-known/ready")
			return rCode == http.StatusServiceUnavailable &&
				checkGRPC("") == healthpb.HealthCheckResponse_NOT_SERVING &&
				checkGRPC("stage4.v1.OrderService") == healthpb.HealthCheckResponse_NOT_SERVING
		}, 2*time.Second, 10*time.Millisecond)

		sCode, _ := getStatus("/.well-known/startup")
		assert.Equal(t, http.StatusOK, sCode, "Startup must remain locked true during local component restart")
		aCode, _ := getStatus("/.well-known/alive")
		assert.Equal(t, http.StatusOK, aCode, "Alive must remain 200 during local component restart")
		_, degHealth := getStatus("/.well-known/health")
		if dataMap, ok := degHealth["data"].(map[string]any); ok {
			assert.Equal(t, "DEGRADED", dataMap["status"])
		}

		// 3. Allow gen 2 to call MarkReady -> Ready & gRPC return to 200 / SERVING!
		close(allowGen2ReadyCh)
		require.Eventually(t, func() bool {
			rCode, _ := getStatus("/.well-known/ready")
			return rCode == http.StatusOK &&
				checkGRPC("stage4.v1.OrderService") == healthpb.HealthCheckResponse_SERVING
		}, 2*time.Second, 10*time.Millisecond)

		// 4. Simulate pi-root heartbeat watermark stall -> Alive flips to 503 without any DB/MQ I/O!
		app.runtimeMu.Lock()
		rt := app.actorRT
		app.runtimeMu.Unlock()
		require.NotNil(t, rt)
		rt.rootHeartbeatMaxAge.Store(int64(50 * time.Millisecond))
		rt.rootLastBeatNano.Store(time.Now().Add(-time.Second).UnixNano())
		aCodeStalled, _ := getStatus("/.well-known/alive")
		assert.Equal(t, http.StatusServiceUnavailable, aCodeStalled, "stale pi-root heartbeat watermark must fail liveness")
		// Restore watermark.
		rt.rootLastBeatNano.Store(time.Now().UnixNano())
		rt.rootHeartbeatMaxAge.Store(int64(defaultPiRootHeartbeatMaxAge))
		aCodeRestored, _ := getStatus("/.well-known/alive")
		assert.Equal(t, http.StatusOK, aCodeRestored)

		// 5. Stop immediately flips Ready to false / NOT_SERVING.
		require.NoError(t, app.Stop(context.Background()))
		assert.False(t, app.isReady())
	})
}

type hungLegacyPubSubAdapter struct {
	healthCalls atomic.Int32
	unblockCh   chan struct{}
}

func (h *hungLegacyPubSubAdapter) Publish(context.Context, string, []byte) error { return nil }
func (h *hungLegacyPubSubAdapter) Subscribe(context.Context, string) (*pubsub.Message, error) {
	return nil, context.Canceled
}
func (h *hungLegacyPubSubAdapter) CreateTopic(context.Context, string) error { return nil }
func (h *hungLegacyPubSubAdapter) DeleteTopic(context.Context, string) error { return nil }
func (h *hungLegacyPubSubAdapter) Query(context.Context, string, ...any) ([]byte, error) {
	return nil, nil
}
func (h *hungLegacyPubSubAdapter) Close() error { return nil }
func (h *hungLegacyPubSubAdapter) Health() datasource.Health {
	h.healthCalls.Add(1)
	<-h.unblockCh
	return datasource.Health{Status: "UP"}
}

type ctxAwareRedisStub struct {
	piRedis.Client
	sawDeadline atomic.Bool
}

func (r *ctxAwareRedisStub) HealthCheck() datasource.Health {
	return datasource.Health{Status: "UP"}
}

func (r *ctxAwareRedisStub) HealthCheckContext(ctx context.Context) datasource.Health {
	if _, ok := ctx.Deadline(); ok {
		r.sawDeadline.Store(true)
	}
	<-ctx.Done()
	return datasource.Health{Status: "DOWN", Details: map[string]any{"error": ctx.Err().Error()}}
}

func TestStage4_DeepHealth_ContextTimeoutAndHungAdapterSingleflight(t *testing.T) {
	unblock := make(chan struct{})
	defer close(unblock)

	hungPS := &hungLegacyPubSubAdapter{unblockCh: unblock}
	ctxRedis := &ctxAwareRedisStub{}

	c := newTestKernel(nil)
	probeApp := &App{kernel: c}
	require.NoError(t, probeApp.SetPubSub(hungPS, Borrowed))
	require.NoError(t, probeApp.SetRedis(ctxRedis, Borrowed))

	// Launch 12 concurrent deep health probes with a 60ms timeout.
	// Context-aware Redis receives probeCtx deadline and cancels cleanly;
	// hung legacy PubSub is isolated behind singleflight so ONLY 1 goroutine is spawned!
	const callers = 12
	var wg sync.WaitGroup
	results := make([]map[string]any, callers)

	start := time.Now()
	for i := range callers {
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			pCtx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
			defer cancel()
			res := c.Health(pCtx)
			require.NotNil(t, res)
			results[idx] = res
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	assert.Less(t, elapsed, 1500*time.Millisecond, "deep health must bound hung legacy adapters by caller ctx")
	assert.True(t, ctxRedis.sawDeadline.Load(), "HealthCheckContext must receive bounded deadline")
	assert.Equal(t, int32(1), hungPS.healthCalls.Load(), "singleflight must prevent goroutine storm across concurrent /health calls")

	for _, res := range results {
		assert.Equal(t, "DEGRADED", res["status"])
		psHealth, ok := res["pubsub"].(datasource.Health)
		require.True(t, ok)
		assert.Equal(t, "DOWN", psHealth.Status)
		assert.Equal(t, "singleflight_in_progress", psHealth.Details["isolation"])
	}
}

// --- 4. Instance-Scoped Observability, Borrowed Resource Safety & Redacted RuntimeSnapshot (V050-06) ---

type shutdownableTracerProvider struct {
	*sdktrace.TracerProvider
	mu        sync.Mutex
	spans     []string
	shutdowns atomic.Int32
}

type stpExporter struct {
	owner *shutdownableTracerProvider
}

func (e *stpExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.owner.mu.Lock()
	for _, s := range spans {
		e.owner.spans = append(e.owner.spans, s.Name())
	}
	e.owner.mu.Unlock()
	return nil
}

func (e *stpExporter) Shutdown(context.Context) error { return nil }

func newShutdownableTP() *shutdownableTracerProvider {
	holder := &shutdownableTracerProvider{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(&stpExporter{owner: holder}))
	holder.TracerProvider = tp
	return holder
}

func (s *shutdownableTracerProvider) Shutdown(ctx context.Context) error {
	s.shutdowns.Add(1)
	return s.TracerProvider.Shutdown(ctx)
}

func (s *shutdownableTracerProvider) SpanNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.spans...)
}

type stage4RecordingLogger struct {
	logging.Logger
	mu     sync.Mutex
	lines  []string
	closed atomic.Int32
}

func newStage4RecordingLogger() *stage4RecordingLogger {
	return &stage4RecordingLogger{
		Logger: logging.NewLogger(logging.DEBUG),
	}
}

func (l *stage4RecordingLogger) record(msg string) {
	l.mu.Lock()
	l.lines = append(l.lines, msg)
	l.mu.Unlock()
}

func (l *stage4RecordingLogger) Infof(format string, args ...any) {
	l.record(fmt.Sprintf(format, args...))
}
func (l *stage4RecordingLogger) Debugf(format string, args ...any) {
	l.record(fmt.Sprintf(format, args...))
}
func (l *stage4RecordingLogger) Warnf(format string, args ...any) {
	l.record(fmt.Sprintf(format, args...))
}
func (l *stage4RecordingLogger) Errorf(format string, args ...any) {
	l.record(fmt.Sprintf(format, args...))
}
func (l *stage4RecordingLogger) Close() error {
	l.closed.Add(1)
	return nil
}
func (l *stage4RecordingLogger) Joined() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n")
}

func collectMetricNames(t *testing.T, reader *metricSdk.ManualReader) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	out := make(map[string]metricdata.Metrics)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

func TestStage4_DualAppObservabilityIsolationAndRedactedSnapshot(t *testing.T) {
	globalTPBefore := otel.GetTracerProvider()

	portA := testutil.GetFreePort(t)
	portB := testutil.GetFreePort(t)

	// Upstream WS server for AppA's AddWSClient.
	wsUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up := gWebsocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for {
			if _, _, rErr := c.ReadMessage(); rErr != nil {
				return
			}
		}
	}))
	defer wsUpstream.Close()
	wsURLWithSecrets := "ws://" + strings.TrimPrefix(wsUpstream.URL, "http://") + "/stream?token=my-jwt-token&api_key=ak_live_999&env=prod"
	wsURLWithUserInfo := "ws://svcuser:topsecretpass@127.0.0.1:1/unreachable?token=my-jwt-token&env=stage"

	logA := newStage4RecordingLogger()
	logB := newStage4RecordingLogger()

	readerA := metricSdk.NewManualReader()
	mpA := metricSdk.NewMeterProvider(metricSdk.WithReader(readerA))
	metricsA := metrics.NewMetricsManager(mpA.Meter("app-a"), logA)

	readerB := metricSdk.NewManualReader()
	mpB := metricSdk.NewMeterProvider(metricSdk.WithReader(readerB))
	metricsB := metrics.NewMetricsManager(mpB.Meter("app-b"), logB)

	tpOwnedA := newShutdownableTP()
	tpBorrowedB := newShutdownableTP()

	// PubSub mock for AppA to record a handler failure metric with low-cardinality labels.
	var psDelivered atomic.Bool
	psMockA := &stage3BrokerMock{
		semantics: pubsub.AckSemantics{Mode: pubsub.AckModeIndependent, SupportsNack: true},
		subFn: func(ctx context.Context, topic string) (*pubsub.Message, error) {
			if !psDelivered.CompareAndSwap(false, true) {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			msg := pubsub.NewMessage(ctx)
			msg.Topic = topic
			msg.Value = []byte("bad-event")
			msg.Committer = &stage3IndependentCommitter{}
			return msg, nil
		},
	}

	appA, err := Build(
		WithConfig(map[string]string{
			"APP_NAME":       "alpha-service",
			"HTTP_HOST":      "127.0.0.1",
			"HTTP_PORT":      strconv.Itoa(portA),
			"DB_PASSWORD":    "super-secret-db-pass",
			"JWT_SECRET":     "jwt-signing-secret-key",
			"REDIS_PASSWORD": "redis-pass-value",
		}),
		WithLogger(logA),
		WithMetrics(metricsA),
		WithTracerProvider(tpOwnedA, Owned),
		WithPubSub(psMockA, Borrowed),
	)
	require.NoError(t, err)

	appB, err := Build(
		WithConfig(map[string]string{
			"APP_NAME":  "beta-service",
			"HTTP_HOST": "127.0.0.1",
			"HTTP_PORT": strconv.Itoa(portB),
		}),
		WithLogger(logB),
		WithMetrics(metricsB),
		WithTracerProvider(tpBorrowedB, Borrowed),
	)
	require.NoError(t, err)

	// Global OTel TracerProvider must remain untouched by Build + WithTracerProvider!
	assert.Same(t, globalTPBefore, otel.GetTracerProvider())

	// Register routes, worker retry + controller panic, cron, subscriber, and WSClient on AppA; only HTTP route on AppB.
	appA.GET("/alpha", func(c *Context) (any, error) {
		child := c.Trace("alpha-inner-op")
		child.End()
		return "ok-a", nil
	})
	appB.GET("/beta", func(c *Context) (any, error) {
		child := c.Trace("beta-inner-op")
		child.End()
		return "ok-b", nil
	})

	var workerAttempts atomic.Int32
	workerReadyDone := make(chan struct{})
	require.NoError(t, appA.AddWorker("sync-indexer", WorkerPolicy{
		Mode:           SuperviseRestart,
		Required:       false,
		MaxRestarts:    3,
		RestartWindow:  10 * time.Second,
		BackoffInitial: 10 * time.Millisecond,
		BackoffMax:     20 * time.Millisecond,
	}, func(c *Context) error {
		if workerAttempts.Add(1) == 1 {
			return errors.New("first attempt transient error")
		}
		select {
		case <-workerReadyDone:
		default:
			close(workerReadyDone)
		}
		<-c.Done()
		return nil
	}))

	cronRan := make(chan struct{}, 1)
	require.NoError(t, appA.AddCron("* * * * * *", "cleanup-job", func(c *Context) error {
		select {
		case cronRan <- struct{}{}:
		default:
		}
		return nil
	}, CronJobOptions{
		Overlap: CronSkipIfRunning,
	}))

	subFailed := make(chan struct{}, 1)
	require.NoError(t, appA.AddSubscriber("orders-events", SubscribePolicy{
		Concurrency:       1,
		QueueSize:         8,
		MaxHandlerRetries: 0,
		Shutdown:          SubscriberCancel,
	}, func(c *Context) error {
		select {
		case subFailed <- struct{}{}:
		default:
		}
		return errors.New("poison payload")
	}))

	require.NoError(t, appA.AddWSClient("realtime-feed", wsURLWithSecrets, WSClientPolicy{
		Required:           true,
		EnableReconnection: true,
		HandshakeTimeout:   2 * time.Second,
		WriteTimeout:       2 * time.Second,
	}))
	require.NoError(t, appA.AddWSClient("credential-feed", wsURLWithUserInfo, WSClientPolicy{
		Required:           false,
		EnableReconnection: false,
		HandshakeTimeout:   150 * time.Millisecond,
		WriteTimeout:       200 * time.Millisecond,
	}))

	t.Cleanup(func() {
		_ = appA.Stop(context.Background())
		_ = appB.Stop(context.Background())
	})
	startCtx, startCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer startCancel()
	require.NoError(t, appA.Start(startCtx))
	require.NoError(t, appB.Start(startCtx))

	// Exercise HTTP endpoints on both apps.
	respA, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/alpha", portA))
	require.NoError(t, err)
	_ = respA.Body.Close()

	respB, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/beta", portB))
	require.NoError(t, err)
	_ = respB.Body.Close()

	<-workerReadyDone
	<-subFailed
	select {
	case <-cronRan:
	case <-time.After(2500 * time.Millisecond):
		t.Fatal("expected per-second cron job to execute on AppA")
	}

	// Inject a controller actor panic on sync-indexer to verify pi_actor_restarts_total and Tao slog adapter routing to logA!
	appA.runtimeMu.Lock()
	wSlot := appA.backgroundWorkers[0]
	rtA := appA.actorRT
	appA.runtimeMu.Unlock()
	require.NoError(t, rtA.node.Send(wSlot.actorRef, actorInjectPanicMsg{reason: "synthetic controller crash"}))
	require.Eventually(t, func() bool {
		return wSlot.snapshot().ActorRestarts >= 1
	}, 2*time.Second, 10*time.Millisecond)

	// Inspect AppA RuntimeSnapshot: secrets & URL credentials/tokens MUST be redacted!
	snapA := appA.RuntimeSnapshot()
	assert.Equal(t, "running", snapA.State)
	assert.True(t, snapA.Startup)
	assert.True(t, snapA.Alive)
	assert.True(t, snapA.Ready)
	assert.NotNil(t, snapA.ActorTree)
	assert.Equal(t, "[redacted]", snapA.Config["DB_PASSWORD"])
	assert.Equal(t, "[redacted]", snapA.Config["JWT_SECRET"])
	assert.Equal(t, "[redacted]", snapA.Config["REDIS_PASSWORD"])
	assert.Equal(t, "alpha-service", snapA.Config["APP_NAME"])

	require.Len(t, snapA.WSClients, 2)
	wsByName := make(map[string]string, len(snapA.WSClients))
	for _, wsc := range snapA.WSClients {
		wsByName[wsc.ServiceName] = wsc.URL
	}
	assert.NotContains(t, wsByName["realtime-feed"], "my-jwt-token")
	assert.NotContains(t, wsByName["realtime-feed"], "ak_live_999")
	assert.Contains(t, wsByName["realtime-feed"], "env=prod")
	assert.NotContains(t, wsByName["credential-feed"], "svcuser")
	assert.NotContains(t, wsByName["credential-feed"], "topsecretpass")
	assert.NotContains(t, wsByName["credential-feed"], "my-jwt-token")
	assert.Contains(t, wsByName["credential-feed"], "env=stage")

	// Verify metric isolation: AppA has supervision metrics, AppB has ZERO supervision metric increments from AppA!
	mMapA := collectMetricNames(t, readerA)
	mMapB := collectMetricNames(t, readerB)
	assert.Contains(t, mMapA, metricWorkerRestartsTotal)
	assert.Contains(t, mMapA, metricActorRestartsTotal)
	assert.Contains(t, mMapA, metricWorkerBackoffSeconds)
	assert.Contains(t, mMapA, metricCronRunsTotal)
	assert.Contains(t, mMapA, metricCronDurationSeconds)
	assert.Contains(t, mMapA, metricPubSubFailuresTotal)
	assert.NotContains(t, mMapB, metricWorkerRestartsTotal, "AppB without supervision runtime must not register AppA supervision metrics")

	// Verify Tao actor crash logs went to logA, NOT logB!
	assert.Contains(t, logA.Joined(), "[actor]")
	assert.NotContains(t, logB.Joined(), "synthetic controller crash")

	// Stop both apps and verify Owned vs Borrowed lifecycle semantics!
	require.NoError(t, appA.Stop(context.Background()))
	require.NoError(t, appB.Stop(context.Background()))

	assert.Equal(t, int32(1), tpOwnedA.shutdowns.Load(), "Owned TracerProvider on AppA must be shut down")
	assert.Equal(t, int32(0), tpBorrowedB.shutdowns.Load(), "Borrowed TracerProvider on AppB must NOT be shut down")
	assert.Equal(t, int32(0), logA.closed.Load(), "Borrowed Logger on AppA must NOT be closed")
	assert.Equal(t, int32(0), logB.closed.Load(), "Borrowed Logger on AppB must NOT be closed")
	assert.Equal(t, int32(0), psMockA.closed.Load(), "Borrowed PubSub on AppA must NOT be closed")

	spansA := tpOwnedA.SpanNames()
	spansB := tpBorrowedB.SpanNames()
	assert.Contains(t, spansA, "alpha-inner-op")
	assert.Contains(t, spansA, "cleanup-job")
	assert.NotContains(t, spansA, "beta-inner-op")
	assert.Contains(t, spansB, "beta-inner-op")
	assert.NotContains(t, spansB, "alpha-inner-op")
	assert.Same(t, globalTPBefore, otel.GetTracerProvider())
}

type stage3IndependentCommitter struct {
	nacked atomic.Int32
}

func (c *stage3IndependentCommitter) Commit()                             {}
func (c *stage3IndependentCommitter) CommitContext(context.Context) error { return nil }
func (c *stage3IndependentCommitter) Nack()                               { c.nacked.Add(1) }

type syncTestBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncTestBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncTestBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// --- 5. Comprehensive Fault Matrix & Real Subprocess SIGTERM Verification (V050-07) ---

func TestStage4_V050_07_FaultMatrixAndRealProcessSIGTERM(t *testing.T) {
	t.Run("HTTP port bind conflict rolls back actor tree and workers cleanly", func(t *testing.T) {
		occupiedLn, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer occupiedLn.Close()
		busyPort := occupiedLn.Addr().(*net.TCPAddr).Port

		workerStarted := make(chan struct{}, 1)
		workerStopped := make(chan struct{}, 1)

		app, err := Build(WithConfig(map[string]string{
			"HTTP_HOST": "127.0.0.1",
			"HTTP_PORT": strconv.Itoa(busyPort),
		}))
		require.NoError(t, err)
		t.Cleanup(func() { _ = app.Stop(context.Background()) })
		app.GET("/ping", func(*Context) (any, error) {
			return "pong", nil
		})
		require.NoError(t, app.AddWorker("rollback-worker", WorkerPolicy{
			Mode: SuperviseRestart,
		}, func(c *Context) error {
			select {
			case workerStarted <- struct{}{}:
			default:
			}
			<-c.Done()
			select {
			case workerStopped <- struct{}{}:
			default:
			}
			return nil
		}))

		startErr := app.Start(context.Background())
		require.Error(t, startErr)
		assert.False(t, app.isStartupComplete())

		select {
		case <-workerStopped:
		case <-time.After(2 * time.Second):
			t.Fatal("worker started before port bind failure must be rolled back and stopped")
		}
		require.Error(t, app.Wait(context.Background()))
	})

	t.Run("stubborn worker exceeding Stop timeout keeps ManagedSQL open until real exit", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "stubborn.db")
		app, err := Build(
			WithConfig(map[string]string{
				"HTTP_ENABLED": "false",
				"DB_DIALECT":   "sqlite",
				"DB_NAME":      dbPath,
			}),
			WithManagedSQL(),
		)
		require.NoError(t, err)

		workerRunning := make(chan struct{})
		releaseStubborn := make(chan struct{})
		var lateSQLErr atomic.Value

		require.NoError(t, app.AddWorker("stubborn-flusher", WorkerPolicy{
			Mode:            SuperviseRestart,
			ShutdownTimeout: 40 * time.Millisecond,
		}, func(c *Context) error {
			close(workerRunning)
			<-c.Done()
			// Ignore c.Done() past Stop's short timeout, then write to ManagedSQL before returning!
			<-releaseStubborn
			_, execErr := c.SQL().ExecContext(context.Background(), "INSERT INTO final_flush(tag) VALUES (?)", "flushed-after-stop-timeout")
			if execErr != nil {
				lateSQLErr.Store(execErr)
				return execErr
			}
			return nil
		}))

		require.NoError(t, app.Start(context.Background()))
		_, err = app.SQL().ExecContext(context.Background(), "CREATE TABLE final_flush (id INTEGER PRIMARY KEY AUTOINCREMENT, tag TEXT)")
		require.NoError(t, err)
		<-workerRunning

		shortStopCtx, shortCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer shortCancel()
		stopErr := app.Stop(shortStopCtx)
		require.ErrorIs(t, stopErr, context.DeadlineExceeded)

		// Even though Stop caller timed out, ManagedSQL MUST remain open while stubborn-flusher is still running!
		close(releaseStubborn)

		waitCtx, waitCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer waitCancel()
		_ = app.Wait(waitCtx)
		require.Nil(t, lateSQLErr.Load(), "ManagedSQL must NOT close before stubborn worker goroutine actually exits")

		rawDB, err := sql.Open("sqlite", dbPath)
		require.NoError(t, err)
		defer rawDB.Close()
		var cnt int
		require.NoError(t, rawDB.QueryRow("SELECT COUNT(*) FROM final_flush WHERE tag = ?", "flushed-after-stop-timeout").Scan(&cnt))
		assert.Equal(t, 1, cnt)
	})

	t.Run("real OS subprocess SIGTERM drains cleanly and commits SQLite checkpoint", func(t *testing.T) {
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "sigterm.db")
		readyFile := filepath.Join(tmpDir, "ready.addr")

		exePath, err := os.Executable()
		require.NoError(t, err)
		cmd := exec.Command(exePath, "-test.run=^TestStage4_HelperProcessSIGTERM$")
		cmd.Env = append(os.Environ(),
			"PI_STAGE4_SIGTERM_HELPER=1",
			"PI_STAGE4_DB_PATH="+dbPath,
			"PI_STAGE4_READY_FILE="+readyFile,
		)
		var outBuf syncTestBuffer
		cmd.Stdout = &outBuf
		cmd.Stderr = &outBuf

		require.NoError(t, cmd.Start())

		var httpAddr string
		require.Eventually(t, func() bool {
			data, rErr := os.ReadFile(readyFile)
			if rErr != nil || len(bytes.TrimSpace(data)) == 0 {
				return false
			}
			httpAddr = strings.TrimSpace(string(data))
			resp, hErr := http.Get("http://" + httpAddr + "/.well-known/ready")
			if hErr != nil {
				return false
			}
			_ = resp.Body.Close()
			return resp.StatusCode == http.StatusOK
		}, 5*time.Second, 25*time.Millisecond)

		// Open an active WebSocket connection before sending SIGTERM.
		wsCli, wsResp, err := gWebsocket.DefaultDialer.Dial("ws://"+httpAddr+"/ws", nil)
		if wsResp != nil && wsResp.Body != nil {
			_ = wsResp.Body.Close()
		}
		require.NoError(t, err)
		defer wsCli.Close()

		require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))

		waitDone := make(chan error, 1)
		go func() { waitDone <- cmd.Wait() }()

		select {
		case wErr := <-waitDone:
			require.NoError(t, wErr, "subprocess SIGTERM exited non-zero: %s", outBuf.String())
		case <-time.After(8 * time.Second):
			_ = cmd.Process.Kill()
			t.Fatalf("subprocess hung on SIGTERM: %s", outBuf.String())
		}

		rawDB, err := sql.Open("sqlite", dbPath)
		require.NoError(t, err)
		defer rawDB.Close()
		var workerCkpt int
		require.NoError(t, rawDB.QueryRow("SELECT COUNT(*) FROM sigterm_audit WHERE source = ?", "worker-shutdown-checkpoint").Scan(&workerCkpt))
		assert.Equal(t, 1, workerCkpt, "worker shutdown checkpoint must be persisted before process exit")
	})
}

func TestStage4_HelperProcessSIGTERM(t *testing.T) {
	if os.Getenv("PI_STAGE4_SIGTERM_HELPER") != "1" {
		return
	}
	dbPath := os.Getenv("PI_STAGE4_DB_PATH")
	readyFile := os.Getenv("PI_STAGE4_READY_FILE")
	port := testutil.GetFreePort(t)

	app, err := Build(
		WithConfig(map[string]string{
			"APP_NAME":              "sigterm-helper",
			"HTTP_HOST":             "127.0.0.1",
			"HTTP_PORT":             strconv.Itoa(port),
			"DB_DIALECT":            "sqlite",
			"DB_NAME":               dbPath,
			"SHUTDOWN_GRACE_PERIOD": "5s",
		}),
		WithManagedSQL(),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "build failed: %v\n", err)
		os.Exit(2)
	}

	app.OnStart(func(c *Context) error {
		_, execErr := c.SQL().ExecContext(c.Context, "CREATE TABLE IF NOT EXISTS sigterm_audit (id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT)")
		return execErr
	})

	_ = app.AddWorker("ckpt-worker", WorkerPolicy{
		Mode:      SuperviseFatal,
		Required:  true,
		WaitReady: true,
	}, func(c *Context) error {
		if mErr := c.MarkReady(); mErr != nil {
			return mErr
		}
		_ = os.WriteFile(readyFile, []byte(fmt.Sprintf("127.0.0.1:%d", port)), 0o600)
		<-c.Done()
		_, execErr := c.SQL().ExecContext(context.Background(), "INSERT INTO sigterm_audit(source) VALUES (?)", "worker-shutdown-checkpoint")
		return execErr
	})

	_ = app.AddCron("* * * * * *", "tick-audit", func(c *Context) error {
		_, execErr := c.SQL().ExecContext(c.Context, "INSERT INTO sigterm_audit(source) VALUES (?)", "cron-tick")
		return execErr
	}, CronJobOptions{Overlap: CronSkipIfRunning})

	app.WebSocket("/ws", func(c *Context) (any, error) {
		var msg string
		if bErr := c.Bind(&msg); bErr != nil {
			return nil, bErr
		}
		return msg, nil
	})

	app.Run()
}

func TestStage4_ReviewHardeningRegressions(t *testing.T) {
	t.Run("Phase B timeout stops healthy actors without spurious ErrUnresponsive", func(t *testing.T) {
		app, err := Build(WithConfig(map[string]string{"HTTP_ENABLED": "false"}))
		require.NoError(t, err)

		workerReady := make(chan struct{})
		require.NoError(t, app.AddWorker("clean-worker", WorkerPolicy{
			Mode: SuperviseRestart,
		}, func(c *Context) error {
			close(workerReady)
			<-c.Done()
			return nil
		}))

		// Register a Phase B drain hook that exceeds shortStopCtx so ctx is already expired entering Phase C.
		app.registerPhaseBDrain(func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})

		require.NoError(t, app.Start(context.Background()))
		<-workerReady

		shortStopCtx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		defer cancel()
		stopErr := app.Stop(shortStopCtx)
		require.ErrorIs(t, stopErr, context.DeadlineExceeded)

		waitCtx, waitCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer waitCancel()
		waitErr := app.Wait(waitCtx)
		require.ErrorIs(t, waitErr, context.DeadlineExceeded)
		assert.NotErrorIs(t, waitErr, actor.ErrUnresponsive, "healthy actors must get fallback cleanup budget instead of failing in 0ns")
	})

	t.Run("ws-hub panic preserves open server WS sessions and SetWriteDeadline(time.Now()) succeeds", func(t *testing.T) {
		port := testutil.GetFreePort(t)
		app, err := Build(WithConfig(map[string]string{
			"HTTP_HOST": "127.0.0.1",
			"HTTP_PORT": strconv.Itoa(port),
		}))
		require.NoError(t, err)
		t.Cleanup(func() { _ = app.Stop(context.Background()) })

		app.WebSocket("/ws", func(c *Context) (any, error) {
			var msg string
			if bErr := c.Bind(&msg); bErr != nil {
				return nil, bErr
			}
			return "ack:" + msg, nil
		})

		require.NoError(t, app.Start(context.Background()))
		wsURL := fmt.Sprintf("ws://127.0.0.1:%d/ws", port)
		cli, resp, err := gWebsocket.DefaultDialer.Dial(wsURL, nil)
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		require.NoError(t, err)
		defer cli.Close()

		require.NoError(t, cli.WriteMessage(gWebsocket.TextMessage, []byte("ping")))
		_, rawEcho, err := cli.ReadMessage()
		require.NoError(t, err)
		assert.Contains(t, string(rawEcho), "ack:ping")
		conns := app.httpServer.ws.ListConnections()
		require.Len(t, conns, 1)
		connID := conns[0]

		// Verify SetWriteDeadline(time.Now()) succeeds immediately without ErrWriteLockTimeout, then clear it.
		srvConn := app.httpServer.ws.GetWebsocketConnection(connID)
		require.NotNil(t, srvConn)
		require.NoError(t, srvConn.SetWriteDeadline(time.Now()))
		require.NoError(t, srvConn.SetWriteDeadline(time.Time{}))

		// Inject panic into ws-hub and verify the active session actor keeps delivering frames in order!
		app.runtimeMu.Lock()
		rt := app.actorRT
		app.runtimeMu.Unlock()
		require.NotNil(t, rt)
		require.NoError(t, rt.node.Send(actor.Ref{Path: wsHubActorPath}, actorInjectPanicMsg{reason: "hub-boom"}))

		require.Eventually(t, func() bool {
			return app.wsHubBoots.Load() >= 2
		}, 2*time.Second, 10*time.Millisecond)

		for i := 0; i < 5; i++ {
			require.NoError(t, app.httpServer.ws.Send(connID, websocket.TextMessage, []byte(fmt.Sprintf("frame-%d", i))))
		}
		for i := 0; i < 5; i++ {
			_ = cli.SetReadDeadline(time.Now().Add(2 * time.Second))
			_, got, rErr := cli.ReadMessage()
			require.NoError(t, rErr)
			assert.Equal(t, fmt.Sprintf("frame-%d", i), string(got))
		}
	})

	t.Run("wsClientActor panic re-arms PingInterval and dial errors redact URL secrets", func(t *testing.T) {
		var pingCount atomic.Int32
		upgrader := gWebsocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		wsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			c, uErr := upgrader.Upgrade(w, r, nil)
			if uErr != nil {
				return
			}
			defer c.Close()
			c.SetPingHandler(func(appData string) error {
				pingCount.Add(1)
				return c.WriteControl(gWebsocket.PongMessage, []byte(appData), time.Now().Add(time.Second))
			})
			for {
				if _, _, rErr := c.ReadMessage(); rErr != nil {
					return
				}
			}
		}))
		defer wsSrv.Close()

		closedLn, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		deadPort := closedLn.Addr().(*net.TCPAddr).Port
		_ = closedLn.Close()

		app, err := Build(WithConfig(map[string]string{"HTTP_ENABLED": "false"}))
		require.NoError(t, err)
		t.Cleanup(func() { _ = app.Stop(context.Background()) })

		wsTarget := "ws" + strings.TrimPrefix(wsSrv.URL, "http")
		require.NoError(t, app.AddWSClient("live-ws", wsTarget, WSClientPolicy{
			EnableReconnection: true,
			Required:           true,
			PingInterval:       40 * time.Millisecond,
		}))
		secretWS := fmt.Sprintf("ws://user:supersecret@127.0.0.1:%d/feed?access_token=topsecret123", deadPort)
		require.NoError(t, app.AddWSClient("failing-secret-ws", secretWS, WSClientPolicy{
			EnableReconnection: true,
			BackoffInitial:     200 * time.Millisecond,
			BackoffMax:         500 * time.Millisecond,
		}))

		require.NoError(t, app.Start(context.Background()))

		// Wait for failing-secret-ws to record a dial error and verify secrets are redacted in RuntimeSnapshot!
		require.Eventually(t, func() bool {
			snap := app.RuntimeSnapshot()
			for _, w := range snap.WSClients {
				if w.ServiceName == "failing-secret-ws" && w.LastError != "" {
					assert.NotContains(t, w.LastError, "topsecret123")
					assert.NotContains(t, w.LastError, "supersecret")
					return true
				}
			}
			return false
		}, 2*time.Second, 15*time.Millisecond)

		// Inject panic into live-ws controller and verify PingInterval is re-armed on the existing connection!
		slot := app.wsClientSlots()[0]
		app.runtimeMu.Lock()
		rt := app.actorRT
		app.runtimeMu.Unlock()
		require.NoError(t, rt.node.Send(slot.actorRef, actorInjectPanicMsg{reason: "ws-client-boom"}))
		require.Eventually(t, func() bool {
			for _, w := range app.RuntimeSnapshot().WSClients {
				if w.ServiceName == "live-ws" && w.ActorRestarts >= 1 {
					return true
				}
			}
			return false
		}, 2*time.Second, 10*time.Millisecond)

		before := pingCount.Load()
		require.Eventually(t, func() bool {
			return pingCount.Load() >= before+2
		}, 2*time.Second, 15*time.Millisecond, "restarted wsClientActor must re-arm PingInterval on active connection")
	})

	t.Run("EnableOAuth and UseRateLimiter on Build stay pure before Start andisReady tracks root heartbeat", func(t *testing.T) {
		var jwksHits atomic.Int32
		jwksSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			jwksHits.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"keys":[]}`))
		}))
		defer jwksSrv.Close()

		app, err := Build(WithConfig(map[string]string{"HTTP_ENABLED": "false"}))
		require.NoError(t, err)
		t.Cleanup(func() { _ = app.Stop(context.Background()) })

		app.EnableOAuth(jwksSrv.URL, 1)
		app.UseRateLimiter(middleware.RateLimiterConfig{RequestsPerSecond: 10, Burst: 10, PerIP: true})
		require.NoError(t, app.AddWorker("bg", WorkerPolicy{Mode: SuperviseRestart}, func(c *Context) error {
			<-c.Done()
			return nil
		}))

		// Before Start, pure Build + EnableOAuth must NOT perform any JWKS HTTP requests!
		time.Sleep(40 * time.Millisecond)
		assert.Zero(t, jwksHits.Load(), "EnableOAuth on pure Build must defer JWKS fetch until Start")

		require.NoError(t, app.Start(context.Background()))
		require.Eventually(t, func() bool {
			return jwksHits.Load() >= 1
		}, 2*time.Second, 15*time.Millisecond)
		assert.True(t, app.isReady())
		assert.True(t, app.isAlive())

		// Simulate wedged pi-root heartbeat: both isAlive() and isReady() must immediately return false!
		app.runtimeMu.Lock()
		rt := app.actorRT
		app.runtimeMu.Unlock()
		rt.rootLastBeatNano.Store(time.Now().Add(-time.Hour).UnixNano())
		assert.False(t, app.isAlive())
		assert.False(t, app.isReady(), "isReady must return false whenever actorTreeUnexpectedlyDead() is true")

		// Restore heartbeat and stop cleanly.
		rt.rootLastBeatNano.Store(time.Now().UnixNano())
		assert.True(t, app.isReady())
		require.NoError(t, app.Stop(context.Background()))
	})
}

// --- 6. Full Stage 1A–4 Review Regressions (Panic Containment, Health Barrier, WS Mailbox Full Close, gRPC Instance Tracing, Snapshot AckMode) ---

type stage4HealthAdapter struct {
	*stage3BrokerMock
	healthFn func() datasource.Health
}

func (s *stage4HealthAdapter) Health() datasource.Health {
	return s.healthFn()
}

func TestStage4_PanicContainment_KeyExtractorAndLegacyHealthSubprocess(t *testing.T) {
	exe, err := os.Executable()
	require.NoError(t, err)

	for _, scenario := range []string{"key-extractor", "legacy-health"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()

			cmd := exec.CommandContext(ctx, exe, "-test.run=^TestStage4_PanicContainmentHelper$", "-test.timeout=5s")
			cmd.Env = append(os.Environ(), "PI_STAGE4_PANIC_SCENARIO="+scenario)
			out, runErr := cmd.CombinedOutput()
			require.NoErrorf(t, runErr, "%s panic escaped managed runtime and terminated process:\n%s", scenario, string(out))
		})
	}
}

func TestStage4_PanicContainmentHelper(t *testing.T) {
	scenario := os.Getenv("PI_STAGE4_PANIC_SCENARIO")
	if scenario == "" {
		t.Skip("helper only runs when PI_STAGE4_PANIC_SCENARIO is set")
	}

	switch scenario {
	case "key-extractor":
		var step atomic.Int32
		badCommitter := &stage3Committer{}
		goodCommitter := &stage3Committer{}
		goodHandled := make(chan struct{})

		broker := &stage3BrokerMock{
			semantics: pubsub.AckSemantics{Mode: pubsub.AckModeIndependent, SupportsContextCommit: true},
			subFn: func(ctx context.Context, topic string) (*pubsub.Message, error) {
				switch step.Add(1) {
				case 1:
					m := pubsub.NewMessage(ctx)
					m.Topic = topic
					m.Value = []byte("poison")
					m.Committer = badCommitter
					return m, nil
				case 2:
					m := pubsub.NewMessage(ctx)
					m.Topic = topic
					m.Value = []byte("healthy")
					m.Committer = goodCommitter
					return m, nil
				default:
					<-ctx.Done()
					return nil, ctx.Err()
				}
			},
		}

		app := buildStage3TestApp(t, broker, Borrowed)
		require.NoError(t, app.AddSubscriber("key-extractor-panic-topic", SubscribePolicy{
			Concurrency: 2,
			Required:    true,
			KeyExtractor: func(msg *pubsub.Message) string {
				if string(msg.Value) == "poison" {
					panic("key-extractor-boom")
				}
				return "shard-a"
			},
		}, func(*Context) error {
			close(goodHandled)
			return nil
		}))
		require.NoError(t, app.Start(context.Background()))

		select {
		case <-goodHandled:
		case <-time.After(2 * time.Second):
			t.Fatal("healthy message after KeyExtractor panic was not processed")
		}

		require.Eventually(t, func() bool {
			snap := app.RuntimeSnapshot()
			return len(snap.Subscribers) == 1 &&
				snap.Subscribers[0].PanicsTotal == 1 &&
				snap.Subscribers[0].FailuresTotal == 1 &&
				snap.Subscribers[0].ProcessedTotal == 1 &&
				snap.Subscribers[0].CommittedTotal == 1
		}, 2*time.Second, 10*time.Millisecond)

		assert.Zero(t, badCommitter.commits.Load(), "panicking KeyExtractor message must not be committed")
		assert.EqualValues(t, 1, badCommitter.nacks.Load(), "panicking KeyExtractor message must be nacked")
		assert.EqualValues(t, 1, goodCommitter.commits.Load())
		require.NoError(t, app.Stop(context.Background()))

	case "legacy-health":
		client := &stage4HealthAdapter{
			stage3BrokerMock: &stage3BrokerMock{},
			healthFn: func() datasource.Health {
				panic("legacy-health-adapter-boom")
			},
		}
		app := buildStage3TestApp(t, client, Borrowed)
		require.NoError(t, app.Start(context.Background()))

		res := app.Health(context.Background())
		require.NotNil(t, res)
		assert.Equal(t, "DEGRADED", res["status"])
		psHealth, ok := res["pubsub"].(datasource.Health)
		require.True(t, ok)
		assert.Equal(t, "DOWN", psHealth.Status)
		assert.Contains(t, fmt.Sprint(psHealth.Details["error"]), "legacy-health-adapter-boom")
		require.NoError(t, app.Stop(context.Background()))

	default:
		t.Fatalf("unknown scenario %q", scenario)
	}
}

func TestStage4_KeyExtractorPanic_DeadLetterSkipSupport(t *testing.T) {
	committer := &stage3Committer{}
	dlqDone := make(chan error, 1)
	var sent atomic.Bool

	broker := &stage3BrokerMock{
		semantics: pubsub.AckSemantics{Mode: pubsub.AckModeIndependent, SupportsContextCommit: true},
		subFn: func(ctx context.Context, topic string) (*pubsub.Message, error) {
			if !sent.Swap(true) {
				m := pubsub.NewMessage(ctx)
				m.Topic = topic
				m.Value = []byte("bad-key")
				m.Committer = committer
				return m, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	app := buildStage3TestApp(t, broker, Borrowed)
	require.NoError(t, app.AddSubscriber("key-dlq-topic", SubscribePolicy{
		Concurrency:         2,
		AllowDeadLetterSkip: true,
		KeyExtractor: func(*pubsub.Message) string {
			panic("bad-key-panic")
		},
		DeadLetterHandler: func(_ *Context, _ *pubsub.Message, cause error) error {
			dlqDone <- cause
			return nil
		},
	}, func(*Context) error { return nil }))

	require.NoError(t, app.Start(context.Background()))
	select {
	case cause := <-dlqDone:
		assert.Contains(t, cause.Error(), "bad-key-panic")
	case <-time.After(2 * time.Second):
		t.Fatal("DeadLetterHandler was not invoked on KeyExtractor panic")
	}

	require.Eventually(t, func() bool {
		snap := app.RuntimeSnapshot()
		return len(snap.Subscribers) == 1 &&
			snap.Subscribers[0].DLQSentTotal == 1 &&
			snap.Subscribers[0].DLQSkippedTotal == 1 &&
			committer.commits.Load() == 1
	}, 2*time.Second, 10*time.Millisecond)
}

func TestStage4_HealthSamplerLifecycleBarrier_ProtectsOwnedResources(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	probeReturned := make(chan struct{})
	var samplerDone atomic.Bool

	base := &stage3BrokerMock{}
	client := &stage4HealthAdapter{
		stage3BrokerMock: base,
		healthFn: func() datasource.Health {
			close(entered)
			<-release
			samplerDone.Store(true)
			return datasource.Health{Status: "UP"}
		},
	}

	app := buildStage3TestApp(t, client, Owned)
	require.NoError(t, app.Start(context.Background()))

	probeCtx, cancelProbe := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancelProbe()
	go func() {
		_ = app.Health(probeCtx)
		close(probeReturned)
	}()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("health sampler goroutine did not start")
	}
	<-probeReturned

	stopCtx, cancelStop := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancelStop()
	stopErr := app.Stop(stopCtx)
	require.ErrorIs(t, stopErr, context.DeadlineExceeded)
	assert.Zero(t, base.closed.Load(), "Owned PubSub must not close while in-flight health sampler is still blocked")
	assert.False(t, samplerDone.Load())

	// New health checks after shutdown started must be rejected immediately without spawning new samplers.
	postStopRes := app.Health(context.Background())
	require.NotNil(t, postStopRes)
	assert.Equal(t, "DEGRADED", postStopRes["status"])

	close(release)
	_ = app.Wait(context.Background())
	assert.True(t, samplerDone.Load())
	assert.EqualValues(t, 1, base.closed.Load(), "Owned PubSub must close once health sampler finishes")
}

func TestStage4_RuntimeSnapshot_SubscriberAckModeString(t *testing.T) {
	client := &stage3BrokerMock{
		semantics: pubsub.AckSemantics{Mode: pubsub.AckModeIndependent, SupportsContextCommit: true},
	}
	app := buildStage3TestApp(t, client, Borrowed)
	require.NoError(t, app.AddSubscriber("snapshot-ackmode", SubscribePolicy{}, func(*Context) error { return nil }))

	snap := app.RuntimeSnapshot()
	require.Len(t, snap.Subscribers, 1)
	assert.Equal(t, "independent", snap.Subscribers[0].AckMode)
}

func TestStage4_WSSessionCloseSurvivesFullMailbox(t *testing.T) {
	paired := make(chan *gWebsocket.Conn, 1)
	hold := make(chan struct{})
	upgrader := gWebsocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		paired <- c
		<-hold
	}))
	defer srv.Close()
	defer close(hold)

	client, resp, err := gWebsocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	require.NoError(t, err)
	defer client.Close()

	conn := &websocket.Connection{Conn: <-paired}
	app := buildStage3TestApp(t, nil, Borrowed)
	app.ConfigureWebSocketHub(websocket.HubPolicy{MailboxSize: 1, WriteTimeout: time.Second})
	app.WebSocket("/unused", func(*Context) (any, error) { return nil, nil })
	require.NoError(t, app.Start(context.Background()))

	app.httpServer.ws.AddWebsocketConnection("full-mb-session", conn)
	sess, err := app.registerServerWSSession(context.Background(), "full-mb-session", conn)
	require.NoError(t, err)

	pid := sess.getActorPID()
	app.runtimeMu.Lock()
	node := app.actorRT.node
	app.runtimeMu.Unlock()

	writer, err := conn.NextWriter(websocket.TextMessage)
	require.NoError(t, err)
	defer writer.Close()

	require.NoError(t, app.httpServer.ws.Send("full-mb-session", websocket.TextMessage, []byte("blocked-frame")))
	require.Eventually(t, func() bool {
		return node.Send(pid, wsHubCheckMsg{}) == nil
	}, time.Second, time.Millisecond)
	require.ErrorIs(t, node.Send(pid, wsHubCheckMsg{}), actor.ErrMailboxFull)

	app.unregisterServerWSSession(sess)

	require.Eventually(t, func() bool {
		sErr := node.Send(pid, wsHubCheckMsg{})
		return errors.Is(sErr, actor.ErrNoProcess) || errors.Is(sErr, actor.ErrStopped)
	}, 200*time.Millisecond, 2*time.Millisecond, "closed session actor must terminate promptly even when mailbox was full")
	assert.Empty(t, app.snapshotActiveWSSessions())
}

func TestStage4_GRPC_UnaryAndStreamUseInstanceTracerProvider(t *testing.T) {
	globalBefore := otel.GetTracerProvider()
	globalTP := newShutdownableTP()
	instanceTP := newShutdownableTP()
	otel.SetTracerProvider(globalTP)
	defer otel.SetTracerProvider(globalBefore)
	defer globalTP.Shutdown(context.Background())
	defer instanceTP.Shutdown(context.Background())

	app, err := Build(
		WithConfig(map[string]string{
			"HTTP_ENABLED": "false",
			"GRPC_ENABLED": "true",
			"GRPC_ADDR":    "127.0.0.1:0",
		}),
		WithTracerProvider(instanceTP, Borrowed),
	)
	require.NoError(t, err)
	defer app.Stop(context.Background())

	app.RegisterService(&stage4DummyGRPCDesc, &stage4DummyGRPCImpl{})
	require.NoError(t, app.Start(context.Background()))

	conn, err := grpc.NewClient(app.GRPCAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	rpcCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	hc := healthpb.NewHealthClient(conn)

	// 1. Unary RPC
	_, err = hc.Check(rpcCtx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)

	// 2. Server-Streaming RPC
	watchCtx, cancelWatch := context.WithCancel(rpcCtx)
	stream, err := hc.Watch(watchCtx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.NoError(t, err)
	cancelWatch()
	_, _ = stream.Recv()

	require.NoError(t, app.Stop(context.Background()))

	instanceSpans := instanceTP.SpanNames()
	assert.Contains(t, instanceSpans, "/grpc.health.v1.Health/Check")
	assert.Contains(t, instanceSpans, "/grpc.health.v1.Health/Watch")
	assert.Empty(t, globalTP.SpanNames(), "gRPC unary and stream spans must not leak to global TracerProvider")
}

// --- 8. Self-Review Adversarial Regressions ---

type stage4IdleRecoverPubSub struct {
	failNext atomic.Bool
	polls    atomic.Int32
}

func (b *stage4IdleRecoverPubSub) AckSemantics() pubsub.AckSemantics {
	return pubsub.AckSemantics{
		Mode:                  pubsub.AckModeIndependent,
		SupportsNack:          true,
		SupportsContextCommit: true,
	}
}
func (b *stage4IdleRecoverPubSub) CreateTopic(context.Context, string) error { return nil }
func (b *stage4IdleRecoverPubSub) DeleteTopic(context.Context, string) error { return nil }
func (b *stage4IdleRecoverPubSub) Health() datasource.Health {
	return datasource.Health{Status: datasource.StatusUp}
}
func (b *stage4IdleRecoverPubSub) Publish(context.Context, string, []byte) error { return nil }
func (b *stage4IdleRecoverPubSub) Query(context.Context, string, ...any) ([]byte, error) {
	return nil, nil
}
func (b *stage4IdleRecoverPubSub) Close() error { return nil }
func (b *stage4IdleRecoverPubSub) Subscribe(ctx context.Context, _ string) (*pubsub.Message, error) {
	b.polls.Add(1)
	if b.failNext.Load() {
		return nil, errors.New("transient broker disconnect")
	}
	timer := time.NewTimer(15 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		// Idle topic: healthy empty poll returns (nil, nil).
		return nil, nil
	}
}

func TestSelfReview_Subscriber_IdleTopicRecoversReadinessFromBackingOff(t *testing.T) {
	backend := &stage4IdleRecoverPubSub{}
	app, err := Build(
		WithConfig(map[string]string{
			"HTTP_ENABLED": "true",
			"HTTP_ADDR":    "127.0.0.1:0",
			"GRPC_ENABLED": "true",
			"GRPC_ADDR":    "127.0.0.1:0",
		}),
		WithPubSub(backend, Borrowed),
	)
	require.NoError(t, err)
	defer app.Stop(context.Background())

	app.GET("/ping", func(*Context) (any, error) { return "ok", nil })
	app.RegisterService(&stage4DummyGRPCDesc, &stage4DummyGRPCImpl{})
	require.NoError(t, app.AddSubscriber("idle.orders", SubscribePolicy{
		Concurrency: 1,
		Required:    true,
	}, func(*Context) error { return nil }))

	// Shorten pull retry backoff for fast deterministic test turnaround.
	app.subscriberSlotsList[0].pullRetryDelay = 20 * time.Millisecond

	require.NoError(t, app.Start(context.Background()))
	require.True(t, app.isReady())

	// Inject transient broker error: subscriber enters backing_off and revokes readiness.
	backend.failNext.Store(true)
	require.Eventually(t, func() bool {
		return !app.isReady()
	}, time.Second, 5*time.Millisecond, "transient Subscribe error must revoke readiness on Required subscriber")

	// Clear broker error while topic remains completely idle ((nil, nil) polls only).
	backend.failNext.Store(false)
	require.Eventually(t, func() bool {
		snap := app.RuntimeSnapshot()
		return app.isReady() && len(snap.Subscribers) == 1 && snap.Subscribers[0].State == "Running"
	}, time.Second, 5*time.Millisecond, "healthy empty (nil, nil) poll must restore subscriber from BackingOff to Running")
}

type stage4PanickingCommitter struct {
	panicCommit bool
	panicNack   bool
}

func (c *stage4PanickingCommitter) Commit() {
	if c.panicCommit {
		panic("committer Commit exploded")
	}
}
func (c *stage4PanickingCommitter) CommitContext(context.Context) error {
	if c.panicCommit {
		panic("committer CommitContext exploded")
	}
	return nil
}
func (c *stage4PanickingCommitter) Nack() {
	if c.panicNack {
		panic("committer Nack exploded")
	}
}

type stage4QueuePubSub struct {
	ch chan *pubsub.Message
}

func (q *stage4QueuePubSub) AckSemantics() pubsub.AckSemantics {
	return pubsub.AckSemantics{
		Mode:                  pubsub.AckModeIndependent,
		SupportsNack:          true,
		SupportsContextCommit: true,
	}
}
func (q *stage4QueuePubSub) CreateTopic(context.Context, string) error { return nil }
func (q *stage4QueuePubSub) DeleteTopic(context.Context, string) error { return nil }
func (q *stage4QueuePubSub) Health() datasource.Health {
	return datasource.Health{Status: datasource.StatusUp}
}
func (q *stage4QueuePubSub) Publish(context.Context, string, []byte) error { return nil }
func (q *stage4QueuePubSub) Query(context.Context, string, ...any) ([]byte, error) {
	return nil, nil
}
func (q *stage4QueuePubSub) Close() error { return nil }
func (q *stage4QueuePubSub) Subscribe(ctx context.Context, _ string) (*pubsub.Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case m := <-q.ch:
		return m, nil
	}
}

func TestSelfReview_Subscriber_PanickingNackAndCommitContained(t *testing.T) {
	backend := &stage4QueuePubSub{ch: make(chan *pubsub.Message, 4)}
	app, err := Build(
		WithConfig(map[string]string{"HTTP_ENABLED": "false"}),
		WithPubSub(backend, Borrowed),
	)
	require.NoError(t, err)
	defer app.Stop(context.Background())

	require.NoError(t, app.AddSubscriber("events.panic-ack", SubscribePolicy{
		Concurrency: 2,
		Shutdown:    SubscriberDrain,
	}, func(c *Context) error {
		var payload string
		_ = c.Bind(&payload)
		if payload == "msg1" {
			return errors.New("force handler failure to trigger Nack")
		}
		return nil
	}))

	require.NoError(t, app.Start(context.Background()))

	// 1. Failing handler triggers Nack() which panics.
	m1 := pubsub.NewMessage(context.Background())
	m1.Topic = "events.panic-ack"
	m1.Value = []byte("msg1")
	m1.Committer = &stage4PanickingCommitter{panicNack: true}
	backend.ch <- m1

	// 2. Succeeding handler triggers CommitContext() which panics.
	m2 := pubsub.NewMessage(context.Background())
	m2.Topic = "events.panic-ack"
	m2.Value = []byte("msg2")
	m2.Committer = &stage4PanickingCommitter{panicCommit: true}
	backend.ch <- m2

	require.Eventually(t, func() bool {
		snap := app.RuntimeSnapshot()
		return len(snap.Subscribers) == 1 &&
			snap.Subscribers[0].CommitFailuresTotal >= 1 &&
			snap.Subscribers[0].PanicsTotal >= 2 &&
			snap.Subscribers[0].InFlightTickets == 0
	}, 2*time.Second, 10*time.Millisecond, "panicking Nack and CommitContext must be recovered and release tickets")

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, app.Stop(stopCtx))
}

func TestSelfReview_OTel_HTTPRouteSQLOutboundServiceAndGRPCTraceparentIsolation(t *testing.T) {
	globalBefore := otel.GetTracerProvider()
	globalTP := newShutdownableTP()
	instanceTP := newShutdownableTP()
	otel.SetTracerProvider(globalTP)
	defer otel.SetTracerProvider(globalBefore)
	defer globalTP.Shutdown(context.Background())
	defer instanceTP.Shutdown(context.Background())

	var downstreamTraceparent atomic.Value
	downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downstreamTraceparent.Store(r.Header.Get("Traceparent"))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer downstream.Close()

	dbPath := filepath.Join(t.TempDir(), "trace.db")
	app, err := Build(
		WithConfig(map[string]string{
			"HTTP_ENABLED": "true",
			"HTTP_ADDR":    "127.0.0.1:0",
			"GRPC_ENABLED": "true",
			"GRPC_ADDR":    "127.0.0.1:0",
			"DB_DIALECT":   "sqlite",
			"DB_NAME":      dbPath,
		}),
		WithManagedSQL(),
		WithTracerProvider(instanceTP, Borrowed),
	)
	require.NoError(t, err)
	defer app.Stop(context.Background())

	app.AddHTTPService("downstream", downstream.URL)
	app.RegisterService(&stage4DummyGRPCDesc, &stage4DummyGRPCImpl{})

	var (
		handlerRecording atomic.Bool
		handlerTraceID   atomic.Value
	)
	app.GET("/trace-check", func(c *Context) (any, error) {
		sp := oteltrace.SpanFromContext(c.Context)
		handlerRecording.Store(sp.IsRecording())
		handlerTraceID.Store(sp.SpanContext().TraceID().String())

		if _, execErr := c.SQL().ExecContext(c.Context, "CREATE TABLE IF NOT EXISTS t_trace (id INTEGER PRIMARY KEY)"); execErr != nil {
			return nil, execErr
		}
		resp, httpErr := c.GetHTTPService("downstream").Get(c.Context, "ping", nil)
		if httpErr != nil {
			return nil, httpErr
		}
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		return "traced", nil
	})

	require.NoError(t, app.Start(context.Background()))

	const wantTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	const incomingTraceparent = "00-" + wantTraceID + "-00f067aa0ba902b7-01"

	req, err := http.NewRequest(http.MethodGet, "http://"+app.HTTPAddress()+"/trace-check", http.NoBody)
	require.NoError(t, err)
	req.Header.Set("Traceparent", incomingTraceparent)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.True(t, handlerRecording.Load(), "active span inside HTTP route handler must remain recording")
	assert.Equal(t, wantTraceID, handlerTraceID.Load(), "HTTP route handler must inherit incoming W3C traceparent")
	gotDownstreamTP, _ := downstreamTraceparent.Load().(string)
	assert.Contains(t, gotDownstreamTP, "00-"+wantTraceID+"-", "outbound HTTP service must propagate W3C traceparent")

	// Verify gRPC also extracts standard W3C traceparent metadata.
	conn, err := grpc.NewClient(app.GRPCAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	const grpcWantTraceID = "11112222333344445555666677778888"
	grpcCtx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(
		"traceparent", "00-"+grpcWantTraceID+"-1234567890abcdef-01",
	))
	rpcCtx, cancel := context.WithTimeout(grpcCtx, time.Second)
	defer cancel()
	_, err = healthpb.NewHealthClient(conn).Check(rpcCtx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)

	require.NoError(t, app.Stop(context.Background()))

	spans := instanceTP.SpanNames()
	assert.Contains(t, spans, "GET /trace-check")
	assert.Contains(t, spans, "pi-router")
	assert.Empty(t, globalTP.SpanNames(), "HTTP router, SQL, outbound HTTP client, and gRPC spans must never leak to global OTel")
}

func TestSelfReview_RateLimiter_DeferredOnBuildAndSubOneRPSAndCircuitBreaker(t *testing.T) {
	t.Run("Build UseRateLimiter stays goroutine-pure before Start and records metric on 429", func(t *testing.T) {
		runtime.GC()
		time.Sleep(15 * time.Millisecond)
		beforeG := runtime.NumGoroutine()

		reader := metricSdk.NewManualReader()
		meterProvider := metricSdk.NewMeterProvider(metricSdk.WithReader(reader))
		defer meterProvider.Shutdown(context.Background())
		mgr := metrics.NewMetricsManager(meterProvider.Meter("self-review-rl"), logging.NewMockLogger(logging.ERROR))

		app, err := Build(
			WithConfig(map[string]string{
				"HTTP_ENABLED": "true",
				"HTTP_ADDR":    "127.0.0.1:0",
			}),
			WithMetrics(mgr),
		)
		require.NoError(t, err)
		defer app.Stop(context.Background())

		app.UseRateLimiter(middleware.RateLimiterConfig{
			RequestsPerSecond: 1,
			Burst:             1,
			PerIP:             false,
		})
		app.GET("/limited", func(*Context) (any, error) { return "ok", nil })

		time.Sleep(15 * time.Millisecond)
		afterBuildG := runtime.NumGoroutine()
		assert.LessOrEqual(t, afterBuildG, beforeG+1, "Build + UseRateLimiter must not spawn background cleanup before Start")

		require.NoError(t, app.Start(context.Background()))

		r1, err := http.Get("http://" + app.HTTPAddress() + "/limited")
		require.NoError(t, err)
		_ = r1.Body.Close()
		assert.Equal(t, http.StatusOK, r1.StatusCode)

		r2, err := http.Get("http://" + app.HTTPAddress() + "/limited")
		require.NoError(t, err)
		_ = r2.Body.Close()
		assert.Equal(t, http.StatusTooManyRequests, r2.StatusCode)

		collected := collectMetricNames(t, reader)
		assert.Contains(t, collected, "app_http_rate_limit_exceeded_total", "429 rejection must record app_http_rate_limit_exceeded_total")
		require.NoError(t, app.Stop(context.Background()))
	})

	t.Run("service LocalRateLimiterStore sub-1-RPS and circuit breaker recovery", func(t *testing.T) {
		store := service.NewLocalRateLimiterStore()
		cfg := service.RateLimiterConfig{
			Requests: 30,
			Window:   time.Minute, // 0.5 RPS (< 1 RPS!)
			Burst:    1,
		}
		allowed1, _, err := store.Allow(context.Background(), "k1", cfg)
		require.NoError(t, err)
		assert.True(t, allowed1)

		// Previously panicked with integer divide-by-zero when refillRate truncated 0.5 -> 0!
		allowed2, retryAfter, err := store.Allow(context.Background(), "k1", cfg)
		require.NoError(t, err)
		assert.False(t, allowed2)
		assert.GreaterOrEqual(t, retryAfter, 1500*time.Millisecond)

		var healthy atomic.Bool
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if healthy.Load() {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"status":"UP"}`))
				return
			}
			w.WriteHeader(http.StatusBadGateway)
		}))
		defer upstream.Close()

		svc := service.NewHTTPService(
			upstream.URL,
			logging.NewMockLogger(logging.ERROR),
			nil,
			&service.HealthConfig{HealthEndpoint: ".well-known/health", Timeout: 1},
			&service.CircuitBreakerConfig{Threshold: 1, Interval: 0},
		)
		defer service.CloseService(svc)

		r1, err := svc.Get(context.Background(), "work", nil)
		require.NoError(t, err) // failureCount = 1 (<= threshold 1)
		if r1 != nil && r1.Body != nil {
			_ = r1.Body.Close()
		}
		_, err = svc.Get(context.Background(), "work", nil)
		require.ErrorIs(t, err, service.ErrCircuitOpen) // failureCount = 2 -> circuit opens

		time.Sleep(2 * time.Millisecond)
		_, err = svc.Get(context.Background(), "work", nil)
		require.ErrorIs(t, err, service.ErrCircuitOpen) // probe fails while upstream is still unhealthy

		healthy.Store(true)
		time.Sleep(2 * time.Millisecond)
		resp, err := svc.Get(context.Background(), "work", nil)
		require.NoError(t, err)
		require.NotNil(t, resp, "recovered circuit breaker call must return a non-nil *http.Response")
		_ = resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
}

func TestSelfReview_HTTPHandler_TimedOutHandlerCompletesBeforeOwnedResourceClose(t *testing.T) {
	var (
		resourceClosed  atomic.Bool
		handlerEntered  = make(chan struct{})
		deadlineExpired = make(chan struct{})
		releaseHandler  = make(chan struct{})
		sawClosedInRun  atomic.Bool
	)
	unblock := sync.OnceFunc(func() { close(releaseHandler) })
	t.Cleanup(unblock)

	app, err := Build(
		WithConfig(map[string]string{
			"HTTP_ENABLED": "true",
			"HTTP_ADDR":    "127.0.0.1:0",
		}),
		WithResource(Resource{
			Name:      "owned-db",
			Ownership: Owned,
			Stop: func(context.Context) error {
				resourceClosed.Store(true)
				return nil
			},
		}),
	)
	require.NoError(t, err)
	defer app.Stop(context.Background())
	defer unblock()

	api := app.Group("/v1")
	api.addRoute("GET", "/slow", func(c *Context) (any, error) {
		close(handlerEntered)
		<-c.Done()
		close(deadlineExpired)
		<-releaseHandler
		if resourceClosed.Load() {
			sawClosedInRun.Store(true)
		}
		return "done", nil
	}, 20*time.Millisecond)

	require.NoError(t, app.Start(context.Background()))

	respCh := make(chan *http.Response, 1)
	go func() {
		r, _ := http.Get("http://" + app.HTTPAddress() + "/v1/slow")
		respCh <- r
	}()

	<-handlerEntered
	<-deadlineExpired

	stopReturned := make(chan error, 1)
	go func() {
		stopReturned <- app.Stop(context.Background())
	}()

	time.Sleep(40 * time.Millisecond)
	assert.False(t, resourceClosed.Load(), "Owned resource must not close while timed-out HTTP handler is still running")

	unblock()
	resp := <-respCh
	require.NotNil(t, resp)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusRequestTimeout, resp.StatusCode)

	require.NoError(t, <-stopReturned)
	assert.True(t, resourceClosed.Load())
	assert.False(t, sawClosedInRun.Load(), "handler must never observe Owned resource closed during execution")
}

func TestSelfReview_GRPCHealth_RootHeartbeatWatchdogAndOnStartHealthServer(t *testing.T) {
	app, err := Build(WithConfig(map[string]string{
		"HTTP_ENABLED": "false",
		"GRPC_ENABLED": "true",
		"GRPC_ADDR":    "127.0.0.1:0",
	}))
	require.NoError(t, err)
	defer app.Stop(context.Background())

	app.OnStart(func(*Context) error {
		_ = app.GRPCHealthServer()
		app.RegisterService(&stage4DummyGRPCDesc, &stage4DummyGRPCImpl{})
		return nil
	})
	require.NoError(t, app.AddWorker("bg", WorkerPolicy{Mode: SuperviseRestart, Required: true}, func(c *Context) error {
		<-c.Done()
		return nil
	}))

	require.NoError(t, app.Start(context.Background()))

	conn, err := grpc.NewClient(app.GRPCAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	hc := healthpb.NewHealthClient(conn)

	checkStatus := func() healthpb.HealthCheckResponse_ServingStatus {
		cctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		res, cErr := hc.Check(cctx, &healthpb.HealthCheckRequest{Service: stage4DummyGRPCDesc.ServiceName})
		if cErr != nil {
			return healthpb.HealthCheckResponse_UNKNOWN
		}
		return res.Status
	}

	require.Equal(t, healthpb.HealthCheckResponse_SERVING, checkStatus())

	// Simulate wedged pi-root heartbeat WITHOUT triggering any worker state event.
	app.runtimeMu.Lock()
	rt := app.actorRT
	app.runtimeMu.Unlock()
	rt.rootLastBeatNano.Store(time.Now().Add(-time.Hour).UnixNano())

	require.Eventually(t, func() bool {
		return checkStatus() == healthpb.HealthCheckResponse_NOT_SERVING
	}, time.Second, 25*time.Millisecond, "gRPC Health watchdog must flip status to NOT_SERVING when pi-root heartbeat expires")

	rt.rootLastBeatNano.Store(time.Now().UnixNano())
	require.Eventually(t, func() bool {
		return checkStatus() == healthpb.HealthCheckResponse_SERVING
	}, time.Second, 25*time.Millisecond)
}

func TestSelfReview_RuntimeSnapshot_ErrorSecretRedactionAndEnumJSONRoundTrip(t *testing.T) {
	pub := &stage4QueuePubSub{ch: make(chan *pubsub.Message, 1)}
	app, err := Build(
		WithConfig(map[string]string{
			"HTTP_ENABLED":         "false",
			"TRACER_HEADERS":       "Authorization=Bearer ultra-secret-otel-token",
			"CORS_ALLOWED_HEADERS": "Content-Type, Authorization",
		}),
		WithPubSub(pub, Borrowed),
	)
	require.NoError(t, err)
	defer app.Stop(context.Background())

	require.NoError(t, app.AddWorker("db-sync", WorkerPolicy{
		Mode:           SuperviseDegrade,
		MaxRestarts:    0,
		BackoffInitial: 5 * time.Millisecond,
		BackoffMax:     10 * time.Millisecond,
	}, func(*Context) error {
		return errors.New("connect postgres://admin:topsecret123@db.internal:5432/prod?api_key=key999; password=rawpass456 failed")
	}))
	require.NoError(t, app.AddCron("0 0 * * * *", "hourly-audit", func(*Context) error {
		return nil
	}, CronJobOptions{Overlap: CronSkipIfRunning, Location: time.UTC}))
	require.NoError(t, app.AddSubscriber("audit.stream", SubscribePolicy{
		Concurrency: 1,
		Shutdown:    SubscriberDrain,
	}, func(*Context) error { return nil }))

	require.NoError(t, app.Start(context.Background()))
	require.Eventually(t, func() bool {
		snap := app.RuntimeSnapshot()
		return len(snap.Workers) == 1 && snap.Workers[0].State == "Degraded"
	}, time.Second, 10*time.Millisecond)

	snap := app.RuntimeSnapshot()
	assert.Equal(t, "[redacted]", snap.Config["TRACER_HEADERS"])
	assert.Equal(t, "Content-Type, Authorization", snap.Config["CORS_ALLOWED_HEADERS"])

	rawJSON, err := json.Marshal(snap)
	require.NoError(t, err)
	jsonStr := string(rawJSON)

	for _, leaked := range []string{"ultra-secret-otel-token", "topsecret123", "key999", "rawpass456", "%5Bredacted%5D"} {
		assert.NotContains(t, jsonStr, leaked, "RuntimeSnapshot JSON must never leak secret %q", leaked)
	}
	assert.Contains(t, jsonStr, `"mode":"degrade"`)
	assert.Contains(t, jsonStr, `"overlap":"skip_if_running"`)
	assert.Contains(t, jsonStr, `"shutdown":"drain"`)

	var roundTrip RuntimeSnapshot
	require.NoError(t, json.Unmarshal(rawJSON, &roundTrip), "RuntimeSnapshot enum string encodings must round-trip cleanly")
	assert.Equal(t, SuperviseDegrade, roundTrip.Workers[0].Mode)
	assert.Equal(t, CronSkipIfRunning, roundTrip.CronJobs[0].Overlap)
	assert.Equal(t, SubscriberDrain, roundTrip.Subscribers[0].Shutdown)
}
