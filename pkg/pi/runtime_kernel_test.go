package pi

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/sllt/pi/pkg/pi/datasource"
	piRedis "github.com/sllt/pi/pkg/pi/datasource/redis"
	piSQL "github.com/sllt/pi/pkg/pi/datasource/sql"
	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/sllt/pi/pkg/pi/service"
)

type sqlWrapperStub struct {
	piSQL.Client
	inner      any
	startCount atomic.Int32
	closeCount atomic.Int32
}

func (w *sqlWrapperStub) UnwrapSQL() any { return w.inner }
func (w *sqlWrapperStub) Start(context.Context) error {
	w.startCount.Add(1)
	return nil
}
func (w *sqlWrapperStub) Close() error {
	w.closeCount.Add(1)
	return nil
}

type sqlTrackableStub struct {
	piSQL.Client
	closeCount atomic.Int32
}

func (s *sqlTrackableStub) Close() error {
	s.closeCount.Add(1)
	return nil
}

type fullCapabilityAdapter struct {
	name       string
	hasLogger  atomic.Bool
	hasMetrics atomic.Bool
	hasTracer  atomic.Bool
	events     *[]string
	mu         *sync.Mutex
	failStart  error
	panicStart bool
}

func (f *fullCapabilityAdapter) UseLogger(v any)  { f.hasLogger.Store(v != nil) }
func (f *fullCapabilityAdapter) UseMetrics(v any) { f.hasMetrics.Store(v != nil) }
func (f *fullCapabilityAdapter) UseTracer(v any)  { f.hasTracer.Store(v != nil) }

func (f *fullCapabilityAdapter) record(ev string) {
	if f.mu != nil && f.events != nil {
		f.mu.Lock()
		*f.events = append(*f.events, ev)
		f.mu.Unlock()
	}
}

func (f *fullCapabilityAdapter) ConnectContext(context.Context) error {
	f.record("start:" + f.name)
	if f.panicStart {
		panic("boom in ConnectContext " + f.name)
	}
	return f.failStart
}

func (f *fullCapabilityAdapter) Shutdown(context.Context) error {
	f.record("stop:" + f.name)
	return nil
}

func (f *fullCapabilityAdapter) HealthCheck(context.Context) (any, error) {
	return datasource.Health{Status: datasource.StatusUp, Details: map[string]any{"adapter": f.name}}, nil
}

type preConnectedOwnedAdapter struct {
	name   string
	events *[]string
	mu     *sync.Mutex
}

func (p *preConnectedOwnedAdapter) Close() error {
	p.mu.Lock()
	*p.events = append(*p.events, "stop:"+p.name)
	p.mu.Unlock()
	return nil
}

type stubUnsealedHTTPService struct {
	closed atomic.Int32
}

func (s *stubUnsealedHTTPService) Get(context.Context, string, map[string]any) (*http.Response, error) {
	return nil, nil
}
func (s *stubUnsealedHTTPService) GetWithHeaders(context.Context, string, map[string]any, map[string]string) (*http.Response, error) {
	return nil, nil
}
func (s *stubUnsealedHTTPService) Post(context.Context, string, map[string]any, []byte) (*http.Response, error) {
	return nil, nil
}
func (s *stubUnsealedHTTPService) PostWithHeaders(context.Context, string, map[string]any, []byte, map[string]string) (*http.Response, error) {
	return nil, nil
}
func (s *stubUnsealedHTTPService) Put(context.Context, string, map[string]any, []byte) (*http.Response, error) {
	return nil, nil
}
func (s *stubUnsealedHTTPService) PutWithHeaders(context.Context, string, map[string]any, []byte, map[string]string) (*http.Response, error) {
	return nil, nil
}
func (s *stubUnsealedHTTPService) Patch(context.Context, string, map[string]any, []byte) (*http.Response, error) {
	return nil, nil
}
func (s *stubUnsealedHTTPService) PatchWithHeaders(context.Context, string, map[string]any, []byte, map[string]string) (*http.Response, error) {
	return nil, nil
}
func (s *stubUnsealedHTTPService) Delete(context.Context, string, []byte) (*http.Response, error) {
	return nil, nil
}
func (s *stubUnsealedHTTPService) DeleteWithHeaders(context.Context, string, []byte, map[string]string) (*http.Response, error) {
	return nil, nil
}
func (s *stubUnsealedHTTPService) HealthCheck(context.Context) *service.Health {
	return &service.Health{Status: "UP"}
}
func (s *stubUnsealedHTTPService) Close() error {
	s.closed.Add(1)
	return nil
}

func TestRuntimeKernel_SlotReplacementGuardsAndImmutability(t *testing.T) {
	t.Run("self-assignment updates ownership without closing, UnwrapSQL transfers ownership, and regular replacement closes displaced Owned", func(t *testing.T) {
		k := newLegacyKernel(nil, logging.NewLogger(logging.FATAL), false)
		app := &App{kernel: k}

		base1, _, _ := piSQL.NewSQLMocks(t)
		defer base1.Close()
		db1 := &sqlTrackableStub{Client: base1}

		// Initial pre-connected Owned registration via setCoreSlot.
		require.NoError(t, app.setCoreSlot("sql", db1, Owned, true, true))

		// Guard 1: Self-assignment with Borrowed updates ownership in place without closing db1!
		require.NoError(t, app.SetSQL(db1, Borrowed))
		assert.Equal(t, int32(0), db1.closeCount.Load())

		// Re-promote db1 back to Owned (pre-connected).
		require.NoError(t, app.SetSQL(db1, Owned))
		assert.Equal(t, int32(0), db1.closeCount.Load())

		// Guard 2: Wrapper handoff via UnwrapSQL() does NOT close db1!
		wrapped := &sqlWrapperStub{Client: db1, inner: db1}
		require.NoError(t, app.SetSQL(wrapped, Owned))
		assert.Equal(t, int32(0), db1.closeCount.Load())
		assert.Same(t, wrapped, app.SQL())

		// Activate wrapped via Start, then replace on a fresh legacy app to verify Guard 3 immediate close.
		k2 := newLegacyKernel(nil, logging.NewLogger(logging.FATAL), false)
		app2 := &App{kernel: k2}
		dbA := &sqlTrackableStub{Client: base1}
		dbB := &sqlTrackableStub{Client: base1}
		require.NoError(t, app2.setCoreSlot("sql", dbA, Owned, true, true))
		require.NoError(t, app2.setCoreSlot("sql", dbB, Owned, true, true))
		assert.Equal(t, int32(1), dbA.closeCount.Load(), "displaced active Owned SQL must be closed immediately")
		assert.Equal(t, int32(0), dbB.closeCount.Load())

		require.NoError(t, app2.Stop(context.Background()))
		assert.Equal(t, int32(1), dbB.closeCount.Load(), "current Owned SQL must be closed on Stop")
	})

	t.Run("reserved names, duplicate names, and post-compile immutability", func(t *testing.T) {
		reserved := []string{"sql", "redis", "pubsub", "file", "tracer", "name", "version", "status", "runtime"}
		for _, rName := range reserved {
			_, err := Build(WithAdapter(rName, &dummyDocStore{id: "x"}, Borrowed))
			require.ErrorIsf(t, err, ErrReservedResourceName, "reserved %q", rName)

			_, err = Build(WithHTTPService(rName, &stubUnsealedHTTPService{}, Borrowed))
			require.ErrorIsf(t, err, ErrReservedResourceName, "reserved http %q", rName)

			_, err = Build(WithResource(Resource{Name: rName, Ownership: Borrowed}))
			require.ErrorIsf(t, err, ErrReservedResourceName, "reserved resource %q", rName)
		}

		// Cross-kind duplicate names rejected.
		_, err := Build(
			WithHTTPService("billing", &stubUnsealedHTTPService{}, Borrowed),
			WithAdapter("billing", &dummyDocStore{id: "b"}, Borrowed),
		)
		require.ErrorIs(t, err, ErrDuplicateResource)

		// Mutually exclusive Managed + Explicit core slots rejected.
		baseDB, _, _ := piSQL.NewSQLMocks(t)
		defer baseDB.Close()
		_, err = Build(
			WithConfig(map[string]string{"DB_DIALECT": "sqlite", "DB_NAME": ":memory:"}),
			WithManagedSQL(),
			WithSQL(baseDB, Borrowed),
		)
		require.ErrorIs(t, err, ErrDuplicateResource)

		// Post-compile immutability: once HTTPHandler() freezes kernel, all mutators return ErrAppAlreadyStarted.
		app, err := Build(WithConfig(map[string]string{"GRPC_ENABLED": "false"}))
		require.NoError(t, err)
		app.GET("/ping", func(*Context) (any, error) { return "pong", nil })

		_, err = app.HTTPHandler()
		require.NoError(t, err)

		require.ErrorIs(t, app.Mount("late", &dummyDocStore{id: "1"}, Borrowed), ErrAppAlreadyStarted)
		require.ErrorIs(t, app.SetSQL(baseDB, Borrowed), ErrAppAlreadyStarted)
		require.ErrorIs(t, app.SetHTTPService("late-svc", &stubUnsealedHTTPService{}, Borrowed), ErrAppAlreadyStarted)
	})
}

func TestRuntimeKernel_LifecycleOrderFullRollbackAndTelemetryProbing(t *testing.T) {
	t.Run("startup order, telemetry probing, and reverse shutdown order", func(t *testing.T) {
		var (
			mu     sync.Mutex
			events []string
		)
		ownedAdp := &fullCapabilityAdapter{name: "owned-adp", events: &events, mu: &mu}
		borrowedAdp := &fullCapabilityAdapter{name: "borrowed-adp", events: &events, mu: &mu}
		borrowedOptIn := &fullCapabilityAdapter{name: "borrowed-optin", events: &events, mu: &mu}
		httpSvc := &stubUnsealedHTTPService{}

		app, err := Build(
			WithConfig(map[string]string{"HTTP_ENABLED": "false", "GRPC_ENABLED": "false"}),
			WithHTTPService("downstream", httpSvc, Owned),
			WithAdapter("owned-adp", ownedAdp, Owned),
			WithAdapter("borrowed-adp", borrowedAdp, Borrowed),
			WithAdapter("borrowed-optin", borrowedOptIn, Borrowed, WithAdapterTelemetry(true)),
			WithResource(Resource{
				Name:      "custom-res",
				Ownership: Owned,
				Start: func(context.Context) error {
					mu.Lock()
					events = append(events, "start:custom-res")
					mu.Unlock()
					return nil
				},
				Stop: func(context.Context) error {
					mu.Lock()
					events = append(events, "stop:custom-res")
					mu.Unlock()
					return nil
				},
			}),
		)
		require.NoError(t, err)

		// Telemetry injected for Owned and explicit WithAdapterTelemetry(true), skipped for default Borrowed!
		assert.True(t, ownedAdp.hasLogger.Load())
		assert.True(t, ownedAdp.hasMetrics.Load())
		assert.True(t, ownedAdp.hasTracer.Load())
		assert.False(t, borrowedAdp.hasLogger.Load())
		assert.False(t, borrowedAdp.hasMetrics.Load())
		assert.False(t, borrowedAdp.hasTracer.Load())
		assert.True(t, borrowedOptIn.hasLogger.Load())
		assert.True(t, borrowedOptIn.hasTracer.Load())

		require.NoError(t, app.Start(context.Background()))
		require.NoError(t, app.Stop(context.Background()))

		assert.Equal(t, int32(1), httpSvc.closed.Load())
		assert.Equal(t, []string{
			"start:owned-adp",
			"start:custom-res",
			"stop:custom-res",
			"stop:owned-adp",
		}, events)
	})

	t.Run("partial startup failure rolls back failing item and pre-connected Owned items registered after it", func(t *testing.T) {
		var (
			mu     sync.Mutex
			events []string
		)
		first := &fullCapabilityAdapter{name: "first", events: &events, mu: &mu}
		failing := &fullCapabilityAdapter{
			name:      "failing",
			events:    &events,
			mu:        &mu,
			failStart: errors.New("postgres://admin:topsecret@127.0.0.1:5432/db dial refused"),
		}
		preConnAfter := &preConnectedOwnedAdapter{name: "preconn-after", events: &events, mu: &mu}
		lazyAfter := &fullCapabilityAdapter{name: "lazy-after", events: &events, mu: &mu}

		app, err := Build(
			WithConfig(map[string]string{"HTTP_ENABLED": "false", "GRPC_ENABLED": "false"}),
			WithAdapter("first", first, Owned),
			WithAdapter("failing", failing, Owned),
			WithAdapter("preconn-after", preConnAfter, Owned),
			WithAdapter("lazy-after", lazyAfter, Owned),
		)
		require.NoError(t, err)

		startErr := app.Start(context.Background())
		require.Error(t, startErr)

		// Reverse scan MUST stop preconn-after (registered after failing, Start==nil), failing, and first,
		// while NOT touching lazy-after (never started).
		assert.Equal(t, []string{
			"start:first",
			"start:failing",
			"stop:preconn-after",
			"stop:failing",
			"stop:first",
		}, events)

		// RuntimeSnapshot redacts credentials in ResourceRuntimeSnapshot.LastError
		// and preserves "failed" state on the failing item while rolled-back items transition to "stopped".
		snap := app.RuntimeSnapshot()
		var foundFirst, foundFailing *ResourceRuntimeSnapshot
		for i := range snap.Resources {
			switch snap.Resources[i].Name {
			case "first":
				foundFirst = &snap.Resources[i]
			case "failing":
				foundFailing = &snap.Resources[i]
			}
		}
		require.NotNil(t, foundFirst)
		assert.Equal(t, "stopped", foundFirst.State)
		require.NotNil(t, foundFailing)
		assert.Equal(t, "failed", foundFailing.State)
		assert.NotContains(t, foundFailing.LastError, "topsecret")
	})

	t.Run("Stop without Start closes pre-connected Owned resources and leaves lazy Owned untouched", func(t *testing.T) {
		var (
			mu     sync.Mutex
			events []string
		)
		preConn := &preConnectedOwnedAdapter{name: "preconn", events: &events, mu: &mu}
		lazy := &fullCapabilityAdapter{name: "lazy", events: &events, mu: &mu}

		app, err := Build(
			WithConfig(map[string]string{"HTTP_ENABLED": "false", "GRPC_ENABLED": "false"}),
			WithAdapter("preconn", preConn, Owned),
			WithAdapter("lazy", lazy, Owned),
		)
		require.NoError(t, err)

		require.NoError(t, app.Stop(context.Background()))
		assert.Equal(t, []string{"stop:preconn"}, events)
	})
}

func TestRuntimeKernel_ManagedRedisAndManagedDatasources(t *testing.T) {
	t.Run("WithManagedRedis lazy handle safely rejects Pipeline/TxPipeline/Watch/Do before Start and after Stop", func(t *testing.T) {
		app, err := Build(
			WithConfig(map[string]string{
				"HTTP_ENABLED": "false",
				"GRPC_ENABLED": "false",
				"REDIS_HOST":   "127.0.0.1",
				"REDIS_PORT":   "6379",
			}),
			WithManagedRedis(),
		)
		require.NoError(t, err)
		require.NotNil(t, app.Redis())

		ctx := context.Background()
		assert.ErrorIs(t, app.Redis().Get(ctx, "k").Err(), piRedis.ErrNotStarted)
		assert.ErrorIs(t, app.Redis().Do(ctx, "PING").Err(), piRedis.ErrNotStarted)
		assert.ErrorIs(t, app.Redis().Watch(ctx, func(*goredis.Tx) error { return nil }, "k"), piRedis.ErrNotStarted)

		pipe := app.Redis().Pipeline()
		pipe.Get(ctx, "k")
		_, err = pipe.Exec(ctx)
		assert.ErrorIs(t, err, piRedis.ErrNotStarted)

		txPipe := app.Redis().TxPipeline()
		txPipe.Set(ctx, "k", "v", 0)
		_, err = txPipe.Exec(ctx)
		assert.ErrorIs(t, err, piRedis.ErrNotStarted)

		_, err = app.Redis().Pipelined(ctx, func(p goredis.Pipeliner) error {
			p.Get(ctx, "k")
			return nil
		})
		assert.ErrorIs(t, err, piRedis.ErrNotStarted)

		// Unstarted lazy resource remains in ErrNotStarted when Stop runs before Start;
		// explicit Close() transitions the handle to ErrClosed.
		require.NoError(t, app.Stop(ctx))
		assert.ErrorIs(t, app.Redis().Get(ctx, "k").Err(), piRedis.ErrNotStarted)

		require.NoError(t, app.Redis().Close())
		assert.ErrorIs(t, app.Redis().Get(ctx, "k").Err(), piRedis.ErrClosed)
		_, err = app.Redis().TxPipelined(ctx, func(p goredis.Pipeliner) error {
			p.Get(ctx, "k")
			return nil
		})
		assert.ErrorIs(t, err, piRedis.ErrClosed)
	})

	t.Run("WithManagedDatasources respects DB_DIALECT boundary and cloud/custom env prefixes", func(t *testing.T) {
		// DB_URL alone without DB_DIALECT MUST NOT trigger managed SQL!
		app, err := Build(
			WithConfig(map[string]string{
				"HTTP_ENABLED":                   "false",
				"GRPC_ENABLED":                   "false",
				"DB_URL":                         "postgres://custom-gorm-url/db",
				"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/gcp.json",
				"REDIS_CACHE_TTL":                "300s",
				"PUBSUB_TOPIC_ORDERS":            "orders.v1",
				"REDIS_HOST":                     "127.0.0.1",
				"PUBSUB_BACKEND":                 "MQTT",
				"MQTT_HOST":                      "127.0.0.1",
			}),
			WithManagedDatasources(),
		)
		require.NoError(t, err)
		assert.Nil(t, app.SQL(), "DB_URL without DB_DIALECT must not enable ManagedSQL")
		assert.NotNil(t, app.Redis())
		assert.NotNil(t, app.PubSub())
		require.NoError(t, app.Stop(context.Background()))

		// Mismatched mTLS client cert/key pairs fail fast in Build.
		_, err = Build(WithConfig(map[string]string{
			"HTTP_ENABLED":   "false",
			"GRPC_ENABLED":   "false",
			"REDIS_TLS_CERT": "/tmp/client.pem",
		}))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "REDIS_TLS_CERT and REDIS_TLS_KEY")
	})
}

func TestRuntimeKernel_SingleflightHealthAndCloseAdmission(t *testing.T) {
	var probeCalls atomic.Int32
	probeEntered := make(chan struct{})
	releaseProbe := make(chan struct{})

	slowAdapter := &dummyDocStore{id: "slow"}
	lazyUnstarted := &fullCapabilityAdapter{name: "lazy-unstarted"}

	app, err := Build(
		WithConfig(map[string]string{"HTTP_ENABLED": "false", "GRPC_ENABLED": "false"}),
		WithAdapter("slow-store", slowAdapter, Borrowed, WithAdapterHealthCheck(func(ctx context.Context) error {
			if probeCalls.Add(1) == 1 {
				close(probeEntered)
			}
			select {
			case <-releaseProbe:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})),
		WithAdapter("lazy-unstarted", lazyUnstarted, Owned),
	)
	require.NoError(t, err)

	// Unstarted Owned lazy adapter reports DOWN ("resource not started") without calling HealthCheck.
	fastCtx, fastCancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer fastCancel()

	// Caller 1 has a short 25ms timeout; Caller 2 has a 2s timeout. Both share ONE singleflight probe!
	var (
		res1, res2 map[string]any
		wg         sync.WaitGroup
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		res1 = app.Health(fastCtx)
	}()
	<-probeEntered
	go func() {
		defer wg.Done()
		longCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		res2 = app.Health(longCtx)
	}()

	time.Sleep(40 * time.Millisecond) // Let fastCtx time out while Caller 2 is still waiting!
	close(releaseProbe)
	wg.Wait()

	assert.Equal(t, int32(1), probeCalls.Load(), "concurrent callers must share one singleflight probe")
	h1 := res1["slow-store"].(datasource.Health)
	assert.Equal(t, datasource.StatusDown, h1.Status)
	assert.Equal(t, "singleflight_in_progress", h1.Details["isolation"])

	h2 := res2["slow-store"].(datasource.Health)
	assert.Equal(t, datasource.StatusUp, h2.Status, "short caller timeout must not cancel shared probe for other callers")

	lazyH := res2["lazy-unstarted"].(datasource.Health)
	assert.Equal(t, datasource.StatusDown, lazyH.Status)
	assert.Equal(t, "resource not started", lazyH.Details["error"])

	// Verify CloseHealthAdmission aborts an in-flight context-aware probe immediately!
	blockEntered := make(chan struct{})
	app2, err := Build(
		WithConfig(map[string]string{"HTTP_ENABLED": "false", "GRPC_ENABLED": "false"}),
		WithAdapter("blocking", &dummyDocStore{id: "b"}, Borrowed, WithAdapterHealthCheck(func(ctx context.Context) error {
			close(blockEntered)
			<-ctx.Done()
			return ctx.Err()
		})),
	)
	require.NoError(t, err)

	doneHealth := make(chan map[string]any, 1)
	go func() {
		doneHealth <- app2.Health(context.Background())
	}()
	<-blockEntered

	startStop := time.Now()
	app2.kernel.CloseHealthAdmission()
	require.NoError(t, app2.kernel.WaitHealthSamplers(context.Background()))
	assert.Less(t, time.Since(startStop), 500*time.Millisecond, "CloseHealthAdmission must abort slow probes immediately")
	<-doneHealth
}

func TestRuntimeKernel_NewCMD_FourFixes(t *testing.T) {
	origArgs := os.Args
	t.Cleanup(func() { os.Args = origArgs })

	t.Run("propagates context cancellation, returns handler error, and closes CMD_LOGS_FILE", func(t *testing.T) {
		logPath := filepath.Join(t.TempDir(), "cmd.log")
		t.Setenv("CMD_LOGS_FILE", logPath)

		os.Args = []string{"pi-cmd", "work"}
		app := NewCMD()

		var (
			observedCancel bool
			onStopCalled   bool
		)
		app.OnStop(func(*Context) error {
			onStopCalled = true
			return nil
		})

		expectedErr := errors.New("subcommand business failure")
		app.SubCommand("work", func(c *Context) (any, error) {
			<-c.Done()
			observedCancel = errors.Is(c.Err(), context.Canceled)
			c.Logger().Info("writing log before exit")
			return nil, expectedErr
		})

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(20 * time.Millisecond)
			cancel()
		}()

		err := app.RunContext(ctx)
		require.ErrorIs(t, err, expectedErr)
		assert.True(t, observedCancel, "CLI subcommand context must observe RunContext cancellation")
		assert.True(t, onStopCalled, "OnStop must execute on CLI exit")
		assert.False(t, app.kernel.loggerOwned, "owned file logger must be closed and cleared on exit")
	})

	t.Run("cleans up active resources via defer when OnStart fails", func(t *testing.T) {
		os.Args = []string{"pi-cmd", "noop"}
		app := NewCMD()

		var resClosed atomic.Bool
		require.NoError(t, app.Mount("temp-res", &dummyDocStore{id: "t"}, Owned,
			WithAdapterStart(func(context.Context) error { return nil }),
			WithAdapterStop(func(context.Context) error {
				resClosed.Store(true)
				return nil
			}),
		))

		boom := errors.New("onstart failure")
		app.OnStart(func(*Context) error { return boom })
		app.SubCommand("noop", func(*Context) (any, error) { return "ok", nil })

		err := app.RunContext(context.Background())
		require.ErrorIs(t, err, boom)
		assert.True(t, resClosed.Load(), "started Owned adapter must be stopped when OnStart fails in CLI mode")
	})
}

func TestRuntimeKernel_ContextHeaderAuthValidatorsAndDynamicTraceLogger(t *testing.T) {
	t.Run("Context.Header works for HTTP, gRPC incoming metadata, and zero-value Context", func(t *testing.T) {
		var nilCtx *Context
		assert.Equal(t, "", nilCtx.Header("X-Test"))
		assert.Equal(t, "", (&Context{}).Header("X-Test"))

		app, err := Build(
			WithConfig(map[string]string{
				"GRPC_ENABLED":        "false",
				"HTTP_MAX_BODY_BYTES": "16",
			}),
			WithAdapter("tenant-store", &dummyDocStore{id: "tenant-ok"}, Borrowed),
		)
		require.NoError(t, err)

		var bodyReadErr error
		app.EnableAPIKeyAuthWithValidator(func(c *Context, apiKey string) bool {
			store, ok := Use[*dummyDocStore](c)
			if !ok || store.id != "tenant-ok" {
				return false
			}
			var payload map[string]any
			bodyReadErr = c.Bind(&payload)
			return apiKey == "secret-key" && c.Header("x-tenant-id") == "acme"
		})
		app.POST("/protected", func(c *Context) (any, error) {
			var payload map[string]any
			if err := c.Bind(&payload); err != nil {
				return nil, err
			}
			return c.Header("X-TENANT-ID"), nil
		})

		h, err := app.HTTPHandler()
		require.NoError(t, err)

		// Valid request within 16-byte body limit (verifies body rewind after auth validator c.Bind).
		reqOK := httptest.NewRequest(http.MethodPost, "/protected", strings.NewReader(`{"a":1}`))
		reqOK.Header.Set("Content-Type", "application/json")
		reqOK.Header.Set("X-API-Key", "secret-key")
		reqOK.Header.Set("X-Tenant-Id", "acme")
		wOK := httptest.NewRecorder()
		h.ServeHTTP(wOK, reqOK)
		assert.Equal(t, http.StatusCreated, wOK.Code)
		require.NoError(t, bodyReadErr)

		// Oversized body (> 16 bytes) is bounded by NewRequestWithLimit inside auth validator,
		// restores req.Body on failure, and maps *http.MaxBytesError to 413 Request Entity Too Large.
		reqBig := httptest.NewRequest(http.MethodPost, "/protected", strings.NewReader(`{"oversized":"0123456789abcdef"}`))
		reqBig.Header.Set("Content-Type", "application/json")
		reqBig.Header.Set("X-API-Key", "secret-key")
		reqBig.Header.Set("X-Tenant-Id", "acme")
		wBig := httptest.NewRecorder()
		h.ServeHTTP(wBig, reqBig)
		var maxBytesErr *http.MaxBytesError
		require.ErrorAs(t, bodyReadErr, &maxBytesErr, "auth HTTP context must enforce HTTP_MAX_BODY_BYTES")
		assert.Equal(t, http.StatusRequestEntityTooLarge, wBig.Code)

		// gRPC incoming metadata case-insensitivity via Context.Header & BasicAuth validator.
		md := metadata.Pairs("x-org-id", "org-42", "authorization", "Basic dXNlcjpwYXNz") // user:pass
		rpcCtx := metadata.NewIncomingContext(context.Background(), md)
		piCtx := app.NewContext(rpcCtx)
		assert.Equal(t, "org-42", piCtx.Header("X-ORG-ID"))

		appGRPC, err := Build(WithConfig(map[string]string{"HTTP_ENABLED": "false"}))
		require.NoError(t, err)
		var capturedOrg string
		appGRPC.EnableBasicAuthWithValidator(func(c *Context, u, p string) bool {
			capturedOrg = c.Header("X-Org-Id")
			return u == "user" && p == "pass"
		})
		require.NotEmpty(t, appGRPC.grpcServer.interceptors)
		authInterceptor := appGRPC.grpcServer.interceptors[len(appGRPC.grpcServer.interceptors)-1]
		_, err = authInterceptor(
			rpcCtx,
			"req",
			&grpc.UnaryServerInfo{FullMethod: "/test.Svc/Call"},
			func(ctx context.Context, req any) (any, error) { return "ok", nil },
		)
		require.NoError(t, err)
		assert.Equal(t, "org-42", capturedOrg)
	})

	t.Run("ctx.Trace dynamically refreshes ContextLogger trace_id", func(t *testing.T) {
		var buf bytes.Buffer
		tp := sdktrace.NewTracerProvider()
		defer func() { _ = tp.Shutdown(context.Background()) }()

		logger := logging.NewWriterLogger(logging.INFO, &buf, &buf)
		app, err := Build(
			WithConfig(map[string]string{"HTTP_ENABLED": "false", "GRPC_ENABLED": "false"}),
			WithLogger(logger),
			WithTracerProvider(tp, Borrowed),
		)
		require.NoError(t, err)

		c := app.NewContext(context.Background())
		span := c.Trace("dynamic-op")
		expectedTraceID := span.SpanContext().TraceID().String()
		c.Info("inside child span")
		span.End()

		out, err := io.ReadAll(&buf)
		require.NoError(t, err)
		assert.NotEmpty(t, expectedTraceID)
		assert.Contains(t, string(out), expectedTraceID)
	})
}

type errorHealthCtxAdapter struct {
	err error
}

func (a *errorHealthCtxAdapter) HealthCheckContext(context.Context) error { return a.err }

type errorHealthAdapter struct {
	err error
}

func (a *errorHealthAdapter) HealthCheck(context.Context) error { return a.err }

func TestRuntimeKernel_OwnedHTTPServiceTelemetryAndErrorHealthAdapters(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	var logBuf bytes.Buffer
	logger := logging.NewWriterLogger(logging.INFO, &logBuf, &logBuf)
	svc := service.NewHTTPService(upstream.URL, nil, nil, &service.RetryConfig{MaxRetries: 1})

	app, err := Build(
		WithConfig(map[string]string{"HTTP_ENABLED": "false", "GRPC_ENABLED": "false"}),
		WithLogger(logger),
		WithHTTPService("payment", svc, Owned),
		WithAdapter("hc-ctx-ok", &errorHealthCtxAdapter{}, Borrowed),
		WithAdapter("hc-err", &errorHealthAdapter{err: errors.New("probe failed")}, Borrowed),
	)
	require.NoError(t, err)
	require.NoError(t, app.Start(context.Background()))
	defer func() { _ = app.Stop(context.Background()) }()

	resp, err := app.HTTPService("payment").Get(context.Background(), "ping", nil)
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Contains(t, logBuf.String(), upstream.URL+"/ping")

	health := app.Health(context.Background())
	assert.Equal(t, "DEGRADED", health["status"])
	hcOK, ok := health["hc-ctx-ok"].(datasource.Health)
	require.True(t, ok)
	assert.Equal(t, datasource.StatusUp, hcOK.Status)
	hcErr, ok := health["hc-err"].(datasource.Health)
	require.True(t, ok)
	assert.Equal(t, datasource.StatusDown, hcErr.Status)

	// Guard nil config / nil context on OpenContextWithTracerProvider.
	_, err = piRedis.OpenContextWithTracerProvider(context.Background(), nil, nil, nil, nil)
	require.Error(t, err)
	_, err = piSQL.OpenContextWithTracerProvider(nil, app.Config, app.Logger(), nil, nil)
	require.Error(t, err)
}

func TestRuntimeKernel_DefaultIsolatedTracerGeneratesRealTraceID(t *testing.T) {
	var logBuf bytes.Buffer
	logger := logging.NewWriterLogger(logging.INFO, &logBuf, &logBuf)

	app, err := Build(
		WithConfig(map[string]string{
			"HTTP_ENABLED": "false",
			"GRPC_ENABLED": "false",
		}),
		WithLogger(logger),
	)
	require.NoError(t, err)
	defer func() { _ = app.Stop(context.Background()) }()

	var handlerTraceID string
	app.GET("/ping", func(c *Context) (any, error) {
		handlerTraceID = c.GetCorrelationID()
		c.Info("handler executed")
		return "pong", nil
	})

	h, err := app.HTTPHandler()
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ping", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	require.Len(t, handlerTraceID, 32)
	assert.NotEqual(t, "00000000000000000000000000000000", handlerTraceID)
	assert.Equal(t, handlerTraceID, rec.Header().Get("X-Correlation-ID"))
	assert.Contains(t, logBuf.String(), handlerTraceID)
	assert.NotContains(t, logBuf.String(), "00000000000000000000000000000000")

	// When TRACER_RATIO=0, sampling is disabled and zero TraceID is omitted.
	var zeroBuf bytes.Buffer
	zeroApp, err := Build(
		WithConfig(map[string]string{
			"HTTP_ENABLED": "false",
			"GRPC_ENABLED": "false",
			"TRACER_RATIO": "0",
		}),
		WithLogger(logging.NewWriterLogger(logging.INFO, &zeroBuf, &zeroBuf)),
	)
	require.NoError(t, err)
	defer func() { _ = zeroApp.Stop(context.Background()) }()

	zeroApp.GET("/ping", func(c *Context) (any, error) {
		c.Info("unsampled")
		return "ok", nil
	})
	zeroH, err := zeroApp.HTTPHandler()
	require.NoError(t, err)

	zeroRec := httptest.NewRecorder()
	zeroH.ServeHTTP(zeroRec, httptest.NewRequest(http.MethodGet, "/ping", nil))
	require.Equal(t, http.StatusOK, zeroRec.Code)
	assert.Empty(t, zeroRec.Header().Get("X-Correlation-ID"))
	assert.NotContains(t, zeroBuf.String(), "00000000000000000000000000000000")
}
