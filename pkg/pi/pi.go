package pi

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/sllt/pi/pkg/pi/cmd/terminal"
	"github.com/sllt/pi/pkg/pi/config"
	"github.com/sllt/pi/pkg/pi/datasource/file"
	"github.com/sllt/pi/pkg/pi/datasource/pubsub"
	piRedis "github.com/sllt/pi/pkg/pi/datasource/redis"
	piSQL "github.com/sllt/pi/pkg/pi/datasource/sql"
	"github.com/sllt/pi/pkg/pi/http/middleware"
	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/sllt/pi/pkg/pi/metrics"
	"github.com/sllt/pi/pkg/pi/migration"
	"github.com/sllt/pi/pkg/pi/service"
	"github.com/sllt/pi/pkg/pi/websocket"
)

const (
	configLocation = "./configs"
)

var errStartupHookPanic = errors.New("startup hook panicked")
var errStopHookPanic = errors.New("stop hook panicked")
var errAppAlreadyStopped = errors.New("application has already been stopped")

// App is the main application in the Pi framework.
type App struct {
	// Config can be used by applications to fetch custom configurations from environment or file.
	Config config.Config // If we directly embed, unnecessary confusion between app.Get and app.GET will happen.

	grpcServer   *grpcServer
	httpServer   *httpServer
	metricServer *metricServer
	initErr      error
	httpDisabled bool
	grpcDisabled bool

	cmd  *cmd
	cron *Crontab

	// kernel is the unexported runtime microkernel managing core datasources, adapters, and telemetry.
	kernel *runtimeKernel

	grpcRegistered bool
	httpRegistered bool

	subscriptionManager SubscriptionManager
	onStartHooks        []func(ctx *Context) error
	onStopHooks         []func(ctx *Context) error
	drainHooks          []func(ctx context.Context) error
	backgroundWorkers   []*workerSlot
	nextWorkerSeq       uint64
	wsClients           []*wsClientSlot
	nextWSClientSeq     uint64
	compatWSServices    []*compatWSService
	subscriberSlotsList []*subscriberSlot
	nextSubscriberSeq   uint64

	wsServerRegistered    bool
	wsDrainOnce           sync.Once
	wsServerSessionsMu    sync.RWMutex
	wsServerSessions      map[string]*wsServerSession
	nextWSSessionSeq      uint64
	wsHubBoots            atomic.Uint64
	wsServerEnqueuedTotal atomic.Uint64
	wsServerDroppedTotal  atomic.Uint64
	wsServerWriteErrTotal atomic.Uint64

	startupCompleted      atomic.Bool
	actorTreeDead         atomic.Bool
	grpcHealthEnabled     atomic.Bool
	grpcHealthOnce        sync.Once
	grpcHealthDirtyCh     chan struct{}
	grpcHealthLoopStarted atomic.Bool
	grpcHealthSyncMu      sync.Mutex
	supMetricsOnce        sync.Once
	supMetricsReady       atomic.Bool
	rateLimitMetricsOnce  sync.Once
	ownedTracerShutdown   func(context.Context) error
	middlewareClosers     []func() error
	defaultIdemMu         sync.Mutex
	defaultIdemStore      *middleware.MemoryIdempotencyStore

	runtimeCtx      context.Context
	runtimeCancel   context.CancelCauseFunc
	intakeCtx       context.Context
	intakeCancel    context.CancelCauseFunc
	intakeStopAfter func() bool
	drainExecCtx    context.Context
	drainCancel     context.CancelCauseFunc

	taskGateMu          sync.Mutex
	intakeClosed        bool
	runtimeTasks        sync.WaitGroup
	controllerStopTasks sync.WaitGroup

	runtimeMu        sync.Mutex
	runtimeState     lifecycleState
	atomicState      atomic.Uint32
	subConfigErr     error
	startupDone      chan struct{}
	startDone        chan struct{}
	startErr         error
	stopDone         chan struct{}
	stopErr          error
	lateRuntimeErr   error
	actorRT          *actorRuntime
	workerJitter     func(maxDelay time.Duration) time.Duration
	cronNow          func() time.Time
	pureBuild        bool
	waitDone         chan struct{}
	customValidate   func(any) error
	typedBind        TypedBindConfig
	openAPI          OpenAPIConfig
	hasOpenAPIConfig bool
	httpSetupMu      sync.Mutex
	httpSetupDone    bool
}

func (a *App) runOnStartHooks(ctx context.Context) error {
	piCtx := newContext(nil, noopRequest{ctx: ctx}, a.kernel)

	a.runtimeMu.Lock()
	hooks := append([]func(*Context) error(nil), a.onStartHooks...)
	a.runtimeMu.Unlock()

	for i, hook := range hooks {
		hookErr := a.runLifecycleHook("OnStart", i, hook, piCtx, errStartupHookPanic)

		if hookErr != nil {
			a.Logger().Errorf("OnStart hook failed: %v", hookErr)
			return hookErr
		}

		// Check if context was canceled
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}

	return nil
}

func (a *App) runOnStopHooks(ctx context.Context) error {
	a.runtimeMu.Lock()
	hooks := append([]func(*Context) error(nil), a.onStopHooks...)
	a.runtimeMu.Unlock()

	piCtx := newContext(nil, noopRequest{ctx: ctx}, a.kernel)
	var err error

	for i := len(hooks) - 1; i >= 0; i-- {
		if ctx.Err() != nil {
			err = joinShutdownErr(err, fmt.Errorf("OnStop hook %d not executed: %w", i, ctx.Err()))
			return err
		}

		hookErr := a.runLifecycleHook("OnStop", i, hooks[i], piCtx, errStopHookPanic)
		if hookErr != nil {
			if l := a.safeLogger(); l != nil {
				l.Errorf("OnStop hook failed: %v", hookErr)
			}
			err = joinShutdownErr(err, hookErr)
		}
	}

	return joinShutdownErr(err, ctx.Err())
}

func (a *App) runShutdownOnStopHooks(callerCtx context.Context, fallbackCtx func() context.Context) error {
	a.runtimeMu.Lock()
	hooks := append([]func(*Context) error(nil), a.onStopHooks...)
	a.runtimeMu.Unlock()

	var err error
	for i := len(hooks) - 1; i >= 0; i-- {
		hookCtx := callerCtx
		if callerErr := callerCtx.Err(); callerErr != nil {
			err = joinShutdownErr(err, callerErr)
			hookCtx = fallbackCtx()
		}
		if hookCtx.Err() != nil {
			err = joinShutdownErr(err, fmt.Errorf("OnStop hook %d not executed: %w", i, hookCtx.Err()))
			break
		}

		piCtx := newContext(nil, noopRequest{ctx: hookCtx}, a.kernel)
		hookErr := a.runLifecycleHook("OnStop", i, hooks[i], piCtx, errStopHookPanic)
		if hookErr != nil {
			if l := a.safeLogger(); l != nil {
				l.Errorf("OnStop hook failed: %v", hookErr)
			}
			err = joinShutdownErr(err, hookErr)
		}
		if callerErr := callerCtx.Err(); callerErr != nil {
			err = joinShutdownErr(err, callerErr)
		}
	}

	return err
}

func joinShutdownErr(existing, next error) error {
	if next == nil {
		return existing
	}
	if existing == nil {
		return next
	}
	if errors.Is(existing, next) && (errors.Is(next, context.Canceled) || errors.Is(next, context.DeadlineExceeded)) {
		return existing
	}
	return errors.Join(existing, next)
}

func (a *App) safeLogger() logging.Logger {
	if a != nil && a.kernel != nil {
		return a.kernel.getLogger()
	}
	return nil
}

func (a *App) runLifecycleHook(name string, idx int, hook func(ctx *Context) error, ctx *Context, panicErr error) (hookErr error) {
	func() {
		defer func() {
			if r := recover(); r != nil {
				a.Logger().Errorf("%s hook %d panicked: %v", name, idx, r)
				hookErr = fmt.Errorf("hook %d: %w: %v", idx, panicErr, r)
			}
		}()

		hookErr = hook(ctx)
	}()

	return hookErr
}

// Stop gracefully stops the application runtime.
// The first caller supplies the cleanup context. Other callers only bound their own
// wait; they cannot cancel or restart that cleanup. Every completed call observes
// the same terminal result. Cancellation can return before cleanup has finished.
func (a *App) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	a.runtimeMu.Lock()
	if a.stopDone == nil {
		a.runtimeState = lifecycleStopping
		a.atomicState.Store(uint32(lifecycleStopping))
		a.stopDone = make(chan struct{})
		startupDone := a.startupDone
		a.runtimeMu.Unlock()

		func() {
			defer func() { _ = recover() }()
			a.beginStopIntake(nil)
		}()
		go a.finishStop(ctx, startupDone)
	} else {
		a.runtimeMu.Unlock()
		func() {
			defer func() { _ = recover() }()
			a.beginStopIntake(nil)
		}()
	}

	return a.waitForStop(ctx)
}

// Shutdown stops the service(s) and closes the application.
// It is kept for backward compatibility; new embedded integrations should prefer Stop.
func (a *App) Shutdown(ctx context.Context) error {
	return a.Stop(ctx)
}

// shutdown executes the three-phase shutdown sequence:
// Phase A (Stop Intake & protocol listeners) -> Phase B (Business Drain while drainExecCtx stays live)
// -> Phase C (Cancel drainExecCtx, stop Actor supervision tree, wait for all tracked runtime tasks)
// -> OnStop hooks -> Owned resources & Metrics server.
func (a *App) shutdown(ctx context.Context) error {
	a.beginStopIntake(nil)

	var (
		err            error
		fallbackCtxVal context.Context
		fallbackCancel context.CancelFunc
	)
	defer func() {
		if fallbackCancel != nil {
			fallbackCancel()
		}
	}()

	getCleanupFallbackCtx := func() context.Context {
		if fallbackCtxVal != nil {
			return fallbackCtxVal
		}
		timeout := 30 * time.Second
		if a.Config != nil {
			if cfgTimeout, cfgErr := a.shutdownTimeout(); cfgErr == nil && cfgTimeout > 0 {
				timeout = cfgTimeout
			}
		}
		fallbackCtxVal, fallbackCancel = context.WithTimeout(context.WithoutCancel(ctx), timeout)
		return fallbackCtxVal
	}

	// Phase A: close protocol listeners and stop subscriber intake (reserve sub-budget when Phase B drain work is registered).
	needsPhaseB := a.hasPhaseBDrainHooks() || a.cronActorCount() > 0 || a.hasDrainSubscribers()
	listenerCtx := ctx
	var listenerCancel context.CancelFunc
	if needsPhaseB {
		if deadline, ok := ctx.Deadline(); ok {
			if rem := time.Until(deadline); rem > 10*time.Millisecond {
				listenerCtx, listenerCancel = context.WithTimeout(ctx, rem/2)
			}
		}
	}
	if a.httpServer != nil {
		err = joinShutdownErr(err, a.httpServer.Shutdown(listenerCtx))
	}
	if a.grpcServer != nil {
		err = joinShutdownErr(err, a.grpcServer.Shutdown(listenerCtx))
	}
	if listenerCancel != nil {
		listenerCancel()
	}

	if a.cron != nil {
		a.cron.Stop()
	}

	if len(a.subscriberSlots()) > 0 {
		intakeStopBudget := 2 * time.Second
		if deadline, ok := ctx.Deadline(); ok {
			if rem := time.Until(deadline); rem > 4*time.Millisecond {
				if quarter := rem / 4; quarter < intakeStopBudget {
					intakeStopBudget = quarter
				}
			} else {
				intakeStopBudget = time.Millisecond
			}
		}
		intakeStopCtx, cancelIntakeWait := context.WithTimeout(ctx, intakeStopBudget)
		_ = a.waitSubscribersIntakeStopped(intakeStopCtx)
		cancelIntakeWait()
	}

	// Phase B: drain finite in-flight business work while drainExecCtx remains un-canceled.
	// Respect caller ctx directly so an already-expired Stop(ctx) advances immediately to Phase C cancellation.
	if needsPhaseB {
		err = joinShutdownErr(err, a.waitCronInFlight(ctx))
		err = joinShutdownErr(err, a.waitSubscribersDrained(ctx))
		err = joinShutdownErr(err, a.runPhaseBDrain(ctx))
	}

	// Phase C: cancel residual drain execution scope, stop Actor supervision tree, and wait for all tracked tasks.
	a.cancelDrainExec(context.Canceled)
	a.sweepSubscribersUnstarted("drain_timeout_abandoned")
	actorStopCtx := ctx
	if callerErr := ctx.Err(); callerErr != nil {
		err = joinShutdownErr(err, callerErr)
		actorStopCtx = getCleanupFallbackCtx()
	}
	err = joinShutdownErr(err, a.stopActorRuntime(actorStopCtx))
	a.stopCompatWSServices()

	if waitErr := a.waitForRuntimeTasks(ctx); waitErr != nil {
		err = joinShutdownErr(err, waitErr)
		// Do not close dependencies while workers still use them. Stop waiters
		// can time out independently while this sole cleanup owner keeps waiting.
		a.runtimeTasks.Wait()
	}
	a.controllerStopTasks.Wait()
	if a.kernel != nil {
		if waitErr := a.kernel.WaitHandlers(ctx); waitErr != nil {
			err = joinShutdownErr(err, waitErr)
			a.kernel.WaitHandlersAll()
		}
		a.kernel.CloseHealthAdmission()
		if waitErr := a.kernel.WaitHealthSamplers(ctx); waitErr != nil {
			err = joinShutdownErr(err, waitErr)
			a.kernel.WaitHealthSamplersAll()
		}
	}
	err = joinShutdownErr(err, a.takeLateRuntimeError())

	// Phase D: execute OnStop hooks in reverse order while core datasources & adapters remain active.
	err = joinShutdownErr(err, a.runShutdownOnStopHooks(ctx, getCleanupFallbackCtx))

	// Phase E: close residual WS connections, middleware loops, Owned resources/adapters/services, tracer, metrics, and owned logger.
	cleanupCtx := ctx
	if callerErr := ctx.Err(); callerErr != nil {
		err = joinShutdownErr(err, callerErr)
		cleanupCtx = getCleanupFallbackCtx()
	}

	if a.kernel != nil {
		a.kernel.closeAllWSConnections(cleanupCtx)
	}
	err = joinShutdownErr(err, a.closeMiddlewareClosers())
	err = joinShutdownErr(err, a.stopResources(cleanupCtx))
	if a.ownedTracerShutdown != nil {
		err = joinShutdownErr(err, a.ownedTracerShutdown(cleanupCtx))
	}
	if a.metricServer != nil {
		err = joinShutdownErr(err, a.metricServer.Shutdown(cleanupCtx))
	}

	if err == nil {
		if l := a.safeLogger(); l != nil {
			l.Info("Application shutdown complete")
		}
	}
	if a.kernel != nil {
		err = joinShutdownErr(err, a.kernel.closeOwnedLogger())
	}

	return err
}

func isPortAvailable(port int) bool {
	dialer := net.Dialer{Timeout: checkPortTimeout}

	conn, err := dialer.DialContext(context.Background(), "tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return true
	}

	conn.Close()

	return false
}

func (a *App) validateRoutesPreStart() error {
	if a == nil || a.httpServer == nil || a.httpServer.registry == nil {
		return nil
	}
	reg := a.httpServer.registry
	reg.mu.Lock()
	defer reg.mu.Unlock()
	return a.syncAndValidateRoutesLocked(false)
}

func (a *App) validateRoutesForStart() error {
	if a == nil || a.httpServer == nil || a.httpServer.registry == nil {
		return nil
	}
	reg := a.httpServer.registry
	reg.mu.Lock()
	defer reg.mu.Unlock()
	return a.syncAndValidateRoutesLocked(a.hasOpenAPIConfig)
}

func (a *App) ensureHTTPCompiled() error {
	if a != nil && a.kernel != nil {
		a.kernel.freeze()
	}
	a.httpSetupMu.Lock()
	defer a.httpSetupMu.Unlock()
	if a.httpSetupDone {
		return nil
	}
	if err := a.setupHTTPRoutes(); err != nil {
		return err
	}
	a.httpSetupDone = true
	return nil
}

func (a *App) setupHTTPRoutes() error {
	// TODO: find a way to read REQUEST_TIMEOUT config only once and log it there. currently doing it twice one for populating
	// the value and other for logging
	requestTimeout := a.Config.Get("REQUEST_TIMEOUT")
	if requestTimeout != "" {
		timeoutVal, err := strconv.Atoi(requestTimeout)
		if err != nil || timeoutVal < 0 {
			a.Logger().Error("invalid value of config REQUEST_TIMEOUT.")
		}
	}

	if err := a.setupAndCompileHTTPRoutes(); err != nil {
		return err
	}

	for dirName, endpoint := range a.httpServer.staticFiles {
		a.httpServer.router.AddStaticFiles(a.Logger(), endpoint, dirName)
	}

	a.httpServer.router.NotFound(handler{
		function: catchAllHandler,
		rt:       a.kernel,
	})

	var registeredMethods []string

	_ = a.httpServer.router.Walk(func(method, route string) error {
		if !contains(registeredMethods, method) {
			registeredMethods = append(registeredMethods, method)
		}
		return nil
	})

	*a.httpServer.router.RegisteredRoutes = registeredMethods
	return nil
}

func (a *App) setupAndCompileHTTPRoutes() error {
	reg := a.httpServer.registry
	reg.mu.Lock()
	defer reg.mu.Unlock()

	if err := a.syncAndValidateRoutesLocked(a.hasOpenAPIConfig); err != nil {
		return err
	}

	// Register default routes - these are only added when HTTP server is actually starting
	a.addSystemRouteLocked(http.MethodGet, service.StartupPath, a.startupProbeHandler)
	a.addSystemRouteLocked(http.MethodGet, service.ReadyPath, a.readyProbeHandler)
	a.addSystemRouteLocked(http.MethodGet, service.HealthPath, a.deepHealthHandler)
	a.addSystemRouteLocked(http.MethodGet, service.AlivePath, a.aliveProbeHandler)
	a.addSystemRouteLocked(http.MethodGet, "/favicon.ico", faviconHandler)

	// Add OpenAPI/Swagger routes
	a.checkAndAddOpenAPIDocumentationLocked()

	// Compile the route registry: walks the GroupNode tree and registers
	// all routes and middleware onto the chi router.
	reg.compileLocked(a.httpServer.router.Mux(), a.kernel, a.getRequestTimeout())
	return nil
}

func (a *App) startSubscriptions(ctx context.Context) error {
	if len(a.subscriptionManager.subscriptions) == 0 {
		return nil
	}

	group := errgroup.Group{}
	// Start subscribers concurrently using go-routines
	for topic, handler := range a.subscriptionManager.subscriptions {
		subscriberTopic, subscriberHandler := topic, handler

		group.Go(func() error {
			return a.subscriptionManager.startSubscriber(ctx, subscriberTopic, subscriberHandler)
		})
	}

	return group.Wait()
}

// readConfig reads the configuration from the default location.
func (a *App) readConfig(isAppCMD bool) {
	var location string

	if _, err := os.Stat(configLocation); err == nil {
		location = configLocation
	}

	if isAppCMD {
		a.Config = config.NewEnvFile(location, logging.NewFileLogger(""))

		return
	}

	a.Config = config.NewEnvFile(location, logging.NewLogger(logging.INFO))
}

// AddHTTPService constructs and registers an Owned downstream HTTP service client before Start.
func (a *App) AddHTTPService(serviceName, serviceAddress string, options ...service.Options) {
	if err := a.checkMutable(); err != nil {
		if l := a.safeLogger(); l != nil {
			l.Errorf("failed to register HTTP service %q: %v", serviceName, err)
		}
		return
	}

	options = append([]service.Options{service.WithAttributes(map[string]string{"name": serviceName})}, options...)

	svc := service.NewHTTPServiceWithTracerProvider(
		serviceAddress,
		a.Logger(),
		a.Metrics(),
		a.TracerProvider(),
		options...,
	)
	if err := a.setHTTPServiceInternal(serviceName, svc, Owned, true, !a.pureBuild); err != nil {
		_ = service.CloseService(svc)
		a.Logger().Errorf("failed to register HTTP service %q: %v", serviceName, err)
	}
}

func (a *App) ensureRateLimitMetrics() {
	if a == nil || a.kernel == nil {
		return
	}
	m := a.kernel.getMetrics()
	if m == nil {
		return
	}
	if sm, ok := m.(metrics.StrictMock); ok && sm.IsStrictMock() {
		return
	}
	a.rateLimitMetricsOnce.Do(func() {
		m.NewCounter("app_http_rate_limit_exceeded_total", "Total number of HTTP requests rejected due to rate limiting.")
	})
}

// UseRateLimiter registers an HTTP rate-limiting middleware whose background bucket sweeper
// is automatically stopped during App.Stop.
func (a *App) UseRateLimiter(cfg middleware.RateLimiterConfig) {
	if a == nil {
		return
	}
	a.ensureRateLimitMetrics()
	var (
		mw     func(http.Handler) http.Handler
		closer func() error
	)
	if a.pureBuild {
		var start func(context.Context)
		mw, start, closer = middleware.RateLimiterDeferred(cfg, a.Metrics())
		a.OnStart(func(*Context) error {
			start(a.intakeContext())
			return nil
		})
	} else {
		mw, closer = middleware.RateLimiterWithCloser(cfg, a.Metrics())
	}
	a.registerMiddlewareCloser(closer)
	if a.httpServer != nil {
		a.Use(mw)
	}
}

func (a *App) registerMiddlewareCloser(fn func() error) {
	if a == nil || fn == nil {
		return
	}
	a.runtimeMu.Lock()
	if a.runtimeState == lifecycleStopping || a.runtimeState == lifecycleStopped || a.stopDone != nil {
		a.runtimeMu.Unlock()
		_ = fn()
		return
	}
	a.middlewareClosers = append(a.middlewareClosers, fn)
	a.runtimeMu.Unlock()
}

func (a *App) closeMiddlewareClosers() error {
	if a == nil {
		return nil
	}
	a.runtimeMu.Lock()
	closers := a.middlewareClosers
	a.middlewareClosers = nil
	a.runtimeMu.Unlock()

	var err error
	for i := len(closers) - 1; i >= 0; i-- {
		if closers[i] != nil {
			closerFn := closers[i]
			func() {
				defer func() {
					if r := recover(); r != nil {
						err = joinShutdownErr(err, fmt.Errorf("middleware closer panicked: %v", r))
					}
				}()
				err = joinShutdownErr(err, closerFn())
			}()
		}
	}
	return err
}

func (a *App) runtimeKernelRef() *runtimeKernel {
	if a == nil {
		return nil
	}
	return a.kernel
}

// SQL returns the primary SQL client, or untyped nil when SQL is not configured.
func (a *App) SQL() piSQL.Client {
	if a == nil || a.kernel == nil {
		return nil
	}
	return a.kernel.getSQL()
}

// Redis returns the primary Redis client, or untyped nil when Redis is not configured.
func (a *App) Redis() piRedis.Client {
	if a == nil || a.kernel == nil {
		return nil
	}
	return a.kernel.getRedis()
}

// PubSub returns the primary Pub/Sub client, or untyped nil when Pub/Sub is not configured.
func (a *App) PubSub() pubsub.Client {
	if a == nil || a.kernel == nil {
		return nil
	}
	return a.kernel.getPubSub()
}

// GetPublisher returns the primary Pub/Sub publisher, or untyped nil when unconfigured.
func (a *App) GetPublisher() pubsub.Publisher {
	if a == nil || a.kernel == nil {
		return nil
	}
	return a.kernel.getPublisher()
}

// GetSubscriber returns the primary Pub/Sub subscriber, or untyped nil when unconfigured.
func (a *App) GetSubscriber() pubsub.Subscriber {
	if a == nil || a.kernel == nil {
		return nil
	}
	return a.kernel.getSubscriber()
}

// File returns the configured FileSystem (defaulting to local filesystem).
func (a *App) File() file.FileSystem {
	if a == nil || a.kernel == nil {
		return nil
	}
	return a.kernel.getFile()
}

// Logger returns the logger instance associated with the App (never nil).
func (a *App) Logger() logging.Logger {
	if a == nil || a.kernel == nil {
		return nopFallbackLogger{}
	}
	return a.kernel.getLogger()
}

// Metrics returns the metrics manager associated with the App.
func (a *App) Metrics() metrics.Manager {
	if a == nil || a.kernel == nil {
		return nil
	}
	return a.kernel.getMetrics()
}

// TracerProvider returns the OpenTelemetry TracerProvider associated with the App.
func (a *App) TracerProvider() trace.TracerProvider {
	if a == nil || a.kernel == nil {
		return otel.GetTracerProvider()
	}
	return a.kernel.getTracerProvider()
}

func (a *App) tracerProvider() trace.TracerProvider {
	return a.TracerProvider()
}

// HTTPService returns the registered downstream HTTP service client with the given name, or nil.
func (a *App) HTTPService(name string) service.HTTP {
	if a == nil || a.kernel == nil {
		return nil
	}
	return a.kernel.getHTTPService(name)
}

// GetHTTPService returns the registered downstream HTTP service client with the given name, or nil.
func (a *App) GetHTTPService(name string) service.HTTP {
	return a.HTTPService(name)
}

// WebSocketHub returns the WebSocket connection manager associated with the App.
func (a *App) WebSocketHub() *websocket.Manager {
	if a == nil || a.kernel == nil {
		return nil
	}
	return a.kernel.wsManager
}

// Health executes bounded singleflight deep health checks across core datasources, HTTP services, and adapters.
func (a *App) Health(ctx context.Context) map[string]any {
	if a == nil || a.kernel == nil {
		return (*runtimeKernel)(nil).Health(ctx)
	}
	return a.kernel.Health(ctx)
}

// AppName returns the configured application name.
func (a *App) AppName() string {
	if a == nil || a.kernel == nil {
		return ""
	}
	return a.kernel.getAppName()
}

// AppVersion returns the configured application version.
func (a *App) AppVersion() string {
	if a == nil || a.kernel == nil {
		return ""
	}
	return a.kernel.getAppVersion()
}

// MigrationDeps implements migration.Target for running migrations against App datasources and external adapters.
func (a *App) MigrationDeps() migration.Deps {
	if a == nil || a.kernel == nil {
		return migration.Deps{}
	}
	deps := migration.Deps{
		Logger: a.Logger(),
	}
	if sqlClient := a.SQL(); !isNilValue(sqlClient) {
		deps.SQL = sqlClient
	}
	if redisClient := a.Redis(); !isNilValue(redisClient) {
		deps.Redis = redisClient
	}
	if pubsubClient := a.PubSub(); !isNilValue(pubsubClient) {
		deps.PubSub = pubsubClient
	}
	if a.kernel.adapters != nil {
		deps.Adapters = migrationAdapterLookup{reg: a.kernel.adapters}
	}
	return deps
}

// SubCommand adds a sub-command to the CLI application.
// Can be used to create commands like "kubectl get" or "kubectl get ingress".
func (a *App) SubCommand(pattern string, handler Handler, options ...Options) {
	if a == nil {
		return
	}
	if a.cmd == nil {
		a.cmd = &cmd{out: terminal.New()}
	}
	a.cmd.addRoute(pattern, handler, options...)
}

// Migrate applies a set of migrations to the application's database using context.Background.
//
// The migrationsMap argument is a map where the key is the version number of the migration
// and the value is a migration.Migrate instance that implements the migration logic.
//
// The returned Result and error should be checked by production callers. Existing code may
// still call Migrate as a statement and ignore the return values, but new code should prefer
// MigrateContext so deployment cancellation and timeouts can propagate into migration work.
func (a *App) Migrate(migrationsMap map[int64]migration.Migrate, opts ...migration.Option) (migration.Result, error) {
	return a.MigrateContext(context.Background(), migrationsMap, opts...)
}

// MigrateContext applies migrations with caller-controlled cancellation and returns a structured result.
func (a *App) MigrateContext(
	ctx context.Context,
	migrationsMap map[int64]migration.Migrate,
	opts ...migration.Option,
) (result migration.Result, err error) {
	defer func() {
		if re := recover(); re != nil {
			panicRecovery(re, a.safeLogger())
			err = fmt.Errorf("migration panic: %v", re)
		}
	}()

	return migration.Run(ctx, migrationsMap, a, opts...)
}

// MustMigrate applies migrations and panics if they fail. It is intended for small programs
// that deliberately want fail-fast startup semantics. Deployment commands should normally
// call MigrateContext and handle the returned error explicitly.
func (a *App) MustMigrate(migrationsMap map[int64]migration.Migrate, opts ...migration.Option) {
	if _, err := a.Migrate(migrationsMap, opts...); err != nil {
		panic(err)
	}
}

// MigrationPlan builds a plan without running user migrations. It may initialize the state store.
func (a *App) MigrationPlan(migrationsMap map[int64]migration.Migrate, opts ...migration.Option) (migration.PlanResult, error) {
	return a.MigrationPlanContext(context.Background(), migrationsMap, opts...)
}

// MigrationPlanContext builds a plan using caller context. It may initialize the state store.
func (a *App) MigrationPlanContext(
	ctx context.Context,
	migrationsMap map[int64]migration.Migrate,
	opts ...migration.Option,
) (migration.PlanResult, error) {
	return migration.Plan(ctx, migrationsMap, a, opts...)
}

// MigrationStatus returns migration status. It may initialize the state store.
func (a *App) MigrationStatus(migrationsMap map[int64]migration.Migrate, opts ...migration.Option) (migration.StatusResult, error) {
	return a.MigrationStatusContext(context.Background(), migrationsMap, opts...)
}

// MigrationStatusContext returns status using caller context. It may initialize the state store.
func (a *App) MigrationStatusContext(
	ctx context.Context,
	migrationsMap map[int64]migration.Migrate,
	opts ...migration.Option,
) (migration.StatusResult, error) {
	return migration.Status(ctx, migrationsMap, a, opts...)
}

// Subscribe registers a handler for the given topic.
//
// If the subscriber is not initialized, an error is logged and
// the subscription is not registered.
func (a *App) Subscribe(topic string, handler SubscribeFunc) {
	if topic == "" || handler == nil {
		if l := a.safeLogger(); l != nil {
			l.Errorf("invalid subscription: topic and handler must not be empty or nil")
		}

		return
	}

	if a == nil || a.GetSubscriber() == nil {
		if l := a.safeLogger(); l != nil {
			l.Errorf("subscriber not initialized in the application")
		}

		return
	}

	a.runtimeMu.Lock()
	defer a.runtimeMu.Unlock()
	if a.runtimeState != lifecycleNew {
		if l := a.safeLogger(); l != nil {
			l.Errorf("%v", errSubscriberRegisterTooLate)
		}
		return
	}
	if mh, ok := a.PubSub().(*managedPubSubHandle); ok {
		if err := mh.validateSubscriberConfig(); err != nil {
			a.subConfigErr = errors.Join(a.subConfigErr, err)
			if l := a.safeLogger(); l != nil {
				l.Errorf("%v", err)
			}
			return
		}
	}
	if a.subscriptionManager.subscriptions == nil {
		a.subscriptionManager.subscriptions = make(map[string]SubscribeFunc)
	}
	a.subscriptionManager.subscriptions[topic] = handler
}

// Use registers HTTP middleware (func(http.Handler) http.Handler) at the global level.
// These run at the HTTP layer before the pi Context is created.
func (a *App) Use(middlewares ...func(http.Handler) http.Handler) {
	if a == nil || a.httpServer == nil || a.httpServer.registry == nil {
		return
	}
	reg := a.httpServer.registry
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if !a.canMutateRoutesLocked("register HTTP middlewares") {
		return
	}

	reg.root.httpMWs = append(reg.root.httpMWs, middlewares...)
	reg.markDirtyLocked()
}

// UseMiddleware registers PiMiddleware that runs at the application layer with *Context access.
// This is a BREAKING CHANGE: the signature changed from func(http.Handler) http.Handler
// to func(next Handler) Handler (PiMiddleware).
func (a *App) UseMiddleware(middlewares ...PiMiddleware) {
	if a == nil || a.httpServer == nil || a.httpServer.registry == nil {
		return
	}
	reg := a.httpServer.registry
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if !a.canMutateRoutesLocked("register Pi middlewares") {
		return
	}

	reg.root.piMWs = append(reg.root.piMWs, middlewares...)
	reg.markDirtyLocked()
}

// Group creates or gets a route group with the given prefix and returns it.
// An optional callback can be provided for backward-compatible inline registration.
func (a *App) Group(prefix string, fns ...func(sub *RouteGroup)) *RouteGroup {
	if a == nil || a.httpServer == nil || a.httpServer.registry == nil {
		return a.rootRouteGroup()
	}

	validCallbacks := make([]func(sub *RouteGroup), 0, len(fns))
	for _, fn := range fns {
		if fn == nil {
			if l := a.safeLogger(); l != nil {
				l.Error("route group callback cannot be nil")
			}
			continue
		}

		validCallbacks = append(validCallbacks, fn)
	}

	// Preserve old behavior for explicit nil callback: log and don't create groups.
	if len(fns) > 0 && len(validCallbacks) == 0 {
		return a.rootRouteGroup()
	}

	normalizedPrefix := normalizeGroupPrefix(prefix)

	reg := a.httpServer.registry
	reg.mu.Lock()
	if !a.canMutateRoutesLocked("create route groups") {
		reg.mu.Unlock()
		return a.rootRouteGroup()
	}

	a.ensureHTTPAvailable()

	var targetGroup *RouteGroup
	if normalizedPrefix == "" {
		targetGroup = &RouteGroup{node: reg.root, app: a}
	} else {
		child := reg.root.getOrCreateChild(normalizedPrefix)
		reg.markDirtyLocked()
		targetGroup = &RouteGroup{node: child, app: a}
	}
	reg.mu.Unlock()

	for _, fn := range validCallbacks {
		fn(targetGroup)
	}

	return targetGroup
}

// Scope creates an isolated child RouteGroup at root scope with prefix "".
// Unlike Group(""), Scope always allocates an isolated child node so middleware
// and WithDoc attributes attached to it never leak back to the root App or sibling scopes.
func (a *App) Scope(fns ...func(sub *RouteGroup)) *RouteGroup {
	if a == nil || a.httpServer == nil || a.httpServer.registry == nil {
		return &RouteGroup{node: &GroupNode{isScope: true}}
	}
	return a.rootRouteGroup().Scope(fns...)
}

func (a *App) rootRouteGroup() *RouteGroup {
	if a == nil || a.httpServer == nil || a.httpServer.registry == nil {
		return &RouteGroup{}
	}

	return &RouteGroup{node: a.httpServer.registry.root, app: a}
}

// AddCronJob registers a cron job to the cron table without starting background goroutines;
// scheduling starts when the application starts, using CronSkipIfRunning overlap protection.
// The cron expression can be either a 5-part or 6-part format. The 6-part format includes an
// optional second field (in beginning) and others being minute, hour, day, month and day of week respectively.
func (a *App) AddCronJob(schedule, jobName string, job CronFunc) {
	if err := a.Cron().AddJob(schedule, jobName, job); err != nil {
		a.Logger().Errorf("error adding cron job, err: %v", err)
	}
}

// contains is a helper function checking for duplicate entry in a slice.
func contains(elems []string, v string) bool {
	for _, s := range elems {
		if v == s {
			return true
		}
	}

	return false
}

// AddStaticFiles registers a static file endpoint for the application.
//
// The provided `endpoint` will be used as the prefix for the static file
// server. The `filePath` specifies the directory containing the static files.
// If `filePath` starts with "./", it will be interpreted as a relative path
// to the current working directory.
//
// Deprecated: Prefer App.StaticFS with embed.FS or os.DirFS.
func (a *App) AddStaticFiles(endpoint, filePath string) {
	a.httpRegistered = true

	if !strings.HasPrefix(filePath, "./") && !filepath.IsAbs(filePath) {
		filePath = "./" + filePath
	}

	// update file path based on current directory if it starts with ./
	if strings.HasPrefix(filePath, "./") {
		currentWorkingDir, _ := os.Getwd()
		filePath = filepath.Join(currentWorkingDir, filePath)
	}

	endpoint = "/" + strings.TrimPrefix(endpoint, "/")

	if _, err := os.Stat(filePath); err != nil {
		a.Logger().Errorf("error in registering '%s' static endpoint, error: %v", endpoint, err)
		return
	}

	a.httpServer.staticFiles[filePath] = endpoint
}

// OnStart registers a startup hook that will be executed when the application starts.
// The hook function receives a Context that provides access to the application's
// configured datasources, adapters, logger, and telemetry after owned resources have started.
func (a *App) OnStart(hook func(ctx *Context) error) {
	a.runtimeMu.Lock()
	defer a.runtimeMu.Unlock()

	if a.runtimeState != lifecycleNew {
		a.Logger().Error("cannot register OnStart hook after startup or shutdown has begun")
		return
	}
	if hook == nil {
		a.Logger().Error("OnStart hook cannot be nil")
		return
	}

	a.onStartHooks = append(a.onStartHooks, hook)
}

// OnStop registers a shutdown hook that will be executed during graceful shutdown.
// Hooks are executed in reverse registration order so that cleanup naturally mirrors setup.
// Unlike OnStart, all OnStop hooks are attempted and their errors are joined together.
func (a *App) OnStop(hook func(ctx *Context) error) {
	a.runtimeMu.Lock()
	defer a.runtimeMu.Unlock()

	if a.stopDone != nil && startupFinished(a.startupDone) {
		a.Logger().Error("cannot register OnStop hook after cleanup has begun")
		return
	}
	if hook == nil {
		a.Logger().Error("OnStop hook cannot be nil")
		return
	}

	a.onStopHooks = append(a.onStopHooks, hook)
}
