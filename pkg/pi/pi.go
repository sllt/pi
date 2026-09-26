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

	"golang.org/x/sync/errgroup"

	"github.com/sllt/pi/pkg/pi/config"
	"github.com/sllt/pi/pkg/pi/infra"
	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/sllt/pi/pkg/pi/metrics"
	"github.com/sllt/pi/pkg/pi/migration"
	"github.com/sllt/pi/pkg/pi/service"
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

	// container is unexported because this is an internal implementation and applications are provided access to it via Context
	container *infra.Container

	grpcRegistered bool
	httpRegistered bool

	subscriptionManager SubscriptionManager
	onStartHooks        []func(ctx *Context) error
	onStopHooks         []func(ctx *Context) error
	backgroundWorkers   []backgroundWorker

	runtimeCtx    context.Context
	runtimeCancel context.CancelCauseFunc
	runtimeTasks  sync.WaitGroup
	runtimeMu     sync.Mutex
	runtimeState  lifecycleState
	startupDone   chan struct{}
	startDone     chan struct{}
	startErr      error
	stopDone      chan struct{}
	stopErr       error
	pureBuild     bool
	resources     []managedResource
	waitDone      chan struct{}
	httpSetup     sync.Once
}

func (a *App) runOnStartHooks(ctx context.Context) error {
	piCtx := newContext(nil, noopRequest{ctx: ctx}, a.container)

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

	piCtx := newContext(nil, noopRequest{ctx: ctx}, a.container)
	var err error

	for i := len(hooks) - 1; i >= 0; i-- {
		if ctx.Err() != nil {
			return errors.Join(err, ctx.Err())
		}

		hookErr := a.runLifecycleHook("OnStop", i, hooks[i], piCtx, errStopHookPanic)
		if hookErr != nil {
			a.Logger().Errorf("OnStop hook failed: %v", hookErr)
			err = errors.Join(err, hookErr)
		}
	}

	return errors.Join(err, ctx.Err())
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
		a.stopDone = make(chan struct{})
		startupDone := a.startupDone
		cancel := a.runtimeCancel
		a.runtimeMu.Unlock()

		if cancel != nil {
			cancel(nil)
		}
		go a.finishStop(ctx, startupDone)
	} else {
		a.runtimeMu.Unlock()
	}

	return a.waitForStop(ctx)
}

// Shutdown stops the service(s) and closes the application.
// It is kept for backward compatibility; new embedded integrations should prefer Stop.
func (a *App) Shutdown(ctx context.Context) error {
	return a.Stop(ctx)
}

// shutdown stops HTTP, gRPC, Metrics servers and closes the container's active connections to datasources.
func (a *App) shutdown(ctx context.Context) error {
	a.requestShutdown(nil)

	var err error
	if a.httpServer != nil {
		err = errors.Join(err, a.httpServer.Shutdown(ctx))
	}

	if a.grpcServer != nil {
		err = errors.Join(err, a.grpcServer.Shutdown(ctx))
	}

	if a.cron != nil {
		a.cron.Stop()
	}

	if waitErr := a.waitForRuntimeTasks(ctx); waitErr != nil {
		err = errors.Join(err, waitErr)
		// Do not close dependencies while workers still use them. Stop waiters
		// can time out independently while this sole cleanup owner keeps waiting.
		a.runtimeTasks.Wait()
	}
	err = errors.Join(err, a.runOnStopHooks(ctx))

	if a.pureBuild {
		err = errors.Join(err, a.stopResources(ctx))
		for _, conn := range a.container.WSManager.ListConnections() {
			a.container.WSManager.CloseConnection(conn)
		}
	} else if a.container != nil {
		err = errors.Join(err, a.container.Close())
	}

	if a.metricServer != nil {
		err = errors.Join(err, a.metricServer.Shutdown(ctx))
	}

	if err != nil {
		return err
	}

	if a.container != nil && a.container.Logger != nil {
		a.container.Logger.Info("Application shutdown complete")
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

func (a *App) httpServerSetup() {
	a.httpSetup.Do(a.setupHTTPRoutes)
}

func (a *App) setupHTTPRoutes() {
	// TODO: find a way to read REQUEST_TIMEOUT config only once and log it there. currently doing it twice one for populating
	// the value and other for logging
	requestTimeout := a.Config.Get("REQUEST_TIMEOUT")
	if requestTimeout != "" {
		timeoutVal, err := strconv.Atoi(requestTimeout)
		if err != nil || timeoutVal < 0 {
			a.container.Error("invalid value of config REQUEST_TIMEOUT.")
		}
	}

	// Register default routes - these are only added when HTTP server is actually starting
	a.add(http.MethodGet, service.HealthPath, healthHandler)
	a.add(http.MethodGet, service.AlivePath, liveHandler)
	a.add(http.MethodGet, "/favicon.ico", faviconHandler)

	// Add OpenAPI/Swagger routes if openapi.json exists
	a.checkAndAddOpenAPIDocumentation()

	// Compile the route registry: walks the GroupNode tree and registers
	// all routes and middleware onto the chi router.
	a.httpServer.registry.compile(a.httpServer.router.Mux(), a.container, a.getRequestTimeout())

	for dirName, endpoint := range a.httpServer.staticFiles {
		a.httpServer.router.AddStaticFiles(a.Logger(), endpoint, dirName)
	}

	a.httpServer.router.NotFound(handler{
		function:  catchAllHandler,
		container: a.container,
	})

	var registeredMethods []string

	_ = a.httpServer.router.Walk(func(method, route string) error {
		if !contains(registeredMethods, method) {
			registeredMethods = append(registeredMethods, method)
		}
		return nil
	})

	*a.httpServer.router.RegisteredRoutes = registeredMethods
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

// AddHTTPService registers HTTP service in infra.
func (a *App) AddHTTPService(serviceName, serviceAddress string, options ...service.Options) {
	if a.container.Services == nil {
		a.container.Services = make(map[string]service.HTTP)
	}

	if _, ok := a.container.Services[serviceName]; ok {
		a.container.Debugf("Service already registered Name: %v", serviceName)
	}

	options = append([]service.Options{service.WithAttributes(map[string]string{"name": serviceName})}, options...)

	a.container.Services[serviceName] = service.NewHTTPService(serviceAddress, a.container.Logger, a.container.Metrics(), options...)
}

// Metrics returns the metrics manager associated with the App.
func (a *App) Metrics() metrics.Manager {
	return a.container.Metrics()
}

// Container returns the container instance associated with the App.
// This allows access to all registered datasources (SQL, Redis, Mongo, etc.)
// for dependency injection in layered architectures.
func (a *App) Container() *infra.Container {
	return a.container
}

// Logger returns the logger instance associated with the App.
func (a *App) Logger() logging.Logger {
	return a.container.Logger
}

// SubCommand adds a sub-command to the CLI application.
// Can be used to create commands like "kubectl get" or "kubectl get ingress".
func (a *App) SubCommand(pattern string, handler Handler, options ...Options) {
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
			panicRecovery(re, a.container.Logger)
			err = fmt.Errorf("migration panic: %v", re)
		}
	}()

	return migration.Run(ctx, migrationsMap, a.container, opts...)
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
	return migration.Plan(ctx, migrationsMap, a.container, opts...)
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
	return migration.Status(ctx, migrationsMap, a.container, opts...)
}

// Subscribe registers a handler for the given topic.
//
// If the subscriber is not initialized in the container, an error is logged and
// the subscription is not registered.
func (a *App) Subscribe(topic string, handler SubscribeFunc) {
	if topic == "" || handler == nil {
		a.container.Logger.Errorf("invalid subscription: topic and handler must not be empty or nil")

		return
	}

	if a.container.GetSubscriber() == nil {
		a.container.Logger.Errorf("subscriber not initialized in the container")

		return
	}

	a.subscriptionManager.subscriptions[topic] = handler
}

// Use registers HTTP middleware (func(http.Handler) http.Handler) at the global level.
// These run at the HTTP layer before the pi Context is created.
func (a *App) Use(middlewares ...func(http.Handler) http.Handler) {
	if !a.canMutateRoutes("register HTTP middlewares") {
		return
	}

	a.httpServer.registry.root.httpMWs = append(a.httpServer.registry.root.httpMWs, middlewares...)
}

// UseMiddleware registers PiMiddleware that runs at the application layer with *Context access.
// This is a BREAKING CHANGE: the signature changed from func(http.Handler) http.Handler
// to func(next Handler) Handler (PiMiddleware).
func (a *App) UseMiddleware(middlewares ...PiMiddleware) {
	if !a.canMutateRoutes("register Pi middlewares") {
		return
	}

	a.httpServer.registry.root.piMWs = append(a.httpServer.registry.root.piMWs, middlewares...)
}

// UseMiddlewareWithContainer adds a middleware that has access to the container
// and wraps the provided handler with the middleware logic.
//
// The `middleware` function receives the container and the handler, allowing
// the middleware to modify the request processing flow.
// Deprecated: UseMiddlewareWithContainer will be removed in a future release.
// Please use the [*App.UseMiddleware] method that does not depend on the infra.
func (a *App) UseMiddlewareWithContainer(middlewareHandler func(c *infra.Container, handler http.Handler) http.Handler) {
	a.Use(func(h http.Handler) http.Handler {
		return middlewareHandler(a.container, h)
	})
}

// Group creates or gets a route group with the given prefix and returns it.
// An optional callback can be provided for backward-compatible inline registration.
func (a *App) Group(prefix string, fns ...func(sub *RouteGroup)) *RouteGroup {
	if !a.canMutateRoutes("create route groups") {
		return a.rootRouteGroup()
	}

	a.ensureHTTPAvailable()

	validCallbacks := make([]func(sub *RouteGroup), 0, len(fns))
	for _, fn := range fns {
		if fn == nil {
			a.container.Logger.Error("route group callback cannot be nil")
			continue
		}

		validCallbacks = append(validCallbacks, fn)
	}

	// Preserve old behavior for explicit nil callback: log and don't create groups.
	if len(fns) > 0 && len(validCallbacks) == 0 {
		return a.rootRouteGroup()
	}

	normalizedPrefix := normalizeGroupPrefix(prefix)
	if normalizedPrefix == "" {
		root := a.rootRouteGroup()
		for _, fn := range validCallbacks {
			fn(root)
		}

		return root
	}

	child := a.httpServer.registry.root.getOrCreateChild(normalizedPrefix)
	sub := &RouteGroup{node: child, app: a}
	for _, fn := range validCallbacks {
		fn(sub)
	}

	return sub
}

func (a *App) rootRouteGroup() *RouteGroup {
	if a == nil || a.httpServer == nil || a.httpServer.registry == nil {
		return &RouteGroup{}
	}

	return &RouteGroup{node: a.httpServer.registry.root, app: a}
}

// AddCronJob registers a cron job to the cron table.
// The cron expression can be either a 5-part or 6-part format. The 6-part format includes an
// optional second field (in beginning) and others being minute, hour, day, month and day of week respectively.
func (a *App) AddCronJob(schedule, jobName string, job CronFunc) {
	if a.cron == nil {
		a.cron = NewCron(a.container)
	}

	if err := a.cron.AddJob(schedule, jobName, job); err != nil {
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
		a.container.Logger.Errorf("error in registering '%s' static endpoint, error: %v", endpoint, err)
		return
	}

	a.httpServer.staticFiles[filePath] = endpoint
}

// OnStart registers a startup hook that will be executed when the application starts.
// The hook function receives a Context that provides access to the application's
// container, logger, and configuration. This is useful for performing initialization
// tasks such as database connections, service registrations, or other setup operations
// that need to be completed before the application begins serving requests.
//
// Example usage:
//
//	app := pi.New()
//	app.OnStart(func(ctx *pi.Context) error {
//	    // Initialize database connection
//	    db, err := database.Connect(ctx.Config.Get("DB_URL"))
//	    if err != nil {
//	        return err
//	    }
//	    ctx.Container.SQL = db
//	    return nil
//	})
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
