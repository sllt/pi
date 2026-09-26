package pi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Run starts the application. If it is an HTTP server, it will start the server.
func (a *App) Run() {
	// Create a context that is canceled on receiving termination signals.
	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := a.RunContext(signalCtx); err != nil {
		a.Logger().Errorf("Application stopped with error: %v", err)
	}
}

// RunContext starts the application and blocks until ctx is canceled or a managed runtime component requests shutdown.
func (a *App) RunContext(ctx context.Context) error {
	if a.cmd != nil {
		a.cmd.Run(a.container)
		return nil
	}

	if ctx == nil {
		ctx = context.Background()
	}

	if err := a.Start(ctx); err != nil {
		return err
	}

	runtimeCtx := a.runtimeContext()
	if runtimeCtx == nil {
		return nil
	}

	select {
	case <-runtimeCtx.Done():
	case <-ctx.Done():
		// Start may have reused a runtime owned by an earlier caller. Once
		// startup succeeds, RunContext also owns shutdown on its own cancellation.
		a.requestShutdown(context.Cause(ctx))
	}

	timeout, err := a.shutdownTimeout()
	if err != nil {
		a.Logger().Errorf("error parsing value of shutdown timeout from config: %v. Setting default timeout of 30 sec.", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()

	if a.hasTelemetry() {
		a.sendTelemetry(http.DefaultClient, false)
	}

	a.Logger().Infof("Shutting down server with a timeout of %v", timeout)

	stopErr := a.Stop(shutdownCtx)
	cause := context.Cause(runtimeCtx)
	if cause == nil || errors.Is(cause, context.Canceled) || (!a.pureBuild && errors.Is(cause, context.DeadlineExceeded)) {
		return stopErr
	}

	return errors.Join(cause, stopErr)
}

// Start starts the application without installing signal handlers.
// It returns after startup hooks and registered servers/workers have been started.
// Concurrent calls share one startup result. The first caller's context remains
// the runtime parent; later callers' contexts only bound their own startup wait.
// A stopped application cannot be restarted.
func (a *App) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	a.runtimeMu.Lock()
	switch a.runtimeState {
	case lifecycleStarting:
		done := a.startDone
		a.runtimeMu.Unlock()
		return a.waitForStart(ctx, done)
	case lifecycleRunning:
		a.runtimeMu.Unlock()
		return nil
	case lifecycleStopping, lifecycleStopped:
		a.runtimeMu.Unlock()
		return errAppAlreadyStopped
	}
	a.runtimeState = lifecycleStarting
	a.startupDone = make(chan struct{})
	a.startDone = make(chan struct{})
	runtimeParent := ctx
	if a.pureBuild {
		runtimeParent = context.WithoutCancel(ctx)
	}
	a.runtimeCtx, a.runtimeCancel = context.WithCancelCause(runtimeParent)
	runtimeCtx := a.runtimeCtx
	a.runtimeMu.Unlock()

	var err error
	if a.pureBuild {
		startupCtx, cancel := context.WithCancel(ctx)
		detach := context.AfterFunc(runtimeCtx, cancel)
		err = a.startPure(startupCtx, runtimeCtx)
		detach()
		cancel()
	} else {
		err = a.startOnce(runtimeCtx)
	}
	if err != nil {
		a.requestShutdown(err)
	}

	a.runtimeMu.Lock()
	if err == nil {
		err = context.Cause(runtimeCtx)
	}
	if err == nil && a.runtimeState == lifecycleStopping {
		err = context.Canceled
	}
	close(a.startupDone)
	if err == nil {
		a.runtimeState = lifecycleRunning
		close(a.startDone)
		a.runtimeMu.Unlock()
		if a.pureBuild {
			go a.stopWhenCanceled(runtimeCtx)
		}
		return nil
	}
	a.runtimeMu.Unlock()

	err = errors.Join(err, a.rollbackStart(ctx))
	a.runtimeMu.Lock()
	a.startErr = err
	close(a.startDone)
	a.runtimeMu.Unlock()
	return err
}

func (a *App) startPure(startupCtx, runtimeCtx context.Context) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("%w: %v", errRuntimeStartPanic, v)
		}
	}()
	if err = a.startResources(startupCtx); err != nil {
		return err
	}
	if err = a.handleStartupHooks(startupCtx); err != nil {
		return err
	}
	if err = startupCtx.Err(); err != nil {
		return err
	}
	if err = a.startAllServers(runtimeCtx); err != nil {
		return err
	}
	return startupCtx.Err()
}

func (a *App) stopWhenCanceled(ctx context.Context) {
	<-ctx.Done()
	timeout, _ := a.shutdownTimeout()
	stopCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_ = a.Stop(stopCtx)
}

// Wait waits for complete cleanup, including owned resources, and returns the
// runtime cause plus cleanup errors. Its context only bounds this caller's wait.
// Legacy New callers should continue using RunContext; Wait requires Build.
func (a *App) Wait(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	a.runtimeMu.Lock()
	state := a.runtimeState
	done := a.waitDone
	a.runtimeMu.Unlock()
	if done == nil {
		return errors.New("Wait requires an app created with Build")
	}
	if state == lifecycleNew {
		return errors.New("application has not started")
	}
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	a.runtimeMu.Lock()
	defer a.runtimeMu.Unlock()
	var cause error
	if a.runtimeCtx != nil {
		cause = context.Cause(a.runtimeCtx)
	}
	if errors.Is(cause, context.Canceled) {
		cause = nil
	}
	return errors.Join(cause, a.stopErr)
}

func (a *App) startOnce(ctx context.Context) (err error) {
	defer func() {
		if re := recover(); re != nil {
			err = fmt.Errorf("%w: %v", errRuntimeStartPanic, re)
		}
	}()
	if a.initErr != nil {
		return a.initErr
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if a.cmd != nil {
		a.cmd.Run(a.container)
		return nil
	}
	if err = a.handleStartupHooks(ctx); err != nil {
		return err
	}
	a.startTelemetryIfEnabled()
	return a.startAllServers(ctx)
}

func (a *App) runtimeContext() context.Context {
	a.runtimeMu.Lock()
	defer a.runtimeMu.Unlock()

	return a.runtimeCtx
}

func (a *App) rollbackStart(parent context.Context) error {
	timeout, err := a.shutdownTimeout()
	if err != nil {
		a.Logger().Errorf("error parsing value of shutdown timeout from config: %v. Setting default timeout of 30 sec.", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(parent), timeout)
	defer cancel()

	return a.Stop(shutdownCtx)
}

func (a *App) shutdownTimeout() (time.Duration, error) {
	return getShutdownTimeoutFromConfig(a.Config)
}

// handleStartupHooks runs the startup hooks and returns an error if the application should exit.
func (a *App) handleStartupHooks(ctx context.Context) error {
	if err := a.runOnStartHooks(ctx); err != nil {
		if !errors.Is(err, context.Canceled) {
			a.Logger().Errorf("Startup failed: %v", err)

			return err
		}
		// If the error is context.Canceled, do not exit; allow graceful shutdown.
		a.Logger().Info("Startup canceled by context, shutting down gracefully.")

		return err
	}

	return nil
}

// startShutdownHandler starts a goroutine to handle graceful shutdown.
func (a *App) startShutdownHandler(ctx context.Context, timeout time.Duration) <-chan struct{} {
	done := make(chan struct{})

	// Goroutine to handle shutdown when context is canceled
	go func() {
		defer close(done)

		<-ctx.Done()

		// Create a shutdown context with a timeout
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		defer cancel()

		if a.hasTelemetry() {
			a.sendTelemetry(http.DefaultClient, false)
		}

		a.Logger().Infof("Shutting down server with a timeout of %v", timeout)

		shutdownErr := a.Shutdown(shutdownCtx)
		if shutdownErr != nil {
			a.Logger().Debugf("Server shutdown failed: %v", shutdownErr)
		}
	}()

	return done
}

// startTelemetryIfEnabled starts telemetry if it's enabled.
func (a *App) startTelemetryIfEnabled() {
	if a.hasTelemetry() {
		go a.sendTelemetry(http.DefaultClient, true)
	}
}

// startAllServers starts all registered servers concurrently.
func (a *App) startAllServers(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var err error

	err = errors.Join(err, a.startMetricsServer())
	err = errors.Join(err, a.startHTTPServer())
	err = errors.Join(err, a.startGRPCServer())

	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}

	a.startSubscriptionManager(ctx, nil)
	a.startBackgroundWorkers(ctx, nil)

	return nil
}

// startMetricsServer starts the metrics server if configured.
func (a *App) startMetricsServer() error {
	// Start Metrics Server
	// running metrics server before HTTP and gRPC
	if a.metricServer != nil {
		return a.metricServer.start(a.container, func(err error) {
			a.Logger().Errorf("Metrics server failed: %v", err)
			a.requestShutdown(err)
		})
	}

	return nil
}

// startHTTPServer starts the HTTP server if registered.
func (a *App) startHTTPServer() error {
	if a.httpDisabled {
		return nil
	}
	if a.httpRegistered {
		a.httpServerSetup()

		return a.httpServer.start(a.container, func(err error) {
			a.Logger().Errorf("HTTP server failed: %v", err)
			a.requestShutdown(err)
		})
	}

	return nil
}

// startGRPCServer starts the gRPC server if registered.
func (a *App) startGRPCServer() error {
	if a.grpcDisabled {
		return nil
	}
	if a.grpcRegistered {
		return a.grpcServer.start(a.container, func(err error) {
			a.Logger().Errorf("gRPC server failed: %v", err)
			a.requestShutdown(err)
		})
	}

	return nil
}

// startSubscriptionManager starts the subscription manager.
func (a *App) startSubscriptionManager(ctx context.Context, wg waitGroup) {
	if wg != nil {
		wg.Add(1)
	}
	a.runtimeTasks.Add(1)

	go func() {
		if wg != nil {
			defer wg.Done()
		}
		defer a.runtimeTasks.Done()

		err := a.startSubscriptions(ctx)
		if err != nil {
			a.Logger().Errorf("Subscription Error : %v", err)
		}
	}()
}
