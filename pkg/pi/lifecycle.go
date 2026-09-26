package pi

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	errBackgroundWorkerNameEmpty       = errors.New("background worker name cannot be empty")
	errBackgroundWorkerHandlerNil      = errors.New("background worker handler cannot be nil")
	errBackgroundWorkerDuplicateName   = errors.New("background worker name already registered")
	errBackgroundWorkerRegisterTooLate = errors.New("cannot register background worker after app has started")
	errBackgroundWorkerPanic           = errors.New("background worker panicked")
	errRuntimeStartPanic               = errors.New("application startup panicked")
	errRuntimeStopPanic                = errors.New("application shutdown panicked")
)

type lifecycleState uint8

const (
	lifecycleNew lifecycleState = iota
	lifecycleStarting
	lifecycleRunning
	lifecycleStopping
	lifecycleStopped
)

// BackgroundFunc is a lifecycle-managed background task.
// The passed Pi context is canceled when the application begins shutting down.
type BackgroundFunc func(ctx *Context) error

type backgroundWorker struct {
	name string
	fn   BackgroundFunc
}

type waitGroup interface {
	Add(delta int)
	Done()
}

// Go registers a long-running background worker managed by Pi's application lifecycle.
// Returning a non-nil error from the worker triggers application shutdown.
// Returning nil or context.Canceled is treated as a graceful exit.
func (a *App) Go(name string, fn BackgroundFunc) {
	a.runtimeMu.Lock()
	defer a.runtimeMu.Unlock()

	name = strings.TrimSpace(name)

	switch {
	case name == "":
		a.Logger().Error(errBackgroundWorkerNameEmpty)
		return
	case fn == nil:
		a.Logger().Error(errBackgroundWorkerHandlerNil)
		return
	case a.runtimeCtx != nil || a.runtimeState != lifecycleNew:
		a.Logger().Errorf("%v: %s", errBackgroundWorkerRegisterTooLate, name)
		return
	case a.hasBackgroundWorker(name):
		a.Logger().Errorf("%v: %s", errBackgroundWorkerDuplicateName, name)
		return
	}

	a.backgroundWorkers = append(a.backgroundWorkers, backgroundWorker{
		name: name,
		fn:   fn,
	})
}

func (a *App) hasBackgroundWorker(name string) bool {
	for _, worker := range a.backgroundWorkers {
		if worker.name == name {
			return true
		}
	}

	return false
}

func (a *App) initRuntime(parent context.Context) context.Context {
	a.runtimeMu.Lock()
	defer a.runtimeMu.Unlock()

	ctx, cancel := context.WithCancelCause(parent)
	a.runtimeCtx = ctx
	a.runtimeCancel = cancel

	return ctx
}

func (a *App) requestShutdown(cause error) {
	a.runtimeMu.Lock()
	cancel := a.runtimeCancel
	a.runtimeMu.Unlock()
	if cancel != nil {
		cancel(cause)
	}
}

func (a *App) waitForStart(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return a.startErr
	default:
	}
	select {
	case <-done:
		return a.startErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *App) waitForStop(ctx context.Context) error {
	a.runtimeMu.Lock()
	done := a.stopDone
	a.runtimeMu.Unlock()
	select {
	case <-done:
		return a.stopErr
	default:
	}
	select {
	case <-done:
		return a.stopErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func startupFinished(done <-chan struct{}) bool {
	if done == nil {
		return true
	}
	select {
	case <-done:
		return true
	default:
		return false
	}
}

func (a *App) finishStop(ctx context.Context, startupDone <-chan struct{}) {
	var err error
	defer func() {
		if re := recover(); re != nil {
			err = errors.Join(err, fmt.Errorf("%w: %v", errRuntimeStopPanic, re))
		}
		a.runtimeMu.Lock()
		a.stopErr = errors.Join(err, ctx.Err())
		a.runtimeState = lifecycleStopped
		close(a.stopDone)
		if a.waitDone != nil {
			close(a.waitDone)
		}
		a.runtimeMu.Unlock()
	}()
	if startupDone != nil {
		// Startup must finish adding resources before the sole cleanup owner
		// can roll them back, even if an individual Stop caller has timed out.
		<-startupDone
	}
	err = a.shutdown(ctx)
}

func (a *App) startBackgroundWorkers(ctx context.Context, wg waitGroup) {
	for _, worker := range a.backgroundWorkers {
		worker := worker
		if wg != nil {
			wg.Add(1)
		}
		a.runtimeTasks.Add(1)

		go func() {
			if wg != nil {
				defer wg.Done()
			}
			defer a.runtimeTasks.Done()

			a.runBackgroundWorker(ctx, worker)
		}()
	}
}

func (a *App) runBackgroundWorker(ctx context.Context, worker backgroundWorker) {
	c := newContext(nil, noopRequest{ctx: ctx}, a.container)
	start := time.Now()

	a.Logger().Infof("Starting background worker: %s", worker.name)

	var err error

	defer func() {
		if r := recover(); r != nil {
			panicRecovery(r, a.Logger())
			err = fmt.Errorf("worker %s: %w: %v", worker.name, errBackgroundWorkerPanic, r)
		}

		switch {
		case err == nil, errors.Is(err, context.Canceled):
			a.Logger().Infof("Background worker stopped gracefully: %s in %s", worker.name, time.Since(start))
		default:
			a.Logger().Errorf("Background worker failed: %s in %s, err: %v", worker.name, time.Since(start), err)
			a.requestShutdown(err)
		}
	}()

	err = worker.fn(c)
}

func (a *App) waitForRuntimeTasks(ctx context.Context) error {
	done := make(chan struct{})

	go func() {
		a.runtimeTasks.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *App) waitForShutdown(done <-chan struct{}, timeout time.Duration) {
	if done == nil {
		return
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
	case <-timer.C:
		a.Logger().Warnf("Timed out waiting for shutdown to finish within %v", timeout)
	}
}
