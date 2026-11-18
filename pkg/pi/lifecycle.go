package pi

import (
	"context"
	"errors"
	"fmt"
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

type waitGroup interface {
	Add(delta int)
	Done()
}

// Go registers a long-running background worker managed by Pi's application lifecycle.
// Returning a non-nil error from the worker triggers application shutdown without retry.
// Returning nil or context.Canceled is treated as a graceful exit.
func (a *App) Go(name string, fn BackgroundFunc) {
	if err := a.AddWorker(name, WorkerPolicy{
		Mode:      SuperviseFatal,
		Required:  true,
		WaitReady: false,
	}, fn); err != nil {
		if l := a.safeLogger(); l != nil {
			l.Error(err)
		}
	}
}

func (a *App) hasBackgroundWorker(name string) bool {
	for _, worker := range a.backgroundWorkers {
		if worker.name == name {
			return true
		}
	}
	return false
}

func (a *App) initRuntimeContextsLocked(runtimeParent, valuesSource context.Context) context.Context {
	if runtimeParent == nil {
		runtimeParent = context.Background()
	}
	if valuesSource == nil {
		valuesSource = runtimeParent
	}
	if a.intakeStopAfter != nil {
		a.intakeStopAfter()
		a.intakeStopAfter = nil
	}
	if a.kernel != nil {
		a.kernel.ResetStoppingChan()
	}

	runtimeCtx, runtimeCancel := context.WithCancelCause(runtimeParent)
	intakeCtx, intakeCancel := context.WithCancelCause(runtimeCtx)
	drainExecCtx, drainCancel := context.WithCancelCause(context.WithoutCancel(valuesSource))

	a.runtimeCtx = runtimeCtx
	a.runtimeCancel = runtimeCancel
	a.intakeCtx = intakeCtx
	a.intakeCancel = intakeCancel
	a.drainExecCtx = drainExecCtx
	a.drainCancel = drainCancel

	a.intakeStopAfter = context.AfterFunc(runtimeCtx, func() {
		a.closeIntakeGate()
		intakeCancel(context.Cause(runtimeCtx))
	})

	return runtimeCtx
}

func (a *App) initRuntime(parent context.Context) context.Context {
	a.runtimeMu.Lock()
	defer a.runtimeMu.Unlock()
	return a.initRuntimeContextsLocked(parent, parent)
}

func (a *App) tryAdmitRuntimeTask() bool {
	a.taskGateMu.Lock()
	defer a.taskGateMu.Unlock()
	if a.intakeClosed {
		return false
	}
	a.runtimeTasks.Add(1)
	return true
}

func (a *App) closeIntakeGate() {
	a.taskGateMu.Lock()
	a.intakeClosed = true
	a.taskGateMu.Unlock()
	if a.kernel != nil {
		a.kernel.MarkStopping()
	}
}

func (a *App) isIntakeClosed() bool {
	a.taskGateMu.Lock()
	closed := a.intakeClosed
	a.taskGateMu.Unlock()
	return closed
}

func (a *App) intakeContext() context.Context {
	a.runtimeMu.Lock()
	ctx := a.intakeCtx
	if ctx == nil {
		ctx = a.runtimeCtx
	}
	a.runtimeMu.Unlock()
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func (a *App) workerParentContext() context.Context {
	return a.intakeContext()
}

func (a *App) intakeDone() <-chan struct{} {
	a.runtimeMu.Lock()
	ctx := a.intakeCtx
	if ctx == nil {
		ctx = a.runtimeCtx
	}
	a.runtimeMu.Unlock()
	if ctx == nil {
		return nil
	}
	return ctx.Done()
}

func (a *App) drainExecutionContext() context.Context {
	a.runtimeMu.Lock()
	ctx := a.drainExecCtx
	a.runtimeMu.Unlock()
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func (a *App) registerPhaseBDrain(hook func(ctx context.Context) error) {
	if hook == nil {
		return
	}
	a.runtimeMu.Lock()
	a.drainHooks = append(a.drainHooks, hook)
	a.runtimeMu.Unlock()
}

func (a *App) hasPhaseBDrainHooks() bool {
	a.runtimeMu.Lock()
	n := len(a.drainHooks)
	a.runtimeMu.Unlock()
	return n > 0
}

func (a *App) runPhaseBDrain(ctx context.Context) error {
	a.runtimeMu.Lock()
	hooks := append([]func(context.Context) error(nil), a.drainHooks...)
	a.runtimeMu.Unlock()

	var err error
	for i := len(hooks) - 1; i >= 0; i-- {
		if hookErr := hooks[i](ctx); hookErr != nil {
			err = joinShutdownErr(err, hookErr)
		}
	}
	return err
}

func (a *App) beginStopIntake(cause error) {
	a.runtimeMu.Lock()
	stopAfter := a.intakeStopAfter
	a.intakeStopAfter = nil
	intakeCancel := a.intakeCancel
	runtimeCancel := a.runtimeCancel
	a.runtimeMu.Unlock()

	if stopAfter != nil {
		stopAfter()
	}
	if intakeCancel != nil {
		intakeCancel(cause)
	}
	if runtimeCancel != nil {
		runtimeCancel(cause)
	}

	a.closeIntakeGate()
	if a.kernel != nil {
		a.kernel.CloseHealthAdmission()
	}
	a.syncGRPCHealth()
	a.cancelCompatWSServices()
	a.cancelCronIntake()
	a.cancelSubscribersIntake()
}

func (a *App) cancelDrainExec(cause error) {
	a.runtimeMu.Lock()
	cancel := a.drainCancel
	a.runtimeMu.Unlock()
	if cancel != nil {
		cancel(cause)
	}
}

func (a *App) requestShutdown(cause error) {
	a.beginStopIntake(cause)
}

func (a *App) recordFatalCause(cause error) {
	if cause == nil {
		return
	}
	a.beginStopIntake(cause)

	a.runtimeMu.Lock()
	runtimeCtx := a.runtimeCtx
	recordedInCtx := runtimeCtx != nil && errors.Is(context.Cause(runtimeCtx), cause)
	if !recordedInCtx {
		a.lateRuntimeErr = joinShutdownErr(a.lateRuntimeErr, cause)
	}
	a.runtimeMu.Unlock()
}

func (a *App) recordShutdownError(err error) {
	if err == nil {
		return
	}
	a.beginStopIntake(err)

	a.runtimeMu.Lock()
	a.lateRuntimeErr = joinShutdownErr(a.lateRuntimeErr, err)
	a.runtimeMu.Unlock()
}

func (a *App) fatalRuntimeCause() error {
	a.runtimeMu.Lock()
	defer a.runtimeMu.Unlock()
	var cause error
	if a.runtimeCtx != nil {
		c := context.Cause(a.runtimeCtx)
		if c != nil && !errors.Is(c, context.Canceled) {
			cause = c
		}
	}
	return joinShutdownErr(cause, a.lateRuntimeErr)
}

func (a *App) takeLateRuntimeError() error {
	a.runtimeMu.Lock()
	defer a.runtimeMu.Unlock()
	err := a.lateRuntimeErr
	a.lateRuntimeErr = nil
	return err
}

func contextCauseOrErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		return err
	}
	return nil
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
			err = joinShutdownErr(err, fmt.Errorf("%w: %v", errRuntimeStopPanic, re))
		}
		a.runtimeMu.Lock()
		a.stopErr = joinShutdownErr(err, ctx.Err())
		a.runtimeState = lifecycleStopped
		a.atomicState.Store(uint32(lifecycleStopped))
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
	if ctx == nil {
		ctx = context.Background()
	}

	a.runtimeMu.Lock()
	if len(a.backgroundWorkers) == 0 {
		a.runtimeMu.Unlock()
		return
	}
	standalone := a.runtimeState == lifecycleNew
	if a.intakeCtx == nil || (standalone && a.runtimeCtx != ctx) {
		if a.runtimeCtx == ctx {
			// initRuntime(parent) already created runtimeCtx & intakeCtx.
		} else {
			a.initRuntimeContextsLocked(ctx, ctx)
		}
	}
	intakeCtx := a.intakeCtx
	slots := append([]*workerSlot(nil), a.backgroundWorkers...)
	alreadyStarted := a.actorRT != nil
	a.runtimeMu.Unlock()

	for _, slot := range slots {
		slot.attachTerminalWaitGroup(wg)
	}

	if !alreadyStarted {
		if err := a.startActorRuntime(ctx); err != nil {
			a.recordFatalCause(err)
			a.runtimeMu.Lock()
			slots = append([]*workerSlot(nil), a.backgroundWorkers...)
			a.runtimeMu.Unlock()
			for _, slot := range slots {
				slot.mu.Lock()
				var twg waitGroup
				if !slot.terminal {
					twg = slot.markTerminalLocked(workerFailed, err)
				}
				slot.mu.Unlock()
				if twg != nil {
					twg.Done()
				}
			}
			return
		}
	}

	if standalone && intakeCtx != nil {
		context.AfterFunc(intakeCtx, func() {
			a.closeIntakeGate()
			a.runtimeMu.Lock()
			stopStarted := a.stopDone != nil
			rt := a.actorRT
			a.runtimeMu.Unlock()
			if !stopStarted && rt != nil {
				stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = rt.stop(stopCtx)
			}
		})
	}
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
		if l := a.safeLogger(); l != nil {
			l.Warnf("Timed out waiting for shutdown to finish within %v", timeout)
		}
	}
}
