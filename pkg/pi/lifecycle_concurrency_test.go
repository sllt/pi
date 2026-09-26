package pi

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sllt/pi/pkg/pi/config"
	"github.com/sllt/pi/pkg/pi/infra"
	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/sllt/pi/pkg/pi/websocket"
)

type lifecycleCloseProbe struct {
	mockSubscriber
	closed atomic.Int32
	err    error
}

func (p *lifecycleCloseProbe) Close() error {
	p.closed.Add(1)
	return p.err
}

func newLifecycleProbe(t *testing.T) (*App, *lifecycleCloseProbe) {
	t.Helper()
	p := &lifecycleCloseProbe{}
	a := &App{
		Config:    config.NewMockConfig(map[string]string{"SHUTDOWN_GRACE_PERIOD": "2s"}),
		container: &infra.Container{Logger: logging.NewLogger(logging.FATAL), PubSub: p, WSManager: websocket.New()},
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = a.Stop(ctx)
	})
	return a, p
}

// Signals when a caller is actually waiting, without relying on scheduler sleeps.
type lifecycleWaitContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *lifecycleWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

func lifecycleResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("lifecycle operation did not complete")
		return nil
	}
}

func lifecycleGate(t *testing.T) (chan struct{}, func()) {
	t.Helper()
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	t.Cleanup(release)
	return gate, release
}

func TestLifecycle_ConcurrentStartRunsOnce(t *testing.T) {
	a, closed := newLifecycleProbe(t)
	gate, release := lifecycleGate(t)
	entered, workerStarted := make(chan struct{}), make(chan struct{}, 32)
	var starts, workers atomic.Int32
	a.OnStart(func(*Context) error {
		if starts.Add(1) == 1 {
			close(entered)
		}
		<-gate
		return nil
	})
	a.Go("worker", func(c *Context) error {
		workers.Add(1)
		workerStarted <- struct{}{}
		<-c.Done()
		return c.Err()
	})
	first := make(chan error, 1)
	go func() { first <- a.Start(context.Background()) }()
	waitForHandlerSignal(t, entered)
	results := make(chan error, 16)
	for range 16 {
		waiter := &lifecycleWaitContext{Context: context.Background(), entered: make(chan struct{})}
		go func() { results <- a.Start(waiter) }()
		waitForHandlerSignal(t, waiter.entered)
	}
	release()
	require.NoError(t, lifecycleResult(t, first))
	for range 16 {
		require.NoError(t, lifecycleResult(t, results))
	}
	waitForHandlerSignal(t, workerStarted)
	require.NoError(t, a.Start(context.Background()))
	require.NoError(t, a.Stop(context.Background()))
	assert.EqualValues(t, 1, starts.Load())
	assert.EqualValues(t, 1, workers.Load())
	assert.EqualValues(t, 1, closed.closed.Load())
	require.ErrorIs(t, a.Start(context.Background()), errAppAlreadyStopped)
}

func TestLifecycle_ConcurrentStartSharesRollbackResult(t *testing.T) {
	a, closed := newLifecycleProbe(t)
	gate, release := lifecycleGate(t)
	entered := make(chan struct{})
	startErr, cleanupErr := errors.New("start failed"), errors.New("cleanup failed")
	var stops atomic.Int32
	a.OnStart(func(*Context) error { close(entered); <-gate; return startErr })
	a.OnStop(func(*Context) error { stops.Add(1); return cleanupErr })
	first := make(chan error, 1)
	go func() { first <- a.Start(context.Background()) }()
	waitForHandlerSignal(t, entered)
	results := make(chan error, 8)
	for range 8 {
		waiter := &lifecycleWaitContext{Context: context.Background(), entered: make(chan struct{})}
		go func() { results <- a.Start(waiter) }()
		waitForHandlerSignal(t, waiter.entered)
	}
	release()
	err := lifecycleResult(t, first)
	require.ErrorIs(t, err, startErr)
	require.ErrorIs(t, err, cleanupErr)
	for range 8 {
		assert.Same(t, err, lifecycleResult(t, results))
	}
	assert.EqualValues(t, 1, stops.Load())
	assert.EqualValues(t, 1, closed.closed.Load())
	require.ErrorIs(t, a.Stop(context.Background()), cleanupErr)
}

func TestLifecycle_StartWaiterDoesNotCancelRuntime(t *testing.T) {
	a, _ := newLifecycleProbe(t)
	gate, release := lifecycleGate(t)
	entered := make(chan struct{})
	a.OnStart(func(*Context) error { close(entered); <-gate; return nil })
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	first := make(chan error, 1)
	go func() { first <- a.Start(parent) }()
	waitForHandlerSignal(t, entered)
	waitCtx, cancelWait := context.WithCancel(context.Background())
	cancelWait()
	require.ErrorIs(t, a.Start(waitCtx), context.Canceled)
	require.NoError(t, a.runtimeContext().Err())
	release()
	require.NoError(t, lifecycleResult(t, first))
	cancelParent()
	waitForHandlerSignal(t, a.runtimeContext().Done())
	require.NoError(t, a.Stop(context.Background()))
}

func TestLifecycle_StopWaitersShareError(t *testing.T) {
	a, closed := newLifecycleProbe(t)
	gate, release := lifecycleGate(t)
	entered := make(chan struct{})
	hookErr, closeErr := errors.New("stop failed"), errors.New("close failed")
	closed.err = closeErr
	var hooks atomic.Int32
	a.OnStop(func(*Context) error { hooks.Add(1); close(entered); <-gate; return hookErr })
	require.NoError(t, a.Start(context.Background()))
	first := make(chan error, 1)
	go func() { first <- a.Stop(context.Background()) }()
	waitForHandlerSignal(t, entered)
	results := make(chan error, 8)
	for range 8 {
		waiter := &lifecycleWaitContext{Context: context.Background(), entered: make(chan struct{})}
		go func() { results <- a.Stop(waiter) }()
		waitForHandlerSignal(t, waiter.entered)
	}
	waitCtx, cancelWait := context.WithCancel(context.Background())
	cancelWait()
	require.ErrorIs(t, a.Stop(waitCtx), context.Canceled)
	assert.Zero(t, closed.closed.Load())
	release()
	err := lifecycleResult(t, first)
	require.ErrorIs(t, err, hookErr)
	require.ErrorIs(t, err, closeErr)
	for range 8 {
		assert.Same(t, err, lifecycleResult(t, results))
	}
	assert.Same(t, err, a.Stop(waitCtx), "terminal result takes precedence over a new wait budget")
	assert.EqualValues(t, 1, hooks.Load())
	assert.EqualValues(t, 1, closed.closed.Load())
}

func TestLifecycle_StopTimeoutDoesNotMarkCleanupComplete(t *testing.T) {
	a, closed := newLifecycleProbe(t)
	gate, release := lifecycleGate(t)
	entered := make(chan struct{})
	hookErr := errors.New("late cleanup failure")
	var hooks atomic.Int32
	a.OnStop(func(*Context) error { hooks.Add(1); close(entered); <-gate; return hookErr })
	require.NoError(t, a.Start(context.Background()))
	execCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := make(chan error, 1)
	go func() { first <- a.Stop(execCtx) }()
	waitForHandlerSignal(t, entered)
	cancel()
	require.ErrorIs(t, lifecycleResult(t, first), context.Canceled)
	waitCtx, cancelWait := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancelWait()
	require.ErrorIs(t, a.Stop(waitCtx), context.DeadlineExceeded)
	assert.Zero(t, closed.closed.Load())
	release()
	err := a.Stop(context.Background())
	require.ErrorIs(t, err, hookErr)
	require.ErrorIs(t, err, context.Canceled)
	assert.Same(t, err, a.Stop(context.Background()))
	assert.EqualValues(t, 1, hooks.Load())
	assert.EqualValues(t, 1, closed.closed.Load())
}

func TestLifecycle_StopWaitsForWorkerBeforeClosingDependencies(t *testing.T) {
	a, closed := newLifecycleProbe(t)
	gate, release := lifecycleGate(t)
	entered := make(chan struct{})
	a.Go("slow-exit", func(*Context) error { close(entered); <-gate; return nil })
	require.NoError(t, a.Start(context.Background()))
	waitForHandlerSignal(t, entered)
	execCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	require.ErrorIs(t, a.Stop(execCtx), context.DeadlineExceeded)
	waitCtx, cancelWait := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancelWait()
	require.ErrorIs(t, a.Stop(waitCtx), context.DeadlineExceeded)
	assert.Zero(t, closed.closed.Load(), "worker still owns dependencies")
	release()
	require.ErrorIs(t, a.Stop(context.Background()), context.DeadlineExceeded)
	assert.EqualValues(t, 1, closed.closed.Load())
}

func TestLifecycle_StopDuringStartWaitsForSetup(t *testing.T) {
	a, closed := newLifecycleProbe(t)
	gate, release := lifecycleGate(t)
	entered, canceled := make(chan struct{}), make(chan struct{})
	var hooks atomic.Int32
	a.OnStart(func(c *Context) error {
		close(entered)
		<-c.Done()
		close(canceled)
		<-gate
		a.OnStop(func(*Context) error { hooks.Add(1); return nil })
		return nil
	})
	started, stopped := make(chan error, 1), make(chan error, 1)
	go func() { started <- a.Start(context.Background()) }()
	waitForHandlerSignal(t, entered)
	go func() { stopped <- a.Stop(context.Background()) }()
	waitForHandlerSignal(t, canceled)
	assert.Zero(t, closed.closed.Load())
	require.ErrorIs(t, a.Start(context.Background()), errAppAlreadyStopped)
	release()
	require.ErrorIs(t, lifecycleResult(t, started), context.Canceled)
	require.NoError(t, lifecycleResult(t, stopped))
	assert.EqualValues(t, 1, hooks.Load())
	assert.EqualValues(t, 1, closed.closed.Load())
}

func TestLifecycle_StopBeforeStartAndPanic(t *testing.T) {
	a, closed := newLifecycleProbe(t)
	a.OnStop(func(*Context) error { panic("cleanup panic") })
	err := a.Stop(context.Background())
	require.ErrorIs(t, err, errStopHookPanic)
	assert.Same(t, err, a.Stop(context.Background()))
	require.ErrorIs(t, a.Start(context.Background()), errAppAlreadyStopped)
	assert.EqualValues(t, 1, closed.closed.Load())
}

func TestLifecycle_WorkerRegistrationRacesStartup(t *testing.T) {
	a, _ := newLifecycleProbe(t)
	var calls atomic.Int32
	var registrations sync.WaitGroup
	for range 16 {
		registrations.Add(1)
		go func() {
			defer registrations.Done()
			a.Go("one-worker", func(c *Context) error { calls.Add(1); <-c.Done(); return c.Err() })
		}()
	}
	require.NoError(t, a.Start(context.Background()))
	registrations.Wait()
	require.NoError(t, a.Stop(context.Background()))
	assert.EqualValues(t, len(a.backgroundWorkers), calls.Load())
	assert.LessOrEqual(t, calls.Load(), int32(1))
}
