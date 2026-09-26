package pi

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLifecycle_RunContextObservesItsOwnCancellationAfterStart(t *testing.T) {
	a, closed := newLifecycleProbe(t)
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	require.NoError(t, a.Start(parent))

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	finished := make(chan error, 1)
	go func() { finished <- a.RunContext(runCtx) }()
	cancelRun()

	select {
	case err := <-finished:
		require.NoError(t, err)
	case <-time.After(time.Second):
		// Keep the failing baseline test bounded and release its runtime too.
		cancelParent()
		_ = lifecycleResult(t, finished)
		t.Fatal("RunContext ignored its own canceled context after the app was already started")
	}
	assert.EqualValues(t, 1, closed.closed.Load())
	require.ErrorIs(t, a.runtimeContext().Err(), context.Canceled)
}

// A valid canceled context that lets an already scheduled shutdown function run
// before select evaluates the cancellation arm. Both completions can be ready.
type shutdownSelectionContext struct{ context.Context }

func (c shutdownSelectionContext) Done() <-chan struct{} {
	runtime.Gosched()
	return c.Context.Done()
}

func TestShutdownWithContext_ExpiredBudgetAlwaysForcesClose(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancelDeadline := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelDeadline()
	forceErr := errors.New("force close failed")
	for _, ctx := range []context.Context{canceled, expired} {
		t.Run(ctx.Err().Error(), func(t *testing.T) {
			for range 32 {
				var forced atomic.Int32
				err := ShutdownWithContext(shutdownSelectionContext{ctx}, func(context.Context) error {
					return nil
				}, func() error {
					forced.Add(1)
					return forceErr
				})
				require.ErrorIs(t, err, ctx.Err(), "an expired execution budget must not report success")
				require.ErrorIs(t, err, forceErr)
				require.EqualValues(t, 1, forced.Load(), "selection of the graceful result must not bypass force-close")
			}
		})
	}
}

func TestLifecycle_RunContextPreservesCancellationCauseAfterStart(t *testing.T) {
	a, closed := newLifecycleProbe(t)
	require.NoError(t, a.Start(context.Background()))
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("host requested shutdown after failure")
	cancel(cause)
	finished := make(chan error, 1)
	go func() { finished <- a.RunContext(ctx) }()
	require.ErrorIs(t, lifecycleResult(t, finished), cause)
	assert.EqualValues(t, 1, closed.closed.Load())
}
