package pi

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sllt/tao/actor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sllt/pi/pkg/pi/config"
	piSQL "github.com/sllt/pi/pkg/pi/datasource/sql"
	"github.com/sllt/pi/pkg/pi/logging"
)

type spyClosableLogger struct {
	logging.Logger
	mu     sync.Mutex
	buf    bytes.Buffer
	closed atomic.Int32
}

func newSpyClosableLogger() *spyClosableLogger {
	s := &spyClosableLogger{}
	w := &syncBufferWriter{mu: &s.mu, buf: &s.buf}
	s.Logger = logging.NewWriterLogger(logging.DEBUG, w, w)
	return s
}

func (s *spyClosableLogger) Close() error {
	s.closed.Add(1)
	return nil
}

func (s *spyClosableLogger) Output() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

type syncBufferWriter struct {
	mu  *sync.Mutex
	buf *bytes.Buffer
}

func (w *syncBufferWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func buildTestApp(t *testing.T, opts ...Option) *App {
	t.Helper()
	base := []Option{
		WithConfig(map[string]string{
			"HTTP_ENABLED":          "false",
			"GRPC_ENABLED":          "false",
			"METRICS_ENABLED":       "false",
			"SHUTDOWN_GRACE_PERIOD": "2s",
		}),
		WithLogger(logging.NewWriterLogger(logging.ERROR, nil, nil)),
	}
	base = append(base, opts...)
	app, err := Build(base...)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = app.Stop(ctx)
	})
	return app
}

func TestSanitizeActorSegment(t *testing.T) {
	cases := []string{
		"",
		".",
		"..",
		"...",
		"sync/users/v1",
		"db:primary:replica",
		"worker with spaces & symbols!@#$%^&*()",
		"用户同步任务-测试",
		strings.Repeat("very-long-worker-segment-", 12),
	}

	seen := make(map[string]struct{}, len(cases)+2)
	for i, raw := range cases {
		seg := sanitizeActorSegment("worker", uint64(i+1), raw)
		require.Truef(t, isValidActorSegment(seg), "invalid actor segment %q from %q", seg, raw)
		require.LessOrEqual(t, len(seg), maxActorSegmentLen)
		_, dup := seen[seg]
		require.False(t, dup, "unexpected duplicate segment %q", seg)
		seen[seg] = struct{}{}
	}

	// Distinct raw names that normalize to the same slug must still produce distinct segments.
	s1 := sanitizeActorSegment("worker", 1, "orders/sync")
	s2 := sanitizeActorSegment("worker", 2, "orders:sync")
	require.NotEqual(t, s1, s2)
	require.True(t, isValidActorSegment(s1))
	require.True(t, isValidActorSegment(s2))
}

func TestAddWorker_ValidationAndLazyActorTree(t *testing.T) {
	app := buildTestApp(t)

	// Compile-time & runtime method-value checks.
	goMethod := app.Go
	addMethod := app.AddWorker
	require.NotNil(t, goMethod)
	require.NotNil(t, addMethod)

	noop := func(*Context) error { return nil }

	require.ErrorIs(t, app.AddWorker("", WorkerPolicy{}, noop), errBackgroundWorkerNameEmpty)
	require.ErrorIs(t, app.AddWorker("   ", WorkerPolicy{}, noop), errBackgroundWorkerNameEmpty)
	require.ErrorIs(t, app.AddWorker("valid", WorkerPolicy{}, nil), errBackgroundWorkerHandlerNil)
	require.ErrorIs(t, app.AddWorker("bad-mode", WorkerPolicy{Mode: SupervisionMode(99)}, noop), errInvalidWorkerSupervisionMode)
	require.ErrorIs(t, app.AddWorker("neg-init", WorkerPolicy{BackoffInitial: -time.Millisecond}, noop), errNegativeWorkerPolicyValue)
	require.ErrorIs(t, app.AddWorker("neg-max", WorkerPolicy{BackoffMax: -time.Millisecond}, noop), errNegativeWorkerPolicyValue)
	require.ErrorIs(t, app.AddWorker("neg-reset", WorkerPolicy{HealthyResetAfter: -time.Millisecond}, noop), errNegativeWorkerPolicyValue)
	require.ErrorIs(t, app.AddWorker("neg-restarts", WorkerPolicy{MaxRestarts: -1}, noop), errNegativeWorkerPolicyValue)
	require.ErrorIs(t, app.AddWorker("neg-win", WorkerPolicy{RestartWindow: -time.Millisecond}, noop), errNegativeWorkerPolicyValue)
	require.ErrorIs(t, app.AddWorker("neg-stop", WorkerPolicy{ShutdownTimeout: -time.Millisecond}, noop), errNegativeWorkerPolicyValue)
	require.ErrorIs(t, app.AddWorker("bad-order", WorkerPolicy{
		BackoffInitial: 500 * time.Millisecond,
		BackoffMax:     100 * time.Millisecond,
	}, noop), errWorkerBackoffOrder)
	require.ErrorIs(t, app.AddWorker("exceeds-default-max", WorkerPolicy{
		BackoffInitial: 45 * time.Second,
	}, noop), errWorkerBackoffOrder)

	// Pure HTTP/empty app without workers does not create an actor.Node even after Start.
	require.Nil(t, app.actorRT)
	require.NoError(t, app.Start(t.Context()))
	require.Nil(t, app.actorRT)
	require.True(t, app.isReady())

	// Registering after Start must fail cleanly.
	require.ErrorIs(t, app.AddWorker("late", WorkerPolicy{}, noop), errBackgroundWorkerRegisterTooLate)
	require.NoError(t, app.Stop(t.Context()))
	require.False(t, app.isReady())

	// Pure Build with AddWorker does not start actor.Node until Start is called.
	app2 := buildTestApp(t)
	var ran atomic.Int32
	require.NoError(t, app2.AddWorker("orders/sync", WorkerPolicy{Mode: SuperviseRestart}, func(c *Context) error {
		ran.Add(1)
		<-c.Done()
		return c.Err()
	}))
	require.ErrorIs(t, app2.AddWorker(" orders/sync ", WorkerPolicy{}, noop), errBackgroundWorkerDuplicateName)
	// Sanitized-collision sibling name is allowed and gets a distinct actor segment.
	require.NoError(t, app2.AddWorker("orders:sync", WorkerPolicy{Mode: SuperviseRestart}, func(c *Context) error {
		<-c.Done()
		return c.Err()
	}))
	require.Nil(t, app2.actorRT)
	require.Zero(t, ran.Load())
}

func TestWorkerSupervision_FatalZeroRetryOnErrorAndPanic(t *testing.T) {
	t.Run("error triggers immediate shutdown with zero retries", func(t *testing.T) {
		app := buildTestApp(t)
		boom := errors.New("fatal worker failure")
		release := make(chan struct{})
		var runs atomic.Int32

		require.NoError(t, app.AddWorker("fatal-err", WorkerPolicy{
			Mode:     SuperviseFatal,
			Required: true,
		}, func(*Context) error {
			runs.Add(1)
			<-release
			return boom
		}))

		require.NoError(t, app.Start(t.Context()))
		close(release)
		err := app.Wait(t.Context())
		require.ErrorIs(t, err, boom)
		assert.EqualValues(t, 1, runs.Load())
		snap := app.backgroundWorkers[0].snapshot()
		assert.Zero(t, snap.WorkerRetries)
		assert.Equal(t, workerFailed, snap.State)
	})

	t.Run("panic triggers immediate shutdown with zero retries", func(t *testing.T) {
		app := buildTestApp(t)
		release := make(chan struct{})
		var runs atomic.Int32

		require.NoError(t, app.AddWorker("fatal-panic", WorkerPolicy{
			Mode:     SuperviseFatal,
			Required: true,
		}, func(*Context) error {
			runs.Add(1)
			<-release
			panic("kaboom")
		}))

		require.NoError(t, app.Start(t.Context()))
		close(release)
		err := app.Wait(t.Context())
		require.ErrorIs(t, err, errBackgroundWorkerPanic)
		assert.Contains(t, err.Error(), "kaboom")
		assert.EqualValues(t, 1, runs.Load())
		assert.Zero(t, app.backgroundWorkers[0].snapshot().WorkerRetries)
	})

	t.Run("immediate fatal error during Start propagates cause without context.Canceled masking", func(t *testing.T) {
		app := buildTestApp(t)
		boom := errors.New("instant boot worker failure")
		var runs atomic.Int32

		require.NoError(t, app.AddWorker("instant-fatal", WorkerPolicy{
			Mode:     SuperviseFatal,
			Required: true,
		}, func(*Context) error {
			runs.Add(1)
			return boom
		}))

		startErr := app.Start(t.Context())
		if startErr != nil {
			require.ErrorIs(t, startErr, boom)
		}
		waitErr := app.Wait(t.Context())
		require.ErrorIs(t, waitErr, boom)
		assert.EqualValues(t, 1, runs.Load())
		assert.Zero(t, app.backgroundWorkers[0].snapshot().WorkerRetries)
	})
}

func TestWorkerSupervision_SlidingWindowBackoffJitterAndHealthyReset(t *testing.T) {
	t.Run("default full jitter stays within [0, BackoffMax] and MaxRestarts excludes initial launch", func(t *testing.T) {
		app := buildTestApp(t)
		boom := errors.New("transient failure")
		var runs atomic.Int32
		var observedDelays []time.Duration
		var mu sync.Mutex

		const maxBackoff = 12 * time.Millisecond
		require.NoError(t, app.AddWorker("restarting", WorkerPolicy{
			Mode:           SuperviseRestart,
			Required:       true,
			BackoffInitial: 4 * time.Millisecond,
			BackoffMax:     maxBackoff,
			MaxRestarts:    3,
			RestartWindow:  5 * time.Second,
		}, func(*Context) error {
			n := runs.Add(1)
			if n > 1 {
				snap := app.backgroundWorkers[0].snapshot()
				mu.Lock()
				observedDelays = append(observedDelays, snap.LastBackoff)
				mu.Unlock()
			}
			return boom
		}))

		require.NoError(t, app.Start(t.Context()))
		err := app.Wait(t.Context())
		require.ErrorIs(t, err, ErrWorkerRestartBudgetExceeded)
		require.ErrorIs(t, err, boom)
		// 1 initial launch + 3 restarts = 4 total invocations.
		assert.EqualValues(t, 4, runs.Load())

		snap := app.backgroundWorkers[0].snapshot()
		assert.EqualValues(t, 3, snap.WorkerRetries)
		assert.Equal(t, workerFailed, snap.State)

		mu.Lock()
		defer mu.Unlock()
		require.Len(t, observedDelays, 3)
		for _, d := range observedDelays {
			assert.GreaterOrEqual(t, d, time.Duration(0))
			assert.LessOrEqual(t, d, maxBackoff)
		}
	})

	t.Run("HealthyResetAfter resets backoff level without clearing sliding RestartWindow budget", func(t *testing.T) {
		app := buildTestApp(t)
		// Deterministic cap inspection: always pick the cap itself.
		app.workerJitter = func(maxDelay time.Duration) time.Duration { return maxDelay }

		boom := errors.New("periodic drop")
		var runs atomic.Int32
		var caps []time.Duration
		var mu sync.Mutex

		require.NoError(t, app.AddWorker("windowed", WorkerPolicy{
			Mode:              SuperviseRestart,
			Required:          true,
			BackoffInitial:    6 * time.Millisecond,
			BackoffMax:        60 * time.Millisecond,
			HealthyResetAfter: 25 * time.Millisecond,
			MaxRestarts:       2,
			RestartWindow:     3 * time.Second,
		}, func(*Context) error {
			n := runs.Add(1)
			if n > 1 {
				mu.Lock()
				caps = append(caps, app.backgroundWorkers[0].snapshot().LastBackoff)
				mu.Unlock()
			}
			switch n {
			case 1:
				// Immediate failure: consecutive=1 -> cap=6ms, windowRestarts=1
				return boom
			case 2:
				// Run healthy > HealthyResetAfter (25ms), then fail:
				// consecutive resets to 0 -> next failure is consecutive=1 (cap=6ms, NOT 12ms!),
				// while windowRestarts becomes 2.
				time.Sleep(35 * time.Millisecond)
				return boom
			default:
				// Run healthy > HealthyResetAfter again, then fail inside 3s RestartWindow:
				// must STILL hit MaxRestarts=2 budget and shut down!
				time.Sleep(35 * time.Millisecond)
				return boom
			}
		}))

		require.NoError(t, app.Start(t.Context()))
		err := app.Wait(t.Context())
		require.ErrorIs(t, err, ErrWorkerRestartBudgetExceeded)
		assert.EqualValues(t, 3, runs.Load())

		mu.Lock()
		defer mu.Unlock()
		require.Equal(t, []time.Duration{6 * time.Millisecond, 6 * time.Millisecond}, caps)
	})
}

func TestWorkerSupervision_DegradeIsolatesSiblingsAndUpdatesReadiness(t *testing.T) {
	t.Run("optional degraded worker leaves sibling worker and app Readiness healthy", func(t *testing.T) {
		app := buildTestApp(t)
		siblingReady := make(chan struct{})
		degradedDone := make(chan struct{})
		var badRuns atomic.Int32

		require.NoError(t, app.AddWorker("healthy-sibling", WorkerPolicy{
			Mode:      SuperviseFatal,
			Required:  true,
			WaitReady: true,
		}, func(c *Context) error {
			require.NoError(t, c.MarkReady())
			close(siblingReady)
			<-c.Done()
			return c.Err()
		}))

		require.NoError(t, app.AddWorker("optional-sync", WorkerPolicy{
			Mode:           SuperviseDegrade,
			Required:       false,
			BackoffInitial: 2 * time.Millisecond,
			BackoffMax:     5 * time.Millisecond,
			MaxRestarts:    2,
			RestartWindow:  2 * time.Second,
		}, func(*Context) error {
			if badRuns.Add(1) == 3 {
				defer close(degradedDone)
			}
			return errors.New("downstream unavailable")
		}))

		require.NoError(t, app.Start(t.Context()))
		<-siblingReady
		<-degradedDone

		require.Eventually(t, func() bool {
			return app.backgroundWorkers[1].snapshot().State == workerDegraded
		}, time.Second, 5*time.Millisecond)

		assert.EqualValues(t, 3, badRuns.Load())
		assert.True(t, app.isReady(), "optional degraded worker must not fail Readiness")
		require.NoError(t, app.Stop(t.Context()))
		require.NoError(t, app.Wait(t.Context()))
	})

	t.Run("required degraded worker keeps app alive while flipping Readiness to false", func(t *testing.T) {
		app := buildTestApp(t)
		require.NoError(t, app.AddWorker("required-degradable", WorkerPolicy{
			Mode:           SuperviseDegrade,
			Required:       true,
			BackoffInitial: 2 * time.Millisecond,
			BackoffMax:     4 * time.Millisecond,
			MaxRestarts:    1,
			RestartWindow:  2 * time.Second,
		}, func(*Context) error {
			return errors.New("cache cluster offline")
		}))

		require.NoError(t, app.Start(t.Context()))
		require.Eventually(t, func() bool {
			return app.backgroundWorkers[0].snapshot().State == workerDegraded
		}, time.Second, 5*time.Millisecond)

		assert.False(t, app.isReady())
		require.NoError(t, app.Stop(t.Context()))
	})
}

func TestWorkerSupervision_ControllerCrashNonOverlapAndTreeTermination(t *testing.T) {
	t.Run("controller panic recovers via OneForOne without overlapping old worker execution", func(t *testing.T) {
		app := buildTestApp(t)
		gen1Entered := make(chan struct{})
		allowGen1Exit := make(chan struct{})
		gen2Entered := make(chan struct{})
		var genCount atomic.Int32

		require.NoError(t, app.AddWorker("non-overlap", WorkerPolicy{
			Mode:            SuperviseRestart,
			Required:        true,
			ShutdownTimeout: 500 * time.Millisecond,
		}, func(c *Context) error {
			n := genCount.Add(1)
			if n == 1 {
				close(gen1Entered)
				<-c.Done()
				// Hold the first generation briefly after cancel to prove replacement
				// never overlaps with an in-flight execution goroutine.
				<-allowGen1Exit
				return c.Err()
			}
			if n == 2 {
				close(gen2Entered)
			}
			<-c.Done()
			return c.Err()
		}))

		require.NoError(t, app.Start(t.Context()))
		<-gen1Entered

		slot := app.backgroundWorkers[0]
		slot.mu.Lock()
		pid := slot.controllerPID
		node := slot.node
		slot.mu.Unlock()

		// Crash the workerActor controller while generation 1 is still running.
		require.NoError(t, node.Send(pid, actorInjectPanicMsg{reason: "simulated controller bug"}))

		// Give the supervisor time to attempt stopping/restarting while gen1 refuses to exit yet.
		time.Sleep(25 * time.Millisecond)
		assert.EqualValues(t, 1, genCount.Load(), "must not start generation 2 while generation 1 is still running")
		assert.EqualValues(t, 1, slot.maxActiveSeen.Load())

		close(allowGen1Exit)
		select {
		case <-gen2Entered:
		case <-time.After(2 * time.Second):
			t.Fatal("replacement controller did not resume worker after generation 1 exited")
		}

		snap := slot.snapshot()
		assert.EqualValues(t, 1, slot.maxActiveSeen.Load(), "hard non-overlap latch violated")
		assert.GreaterOrEqual(t, snap.ActorRestarts, uint64(1))
		assert.Zero(t, snap.WorkerRetries, "controller restart must not consume business workerRetries")

		require.NoError(t, app.Stop(t.Context()))
	})

	t.Run("supervisor restart exhaustion propagates to App.Wait", func(t *testing.T) {
		app := buildTestApp(t)
		entered := make(chan struct{}, 16)

		require.NoError(t, app.AddWorker("crash-loop-ctrl", WorkerPolicy{
			Mode:     SuperviseRestart,
			Required: true,
		}, func(c *Context) error {
			entered <- struct{}{}
			<-c.Done()
			return c.Err()
		}))

		require.NoError(t, app.Start(t.Context()))
		<-entered

		slot := app.backgroundWorkers[0]
		// Exceed workers-sup MaxRestarts (5 in 10s).
		for range 8 {
			slot.mu.Lock()
			pid := slot.controllerPID
			node := slot.node
			slot.mu.Unlock()
			if node == nil {
				break
			}
			_ = node.Send(pid, actorInjectPanicMsg{reason: "repeated controller crash"})
			time.Sleep(8 * time.Millisecond)
		}

		waitCtx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		err := app.Wait(waitCtx)
		require.Error(t, err)
		require.ErrorIs(t, err, actor.ErrMaxRestartsExceeded)
	})

	t.Run("unexpected pi-root or Node termination propagates to App.Wait", func(t *testing.T) {
		app := buildTestApp(t)
		started := make(chan struct{})
		require.NoError(t, app.AddWorker("steady", WorkerPolicy{Mode: SuperviseFatal, Required: true}, func(c *Context) error {
			close(started)
			<-c.Done()
			return c.Err()
		}))
		require.NoError(t, app.Start(t.Context()))
		<-started

		require.NoError(t, app.actorRT.node.Send(app.actorRT.rootPID, actorInjectPanicMsg{reason: "pi-root boom"}))

		waitCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		err := app.Wait(waitCtx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "pi-root")
	})
}

func TestWorkerSupervision_StoppedErrorVsHungWorkerAndPostTimeoutCleanup(t *testing.T) {
	t.Run("Stopped callback returning error is logged to instance logger and not treated as timeout", func(t *testing.T) {
		spyLog := newSpyClosableLogger()
		app := buildTestApp(t, WithLogger(spyLog))
		started := make(chan struct{})

		require.NoError(t, app.AddWorker("stopped-err-worker", WorkerPolicy{
			Mode:     SuperviseFatal,
			Required: true,
		}, func(c *Context) error {
			close(started)
			<-c.Done()
			return c.Err()
		}))

		require.NoError(t, app.Start(t.Context()))
		<-started

		slot := app.backgroundWorkers[0]
		slot.mu.Lock()
		pid := slot.controllerPID
		node := slot.node
		slot.mu.Unlock()

		ackDone := make(chan struct{})
		stoppedCallbackErr := errors.New("benign stopped callback error")
		require.NoError(t, node.Send(pid, workerSetStoppedErrMsg{err: stoppedCallbackErr}))
		// Ensure message processed before Stop.
		require.Eventually(t, func() bool {
			return slot.stoppedHookErr.Load() != nil
		}, time.Second, 2*time.Millisecond)
		close(ackDone)
		<-ackDone

		require.NoError(t, app.Stop(t.Context()))
		require.NoError(t, app.Wait(t.Context()))
		assert.Contains(t, spyLog.Output(), "Stopped callback failed")
		assert.Zero(t, spyLog.closed.Load(), "injected logger must not be closed")
	})

	t.Run("hung worker times out Stop caller without closing Owned resources early and preserves late errors in Wait", func(t *testing.T) {
		releaseWorker := make(chan struct{})
		workerStarted := make(chan struct{})
		var workerExited atomic.Bool
		var resourceClosed atomic.Bool
		var onStopExecuted atomic.Bool
		var onStopCtxErr error
		var resCtxErr error

		lateWorkerErr := errors.New("late worker write failure")
		hookErr := errors.New("onstop flush failed")
		exporterErr := errors.New("owned exporter close failed")

		app := buildTestApp(t, WithResource(Resource{
			Name:      "owned-exporter",
			Ownership: Owned,
			Start:     func(context.Context) error { return nil },
			Stop: func(ctx context.Context) error {
				resCtxErr = ctx.Err()
				assert.True(t, workerExited.Load(), "Owned resource closed before worker exited")
				resourceClosed.Store(true)
				return exporterErr
			},
		}))

		app.OnStop(func(c *Context) error {
			onStopCtxErr = c.Err()
			assert.True(t, workerExited.Load(), "OnStop ran before worker exited")
			onStopExecuted.Store(true)
			return hookErr
		})

		require.NoError(t, app.AddWorker("hung-worker", WorkerPolicy{
			Mode:            SuperviseFatal,
			Required:        true,
			ShutdownTimeout: 15 * time.Millisecond,
		}, func(c *Context) error {
			close(workerStarted)
			<-c.Done()
			<-releaseWorker
			workerExited.Store(true)
			return lateWorkerErr
		}))

		require.NoError(t, app.Start(t.Context()))
		<-workerStarted

		shortCtx, cancelShort := context.WithTimeout(t.Context(), 8*time.Millisecond)
		defer cancelShort()
		stopErr := app.Stop(shortCtx)
		require.ErrorIs(t, stopErr, context.DeadlineExceeded)

		// Neither OnStop nor Owned resource Stop may run while hung-worker is still executing.
		assert.False(t, onStopExecuted.Load())
		assert.False(t, resourceClosed.Load())

		close(releaseWorker)

		waitErr := app.Wait(t.Context())
		require.ErrorIs(t, waitErr, context.DeadlineExceeded)
		require.ErrorIs(t, waitErr, lateWorkerErr)
		require.ErrorIs(t, waitErr, hookErr)
		require.ErrorIs(t, waitErr, exporterErr)

		assert.True(t, onStopExecuted.Load(), "OnStop must still execute under cleanup fallback budget")
		assert.True(t, resourceClosed.Load())
		assert.NoError(t, onStopCtxErr, "OnStop fallback context must be active")
		assert.NoError(t, resCtxErr, "Owned resource Stop fallback context must be active")
	})
}

func TestWorkerSupervision_MarkReadyAndReadinessContracts(t *testing.T) {
	t.Run("non-worker context rejects MarkReady", func(t *testing.T) {
		var emptyCtx Context
		require.ErrorIs(t, emptyCtx.MarkReady(), ErrMarkReadyInvalidContext)

		app := buildTestApp(t)
		app.OnStart(func(c *Context) error {
			return c.MarkReady()
		})
		require.ErrorIs(t, app.Start(t.Context()), ErrMarkReadyInvalidContext)
	})

	t.Run("MarkReady idempotency, stale generation rejection, and restart readiness reset", func(t *testing.T) {
		app := buildTestApp(t)
		var staleGen1Ctx atomic.Pointer[Context]
		failGen1 := make(chan struct{})
		gen2Started := make(chan struct{})
		allowGen2Ready := make(chan struct{})
		gen2ReadyDone := make(chan struct{})
		var gen atomic.Int32

		require.NoError(t, app.AddWorker("ready-stateful", WorkerPolicy{
			Mode:           SuperviseRestart,
			Required:       true,
			WaitReady:      true,
			BackoffInitial: 2 * time.Millisecond,
			BackoffMax:     5 * time.Millisecond,
		}, func(c *Context) error {
			switch gen.Add(1) {
			case 1:
				staleGen1Ctx.Store(c)
				require.NoError(t, c.MarkReady())
				require.NoError(t, c.MarkReady(), "duplicate MarkReady on active generation must be idempotent")
				<-failGen1
				return errors.New("connection lost")
			default:
				close(gen2Started)
				<-allowGen2Ready
				require.NoError(t, c.MarkReady())
				close(gen2ReadyDone)
				<-c.Done()
				return c.Err()
			}
		}))

		require.NoError(t, app.Start(t.Context()))
		require.True(t, app.isReady())

		// Trigger Gen 1 failure -> Gen 2 enters Starting (not Ready!).
		close(failGen1)
		<-gen2Started
		assert.False(t, app.isReady(), "restarted worker must re-enter Starting and clear Ready")

		// Calling MarkReady on stale Gen 1 context must fail and must NOT mark Gen 2 ready!
		oldCtx := staleGen1Ctx.Load()
		require.NotNil(t, oldCtx)
		require.ErrorIs(t, oldCtx.MarkReady(), ErrMarkReadyInvalidContext)
		assert.False(t, app.isReady())

		close(allowGen2Ready)
		<-gen2ReadyDone
		assert.True(t, app.isReady())

		require.NoError(t, app.Stop(t.Context()))
	})

	t.Run("Required WaitReady timeout rolls back Owned resources", func(t *testing.T) {
		var startedRes, stoppedRes atomic.Int32
		app := buildTestApp(t, WithResource(Resource{
			Name:      "db-conn",
			Ownership: Owned,
			Start:     func(context.Context) error { startedRes.Add(1); return nil },
			Stop:      func(context.Context) error { stoppedRes.Add(1); return nil },
		}))

		require.NoError(t, app.AddWorker("never-ready", WorkerPolicy{
			Mode:      SuperviseRestart,
			Required:  true,
			WaitReady: true,
		}, func(c *Context) error {
			<-c.Done()
			return c.Err()
		}))

		startCtx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
		defer cancel()
		err := app.Start(startCtx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.EqualValues(t, 1, startedRes.Load())
		assert.EqualValues(t, 1, stoppedRes.Load())
		assert.False(t, app.isReady())
	})

	t.Run("WaitReady exiting nil before MarkReady fails Required startup and marks Optional unready", func(t *testing.T) {
		appReq := buildTestApp(t)
		require.NoError(t, appReq.AddWorker("premature-exit-req", WorkerPolicy{
			Mode:      SuperviseFatal,
			Required:  true,
			WaitReady: true,
		}, func(*Context) error {
			return nil
		}))
		err := appReq.Start(t.Context())
		require.ErrorIs(t, err, ErrWorkerExitedBeforeReady)

		appOpt := buildTestApp(t)
		optDone := make(chan struct{})
		require.NoError(t, appOpt.AddWorker("premature-exit-opt", WorkerPolicy{
			Mode:      SuperviseFatal,
			Required:  false,
			WaitReady: true,
		}, func(*Context) error {
			defer close(optDone)
			return nil
		}))
		require.NoError(t, appOpt.Start(t.Context()))
		<-optDone
		require.Eventually(t, func() bool {
			return appOpt.backgroundWorkers[0].snapshot().State == workerFailed
		}, time.Second, 2*time.Millisecond)
		assert.False(t, appOpt.backgroundWorkers[0].snapshot().Ready)
		assert.True(t, appOpt.isReady(), "optional worker failure must not break App Readiness")
		require.NoError(t, appOpt.Stop(t.Context()))
	})

	t.Run("default one-shot Completed task keeps Readiness true", func(t *testing.T) {
		app := buildTestApp(t)
		done := make(chan struct{})
		app.Go("one-shot-warmup", func(*Context) error {
			defer close(done)
			return nil
		})
		require.NoError(t, app.Start(t.Context()))
		<-done
		require.Eventually(t, func() bool {
			return app.backgroundWorkers[0].snapshot().State == workerCompleted
		}, time.Second, 2*time.Millisecond)
		assert.True(t, app.isReady())
		require.NoError(t, app.Stop(t.Context()))
	})
}

func TestWorkerSupervision_ThreePhaseContextsRacesMailboxFullAndBorrowedSurvival(t *testing.T) {
	t.Run("Phase A cancels intakeCtx while Phase B preserves drainExecCtx until Phase C", func(t *testing.T) {
		app := buildTestApp(t)
		var intakeCanceledInPhaseB, drainAliveInPhaseB, unreadyInPhaseB atomic.Bool

		app.registerPhaseBDrain(func(context.Context) error {
			intakeCanceledInPhaseB.Store(app.intakeContext().Err() != nil)
			drainAliveInPhaseB.Store(app.drainExecutionContext().Err() == nil)
			unreadyInPhaseB.Store(!app.isReady())
			return nil
		})
		app.Go("bg", func(c *Context) error {
			<-c.Done()
			return c.Err()
		})

		require.NoError(t, app.Start(t.Context()))
		require.NoError(t, app.drainExecutionContext().Err())
		require.NoError(t, app.Stop(t.Context()))

		assert.True(t, intakeCanceledInPhaseB.Load())
		assert.True(t, drainAliveInPhaseB.Load())
		assert.True(t, unreadyInPhaseB.Load())
		assert.ErrorIs(t, app.drainExecutionContext().Err(), context.Canceled)
	})

	t.Run("Stop races with fast backoff timers without WaitGroup reuse or post-Stop launches", func(t *testing.T) {
		errTransient := errors.New("retry")
		for range 12 {
			app := buildTestApp(t)
			var runs atomic.Int32
			for i := range 8 {
				require.NoError(t, app.AddWorker(
					"racer-"+strings.Repeat("x", i+1),
					WorkerPolicy{
						Mode:           SuperviseRestart,
						BackoffInitial: time.Millisecond,
						BackoffMax:     2 * time.Millisecond,
						MaxRestarts:    100,
						RestartWindow:  time.Minute,
					},
					func(c *Context) error {
						runs.Add(1)
						if err := c.Err(); err != nil {
							return err
						}
						return errTransient
					},
				))
			}
			require.NoError(t, app.Start(t.Context()))
			time.Sleep(4 * time.Millisecond)
			stopErr := app.Stop(t.Context())
			if stopErr != nil {
				// Any worker already mid-execution when Stop closed intake cannot retry,
				// so its final error is surfaced in Stop() rather than swallowed.
				require.ErrorIs(t, stopErr, errTransient)
			}
			afterStop := runs.Load()
			time.Sleep(5 * time.Millisecond)
			assert.Equal(t, afterStop, runs.Load(), "no worker generation may launch after Stop completes")
		}
	})

	t.Run("controller mailbox saturation recovers via retryControllerNotify without losing worker restart", func(t *testing.T) {
		app := buildTestApp(t)
		gen1ReadyToFail := make(chan struct{})
		gen2Reached := make(chan struct{})
		var runs atomic.Int32

		require.NoError(t, app.AddWorker("saturated", WorkerPolicy{
			Mode:           SuperviseRestart,
			BackoffInitial: time.Millisecond,
			BackoffMax:     2 * time.Millisecond,
			MaxRestarts:    3,
		}, func(c *Context) error {
			if runs.Add(1) == 1 {
				<-gen1ReadyToFail
				return errors.New("fail while mailbox saturated")
			}
			close(gen2Reached)
			<-c.Done()
			return c.Err()
		}))

		require.NoError(t, app.Start(t.Context()))
		slot := app.backgroundWorkers[0]
		require.Eventually(t, func() bool {
			slot.mu.Lock()
			pid := slot.controllerPID
			slot.mu.Unlock()
			return pid != (actor.PID{})
		}, time.Second, time.Millisecond)

		slot.mu.Lock()
		pid := slot.controllerPID
		node := slot.node
		slot.mu.Unlock()

		// Saturate controller mailbox with no-op messages until ErrMailboxFull is observed.
		type noopMsg struct{}
		for range 2048 {
			if errors.Is(node.Send(pid, noopMsg{}), actor.ErrMailboxFull) {
				break
			}
		}
		close(gen1ReadyToFail)

		select {
		case <-gen2Reached:
		case <-time.After(2 * time.Second):
			t.Fatal("worker did not restart after controller mailbox saturation")
		}
		require.NoError(t, app.Stop(t.Context()))
	})

	t.Run("Stop before Start and Borrowed Logger/SQL survival", func(t *testing.T) {
		spyLog := newSpyClosableLogger()
		db, err := piSQL.OpenContext(
			t.Context(),
			config.NewSnapshot(map[string]string{
				"DB_DIALECT": "sqlite",
				"DB_NAME":    filepath.Join(t.TempDir(), "borrowed_worker.db"),
			}),
			spyLog,
			nil,
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		unstarted := buildTestApp(t, WithLogger(spyLog), WithSQL(db, Borrowed))
		require.NoError(t, unstarted.AddWorker("never-started", WorkerPolicy{Mode: SuperviseRestart}, func(*Context) error {
			t.Fatal("worker must not run on Stop-before-Start")
			return nil
		}))
		require.NoError(t, unstarted.Stop(t.Context()))
		require.ErrorIs(t, unstarted.Start(t.Context()), errAppAlreadyStopped)

		startedApp := buildTestApp(t, WithLogger(spyLog), WithSQL(db, Borrowed))
		workerRan := make(chan struct{})
		require.NoError(t, startedApp.AddWorker("db-reader", WorkerPolicy{Mode: SuperviseFatal, Required: true}, func(c *Context) error {
			var one int
			if err := c.SQL().QueryRowContext(c, "SELECT 1").Scan(&one); err != nil {
				return err
			}
			close(workerRan)
			<-c.Done()
			return c.Err()
		}))
		require.NoError(t, startedApp.Start(t.Context()))
		<-workerRan
		require.NoError(t, startedApp.Stop(t.Context()))

		// Neither borrowed logger nor borrowed SQL may be closed by App.Stop.
		assert.Zero(t, spyLog.closed.Load())
		var check int
		require.NoError(t, db.QueryRowContext(t.Context(), "SELECT 1").Scan(&check))
		assert.Equal(t, 1, check)
	})
}

func TestWorkerSupervision_ReviewRegressions(t *testing.T) {
	t.Run("P1 controller panic exceeding ShutdownTimeout revokes readiness and escalates ErrUnresponsive", func(t *testing.T) {
		var resourceClosed atomic.Bool
		var workerSawClosedResource atomic.Bool
		app := buildTestApp(t, WithResource(Resource{
			Name:      "owned-dep",
			Ownership: Owned,
			Start:     func(context.Context) error { return nil },
			Stop: func(context.Context) error {
				resourceClosed.Store(true)
				return nil
			},
		}))

		workerStarted := make(chan struct{})
		workerCanceled := make(chan struct{})
		releaseStuckCleanup := make(chan struct{})

		require.NoError(t, app.AddWorker("unresponsive-ctrl-worker", WorkerPolicy{
			Mode:            SuperviseRestart,
			Required:        true,
			WaitReady:       true,
			ShutdownTimeout: 30 * time.Millisecond,
		}, func(c *Context) error {
			require.NoError(t, c.MarkReady())
			close(workerStarted)
			<-c.Done()
			close(workerCanceled)
			// Outlive controller ShutdownTimeout (30ms) so Tao marks the controller ErrUnresponsive
			// and suppresses replacement.
			<-releaseStuckCleanup
			if resourceClosed.Load() {
				workerSawClosedResource.Store(true)
			}
			return c.Err()
		}))

		require.NoError(t, app.Start(t.Context()))
		<-workerStarted
		require.True(t, app.isReady())

		slot := app.backgroundWorkers[0]
		slot.mu.Lock()
		pid := slot.controllerPID
		node := slot.node
		slot.mu.Unlock()
		require.NoError(t, node.Send(pid, actorInjectPanicMsg{reason: "controller boom before slow cleanup"}))

		<-workerCanceled
		// Readiness must drop immediately as soon as the controller stops and cancels the worker,
		// even before ShutdownTimeout expires.
		assert.False(t, app.isReady())

		// Once ShutdownTimeout (30ms) expires, Tao marks the controller ErrUnresponsive;
		// Pi must observe controller loss immediately without waiting for the stuck worker to exit first.
		require.Eventually(t, func() bool {
			return app.isIntakeClosed() && slot.snapshot().State == workerFailed
		}, time.Second, 2*time.Millisecond)
		assert.False(t, app.isReady())
		assert.ErrorIs(t, slot.snapshot().LastError, actor.ErrUnresponsive)
		// Owned resource must still remain open until the stuck worker goroutine finishes.
		assert.False(t, resourceClosed.Load())

		close(releaseStuckCleanup)

		waitCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		waitErr := app.Wait(waitCtx)
		require.ErrorIs(t, waitErr, actor.ErrUnresponsive)
		assert.False(t, workerSawClosedResource.Load(), "owned resource closed while stuck worker still running")
		assert.True(t, resourceClosed.Load())
		assert.Equal(t, workerFailed, slot.snapshot().State)
	})

	t.Run("P1 direct StopActor on worker controller escalates unrecoverable controller loss", func(t *testing.T) {
		app := buildTestApp(t)
		ready := make(chan struct{})
		require.NoError(t, app.AddWorker("stopped-ctrl-worker", WorkerPolicy{
			Mode:      SuperviseRestart,
			Required:  true,
			WaitReady: true,
		}, func(c *Context) error {
			require.NoError(t, c.MarkReady())
			close(ready)
			<-c.Done()
			return c.Err()
		}))

		require.NoError(t, app.Start(t.Context()))
		<-ready
		slot := app.backgroundWorkers[0]
		require.NoError(t, slot.node.StopActor(t.Context(), slot.currentControllerPID()))

		waitCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		waitErr := app.Wait(waitCtx)
		require.Error(t, waitErr)
		assert.Contains(t, waitErr.Error(), "controller terminated")
		assert.Equal(t, workerFailed, slot.snapshot().State)
	})

	t.Run("P2 Start re-verifies current generation readiness when earlier worker restarts before sibling is ready", func(t *testing.T) {
		app := buildTestApp(t)
		app.workerJitter = func(maxDelay time.Duration) time.Duration { return maxDelay }

		aGen1Ready := make(chan struct{})
		failAGen1 := make(chan struct{})
		aGen2Started := make(chan struct{})
		allowAGen2Ready := make(chan struct{})
		allowBReady := make(chan struct{})
		bMarkedReady := make(chan struct{})
		var aRuns atomic.Int32

		require.NoError(t, app.AddWorker("worker-a", WorkerPolicy{
			Mode:           SuperviseRestart,
			Required:       true,
			WaitReady:      true,
			BackoffInitial: time.Millisecond,
			BackoffMax:     2 * time.Millisecond,
			MaxRestarts:    3,
		}, func(c *Context) error {
			if aRuns.Add(1) == 1 {
				require.NoError(t, c.MarkReady())
				close(aGen1Ready)
				<-failAGen1
				return errors.New("gen1 transient crash during sibling startup")
			}
			close(aGen2Started)
			<-allowAGen2Ready
			require.NoError(t, c.MarkReady())
			<-c.Done()
			return c.Err()
		}))

		require.NoError(t, app.AddWorker("worker-b", WorkerPolicy{
			Mode:      SuperviseFatal,
			Required:  true,
			WaitReady: true,
		}, func(c *Context) error {
			<-allowBReady
			require.NoError(t, c.MarkReady())
			close(bMarkedReady)
			<-c.Done()
			return c.Err()
		}))

		startErrCh := make(chan error, 1)
		go func() {
			startErrCh <- app.Start(t.Context())
		}()

		// 1. A Gen 1 becomes ready.
		<-aGen1Ready
		// 2. A Gen 1 fails and A Gen 2 enters Starting (not yet ready).
		close(failAGen1)
		<-aGen2Started
		// 3. B becomes ready while A Gen 2 is still unready.
		close(allowBReady)
		<-bMarkedReady

		// Start MUST NOT return nil while A Gen 2 is still in Starting!
		select {
		case err := <-startErrCh:
			t.Fatalf("Start returned prematurely while worker-a gen 2 was not ready: %v", err)
		case <-time.After(35 * time.Millisecond):
		}
		assert.False(t, app.isReady())

		// 4. Allow A Gen 2 to call MarkReady -> Start must now complete with isReady() == true.
		close(allowAGen2Ready)
		select {
		case err := <-startErrCh:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("Start did not complete after worker-a gen 2 marked ready")
		}
		assert.True(t, app.isReady())
		require.NoError(t, app.Stop(t.Context()))
	})

	t.Run("P2 restart budget is checked and deducted at actual re-execution rather than before backoff", func(t *testing.T) {
		app := buildTestApp(t)
		var jitterCall atomic.Int32
		// First backoff (140ms) exceeds RestartWindow (80ms); second backoff is 0ms.
		// Charging restartTimes before the 140ms backoff would let the first record expire
		// before Restart #1 even executed, allowing two actual restarts ~0ms apart.
		app.workerJitter = func(maxDelay time.Duration) time.Duration {
			if jitterCall.Add(1) == 1 {
				return maxDelay
			}
			return 0
		}

		boom := errors.New("rapid crash after long initial backoff")
		var runs atomic.Int32
		var restartExecTimes []time.Time
		var mu sync.Mutex

		require.NoError(t, app.AddWorker("budget-at-reexec", WorkerPolicy{
			Mode:           SuperviseRestart,
			Required:       true,
			BackoffInitial: 140 * time.Millisecond,
			BackoffMax:     140 * time.Millisecond,
			MaxRestarts:    1,
			RestartWindow:  80 * time.Millisecond,
		}, func(*Context) error {
			n := runs.Add(1)
			if n > 1 {
				mu.Lock()
				restartExecTimes = append(restartExecTimes, time.Now())
				mu.Unlock()
			}
			return boom
		}))

		require.NoError(t, app.Start(t.Context()))
		waitErr := app.Wait(t.Context())
		require.ErrorIs(t, waitErr, ErrWorkerRestartBudgetExceeded)
		require.ErrorIs(t, waitErr, boom)

		// Exactly 1 initial launch + 1 actual restart = 2 total runs; never a 2nd restart!
		assert.EqualValues(t, 2, runs.Load())
		mu.Lock()
		assert.Len(t, restartExecTimes, 1)
		mu.Unlock()
		assert.EqualValues(t, 1, app.backgroundWorkers[0].snapshot().WorkerRetries)
	})

	t.Run("P2 SuperviseRestart and SuperviseDegrade shutdown errors propagate to Stop and Wait", func(t *testing.T) {
		for _, mode := range []SupervisionMode{SuperviseRestart, SuperviseDegrade} {
			name := "restart"
			if mode == SuperviseDegrade {
				name = "degrade"
			}
			t.Run(name, func(t *testing.T) {
				app := buildTestApp(t)
				checkpointErr := errors.New("final checkpoint failed: " + name)
				ready := make(chan struct{})

				require.NoError(t, app.AddWorker("checkpoint-"+name, WorkerPolicy{
					Mode:      mode,
					Required:  true,
					WaitReady: true,
				}, func(c *Context) error {
					require.NoError(t, c.MarkReady())
					close(ready)
					<-c.Done()
					return checkpointErr
				}))

				require.NoError(t, app.Start(t.Context()))
				<-ready

				stopErr := app.Stop(t.Context())
				require.ErrorIs(t, stopErr, checkpointErr)

				waitErr := app.Wait(t.Context())
				require.ErrorIs(t, waitErr, checkpointErr)
				assert.Equal(t, workerFailed, app.backgroundWorkers[0].snapshot().State)

				// Verify RunContext also surfaces the final shutdown error on context cancellation.
				runApp := buildTestApp(t)
				require.NoError(t, runApp.AddWorker("runctx-checkpoint-"+name, WorkerPolicy{
					Mode:      mode,
					Required:  true,
					WaitReady: true,
				}, func(c *Context) error {
					require.NoError(t, c.MarkReady())
					<-c.Done()
					return checkpointErr
				}))
				runCtx, cancelRun := context.WithCancel(t.Context())
				runErrCh := make(chan error, 1)
				go func() {
					runErrCh <- runApp.RunContext(runCtx)
				}()
				require.Eventually(t, runApp.isReady, time.Second, time.Millisecond)
				cancelRun()
				require.ErrorIs(t, <-runErrCh, checkpointErr)
			})
		}
	})
}
