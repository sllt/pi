package pi

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sllt/tao/actor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildCronTestApp(t *testing.T, extraOpts ...Option) *App {
	t.Helper()
	opts := []Option{
		WithConfig(map[string]string{
			"LOG_LEVEL":       "ERROR",
			"HTTP_ENABLED":    "false",
			"GRPC_ENABLED":    "false",
			"METRICS_ENABLED": "false",
		}),
	}
	opts = append(opts, extraOpts...)
	app, err := Build(opts...)
	require.NoError(t, err)
	return app
}

func findCronSlot(t *testing.T, app *App, name string) *cronJobSlot {
	t.Helper()
	for _, slot := range app.cronJobSlots() {
		if slot.name == name {
			return slot
		}
	}
	t.Fatalf("cron slot %q not found", name)
	return nil
}

func TestCronSupervision_ZeroTriggersBeforeStartAndMethodValueCompat(t *testing.T) {
	var runs atomic.Int32

	// Verify method-value and interface compatibility of legacy signatures.
	type legacyCronRegistrar interface {
		AddCronJob(schedule, jobName string, job CronFunc)
	}
	type legacyTabRegistrar interface {
		AddJob(schedule, jobName string, fn CronFunc) error
	}

	app := buildCronTestApp(t)
	var appReg legacyCronRegistrar = app
	addCronJobFn := app.AddCronJob
	var tabReg legacyTabRegistrar = app.Cron()
	addJobFn := app.Cron().AddJob

	appReg.AddCronJob("* * * * * *", "legacy-iface", func(*Context) { runs.Add(1) })
	addCronJobFn("* * * * * *", "legacy-method-val", func(*Context) { runs.Add(1) })
	require.NoError(t, tabReg.AddJob("* * * * * *", "tab-iface", func(*Context) { runs.Add(1) }))
	require.NoError(t, addJobFn("* * * * * *", "tab-method-val", func(*Context) { runs.Add(1) }))
	require.NoError(t, app.AddCron("* * * * * *", "modern-cron", func(*Context) error {
		runs.Add(1)
		return nil
	}))

	// Pure registration: sleep past a second boundary before Start and verify zero triggers.
	time.Sleep(1200 * time.Millisecond)
	assert.Zero(t, runs.Load(), "cron jobs must never fire before App.Start")

	// Stop-before-Start should also stay clean with zero triggers.
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, app.Stop(stopCtx))
	assert.Zero(t, runs.Load())
}

func TestCronSupervision_ValidationErrors(t *testing.T) {
	app := buildCronTestApp(t)

	require.ErrorIs(t, app.AddCron("* * * * * *", "  ", func(*Context) error { return nil }), errCronJobNameEmpty)
	require.ErrorIs(t, app.AddCron("* * * * * *", "nil-fn", nil), errCronJobHandlerNil)
	require.Error(t, app.AddCron("*/0 * * * *", "bad-sched", func(*Context) error { return nil }))
	require.ErrorIs(t, app.AddCron("* * * * * *", "bad-overlap", func(*Context) error { return nil }, CronJobOptions{
		Overlap: CronOverlapPolicy(99),
	}), errCronInvalidOverlap)
	require.ErrorIs(t, app.AddCron("* * * * * *", "bad-conc", func(*Context) error { return nil }, CronJobOptions{
		Overlap:        CronSkipIfRunning,
		MaxConcurrency: 2,
	}), errCronInvalidConcurrency)
	require.ErrorIs(t, app.AddCron("* * * * * *", "neg-timeout", func(*Context) error { return nil }, CronJobOptions{
		Timeout: -time.Second,
	}), errCronNegativeTimeout)
	require.ErrorIs(t, app.AddCron("* * * * * *", "multi-opts", func(*Context) error { return nil }, CronJobOptions{}, CronJobOptions{}), errCronMultipleOptions)

	require.NoError(t, app.AddCron("* * * * * *", "dup", func(*Context) error { return nil }))
	require.ErrorIs(t, app.AddCron("* * * * * *", "dup", func(*Context) error { return nil }), errCronJobDuplicateName)

	require.NoError(t, app.Start(context.Background()))
	t.Cleanup(func() { _ = app.Stop(context.Background()) })

	require.ErrorIs(t, app.AddCron("* * * * * *", "late", func(*Context) error { return nil }), errCronRegisterTooLate)
	require.ErrorIs(t, app.Cron().AddJob("* * * * * *", "late-legacy", func(*Context) {}), errCronRegisterTooLate)
}

func TestCronSupervision_SkipIfRunningOverlap(t *testing.T) {
	var (
		clockMu sync.Mutex
		fakeNow = time.Date(2026, 11, 13, 12, 0, 0, 990_000_000, time.UTC)
	)
	app := buildCronTestApp(t)
	app.cronNow = func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return fakeNow
	}

	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	var active, maxActive, runs atomic.Int32

	require.NoError(t, app.AddCron("* * * * * *", "skip-job", func(ctx *Context) error {
		cur := active.Add(1)
		for {
			prev := maxActive.Load()
			if cur <= prev || maxActive.CompareAndSwap(prev, cur) {
				break
			}
		}
		defer active.Add(-1)

		if runs.Add(1) == 1 {
			close(firstEntered)
			select {
			case <-releaseFirst:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}, CronJobOptions{Overlap: CronSkipIfRunning, Location: time.UTC}))

	require.NoError(t, app.Start(context.Background()))
	t.Cleanup(func() { _ = app.Stop(context.Background()) })

	// Advance clock by 1s to trigger first run.
	clockMu.Lock()
	fakeNow = fakeNow.Add(time.Second)
	clockMu.Unlock()

	select {
	case <-firstEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first cron execution")
	}

	// Advance clock by 2 more seconds while first run is still blocked.
	slot := findCronSlot(t, app, "skip-job")
	clockMu.Lock()
	fakeNow = fakeNow.Add(2 * time.Second)
	clockMu.Unlock()

	require.Eventually(t, func() bool {
		return slot.snapshot().SkippedTotal >= 2
	}, 2*time.Second, 10*time.Millisecond)

	assert.Equal(t, int32(1), maxActive.Load())
	assert.Equal(t, int32(1), runs.Load())

	close(releaseFirst)
	require.NoError(t, app.Stop(context.Background()))
}

func TestCronSupervision_QueueNextCoalescesAndStopDropsPending(t *testing.T) {
	var (
		clockMu sync.Mutex
		fakeNow = time.Date(2026, 11, 13, 12, 0, 0, 990_000_000, time.UTC)
	)
	app := buildCronTestApp(t)
	app.cronNow = func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return fakeNow
	}

	run1Entered := make(chan struct{})
	releaseRun1 := make(chan struct{})
	run2Entered := make(chan struct{})
	releaseRun2 := make(chan struct{})
	var runs atomic.Int32

	require.NoError(t, app.AddCron("* * * * * *", "queue-job", func(ctx *Context) error {
		n := runs.Add(1)
		switch n {
		case 1:
			close(run1Entered)
			<-releaseRun1
		case 2:
			close(run2Entered)
			select {
			case <-releaseRun2:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}, CronJobOptions{Overlap: CronQueueNext, Location: time.UTC}))

	require.NoError(t, app.Start(context.Background()))
	slot := findCronSlot(t, app, "queue-job")

	// Trigger run #1 at +1s.
	clockMu.Lock()
	fakeNow = fakeNow.Add(time.Second)
	clockMu.Unlock()
	<-run1Entered

	// Advance +3s while run #1 is running: 1 queuedNext, 2 skipped.
	clockMu.Lock()
	fakeNow = fakeNow.Add(3 * time.Second)
	clockMu.Unlock()

	require.Eventually(t, func() bool {
		snap := slot.snapshot()
		return snap.QueuedNext && snap.SkippedTotal >= 2
	}, 2*time.Second, 10*time.Millisecond)

	// Release run #1 -> coalesced run #2 must start automatically without another tick!
	close(releaseRun1)
	select {
	case <-run2Entered:
	case <-time.After(2 * time.Second):
		t.Fatal("expected coalesced queued run #2 to start")
	}

	// Queue one more pending trigger during run #2, then call Stop: pending QueueNext must be dropped!
	clockMu.Lock()
	fakeNow = fakeNow.Add(time.Second)
	clockMu.Unlock()
	require.Eventually(t, func() bool {
		return slot.snapshot().QueuedNext
	}, 2*time.Second, 10*time.Millisecond)

	close(releaseRun2)
	// Stop immediately before releasing or right around stop: let's test dropping queued trigger deterministically!
	require.NoError(t, app.Stop(context.Background()))

	// Also verify deterministic drop while run is blocked when Stop begins:
	app2 := buildCronTestApp(t)
	var clock2Mu sync.Mutex
	fakeNow2 := time.Date(2026, 11, 13, 13, 0, 0, 990_000_000, time.UTC)
	app2.cronNow = func() time.Time {
		clock2Mu.Lock()
		defer clock2Mu.Unlock()
		return fakeNow2
	}
	entered2 := make(chan struct{})
	var runs2 atomic.Int32
	require.NoError(t, app2.AddCron("* * * * * *", "drop-on-stop", func(ctx *Context) error {
		if runs2.Add(1) == 1 {
			close(entered2)
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}, CronJobOptions{Overlap: CronQueueNext, Location: time.UTC}))
	require.NoError(t, app2.Start(context.Background()))
	slot2 := findCronSlot(t, app2, "drop-on-stop")

	clock2Mu.Lock()
	fakeNow2 = fakeNow2.Add(time.Second)
	clock2Mu.Unlock()
	<-entered2

	clock2Mu.Lock()
	fakeNow2 = fakeNow2.Add(time.Second)
	clock2Mu.Unlock()
	require.Eventually(t, func() bool {
		return slot2.snapshot().QueuedNext
	}, 2*time.Second, 10*time.Millisecond)

	require.NoError(t, app2.Stop(context.Background()))
	snap2 := slot2.snapshot()
	assert.Equal(t, int32(1), runs2.Load(), "Stop must drop queued trigger instead of running a second execution")
	assert.GreaterOrEqual(t, snap2.DroppedQueueTotal, uint64(1))
}

func TestCronSupervision_AllowConcurrentTimeoutIgnoringCancelHoldsSlotAndResource(t *testing.T) {
	var (
		clockMu sync.Mutex
		fakeNow = time.Date(2026, 11, 13, 14, 0, 0, 990_000_000, time.UTC)
	)

	unblockStuck := make(chan struct{})
	var (
		active        atomic.Int32
		maxActive     atomic.Int32
		runsStarted   atomic.Int32
		resourceClose atomic.Bool
	)

	app := buildCronTestApp(t, WithResource(Resource{
		Name:      "owned-db",
		Ownership: Owned,
		Stop: func(context.Context) error {
			assert.Zero(t, active.Load(), "owned resource must not close while timed-out cron goroutines are still executing")
			resourceClose.Store(true)
			return nil
		},
	}))
	app.cronNow = func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return fakeNow
	}

	bothTimedOut := make(chan struct{})
	var timedOutCount atomic.Int32

	require.NoError(t, app.AddCron("* * * * * *", "conc-job", func(ctx *Context) error {
		cur := active.Add(1)
		for {
			prev := maxActive.Load()
			if cur <= prev || maxActive.CompareAndSwap(prev, cur) {
				break
			}
		}
		defer active.Add(-1)
		runsStarted.Add(1)

		// Wait for per-run Timeout to cancel ctx, then deliberately ignore cancellation until unblockStuck!
		<-ctx.Done()
		if timedOutCount.Add(1) == 2 {
			close(bothTimedOut)
		}
		<-unblockStuck
		return ctx.Err()
	}, CronJobOptions{
		Overlap:        CronAllowConcurrent,
		MaxConcurrency: 2,
		Timeout:        40 * time.Millisecond,
		Location:       time.UTC,
	}))

	require.NoError(t, app.Start(context.Background()))
	slot := findCronSlot(t, app, "conc-job")

	// Advance 2 seconds to start 2 concurrent executions, both of which time out after 40ms but stay alive.
	clockMu.Lock()
	fakeNow = fakeNow.Add(2 * time.Second)
	clockMu.Unlock()

	select {
	case <-bothTimedOut:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for both cron executions to hit Timeout")
	}

	// Advance 2 more seconds while both timed-out executions still ignore ctx.Done():
	// concurrency limit (2) MUST prevent starting a 3rd execution!
	clockMu.Lock()
	fakeNow = fakeNow.Add(2 * time.Second)
	clockMu.Unlock()

	require.Eventually(t, func() bool {
		return slot.snapshot().SkippedTotal >= 2
	}, 2*time.Second, 10*time.Millisecond)
	assert.Equal(t, int32(2), maxActive.Load())
	assert.Equal(t, int32(2), runsStarted.Load())

	// Caller with short Stop budget times out, while sole cleanup owner keeps Owned resource open.
	shortCtx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	stopErr := app.Stop(shortCtx)
	require.ErrorIs(t, stopErr, context.DeadlineExceeded)
	assert.False(t, resourceClose.Load(), "owned resource must remain open while stuck cron tasks are still running")

	// Release the stuck goroutines and verify Wait completes and closes Owned resource.
	close(unblockStuck)
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer waitCancel()
	err := app.Wait(waitCtx)
	require.Error(t, err)
	assert.True(t, resourceClose.Load(), "owned resource must close after stuck cron tasks exit")
}

func TestCronSupervision_TimezonePanicAndActorCrashRecovery(t *testing.T) {
	tzPlus8 := time.FixedZone("UTC+8", 8*3600)
	var (
		clockMu sync.Mutex
		// 15:59:59.990 UTC == 23:59:59.990 UTC+8; +1s will be 00:00:00 in UTC+8 (hour 0) vs 16:00:00 in UTC (hour 16).
		fakeNow = time.Date(2026, 11, 13, 15, 59, 59, 990_000_000, time.UTC)
	)

	app := buildCronTestApp(t)
	app.cronNow = func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return fakeNow
	}

	var tzRuns, utcRuns, panicJobRuns atomic.Int32
	require.NoError(t, app.AddCron("0 0 0 * * *", "midnight-plus8", func(*Context) error {
		tzRuns.Add(1)
		return nil
	}, CronJobOptions{Location: tzPlus8}))

	require.NoError(t, app.AddCron("0 0 0 * * *", "midnight-utc", func(*Context) error {
		utcRuns.Add(1)
		return nil
	}, CronJobOptions{Location: time.UTC}))

	require.NoError(t, app.AddCron("* * * * * *", "panic-then-ok", func(*Context) error {
		if panicJobRuns.Add(1) == 1 {
			panic("boom inside cron job")
		}
		return errors.New("recoverable job error")
	}, CronJobOptions{Location: time.UTC}))

	require.NoError(t, app.Start(context.Background()))
	t.Cleanup(func() { _ = app.Stop(context.Background()) })

	// Advance +1s -> 16:00:00 UTC / 00:00:00 UTC+8.
	clockMu.Lock()
	fakeNow = fakeNow.Add(time.Second)
	clockMu.Unlock()

	panicSlot := findCronSlot(t, app, "panic-then-ok")
	require.Eventually(t, func() bool {
		return tzRuns.Load() == 1 && panicSlot.snapshot().PanicsTotal == 1
	}, 2*time.Second, 10*time.Millisecond)
	assert.Zero(t, utcRuns.Load(), "UTC midnight job must not fire at 16:00 UTC")

	// Inject controller actor crash on both cron-scheduler and panicSlot's cronJobActor;
	// verify cron-sup restarts both and subsequent ticks still fire without state loss!
	app.runtimeMu.Lock()
	node := app.actorRT.node
	app.runtimeMu.Unlock()
	require.NoError(t, node.Send(actor.Ref{Path: "pi-root/cron-sup/cron-scheduler"}, actorInjectPanicMsg{reason: "sched crash"}))
	require.NoError(t, node.Send(panicSlot.actorRef, actorInjectPanicMsg{reason: "job controller crash"}))

	require.Eventually(t, func() bool {
		return panicSlot.snapshot().ActorRestarts >= 1
	}, 2*time.Second, 10*time.Millisecond)

	clockMu.Lock()
	fakeNow = fakeNow.Add(time.Second)
	clockMu.Unlock()

	require.Eventually(t, func() bool {
		snap := panicSlot.snapshot()
		return snap.RunsTotal >= 2 && snap.PanicsTotal == 1 && snap.FailuresTotal >= 2
	}, 2*time.Second, 10*time.Millisecond)
}
