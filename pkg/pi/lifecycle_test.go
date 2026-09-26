package pi

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errBackgroundWorkerFailed = errors.New("background worker failed")

func TestApp_OnStop(t *testing.T) {
	t.Setenv("METRICS_PORT", "0")

	t.Run("runs hooks in reverse order", func(t *testing.T) {
		app := New()
		order := make([]int, 0, 2)

		app.OnStop(func(_ *Context) error {
			order = append(order, 1)
			return nil
		})
		app.OnStop(func(_ *Context) error {
			order = append(order, 2)
			return nil
		})

		err := app.runOnStopHooks(t.Context())

		require.NoError(t, err)
		assert.Equal(t, []int{2, 1}, order)
	})

	t.Run("joins returned errors and recovered panics", func(t *testing.T) {
		app := New()
		errHook := errors.New("stop hook failed")

		app.OnStop(func(_ *Context) error {
			return errHook
		})
		app.OnStop(func(_ *Context) error {
			panic("stop panic")
		})

		err := app.runOnStopHooks(t.Context())

		require.Error(t, err)
		assert.ErrorIs(t, err, errHook)
		assert.Contains(t, err.Error(), "panicked")
	})
}

func TestAppGo_Validation(t *testing.T) {
	t.Setenv("METRICS_PORT", "0")

	app := New()

	app.Go("", func(*Context) error { return nil })
	assert.Empty(t, app.backgroundWorkers)

	app.Go("worker", nil)
	assert.Empty(t, app.backgroundWorkers)

	app.Go("worker", func(*Context) error { return nil })
	require.Len(t, app.backgroundWorkers, 1)

	app.Go("worker", func(*Context) error { return nil })
	assert.Len(t, app.backgroundWorkers, 1)

	app.runtimeCtx = context.Background()
	app.Go("too-late", func(*Context) error { return nil })
	assert.Len(t, app.backgroundWorkers, 1)
}

func TestAppGo_WorkerErrorRequestsShutdown(t *testing.T) {
	t.Setenv("METRICS_PORT", "0")

	app := New()
	app.Go("failing-worker", func(*Context) error {
		return errBackgroundWorkerFailed
	})

	ctx := app.initRuntime(context.Background())
	var wg sync.WaitGroup

	app.startBackgroundWorkers(ctx, &wg)
	wg.Wait()

	require.ErrorIs(t, context.Cause(ctx), errBackgroundWorkerFailed)
	require.NoError(t, app.waitForRuntimeTasks(t.Context()))
}

func TestAppGo_WorkerHonorsCancellation(t *testing.T) {
	t.Setenv("METRICS_PORT", "0")

	app := New()
	started := make(chan struct{})
	stopped := make(chan struct{})

	app.Go("cancellable-worker", func(ctx *Context) error {
		close(started)
		<-ctx.Done()
		close(stopped)
		return ctx.Err()
	})

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	app.startBackgroundWorkers(ctx, &wg)
	<-started
	cancel()

	require.NoError(t, app.waitForRuntimeTasks(t.Context()))
	wg.Wait()

	select {
	case <-stopped:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected background worker to stop after context cancellation")
	}
}

func TestAppRun_WaitsForOnStopHooks(t *testing.T) {
	t.Setenv("METRICS_PORT", "0")

	app := New()
	onStopCalled := make(chan struct{})

	app.Go("failing-worker", func(*Context) error {
		return errBackgroundWorkerFailed
	})
	app.OnStop(func(_ *Context) error {
		time.Sleep(50 * time.Millisecond)
		close(onStopCalled)
		return nil
	})

	app.Run()

	select {
	case <-onStopCalled:
	case <-time.After(time.Second):
		t.Fatal("expected App.Run to wait for OnStop hook completion")
	}
}

func TestAppShutdown_CancelsBackgroundWorkers(t *testing.T) {
	t.Setenv("METRICS_PORT", "0")

	app := New()
	started := make(chan struct{})
	stopped := make(chan struct{})

	app.Go("shutdown-aware-worker", func(ctx *Context) error {
		close(started)
		<-ctx.Done()
		close(stopped)
		return ctx.Err()
	})

	runtimeCtx := app.initRuntime(context.Background())
	var wg sync.WaitGroup
	app.startBackgroundWorkers(runtimeCtx, &wg)
	<-started

	shutdownCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	require.NoError(t, app.Shutdown(shutdownCtx))
	wg.Wait()

	select {
	case <-stopped:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected Shutdown to cancel background worker")
	}
}

func TestAppStartStop_Idempotency(t *testing.T) {
	t.Setenv("METRICS_PORT", "0")

	app := New()

	require.NoError(t, app.Start(t.Context()))
	require.NoError(t, app.Start(t.Context()))
	require.NoError(t, app.Stop(t.Context()))
	require.NoError(t, app.Stop(t.Context()))
	require.ErrorIs(t, app.Start(t.Context()), errAppAlreadyStopped)
}

func TestAppStart_OnStartFailureRollsBack(t *testing.T) {
	t.Setenv("METRICS_PORT", "0")

	app := New()
	startErr := errors.New("startup failed")
	onStopCalled := make(chan struct{})

	app.OnStart(func(_ *Context) error {
		return startErr
	})
	app.OnStop(func(_ *Context) error {
		close(onStopCalled)
		return nil
	})

	err := app.Start(t.Context())

	require.ErrorIs(t, err, startErr)
	select {
	case <-onStopCalled:
	case <-time.After(time.Second):
		t.Fatal("expected OnStop to run during Start rollback")
	}
	require.ErrorIs(t, app.Start(t.Context()), errAppAlreadyStopped)
}

func TestAppStart_ReturnsGRPCListenFailure(t *testing.T) {
	t.Setenv("METRICS_PORT", "0")
	httpPort := getFreeLifecyclePort(t)
	t.Setenv("HTTP_PORT", strconv.Itoa(httpPort))

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	port := listener.Addr().(*net.TCPAddr).Port

	app := New()
	app.GET("/rollback", func(*Context) (any, error) { return "ok", nil })
	app.grpcRegistered = true
	app.grpcServer.port = port

	err = app.Start(t.Context())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "gRPC port")
	// HTTP started before the gRPC failure and must be closed by rollback.
	reused, listenErr := net.Listen("tcp", ":"+strconv.Itoa(httpPort))
	require.NoError(t, listenErr)
	t.Cleanup(func() { _ = reused.Close() })
	require.NoError(t, app.Stop(t.Context()))
}

func TestAppStartStop_HTTPServer(t *testing.T) {
	t.Setenv("METRICS_PORT", "0")

	port := getFreeLifecyclePort(t)
	t.Setenv("HTTP_PORT", strconv.Itoa(port))

	app := New()
	app.GET("/lifecycle", func(*Context) (any, error) {
		return map[string]string{"status": "ok"}, nil
	})

	require.NoError(t, app.Start(t.Context()))
	require.NoError(t, app.Start(t.Context()), "repeated startup must reuse the HTTP listener and compiled routes")
	t.Cleanup(func() { _ = app.Stop(t.Context()) })

	resp, err := http.Get("http://127.0.0.1:" + strconv.Itoa(port) + "/lifecycle")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, app.Stop(t.Context()))
	require.NoError(t, app.Stop(t.Context()))
}

func TestAppRunContext_ReturnsBackgroundWorkerError(t *testing.T) {
	t.Setenv("METRICS_PORT", "0")

	app := New()
	app.Go("failing-worker", func(*Context) error {
		return errBackgroundWorkerFailed
	})

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	err := app.RunContext(ctx)

	require.ErrorIs(t, err, errBackgroundWorkerFailed)
}

func getFreeLifecyclePort(t *testing.T) int {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	return listener.Addr().(*net.TCPAddr).Port
}
