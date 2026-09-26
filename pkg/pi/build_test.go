package pi_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sllt/pi/pkg/pi"
	"github.com/sllt/pi/pkg/pi/config"
	piSQL "github.com/sllt/pi/pkg/pi/datasource/sql"
	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/sllt/pi/pkg/pi/testkit"
	"github.com/stretchr/testify/require"
)

func TestBuildPureIsolationAndSQL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.db")
	values := map[string]string{"DB_DIALECT": "sqlite", "DB_NAME": path, "HTTP_ENABLED": "false", "VALIDATION_LOCALE": "en"}
	a := testkit.New(t, pi.WithConfig(values), pi.WithManagedSQL())
	values["DB_NAME"] = "changed"
	t.Setenv("DB_NAME", "ambient")
	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err))
	require.Equal(t, path, a.Config.Get("DB_NAME"))
	db := a.Container().SQL
	require.NotNil(t, db)
	_, err = db.ExecContext(t.Context(), "SELECT 1")
	require.ErrorIs(t, err, piSQL.ErrNotStarted)
	var n int
	require.ErrorIs(t, db.QueryRowContext(t.Context(), "SELECT 1").Scan(&n), piSQL.ErrNotStarted)
	require.NoError(t, a.Start(t.Context()))
	require.Same(t, db, a.Container().SQL)
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT 1").Scan(&n))
	require.Equal(t, 1, n)
	b := testkit.New(t, pi.WithConfig(map[string]string{"VALIDATION_LOCALE": "zh"}))
	for _, app := range []*pi.App{a, b} {
		app.POST("/validate", func(c *pi.Context) (any, error) {
			var v struct {
				Email string `json:"email" binding:"required,email"`
			}
			return nil, c.Bind(&v)
		})
	}
	request := func(app *pi.App) string {
		r := httptest.NewRequest("POST", "/validate", strings.NewReader(`{}`))
		r.Header.Set("Content-Type", "application/json")
		return testkit.Request(t, app, r).Body.String()
	}
	require.Contains(t, request(a), "required")
	require.Contains(t, request(b), "必填")
}

func TestBuildStartupBudgetAndFatalWait(t *testing.T) {
	app := testkit.New(t)
	entered := make(chan context.Context, 1)
	fail := make(chan struct{})
	boom := errors.New("worker failed")
	app.Go("worker", func(c *pi.Context) error { entered <- c.Context; <-fail; return boom })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	require.NoError(t, app.Start(ctx))
	cancel()
	runtimeCtx := <-entered
	require.NoError(t, runtimeCtx.Err())
	waitCtx, waitCancel := context.WithCancel(t.Context())
	waitCancel()
	require.ErrorIs(t, app.Wait(waitCtx), context.Canceled)
	close(fail)
	deadline, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	require.ErrorIs(t, app.Wait(deadline), boom)
}

func TestBuildResourceOwnershipRollback(t *testing.T) {
	var order []string
	boom := errors.New("open failed")
	app, err := pi.Build(
		pi.WithResource(pi.Resource{Name: "first", Ownership: pi.Owned, Start: func(context.Context) error { order = append(order, "start first"); return nil }, Stop: func(context.Context) error { order = append(order, "stop first"); return nil }}),
		pi.WithResource(pi.Resource{Name: "borrowed", Ownership: pi.Borrowed, Start: func(context.Context) error { panic("borrowed start") }, Stop: func(context.Context) error { panic("borrowed stop") }}),
		pi.WithResource(pi.Resource{Name: "second", Ownership: pi.Owned, Start: func(context.Context) error { order = append(order, "start second"); return boom }, Stop: func(context.Context) error { order = append(order, "stop second"); return nil }}),
	)
	require.NoError(t, err)
	require.Empty(t, order)
	require.ErrorIs(t, app.Start(t.Context()), boom)
	require.ErrorIs(t, app.Wait(t.Context()), boom)
	require.Equal(t, []string{"start first", "start second", "stop second", "stop first"}, order)
	require.NoError(t, app.Stop(t.Context()))
}

func TestBuildConcurrentLifecycleAndTimeout(t *testing.T) {
	var starts, stops atomic.Int32
	app := testkit.New(t, pi.WithResource(pi.Resource{Name: "owned", Ownership: pi.Owned, Start: func(context.Context) error { starts.Add(1); return nil }, Stop: func(context.Context) error { stops.Add(1); return nil }}))
	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := app.Start(t.Context()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := app.Stop(t.Context()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	require.EqualValues(t, 1, starts.Load())
	require.EqualValues(t, 1, stops.Load())
	timed, err := pi.Build(pi.WithResource(pi.Resource{Name: "slow", Ownership: pi.Owned, Start: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}))
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	require.ErrorIs(t, timed.Start(ctx), context.DeadlineExceeded)
	require.ErrorIs(t, timed.Wait(t.Context()), context.DeadlineExceeded)
}

func TestBuildBorrowedSQLSurvivesStop(t *testing.T) {
	db, err := piSQL.OpenContext(t.Context(), config.NewSnapshot(map[string]string{"DB_DIALECT": "sqlite", "DB_NAME": filepath.Join(t.TempDir(), "borrowed.db")}), logging.NewLogger(logging.ERROR), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	app, err := pi.Build(pi.WithSQL(db, pi.Borrowed))
	require.NoError(t, err)
	require.NoError(t, app.Start(t.Context()))
	require.NoError(t, app.Stop(t.Context()))
	var n int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT 1").Scan(&n))
	require.Equal(t, 1, n)
}

func TestBuildStopTimeoutDoesNotReportCompletion(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	app, err := pi.Build(pi.WithResource(pi.Resource{Name: "slow close", Ownership: pi.Owned, Stop: func(context.Context) error { close(entered); <-release; return nil }}))
	require.NoError(t, err)
	require.NoError(t, app.Start(t.Context()))
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, app.Stop(ctx), context.DeadlineExceeded)
	<-entered
	waitCtx, end := context.WithTimeout(t.Context(), time.Millisecond)
	defer end()
	require.ErrorIs(t, app.Wait(waitCtx), context.DeadlineExceeded)
	close(release)
	require.ErrorIs(t, app.Wait(t.Context()), context.DeadlineExceeded)
}
