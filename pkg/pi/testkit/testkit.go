// Package testkit exercises public Pi APIs without requiring listening ports.
package testkit

import (
	"context"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/sllt/pi/pkg/pi"
)

type TB interface {
	Helper()
	Cleanup(func())
	Fatalf(string, ...any)
	Errorf(string, ...any)
}

// New uses Build and automatically stops owned resources at test cleanup.
func New(t TB, options ...pi.Option) *pi.App {
	t.Helper()
	app, err := pi.Build(options...)
	if err != nil {
		t.Fatalf("build Pi: %v", err)
		return nil
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := app.Stop(ctx); err != nil {
			t.Errorf("stop Pi: %v", err)
		}
	})
	return app
}

func Request(t TB, app *pi.App, req *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	h, err := app.HTTPHandler()
	if err != nil {
		t.Fatalf("compile Pi routes: %v", err)
		return nil
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func Handler(app *pi.App, fn pi.Handler, req *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	app.Handler(fn).ServeHTTP(w, req)
	return w
}

// Probe exposes lifecycle completion through channels, not sleeps.
type Probe struct {
	Started chan struct{}
	Stopped chan struct{}
}

func NewProbe(app *pi.App) *Probe {
	p := &Probe{Started: make(chan struct{}), Stopped: make(chan struct{})}
	app.OnStart(func(*pi.Context) error { close(p.Started); return nil })
	app.OnStop(func(*pi.Context) error { close(p.Stopped); return nil })
	return p
}
