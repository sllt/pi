package pi

import (
	"context"
	"errors"
	"net/http"
)

// HTTPHandler compiles registered routes without binding a socket. Finish all
// route/middleware registration before calling it; the compiled router is shared
// with Start. Resources used by handlers still require Start or host injection.
func (a *App) HTTPHandler() (http.Handler, error) {
	if a.httpServer == nil {
		return nil, errors.New("application has no HTTP router")
	}
	a.httpServerSetup()
	return a.httpServer.router, nil
}

// Handler adapts one Pi handler, including panic/timeout/response semantics.
func (a *App) Handler(fn Handler) http.Handler {
	return handler{function: fn, container: a.container, requestTimeout: a.getRequestTimeout()}
}

// NewContext creates a context using the application's dependencies. Callers
// must supply any verified identity; this function does not grant authority.
func (a *App) NewContext(ctx context.Context) *Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return newContext(nil, noopRequest{ctx: ctx}, a.container)
}
