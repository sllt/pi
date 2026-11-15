package pi

import (
	"context"
	"errors"
	"net/http"
	"time"
)

// HTTPHandler compiles registered routes without binding a socket. Finish all
// route/middleware registration before calling it; the compiled router is shared
// with Start. Resources used by handlers still require Start or host injection.
func (a *App) HTTPHandler() (http.Handler, error) {
	if a == nil || a.httpServer == nil {
		return nil, errors.New("application has no HTTP router")
	}
	if err := a.ensureHTTPCompiled(); err != nil {
		return nil, err
	}
	return a.httpServer.router, nil
}

// Handler adapts one Pi handler, including panic/timeout/response semantics.
func (a *App) Handler(fn Handler) http.Handler {
	var rt *runtimeKernel
	var timeout time.Duration
	if a != nil {
		rt = a.kernel
		if rt != nil {
			rt.freeze()
		}
		timeout = a.getRequestTimeout()
	}
	return handler{function: fn, rt: rt, requestTimeout: timeout}
}

// NewContext creates a context using the application's dependencies. Callers
// must supply any verified identity; this function does not grant authority.
func (a *App) NewContext(ctx context.Context) *Context {
	if ctx == nil {
		ctx = context.Background()
	}
	var rt *runtimeKernel
	if a != nil {
		rt = a.kernel
	}
	return newContext(nil, noopRequest{ctx: ctx}, rt)
}
