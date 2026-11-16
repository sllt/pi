package pi

import (
	"net/http"
	"strconv"
	"time"
)

// GET adds a typed or legacy handler for HTTP GET method for a route pattern.
func (a *App) GET(pattern string, handler any, opts ...EndpointOption) {
	a.registerUnifiedRoute(http.MethodGet, pattern, handler, opts...)
}

// PUT adds a typed or legacy handler for HTTP PUT method for a route pattern.
func (a *App) PUT(pattern string, handler any, opts ...EndpointOption) {
	a.registerUnifiedRoute(http.MethodPut, pattern, handler, opts...)
}

// POST adds a typed or legacy handler for HTTP POST method for a route pattern.
func (a *App) POST(pattern string, handler any, opts ...EndpointOption) {
	a.registerUnifiedRoute(http.MethodPost, pattern, handler, opts...)
}

// DELETE adds a typed or legacy handler for HTTP DELETE method for a route pattern.
func (a *App) DELETE(pattern string, handler any, opts ...EndpointOption) {
	a.registerUnifiedRoute(http.MethodDelete, pattern, handler, opts...)
}

// PATCH adds a typed or legacy handler for HTTP PATCH method for a route pattern.
func (a *App) PATCH(pattern string, handler any, opts ...EndpointOption) {
	a.registerUnifiedRoute(http.MethodPatch, pattern, handler, opts...)
}

// HEAD adds a typed or legacy handler for HTTP HEAD method for a route pattern.
func (a *App) HEAD(pattern string, handler any, opts ...EndpointOption) {
	a.registerUnifiedRoute(http.MethodHead, pattern, handler, opts...)
}

// OPTIONS adds a typed or legacy handler for HTTP OPTIONS method for a route pattern.
func (a *App) OPTIONS(pattern string, handler any, opts ...EndpointOption) {
	a.registerUnifiedRoute(http.MethodOptions, pattern, handler, opts...)
}

func (a *App) registerUnifiedRoute(method, pattern string, handler any, opts ...EndpointOption) {
	if a == nil || a.httpServer == nil || a.httpServer.registry == nil {
		return
	}
	a.rootRouteGroup().addUnifiedRoute(method, pattern, handler, opts...)
}

func (a *App) add(method, pattern string, h Handler) {
	if a == nil || a.httpServer == nil || a.httpServer.registry == nil {
		return
	}
	reg := a.httpServer.registry
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if !a.canMutateRoutesLocked("register routes") {
		return
	}

	a.ensureHTTPAvailable()

	reg.root.routes = append(reg.root.routes, RouteDef{
		Method:  method,
		Pattern: pattern,
		Handler: h,
	})
	reg.markDirtyLocked()
}

func (a *App) addSystemRouteLocked(method, pattern string, h Handler) {
	if a == nil || a.httpServer == nil || a.httpServer.registry == nil {
		return
	}
	reg := a.httpServer.registry
	if reg.compiled {
		return
	}

	a.ensureHTTPAvailable()

	idx := len(reg.root.routes)
	reg.root.routes = append(reg.root.routes, RouteDef{
		Method:  method,
		Pattern: pattern,
		Handler: h,
	})
	reg.root.setRouteMeta(idx, &routeMeta{isSystem: true})
	reg.markDirtyLocked()
}

func (a *App) ensureHTTPAvailable() {
	a.httpRegistered = true
}

func (a *App) canMutateRoutes(action string) bool {
	if a == nil || a.httpServer == nil || a.httpServer.registry == nil {
		return false
	}
	reg := a.httpServer.registry
	reg.mu.Lock()
	defer reg.mu.Unlock()
	return a.canMutateRoutesLocked(action)
}

func (a *App) canMutateRoutesLocked(action string) bool {
	if a == nil || a.httpServer == nil || a.httpServer.registry == nil {
		return false
	}

	if a.httpServer.registry.compiled {
		if l := a.safeLogger(); l != nil {
			l.Errorf("cannot %s after routes have been compiled; register all routes and middleware before Run", action)
		}
		return false
	}

	return true
}

func (a *App) getRequestTimeout() time.Duration {
	reqTimeout, err := strconv.Atoi(a.Config.Get("REQUEST_TIMEOUT"))
	if (err != nil && a.Config.Get("REQUEST_TIMEOUT") != "") || reqTimeout < 0 {
		reqTimeout = 0
	}

	return time.Duration(reqTimeout) * time.Second
}

// AddRESTHandlers creates and registers CRUD routes for the given struct, the struct should always be passed by reference.
func (a *App) AddRESTHandlers(object any) error {
	cfg, err := scanEntity(object)
	if err != nil {
		a.Logger().Errorf("%s", err.Error())
		return err
	}

	a.registerCRUDHandlers(cfg, object)

	return nil
}
