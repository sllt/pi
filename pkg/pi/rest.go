package pi

import (
	"strconv"
	"time"
)

// GET adds a Handler for HTTP GET method for a route pattern.
func (a *App) GET(pattern string, handler Handler) {
	a.add("GET", pattern, handler)
}

// PUT adds a Handler for HTTP PUT method for a route pattern.
func (a *App) PUT(pattern string, handler Handler) {
	a.add("PUT", pattern, handler)
}

// POST adds a Handler for HTTP POST method for a route pattern.
func (a *App) POST(pattern string, handler Handler) {
	a.add("POST", pattern, handler)
}

// DELETE adds a Handler for HTTP DELETE method for a route pattern.
func (a *App) DELETE(pattern string, handler Handler) {
	a.add("DELETE", pattern, handler)
}

// PATCH adds a Handler for HTTP PATCH method for a route pattern.
func (a *App) PATCH(pattern string, handler Handler) {
	a.add("PATCH", pattern, handler)
}

func (a *App) add(method, pattern string, h Handler) {
	if !a.canMutateRoutes("register routes") {
		return
	}

	a.ensureHTTPAvailable()

	a.httpServer.registry.root.routes = append(a.httpServer.registry.root.routes, RouteDef{
		Method:  method,
		Pattern: pattern,
		Handler: h,
	})
}

func (a *App) ensureHTTPAvailable() {
	a.httpRegistered = true
}

func (a *App) canMutateRoutes(action string) bool {
	if a.httpServer == nil || a.httpServer.registry == nil {
		return false
	}

	if a.httpServer.registry.compiled {
		a.container.Logger.Errorf("cannot %s after routes have been compiled; register all routes and middleware before Run", action)
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
		a.container.Logger.Errorf(err.Error())
		return err
	}

	a.registerCRUDHandlers(cfg, object)

	return nil
}
