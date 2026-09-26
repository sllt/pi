package pi

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"

	"github.com/sllt/pi/pkg/pi/config"
	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"github.com/sllt/pi/pkg/pi/infra"
)

// TestRouteRegistry_BasicCompile tests that routes registered via the registry
// are compiled to chi and respond correctly.
func TestRouteRegistry_BasicCompile(t *testing.T) {
	reg := newRouteRegistry()
	mux := chi.NewRouter()
	container := infra.NewContainer(config.NewMockConfig(nil))

	reg.root.routes = append(reg.root.routes, RouteDef{
		Method:  "GET",
		Pattern: "/hello",
		Handler: func(c *Context) (any, error) {
			return "world", nil
		},
	})

	reg.compile(mux, container, 0)

	req := httptest.NewRequest(http.MethodGet, "/hello", http.NoBody)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	body, _ := io.ReadAll(rec.Body)
	assert.Contains(t, string(body), "world")
}

// TestRouteRegistry_GroupPrefix tests that group prefix is correctly applied.
func TestRouteRegistry_GroupPrefix(t *testing.T) {
	reg := newRouteRegistry()
	mux := chi.NewRouter()
	container := infra.NewContainer(config.NewMockConfig(nil))

	child := &GroupNode{prefix: "/api"}
	child.routes = append(child.routes, RouteDef{
		Method:  "GET",
		Pattern: "/users",
		Handler: func(c *Context) (any, error) {
			return "users-list", nil
		},
	})
	reg.root.children = append(reg.root.children, child)

	reg.compile(mux, container, 0)

	// /api/users should work
	req := httptest.NewRequest(http.MethodGet, "/api/users", http.NoBody)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	body, _ := io.ReadAll(rec.Body)
	assert.Contains(t, string(body), "users-list")

	// /users alone should 404
	req2 := httptest.NewRequest(http.MethodGet, "/users", http.NoBody)
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, req2)

	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

// TestRouteRegistry_NestedGroups tests nested group prefix concatenation.
func TestRouteRegistry_NestedGroups(t *testing.T) {
	reg := newRouteRegistry()
	mux := chi.NewRouter()
	container := infra.NewContainer(config.NewMockConfig(nil))

	api := &GroupNode{prefix: "/api"}
	v1 := &GroupNode{prefix: "/v1"}
	v1.routes = append(v1.routes, RouteDef{
		Method:  "GET",
		Pattern: "/items",
		Handler: func(c *Context) (any, error) {
			return "items-v1", nil
		},
	})
	api.children = append(api.children, v1)
	reg.root.children = append(reg.root.children, api)

	reg.compile(mux, container, 0)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/items", http.NoBody)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	body, _ := io.ReadAll(rec.Body)
	assert.Contains(t, string(body), "items-v1")
}

// TestRouteRegistry_HTTPMiddleware tests that HTTP middleware on a group is applied.
func TestRouteRegistry_HTTPMiddleware(t *testing.T) {
	reg := newRouteRegistry()
	mux := chi.NewRouter()
	container := infra.NewContainer(config.NewMockConfig(nil))

	child := &GroupNode{prefix: "/api"}
	child.httpMWs = append(child.httpMWs, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Group-MW", "applied")
			next.ServeHTTP(w, r)
		})
	})
	child.routes = append(child.routes, RouteDef{
		Method:  "GET",
		Pattern: "/test",
		Handler: func(c *Context) (any, error) {
			return "ok", nil
		},
	})
	reg.root.children = append(reg.root.children, child)

	reg.compile(mux, container, 0)

	req := httptest.NewRequest(http.MethodGet, "/api/test", http.NoBody)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "applied", rec.Header().Get("X-Group-MW"))
}

// TestRouteRegistry_HTTPMiddlewareIsolation tests that group middleware doesn't leak to sibling groups.
func TestRouteRegistry_HTTPMiddlewareIsolation(t *testing.T) {
	reg := newRouteRegistry()
	mux := chi.NewRouter()
	container := infra.NewContainer(config.NewMockConfig(nil))

	// Group A with middleware
	groupA := &GroupNode{prefix: "/a"}
	groupA.httpMWs = append(groupA.httpMWs, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Group-A", "yes")
			next.ServeHTTP(w, r)
		})
	})
	groupA.routes = append(groupA.routes, RouteDef{
		Method: "GET", Pattern: "/test",
		Handler: func(c *Context) (any, error) { return "a", nil },
	})

	// Group B without middleware
	groupB := &GroupNode{prefix: "/b"}
	groupB.routes = append(groupB.routes, RouteDef{
		Method: "GET", Pattern: "/test",
		Handler: func(c *Context) (any, error) { return "b", nil },
	})

	reg.root.children = append(reg.root.children, groupA, groupB)
	reg.compile(mux, container, 0)

	// /a/test should have the header
	req := httptest.NewRequest(http.MethodGet, "/a/test", http.NoBody)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.Equal(t, "yes", rec.Header().Get("X-Group-A"))

	// /b/test should NOT have the header
	req2 := httptest.NewRequest(http.MethodGet, "/b/test", http.NoBody)
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, req2)
	assert.Empty(t, rec2.Header().Get("X-Group-A"))
}

// TestRouteRegistry_PiMiddleware tests that PiMiddleware is pre-composed into the handler.
func TestRouteRegistry_PiMiddleware(t *testing.T) {
	reg := newRouteRegistry()
	mux := chi.NewRouter()
	container := infra.NewContainer(config.NewMockConfig(nil))

	var order []string

	mw1 := PiMiddleware(func(next Handler) Handler {
		return func(c *Context) (any, error) {
			order = append(order, "mw1-before")
			result, err := next(c)
			order = append(order, "mw1-after")
			return result, err
		}
	})

	mw2 := PiMiddleware(func(next Handler) Handler {
		return func(c *Context) (any, error) {
			order = append(order, "mw2-before")
			result, err := next(c)
			order = append(order, "mw2-after")
			return result, err
		}
	})

	reg.root.piMWs = append(reg.root.piMWs, mw1, mw2)
	reg.root.routes = append(reg.root.routes, RouteDef{
		Method:  "GET",
		Pattern: "/test",
		Handler: func(c *Context) (any, error) {
			order = append(order, "handler")
			return "done", nil
		},
	})

	reg.compile(mux, container, 0)

	req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	// Pi middleware should execute in declaration order (mw1 outermost)
	assert.Equal(t, []string{"mw1-before", "mw2-before", "handler", "mw2-after", "mw1-after"}, order)
}

// TestRouteRegistry_PiMiddlewareShortCircuit tests that PiMiddleware can short-circuit.
func TestRouteRegistry_PiMiddlewareShortCircuit(t *testing.T) {
	reg := newRouteRegistry()
	mux := chi.NewRouter()
	container := infra.NewContainer(config.NewMockConfig(nil))

	handlerCalled := false

	blockMW := PiMiddleware(func(next Handler) Handler {
		return func(c *Context) (any, error) {
			// Short-circuit: don't call next
			return nil, fmt.Errorf("blocked")
		}
	})

	reg.root.piMWs = append(reg.root.piMWs, blockMW)
	reg.root.routes = append(reg.root.routes, RouteDef{
		Method: "GET", Pattern: "/test",
		Handler: func(c *Context) (any, error) {
			handlerCalled = true
			return "should-not-reach", nil
		},
	})

	reg.compile(mux, container, 0)

	req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	assert.False(t, handlerCalled, "Handler should not be called when middleware short-circuits")
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

// TestComposePiMiddleware tests the composition helper directly.
func TestComposePiMiddleware(t *testing.T) {
	t.Run("empty middleware returns original handler", func(t *testing.T) {
		called := false
		h := Handler(func(c *Context) (any, error) {
			called = true
			return "ok", nil
		})

		composed := composePiMiddleware(nil, h)
		_, _ = composed(nil)
		assert.True(t, called)
	})

	t.Run("middleware wraps in correct order", func(t *testing.T) {
		var order []int

		mw1 := PiMiddleware(func(next Handler) Handler {
			return func(c *Context) (any, error) {
				order = append(order, 1)
				return next(c)
			}
		})
		mw2 := PiMiddleware(func(next Handler) Handler {
			return func(c *Context) (any, error) {
				order = append(order, 2)
				return next(c)
			}
		})

		h := Handler(func(c *Context) (any, error) {
			order = append(order, 3)
			return "ok", nil
		})

		composed := composePiMiddleware([]PiMiddleware{mw1, mw2}, h)
		_, _ = composed(nil)
		assert.Equal(t, []int{1, 2, 3}, order)
	})
}

func TestRouteRegistry_MergeDuplicatePrefixesDefensively(t *testing.T) {
	reg := newRouteRegistry()
	mux := chi.NewRouter()
	container := infra.NewContainer(config.NewMockConfig(nil))

	first := &GroupNode{prefix: "/api"}
	first.routes = append(first.routes, RouteDef{
		Method:  "GET",
		Pattern: "/one",
		Handler: func(c *Context) (any, error) {
			return "one", nil
		},
	})

	second := &GroupNode{prefix: "/api"}
	second.routes = append(second.routes, RouteDef{
		Method:  "GET",
		Pattern: "/two",
		Handler: func(c *Context) (any, error) {
			return "two", nil
		},
	})

	reg.root.children = append(reg.root.children, first, second)

	assert.NotPanics(t, func() {
		reg.compile(mux, container, 0)
	})

	req := httptest.NewRequest(http.MethodGet, "/api/one", http.NoBody)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	req = httptest.NewRequest(http.MethodGet, "/api/two", http.NoBody)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAppGroup_NormalizesAndMergesPrefixes(t *testing.T) {
	app := newRouteRegistryTestApp()

	app.Group("api", func(sub *RouteGroup) {
		sub.GET("/users", func(c *Context) (any, error) {
			return "users", nil
		})
	})
	app.Group("/api/", func(sub *RouteGroup) {
		sub.GET("/health", func(c *Context) (any, error) {
			return "ok", nil
		})
	})

	app.httpServer.registry.compile(app.httpServer.router.Mux(), app.container, 0)

	req := httptest.NewRequest(http.MethodGet, "/api/users", http.NoBody)
	rec := httptest.NewRecorder()
	app.httpServer.router.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	req = httptest.NewRequest(http.MethodGet, "/api/health", http.NoBody)
	rec = httptest.NewRecorder()
	app.httpServer.router.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAppGroup_ReturnedGroupStyle(t *testing.T) {
	app := newRouteRegistryTestApp()

	v1 := app.Group("/api/v1")
	assert.NotNil(t, v1)

	v1.GET("/health", func(c *Context) (any, error) {
		return "healthy", nil
	})

	app.httpServer.registry.compile(app.httpServer.router.Mux(), app.container, 0)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", http.NoBody)
	rec := httptest.NewRecorder()
	app.httpServer.router.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	body, _ := io.ReadAll(rec.Body)
	assert.Contains(t, string(body), "healthy")
}

func TestRouteGroup_ReturnedNestedGroupStyle(t *testing.T) {
	app := newRouteRegistryTestApp()

	api := app.Group("/api")
	assert.NotNil(t, api)

	v1 := api.Group("/v1")
	assert.NotNil(t, v1)

	v1.GET("/health", func(c *Context) (any, error) {
		return "ok", nil
	})

	app.httpServer.registry.compile(app.httpServer.router.Mux(), app.container, 0)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", http.NoBody)
	rec := httptest.NewRecorder()
	app.httpServer.router.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	body, _ := io.ReadAll(rec.Body)
	assert.Contains(t, string(body), "ok")
}

func TestAppGroup_EmptyPrefixWithMiddlewareDoesNotPanic(t *testing.T) {
	app := newRouteRegistryTestApp()

	app.GET("/base", func(c *Context) (any, error) {
		return "base", nil
	})

	app.Group("", func(sub *RouteGroup) {
		sub.Use(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Root-Group", "true")
				next.ServeHTTP(w, r)
			})
		})

		sub.GET("/from-group", func(c *Context) (any, error) {
			return "group", nil
		})
	})

	assert.NotPanics(t, func() {
		app.httpServer.registry.compile(app.httpServer.router.Mux(), app.container, 0)
	})

	req := httptest.NewRequest(http.MethodGet, "/base", http.NoBody)
	rec := httptest.NewRecorder()
	app.httpServer.router.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "true", rec.Header().Get("X-Root-Group"))

	req = httptest.NewRequest(http.MethodGet, "/from-group", http.NoBody)
	rec = httptest.NewRecorder()
	app.httpServer.router.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "true", rec.Header().Get("X-Root-Group"))
}

func TestAppGroup_NilCallbackIsSafe(t *testing.T) {
	app := newRouteRegistryTestApp()

	assert.NotPanics(t, func() {
		app.Group("/api", nil)
	})
	assert.Len(t, app.httpServer.registry.root.children, 0)
}

func TestRouteRegistry_MutationsAfterCompileAreBlocked(t *testing.T) {
	app := newRouteRegistryTestApp()

	var apiGroup *RouteGroup

	app.GET("/ready", func(c *Context) (any, error) {
		return "ready", nil
	})
	app.Group("/api", func(sub *RouteGroup) {
		apiGroup = sub
		sub.GET("/v1", func(c *Context) (any, error) {
			return "v1", nil
		})
	})

	app.httpServer.registry.compile(app.httpServer.router.Mux(), app.container, 0)

	rootRoutes := len(app.httpServer.registry.root.routes)
	rootHTTPMWs := len(app.httpServer.registry.root.httpMWs)
	rootPiMWs := len(app.httpServer.registry.root.piMWs)
	apiRoutes := len(apiGroup.node.routes)

	app.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Late-HTTP", "blocked")
			next.ServeHTTP(w, r)
		})
	})
	app.UseMiddleware(func(next Handler) Handler {
		return func(c *Context) (any, error) {
			return next(c)
		}
	})
	app.GET("/late", func(c *Context) (any, error) {
		return "late", nil
	})
	app.Group("/late", func(sub *RouteGroup) {
		sub.GET("/x", func(c *Context) (any, error) {
			return "x", nil
		})
	})

	apiGroup.Use(func(next http.Handler) http.Handler {
		return next
	})
	apiGroup.UseMiddleware(func(next Handler) Handler {
		return next
	})
	apiGroup.GET("/v2", func(c *Context) (any, error) {
		return "v2", nil
	})

	assert.Len(t, app.httpServer.registry.root.routes, rootRoutes)
	assert.Len(t, app.httpServer.registry.root.httpMWs, rootHTTPMWs)
	assert.Len(t, app.httpServer.registry.root.piMWs, rootPiMWs)
	assert.Len(t, apiGroup.node.routes, apiRoutes)

	req := httptest.NewRequest(http.MethodGet, "/ready", http.NoBody)
	rec := httptest.NewRecorder()
	app.httpServer.router.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Empty(t, rec.Header().Get("X-Late-HTTP"))

	req = httptest.NewRequest(http.MethodGet, "/late", http.NoBody)
	rec = httptest.NewRecorder()
	app.httpServer.router.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	req = httptest.NewRequest(http.MethodGet, "/api/v2", http.NoBody)
	rec = httptest.NewRecorder()
	app.httpServer.router.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func newRouteRegistryTestApp() *App {
	return &App{
		httpServer: &httpServer{
			router:   piHTTP.NewRouter(),
			registry: newRouteRegistry(),
			port:     8080,
		},
		container:      infra.NewContainer(config.NewMockConfig(nil)),
		Config:         config.NewMockConfig(nil),
		httpRegistered: true,
	}
}
