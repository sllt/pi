package pi

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"

	"github.com/sllt/pi/pkg/pi/config"
	piHTTP "github.com/sllt/pi/pkg/pi/http"
)

// TestRouteRegistry_BasicCompile tests that routes registered via the registry
// are compiled to chi and respond correctly.
func TestRouteRegistry_BasicCompile(t *testing.T) {
	reg := newRouteRegistry()
	mux := chi.NewRouter()
	container := newTestKernel(nil)

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
	container := newTestKernel(nil)

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
	container := newTestKernel(nil)

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
	container := newTestKernel(nil)

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
	container := newTestKernel(nil)

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
	container := newTestKernel(nil)

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
	container := newTestKernel(nil)

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
	container := newTestKernel(nil)

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

	app.httpServer.registry.compile(app.httpServer.router.Mux(), app.kernel, 0)

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

	app.httpServer.registry.compile(app.httpServer.router.Mux(), app.kernel, 0)

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

	app.httpServer.registry.compile(app.httpServer.router.Mux(), app.kernel, 0)

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
		app.httpServer.registry.compile(app.httpServer.router.Mux(), app.kernel, 0)
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

	app.httpServer.registry.compile(app.httpServer.router.Mux(), app.kernel, 0)

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

func TestRouteRegistry_NestedAndFlatParameterGroupsCoexistWithMiddlewareIsolation(t *testing.T) {
	runCase := func(t *testing.T, nestedFirst bool) {
		t.Helper()
		app := newRouteRegistryTestApp()

		var piTrace []string
		recordPi := func(label string) PiMiddleware {
			return func(next Handler) Handler {
				return func(c *Context) (any, error) {
					piTrace = append(piTrace, label)
					return next(c)
				}
			}
		}
		headerMW := func(key, val string) func(http.Handler) http.Handler {
			return func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set(key, val)
					next.ServeHTTP(w, r)
				})
			}
		}

		app.Use(headerMW("X-Root-HTTP", "root"))
		app.UseMiddleware(recordPi("root-pi"))

		registerNested := func() {
			usersGroup := app.Group("/users")
			usersGroup.Use(headerMW("X-Nested-Parent-HTTP", "1"))
			usersGroup.UseMiddleware(recordPi("nested-parent-pi"))

			idGroup := usersGroup.Group("/{id}")
			idGroup.Use(headerMW("X-Nested-Child-HTTP", "1"))
			idGroup.UseMiddleware(recordPi("nested-child-pi"))
			idGroup.GET("/profile", func(c *Context) (any, error) {
				return "profile:" + c.PathParam("id"), nil
			})
		}

		registerFlat := func() {
			flatGroup := app.Group("/users/{id}")
			flatGroup.Use(headerMW("X-Flat-HTTP", "1"))
			flatGroup.UseMiddleware(recordPi("flat-pi"))
			flatGroup.GET("/settings", func(c *Context) (any, error) {
				return "settings:" + c.PathParam("id"), nil
			})
		}

		if nestedFirst {
			registerNested()
			registerFlat()
		} else {
			registerFlat()
			registerNested()
		}

		assert.NotPanics(t, func() {
			app.httpServer.registry.compile(app.httpServer.router.Mux(), app.kernel, 0)
		})

		piTrace = nil
		req := httptest.NewRequest(http.MethodGet, "/users/42/profile", http.NoBody)
		rec := httptest.NewRecorder()
		app.httpServer.router.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		body, _ := io.ReadAll(rec.Body)
		assert.Contains(t, string(body), "profile:42")
		assert.Equal(t, "root", rec.Header().Get("X-Root-HTTP"))
		assert.Equal(t, "1", rec.Header().Get("X-Nested-Parent-HTTP"))
		assert.Equal(t, "1", rec.Header().Get("X-Nested-Child-HTTP"))
		assert.Empty(t, rec.Header().Get("X-Flat-HTTP"))
		assert.Equal(t, []string{"root-pi", "nested-parent-pi", "nested-child-pi"}, piTrace)

		piTrace = nil
		req = httptest.NewRequest(http.MethodGet, "/users/99/settings", http.NoBody)
		rec = httptest.NewRecorder()
		app.httpServer.router.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		body, _ = io.ReadAll(rec.Body)
		assert.Contains(t, string(body), "settings:99")
		assert.Equal(t, "root", rec.Header().Get("X-Root-HTTP"))
		assert.Equal(t, "1", rec.Header().Get("X-Flat-HTTP"))
		assert.Empty(t, rec.Header().Get("X-Nested-Parent-HTTP"))
		assert.Empty(t, rec.Header().Get("X-Nested-Child-HTTP"))
		assert.Equal(t, []string{"root-pi", "flat-pi"}, piTrace)
	}

	t.Run("nested first then flat", func(t *testing.T) {
		runCase(t, true)
	})
	t.Run("flat first then nested", func(t *testing.T) {
		runCase(t, false)
	})
}

func TestRouteRegistry_OverlappingStaticPrefixGroupsAndRecursiveEmptyFolding(t *testing.T) {
	t.Run("overlapping static prefix groups all resolve without shadowing", func(t *testing.T) {
		app := newRouteRegistryTestApp()

		apiGroup := app.Group("/api")
		apiGroup.GET("/v1/orders", func(c *Context) (any, error) {
			return "orders", nil
		})

		apiV1Group := app.Group("/api/v1")
		apiV1Group.GET("/users", func(c *Context) (any, error) {
			return "users", nil
		})

		apiV1Admin := app.Group("/api/v1/admin")
		apiV1Admin.GET("/audit", func(c *Context) (any, error) {
			return "audit", nil
		})

		assert.NotPanics(t, func() {
			app.httpServer.registry.compile(app.httpServer.router.Mux(), app.kernel, 0)
		})

		for _, tc := range []struct {
			path string
			want string
		}{
			{path: "/api/v1/orders", want: "orders"},
			{path: "/api/v1/users", want: "users"},
			{path: "/api/v1/admin/audit", want: "audit"},
		} {
			req := httptest.NewRequest(http.MethodGet, tc.path, http.NoBody)
			rec := httptest.NewRecorder()
			app.httpServer.router.ServeHTTP(rec, req)
			assert.Equal(t, http.StatusOK, rec.Code, tc.path)
			body, _ := io.ReadAll(rec.Body)
			assert.Contains(t, string(body), tc.want, tc.path)
		}

		req := httptest.NewRequest(http.MethodGet, "/api/v1/missing", http.NoBody)
		rec := httptest.NewRecorder()
		app.httpServer.router.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("recursive empty-prefix GroupNode folding preserves middlewares and routes", func(t *testing.T) {
		reg := newRouteRegistry()
		mux := chi.NewRouter()
		container := newTestKernel(nil)

		var piOrder []string
		appendPi := func(name string) PiMiddleware {
			return func(next Handler) Handler {
				return func(c *Context) (any, error) {
					piOrder = append(piOrder, name)
					return next(c)
				}
			}
		}
		headerMW := func(key, val string) func(http.Handler) http.Handler {
			return func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set(key, val)
					next.ServeHTTP(w, r)
				})
			}
		}

		emptyGrandchild := &GroupNode{
			prefix:  "",
			httpMWs: []func(http.Handler) http.Handler{headerMW("X-Empty-2", "2")},
			piMWs:   []PiMiddleware{appendPi("empty-2")},
			routes: []RouteDef{
				{Method: http.MethodGet, Pattern: "/e2", Handler: func(c *Context) (any, error) { return "e2", nil }},
			},
			children: []*GroupNode{
				{
					prefix: "/nested",
					routes: []RouteDef{
						{Method: http.MethodGet, Pattern: "/item", Handler: func(c *Context) (any, error) { return "nested-item", nil }},
					},
				},
			},
		}
		emptyChild := &GroupNode{
			prefix:   "/",
			httpMWs:  []func(http.Handler) http.Handler{headerMW("X-Empty-1", "1")},
			piMWs:    []PiMiddleware{appendPi("empty-1")},
			routes:   []RouteDef{{Method: http.MethodGet, Pattern: "/e1", Handler: func(c *Context) (any, error) { return "e1", nil }}},
			children: []*GroupNode{emptyGrandchild},
		}

		scopedEmptyLeaf := &GroupNode{
			prefix:  "",
			httpMWs: []func(http.Handler) http.Handler{headerMW("X-Scoped-Empty", "deep")},
			piMWs:   []PiMiddleware{appendPi("scoped-empty")},
			routes: []RouteDef{
				{Method: http.MethodGet, Pattern: "/deep", Handler: func(c *Context) (any, error) { return "deep", nil }},
			},
		}
		scoped := &GroupNode{
			prefix: "/scoped",
			children: []*GroupNode{
				{prefix: "", children: []*GroupNode{scopedEmptyLeaf}},
			},
		}

		reg.root.children = append(reg.root.children, emptyChild, scoped)

		assert.NotPanics(t, func() {
			reg.compile(mux, container, 0)
		})

		piOrder = nil
		req := httptest.NewRequest(http.MethodGet, "/e2", http.NoBody)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "1", rec.Header().Get("X-Empty-1"))
		assert.Equal(t, "2", rec.Header().Get("X-Empty-2"))
		assert.Empty(t, rec.Header().Get("X-Scoped-Empty"))
		assert.Equal(t, []string{"empty-1", "empty-2"}, piOrder)

		piOrder = nil
		req = httptest.NewRequest(http.MethodGet, "/nested/item", http.NoBody)
		rec = httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		body, _ := io.ReadAll(rec.Body)
		assert.Contains(t, string(body), "nested-item")
		assert.Equal(t, []string{"empty-1", "empty-2"}, piOrder)

		piOrder = nil
		req = httptest.NewRequest(http.MethodGet, "/scoped/deep", http.NoBody)
		rec = httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		body, _ = io.ReadAll(rec.Body)
		assert.Contains(t, string(body), "deep")
		assert.Equal(t, "deep", rec.Header().Get("X-Scoped-Empty"))
		assert.Equal(t, []string{"empty-1", "empty-2", "scoped-empty"}, piOrder)
	})
}

func newRouteRegistryTestApp() *App {
	return &App{
		httpServer: &httpServer{
			router:   piHTTP.NewRouter(),
			registry: newRouteRegistry(),
			port:     8080,
		},
		kernel:         newTestKernel(nil),
		Config:         config.NewMockConfig(nil),
		httpRegistered: true,
	}
}

func TestRouteScope_IsolationAndLifecycleGuards(t *testing.T) {
	headerMW := func(key, val string) func(http.Handler) http.Handler {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(key, val)
				next.ServeHTTP(w, r)
			})
		}
	}

	t.Run("root app.Scope isolates HTTP and Pi middlewares from root and sibling scopes and activates HTTP", func(t *testing.T) {
		app := &App{
			httpServer: &httpServer{
				router:   piHTTP.NewRouter(),
				registry: newRouteRegistry(),
				port:     8080,
			},
			kernel: newTestKernel(nil),
			Config: config.NewMockConfig(nil),
		}
		assert.False(t, app.httpRegistered)

		var trace []string
		recPi := func(name string) PiMiddleware {
			return func(next Handler) Handler {
				return func(c *Context) (any, error) {
					trace = append(trace, name)
					return next(c)
				}
			}
		}

		// Empty scope with middleware must not leak to chi root router.
		emptyScope := app.Scope()
		assert.NotSame(t, app.rootRouteGroup(), emptyScope)
		assert.True(t, app.httpRegistered)
		emptyScope.Use(headerMW("X-Empty-Scope", "1"))

		// Scope with nil callback still creates an isolated node and runs valid callbacks.
		s1 := app.Scope(nil, func(sub *RouteGroup) {
			sub.Use(headerMW("X-Scope-1", "yes"))
			sub.UseMiddleware(recPi("pi-s1"))
			sub.GET("/s1", func(c *Context) (any, error) {
				return "s1", nil
			})
		})
		assert.NotNil(t, s1)
		assert.True(t, s1.node.isScope)

		// Scope() with only nil callback never returns root group.
		sNil := app.Scope(nil)
		assert.NotSame(t, app.rootRouteGroup(), sNil)
		assert.True(t, sNil.node.isScope)

		s2 := app.Scope().Use(headerMW("X-Scope-2", "yes")).UseMiddleware(recPi("pi-s2"))
		s2.GET("/s2", func(c *Context) (any, error) {
			return "s2", nil
		})

		app.GET("/public", func(c *Context) (any, error) {
			return "pub", nil
		})

		app.httpServer.registry.compile(app.httpServer.router.Mux(), app.kernel, 0)

		trace = nil
		recPub := httptest.NewRecorder()
		app.httpServer.router.ServeHTTP(recPub, httptest.NewRequest(http.MethodGet, "/public", http.NoBody))
		assert.Equal(t, http.StatusOK, recPub.Code)
		assert.Empty(t, recPub.Header().Get("X-Empty-Scope"))
		assert.Empty(t, recPub.Header().Get("X-Scope-1"))
		assert.Empty(t, recPub.Header().Get("X-Scope-2"))
		assert.Empty(t, trace)

		trace = nil
		rec1 := httptest.NewRecorder()
		app.httpServer.router.ServeHTTP(rec1, httptest.NewRequest(http.MethodGet, "/s1", http.NoBody))
		assert.Equal(t, http.StatusOK, rec1.Code)
		assert.Equal(t, "yes", rec1.Header().Get("X-Scope-1"))
		assert.Empty(t, rec1.Header().Get("X-Scope-2"))
		assert.Empty(t, rec1.Header().Get("X-Empty-Scope"))
		assert.Equal(t, []string{"pi-s1"}, trace)

		trace = nil
		rec2 := httptest.NewRecorder()
		app.httpServer.router.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/s2", http.NoBody))
		assert.Equal(t, http.StatusOK, rec2.Code)
		assert.Equal(t, "yes", rec2.Header().Get("X-Scope-2"))
		assert.Empty(t, rec2.Header().Get("X-Scope-1"))
		assert.Equal(t, []string{"pi-s2"}, trace)
	})

	t.Run("group.Scope inherits parent prefix and middlewares while isolating own middlewares", func(t *testing.T) {
		app := newRouteRegistryTestApp()
		var trace []string
		recPi := func(name string) PiMiddleware {
			return func(next Handler) Handler {
				return func(c *Context) (any, error) {
					trace = append(trace, name)
					return next(c)
				}
			}
		}

		v1 := app.Group("/api/v1").Use(headerMW("X-V1", "base")).UseMiddleware(recPi("v1-pi"))
		v1.GET("/public", func(c *Context) (any, error) { return "v1-pub", nil })

		authed := v1.Scope().Use(headerMW("X-Auth", "bearer")).UseMiddleware(recPi("auth-pi"))
		assert.NotSame(t, v1, authed)
		assert.NotSame(t, v1.node, authed.node)
		authed.POST("/posts", func(c *Context) (any, error) { return "created", nil })

		// v1.Group("") must still return v1 and never collide with authed scope.
		sameV1 := v1.Group("")
		assert.Same(t, v1, sameV1)

		// Sibling scope under v1 stays completely separate.
		admin := v1.Scope().Use(headerMW("X-Admin", "root")).UseMiddleware(recPi("admin-pi"))
		admin.DELETE("/posts/{id}", func(c *Context) (any, error) { return "deleted", nil })

		app.httpServer.registry.compile(app.httpServer.router.Mux(), app.kernel, 0)

		trace = nil
		recPub := httptest.NewRecorder()
		app.httpServer.router.ServeHTTP(recPub, httptest.NewRequest(http.MethodGet, "/api/v1/public", http.NoBody))
		assert.Equal(t, http.StatusOK, recPub.Code)
		assert.Equal(t, "base", recPub.Header().Get("X-V1"))
		assert.Empty(t, recPub.Header().Get("X-Auth"))
		assert.Empty(t, recPub.Header().Get("X-Admin"))
		assert.Equal(t, []string{"v1-pi"}, trace)

		trace = nil
		recAuth := httptest.NewRecorder()
		app.httpServer.router.ServeHTTP(recAuth, httptest.NewRequest(http.MethodPost, "/api/v1/posts", http.NoBody))
		assert.Equal(t, http.StatusCreated, recAuth.Code)
		assert.Equal(t, "base", recAuth.Header().Get("X-V1"))
		assert.Equal(t, "bearer", recAuth.Header().Get("X-Auth"))
		assert.Empty(t, recAuth.Header().Get("X-Admin"))
		assert.Equal(t, []string{"v1-pi", "auth-pi"}, trace)

		trace = nil
		recAdmin := httptest.NewRecorder()
		app.httpServer.router.ServeHTTP(recAdmin, httptest.NewRequest(http.MethodDelete, "/api/v1/posts/1", http.NoBody))
		assert.Equal(t, http.StatusNoContent, recAdmin.Code)
		assert.Equal(t, "base", recAdmin.Header().Get("X-V1"))
		assert.Equal(t, "root", recAdmin.Header().Get("X-Admin"))
		assert.Empty(t, recAdmin.Header().Get("X-Auth"))
		assert.Equal(t, []string{"v1-pi", "admin-pi"}, trace)
	})

	t.Run("Scope after compile freeze returns isolated dummy group without mutating parent", func(t *testing.T) {
		app := newRouteRegistryTestApp()
		v1 := app.Group("/api/v1")
		v1.GET("/ok", func(c *Context) (any, error) { return "ok", nil })

		app.httpServer.registry.compile(app.httpServer.router.Mux(), app.kernel, 0)

		rootChildrenBefore := len(app.httpServer.registry.root.children)
		v1ChildrenBefore := len(v1.node.children)

		var callbackCalled bool
		postRootScope := app.Scope(func(sub *RouteGroup) { callbackCalled = true })
		assert.False(t, callbackCalled)
		assert.NotSame(t, app.rootRouteGroup(), postRootScope)
		assert.NotSame(t, app.httpServer.registry.root, postRootScope.node)
		postRootScope.Use(headerMW("X-Late", "1")).GET("/late", func(c *Context) (any, error) { return "late", nil })

		postV1Scope := v1.Scope()
		assert.NotSame(t, v1, postV1Scope)
		assert.NotSame(t, v1.node, postV1Scope.node)
		postV1Scope.Use(headerMW("X-Late-V1", "1")).GET("/late", func(c *Context) (any, error) { return "late", nil })

		assert.Len(t, app.httpServer.registry.root.children, rootChildrenBefore)
		assert.Len(t, v1.node.children, v1ChildrenBefore)
		assert.Empty(t, postRootScope.node.routes)
		assert.Empty(t, postV1Scope.node.routes)
	})
}

func TestUnifiedRoutes_LegacyHeadOptionsAndMaxBodyBytes(t *testing.T) {
	app := newRouteRegistryTestApp()
	app.HEAD("/ping", func(c *Context) (any, error) {
		return map[string]string{"status": "ok"}, nil
	})
	app.OPTIONS("/ping", func(c *Context) (any, error) {
		return map[string]string{"allow": "GET,HEAD,OPTIONS"}, nil
	})

	type customAction func(*Context) (any, error)
	app.GET("/custom-type", customAction(func(c *Context) (any, error) {
		return "custom-action", nil
	}))

	// Legacy endpoint max body override (> default 1 MiB) and small endpoint limit.
	app.POST("/legacy-small", func(c *Context) (any, error) {
		var payload map[string]any
		if err := c.Bind(&payload); err != nil {
			return nil, err
		}
		return payload, nil
	}, WithEndpointMaxBodyBytes(16))

	app.POST("/legacy-large", func(c *Context) (any, error) {
		var payload map[string]any
		if err := c.Bind(&payload); err != nil {
			return nil, err
		}
		return len(payload["data"].(string)), nil
	}, WithEndpointMaxBodyBytes(2<<20))

	app.httpServer.registry.compile(app.httpServer.router.Mux(), app.kernel, 0)

	recHead := httptest.NewRecorder()
	app.httpServer.router.ServeHTTP(recHead, httptest.NewRequest(http.MethodHead, "/ping", http.NoBody))
	assert.Equal(t, http.StatusOK, recHead.Code)
	assert.Empty(t, recHead.Body.Bytes())

	recOpt := httptest.NewRecorder()
	app.httpServer.router.ServeHTTP(recOpt, httptest.NewRequest(http.MethodOptions, "/ping", http.NoBody))
	assert.Equal(t, http.StatusOK, recOpt.Code)
	assert.Contains(t, recOpt.Body.String(), "GET,HEAD,OPTIONS")

	recCustom := httptest.NewRecorder()
	app.httpServer.router.ServeHTTP(recCustom, httptest.NewRequest(http.MethodGet, "/custom-type", http.NoBody))
	assert.Equal(t, http.StatusOK, recCustom.Code)
	assert.Contains(t, recCustom.Body.String(), "custom-action")

	smallReq := httptest.NewRequest(http.MethodPost, "/legacy-small", strings.NewReader(`{"data":"0123456789abcdef"}`))
	smallReq.Header.Set("Content-Type", "application/json")
	recSmall := httptest.NewRecorder()
	app.httpServer.router.ServeHTTP(recSmall, smallReq)
	assert.Equal(t, http.StatusRequestEntityTooLarge, recSmall.Code)

	largePayload := `{"data":"` + strings.Repeat("x", (1<<20)+1024) + `"}`
	largeReq := httptest.NewRequest(http.MethodPost, "/legacy-large", strings.NewReader(largePayload))
	largeReq.Header.Set("Content-Type", "application/json")
	recLarge := httptest.NewRecorder()
	app.httpServer.router.ServeHTTP(recLarge, largeReq)
	assert.Equal(t, http.StatusCreated, recLarge.Code)
}

func TestRouteScope_UninitializedAndNonMutatingCompileAndStart(t *testing.T) {
	t.Run("uninitialized RouteGroup.Scope and nil App.Scope return inert scope without mutating app", func(t *testing.T) {
		app := &App{
			httpServer: &httpServer{
				router:   piHTTP.NewRouter(),
				registry: newRouteRegistry(),
				port:     8080,
			},
			kernel: newTestKernel(nil),
			Config: config.NewMockConfig(nil),
		}

		uninitScope := (&RouteGroup{app: app}).Scope()
		assert.NotNil(t, uninitScope)
		assert.Nil(t, uninitScope.app)
		assert.True(t, uninitScope.node.isScope)
		uninitScope.GET("/should-not-attach", func(c *Context) (any, error) { return "x", nil })
		assert.False(t, app.httpRegistered)
		assert.Empty(t, app.httpServer.registry.root.children)

		var nilApp *App
		nilAppScope := nilApp.Scope()
		assert.NotNil(t, nilAppScope)
		assert.Nil(t, nilAppScope.app)
		assert.True(t, nilAppScope.node.isScope)
		assert.NotPanics(t, func() {
			nilAppScope.Use(func(next http.Handler) http.Handler { return next }).
				GET("/noop", func(c *Context) (any, error) { return "noop", nil })
		})
	})

	t.Run("mergeChildrenByPrefix does not mutate persistent GroupNode slices or child parent", func(t *testing.T) {
		reg := newRouteRegistry()
		mux := chi.NewRouter()
		container := newTestKernel(nil)

		first := &GroupNode{prefix: "/api"}
		first.routes = append(first.routes, RouteDef{
			Method: http.MethodGet, Pattern: "/one",
			Handler: func(c *Context) (any, error) { return "one", nil },
		})
		secondChild := &GroupNode{prefix: "/sub"}
		second := &GroupNode{prefix: "/api", children: []*GroupNode{secondChild}}
		secondChild.parent = second
		secondChild.routes = append(secondChild.routes, RouteDef{
			Method: http.MethodGet, Pattern: "/two",
			Handler: func(c *Context) (any, error) { return "two", nil },
		})

		reg.root.children = append(reg.root.children, first, second)
		reg.compile(mux, container, 0)

		// Persistent GroupNodes remain unmutated after compile.
		assert.Len(t, first.routes, 1)
		assert.Empty(t, first.children)
		assert.Same(t, second, secondChild.parent)

		recOne := httptest.NewRecorder()
		mux.ServeHTTP(recOne, httptest.NewRequest(http.MethodGet, "/api/one", http.NoBody))
		assert.Equal(t, http.StatusOK, recOne.Code)

		recTwo := httptest.NewRecorder()
		mux.ServeHTTP(recTwo, httptest.NewRequest(http.MethodGet, "/api/sub/two", http.NoBody))
		assert.Equal(t, http.StatusOK, recTwo.Code)
	})

	t.Run("standalone app.Scope().GET activates HTTPHandler and Start", func(t *testing.T) {
		app, err := Build(
			WithConfig(map[string]string{
				"HTTP_ADDR":       "127.0.0.1:0",
				"HTTP_ENABLED":    "true",
				"GRPC_ENABLED":    "false",
				"METRICS_ENABLED": "false",
				"PI_TELEMETRY":    "false",
				"LOG_LEVEL":       "ERROR",
			}),
		)
		assert.NoError(t, err)

		app.Scope().Use(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Scoped-App", "active")
				next.ServeHTTP(w, r)
			})
		}).GET("/only-scope", func(c *Context) (any, error) {
			return "scope-ok", nil
		})

		h, err := app.HTTPHandler()
		assert.NoError(t, err)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/only-scope", http.NoBody))
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "active", rec.Header().Get("X-Scoped-App"))

		assert.NoError(t, app.Start(t.Context()))
		defer func() { _ = app.Stop(t.Context()) }()
		assert.NotEmpty(t, app.HTTPAddress())

		resp, err := http.Get("http://" + app.HTTPAddress() + "/only-scope")
		assert.NoError(t, err)
		if resp != nil {
			_ = resp.Body.Close()
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, "active", resp.Header.Get("X-Scoped-App"))
		}
	})
}
