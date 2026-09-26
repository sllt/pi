package http

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sllt/pi/pkg/pi/config"
	"github.com/sllt/pi/pkg/pi/infra"
	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/sllt/pi/pkg/pi/testutil"
)

func TestRouter(t *testing.T) {
	port := testutil.GetFreePort(t)

	cfg := map[string]string{"HTTP_PORT": fmt.Sprint(port), "LOG_LEVEL": "INFO"}
	c := infra.NewContainer(config.NewMockConfig(cfg))

	c.Metrics().NewCounter("test-counter", "test")

	// Create a new router instance using the mock container
	router := NewRouter()

	// Add a test handler to the router
	router.Add(http.MethodGet, "/test", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Send a request to the test handler
	req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	// Verify the response
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestRouterWithMiddleware(t *testing.T) {
	port := testutil.GetFreePort(t)

	cfg := map[string]string{"HTTP_PORT": fmt.Sprint(port), "LOG_LEVEL": "INFO"}
	c := infra.NewContainer(config.NewMockConfig(cfg))

	c.Metrics().NewCounter("test-counter", "test")

	// Create a new router instance using the mock container
	router := NewRouter()

	router.UseMiddleware(func(inner http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Test-Middleware", "applied")
			inner.ServeHTTP(w, r)
		})
	})

	// Add a test handler to the router
	router.Add(http.MethodGet, "/test", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Send a request to the test handler
	req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	// Verify the response
	assert.Equal(t, http.StatusOK, rec.Code)
	// checking if the testMiddleware has added the required header in the response properly.
	testHeaderValue := rec.Header().Get("X-Test-Middleware")
	assert.Equal(t, "applied", testHeaderValue, "Test_UseMiddleware Failed! header value mismatch.")
}

// TestRouter_DoubleSlashPath_GET verifies that GET requests with double slashes
// are normalized and routed correctly to the GET handler.
func TestRouter_DoubleSlashPath_GET(t *testing.T) {
	router := NewRouter()

	getHandlerCalled := false
	postHandlerCalled := false

	// Register both GET and POST handlers for /hello
	router.Add(http.MethodGet, "/hello", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		getHandlerCalled = true

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("GET handler"))
	}))

	router.Add(http.MethodPost, "/hello", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		postHandlerCalled = true

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("POST handler"))
	}))

	tests := []struct {
		desc string
		path string
	}{
		{desc: "GET request to /hello", path: "/hello"},
		{desc: "GET request to //hello", path: "//hello"},
		{desc: "GET request to ///hello", path: "///hello"},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			getHandlerCalled = false
			postHandlerCalled = false

			req := httptest.NewRequest(http.MethodGet, tc.path, http.NoBody)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			assert.Equal(t, http.StatusOK, rec.Code, "Status code mismatch")
			assert.True(t, getHandlerCalled, "GET handler should be called")
			assert.False(t, postHandlerCalled, "POST handler should NOT be called")
			assert.Equal(t, "GET handler", rec.Body.String(), "Response body mismatch")
			assert.Empty(t, rec.Header().Get("Location"), "No redirect should be issued")
		})
	}
}

// TestRouter_PathNormalization tests the path normalization function directly.
func TestRouter_PathNormalization(t *testing.T) {
	router := NewRouter()

	// Register handlers for testing
	router.Add(http.MethodGet, "/hello", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("hello"))
	}))

	router.Add(http.MethodGet, "/api/v1/users", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("users"))
	}))

	router.Add(http.MethodGet, "/bar", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("bar"))
	}))

	tests := []struct {
		name         string
		input        string
		expectedCode int
		expectedBody string
	}{
		{name: "simple path", input: "/hello", expectedCode: http.StatusOK, expectedBody: "hello"},
		{name: "double slash", input: "//hello", expectedCode: http.StatusOK, expectedBody: "hello"},
		{name: "triple slash", input: "///hello", expectedCode: http.StatusOK, expectedBody: "hello"},
		{name: "multiple slashes in middle", input: "/api//v1///users", expectedCode: http.StatusOK, expectedBody: "users"},
		{name: "current directory dot", input: "/.", expectedCode: http.StatusNotFound, expectedBody: "404 page not found\n"},
		{name: "parent directory", input: "/..", expectedCode: http.StatusNotFound, expectedBody: "404 page not found\n"},
		{name: "relative path no leading slash", input: "/hello", expectedCode: http.StatusOK, expectedBody: "hello"},
		{name: "parent directory traversal", input: "/foo/../bar", expectedCode: http.StatusOK, expectedBody: "bar"},
		{name: "parent directory with relative path", input: "/../hello", expectedCode: http.StatusOK, expectedBody: "hello"},
		{name: "root path", input: "/", expectedCode: http.StatusNotFound, expectedBody: "404 page not found\n"},
		{name: "empty path", input: "/", expectedCode: http.StatusNotFound, expectedBody: "404 page not found\n"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.input, http.NoBody)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			assert.Equal(t, tc.expectedCode, rec.Code, "Status code mismatch")
			assert.Equal(t, tc.expectedBody, rec.Body.String(), "Response body mismatch")
		})
	}
}

// TestRouter_DoubleSlashPath_POST verifies that POST requests with double slashes
// are normalized and routed correctly to the POST handler.
func TestRouter_DoubleSlashPath_POST(t *testing.T) {
	router := NewRouter()

	getHandlerCalled := false
	postHandlerCalled := false

	// Register both GET and POST handlers for /hello
	router.Add(http.MethodGet, "/hello", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		getHandlerCalled = true

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("GET handler"))
	}))

	router.Add(http.MethodPost, "/hello", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		postHandlerCalled = true

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("POST handler"))
	}))

	tests := []struct {
		desc string
		path string
	}{
		{desc: "POST request to /hello", path: "/hello"},
		{desc: "POST request to //hello", path: "//hello"},
		{desc: "POST request to ////hello", path: "////hello"},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			getHandlerCalled = false
			postHandlerCalled = false

			req := httptest.NewRequest(http.MethodPost, tc.path, http.NoBody)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			assert.Equal(t, http.StatusOK, rec.Code, "Status code mismatch")
			assert.True(t, postHandlerCalled, "POST handler should be called")
			assert.False(t, getHandlerCalled, "GET handler should NOT be called")
			assert.Equal(t, "POST handler", rec.Body.String(), "Response body mismatch")
			assert.Empty(t, rec.Header().Get("Location"), "No redirect should be issued")
		})
	}
}

func Test_StaticFileServing_Static(t *testing.T) {
	tempDir := t.TempDir()

	testCases := []struct {
		name             string
		setupFiles       func() error
		path             string
		staticServerPath string
		expectedCode     int
		expectedBody     string
	}{
		{
			name: "Serve existing file from /static",
			setupFiles: func() error {
				return os.WriteFile(filepath.Join(tempDir, "test.txt"), []byte("Hello, World!"), 0600)
			},
			path:             "/static/test.txt",
			staticServerPath: "/static",
			expectedCode:     http.StatusOK,
			expectedBody:     "Hello, World!",
		},
		{
			name: "Serve existing file from /",
			setupFiles: func() error {
				return os.WriteFile(filepath.Join(tempDir, "test.txt"), []byte("Hello, Root!"), 0600)
			},
			path:             "/test.txt",
			staticServerPath: "/",
			expectedCode:     http.StatusOK,
			expectedBody:     "Hello, Root!",
		},
		{
			name: "Serve existing file from /public",
			setupFiles: func() error {
				return os.WriteFile(filepath.Join(tempDir, "test.txt"), []byte("Hello, Public!"), 0600)
			},
			path:             "/public/test.txt",
			staticServerPath: "/public",
			expectedCode:     http.StatusOK,
			expectedBody:     "Hello, Public!",
		},
		{
			name: "Serve 404.html for non-existent file",
			setupFiles: func() error {
				return os.WriteFile(filepath.Join(tempDir, "404.html"), []byte("<html>404 Not Found</html>"), 0600)
			},
			path:             "/static/nonexistent.html",
			staticServerPath: "/static",
			expectedCode:     http.StatusNotFound,
			expectedBody:     "<html>404 Not Found</html>",
		},
		{
			name: "Serve default 404 message when 404.html is missing",
			setupFiles: func() error {
				return os.Remove(filepath.Join(tempDir, "404.html"))
			},
			path:             "/static/nonexistent.html",
			staticServerPath: "/static",
			expectedCode:     http.StatusNotFound,
			expectedBody:     "404 Not Found",
		},
		{
			name: "Access forbidden OpenAPI JSON",
			setupFiles: func() error {
				return os.WriteFile(filepath.Join(tempDir, DefaultSwaggerFileName), []byte(`{"openapi": "3.0.0"}`), 0600)
			},
			path:             "/static/openapi.json",
			staticServerPath: "/static",
			expectedCode:     http.StatusForbidden,
			expectedBody:     "403 Forbidden",
		},
		{
			name: "Serving File with no Read permission",
			setupFiles: func() error {
				return os.WriteFile(filepath.Join(tempDir, "restricted.txt"), []byte("Restricted content"), 0000)
			},
			path:             "/static/restricted.txt",
			staticServerPath: "/static",
			expectedCode:     http.StatusInternalServerError,
			expectedBody:     "500 Internal Server Error",
		},
	}

	runStaticFileTests(t, tempDir, testCases)
}

func runStaticFileTests(t *testing.T, tempDir string, testCases []struct {
	name             string
	setupFiles       func() error
	path             string
	staticServerPath string
	expectedCode     int
	expectedBody     string
}) {
	t.Helper()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.setupFiles(); err != nil {
				t.Fatalf("Failed to set up files: %v", err)
			}

			logger := logging.NewMockLogger(logging.DEBUG)

			router := NewRouter()
			router.AddStaticFiles(logger, tc.staticServerPath, tempDir)

			req := httptest.NewRequest(http.MethodGet, tc.path, http.NoBody)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			assert.Equal(t, tc.expectedCode, w.Code)
			assert.Equal(t, tc.expectedBody, strings.TrimSpace(w.Body.String()))
		})
	}
}

// TestRouter_PathParam verifies that chi.URLParam correctly extracts path parameters
// through the pi Request.PathParam method.
func TestRouter_PathParam(t *testing.T) {
	router := NewRouter()

	var capturedID string
	router.Add(http.MethodGet, "/users/{id}", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := NewRequest(r)
		capturedID = req.PathParam("id")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(capturedID))
	}))

	req := httptest.NewRequest(http.MethodGet, "/users/123", http.NoBody)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "123", capturedID)
	assert.Equal(t, "123", rec.Body.String())
}

// TestRouter_MultiplePathParams verifies extraction of multiple path parameters.
func TestRouter_MultiplePathParams(t *testing.T) {
	router := NewRouter()

	var capturedUserID, capturedPostID string
	router.Add(http.MethodGet, "/users/{userId}/posts/{postId}", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := NewRequest(r)
		capturedUserID = req.PathParam("userId")
		capturedPostID = req.PathParam("postId")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(fmt.Sprintf("user:%s,post:%s", capturedUserID, capturedPostID)))
	}))

	req := httptest.NewRequest(http.MethodGet, "/users/456/posts/789", http.NoBody)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "456", capturedUserID)
	assert.Equal(t, "789", capturedPostID)
	assert.Equal(t, "user:456,post:789", rec.Body.String())
}

// TestRouter_UseMiddleware verifies middleware execution order.
func TestRouter_UseMiddleware(t *testing.T) {
	router := NewRouter()

	var executionOrder []string

	middleware1 := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			executionOrder = append(executionOrder, "middleware1-before")
			next.ServeHTTP(w, r)
			executionOrder = append(executionOrder, "middleware1-after")
		})
	}

	middleware2 := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			executionOrder = append(executionOrder, "middleware2-before")
			next.ServeHTTP(w, r)
			executionOrder = append(executionOrder, "middleware2-after")
		})
	}

	router.UseMiddleware(middleware1, middleware2)

	router.Add(http.MethodGet, "/test", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		executionOrder = append(executionOrder, "handler")
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	expectedOrder := []string{
		"middleware1-before",
		"middleware2-before",
		"handler",
		"middleware2-after",
		"middleware1-after",
	}
	assert.Equal(t, expectedOrder, executionOrder)
}

// TestRouter_NotFound verifies custom NotFound handler.
func TestRouter_NotFound(t *testing.T) {
	router := NewRouter()

	notFoundCalled := false
	router.NotFound(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		notFoundCalled = true
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("custom 404"))
	}))

	router.Add(http.MethodGet, "/exists", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Test unmatched route
	req := httptest.NewRequest(http.MethodGet, "/does-not-exist", http.NoBody)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	assert.True(t, notFoundCalled)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Equal(t, "custom 404", rec.Body.String())

	// Test matched route
	notFoundCalled = false
	req = httptest.NewRequest(http.MethodGet, "/exists", http.NoBody)
	rec = httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	assert.False(t, notFoundCalled)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestRouter_Walk verifies route traversal functionality.
func TestRouter_Walk(t *testing.T) {
	router := NewRouter()

	// Register multiple routes with different methods
	router.Add(http.MethodGet, "/users", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	router.Add(http.MethodPost, "/users", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	router.Add(http.MethodGet, "/users/{id}", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	router.Add(http.MethodDelete, "/users/{id}", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	routes := make(map[string][]string)
	err := router.Walk(func(method, route string) error {
		routes[route] = append(routes[route], method)
		return nil
	})

	assert.NoError(t, err)
	assert.Contains(t, routes, "/users")
	assert.Contains(t, routes, "/users/{id}")
	assert.Contains(t, routes["/users"], http.MethodGet)
	assert.Contains(t, routes["/users"], http.MethodPost)
	assert.Contains(t, routes["/users/{id}"], http.MethodGet)
	assert.Contains(t, routes["/users/{id}"], http.MethodDelete)
}

// TestRouter_MethodRouting verifies that different HTTP methods on the same path
// route to different handlers.
func TestRouter_MethodRouting(t *testing.T) {
	router := NewRouter()

	getHandlerCalled := false
	postHandlerCalled := false
	putHandlerCalled := false
	deleteHandlerCalled := false

	router.Add(http.MethodGet, "/resource", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		getHandlerCalled = true
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("GET"))
	}))

	router.Add(http.MethodPost, "/resource", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		postHandlerCalled = true
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("POST"))
	}))

	router.Add(http.MethodPut, "/resource", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		putHandlerCalled = true
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("PUT"))
	}))

	router.Add(http.MethodDelete, "/resource", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deleteHandlerCalled = true
		w.WriteHeader(http.StatusNoContent)
	}))

	tests := []struct {
		method           string
		expectedCode     int
		expectedBody     string
		expectedGetCall  bool
		expectedPostCall bool
		expectedPutCall  bool
		expectedDelCall  bool
	}{
		{http.MethodGet, http.StatusOK, "GET", true, false, false, false},
		{http.MethodPost, http.StatusCreated, "POST", false, true, false, false},
		{http.MethodPut, http.StatusOK, "PUT", false, false, true, false},
		{http.MethodDelete, http.StatusNoContent, "", false, false, false, true},
	}

	for _, tc := range tests {
		t.Run(tc.method, func(t *testing.T) {
			// Reset flags
			getHandlerCalled = false
			postHandlerCalled = false
			putHandlerCalled = false
			deleteHandlerCalled = false

			req := httptest.NewRequest(tc.method, "/resource", http.NoBody)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			assert.Equal(t, tc.expectedCode, rec.Code)
			assert.Equal(t, tc.expectedBody, rec.Body.String())
			assert.Equal(t, tc.expectedGetCall, getHandlerCalled)
			assert.Equal(t, tc.expectedPostCall, postHandlerCalled)
			assert.Equal(t, tc.expectedPutCall, putHandlerCalled)
			assert.Equal(t, tc.expectedDelCall, deleteHandlerCalled)
		})
	}
}

// TestRouter_MiddlewareBeforeRoutes verifies that middleware can be added before routes.
func TestRouter_MiddlewareBeforeRoutes(t *testing.T) {
	router := NewRouter()

	middlewareCalled := false
	middleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			middlewareCalled = true
			w.Header().Set("X-Middleware", "applied")
			next.ServeHTTP(w, r)
		})
	}

	// Add middleware BEFORE routes
	router.Use(middleware)

	// Add route AFTER middleware
	router.Add(http.MethodGet, "/test", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, middlewareCalled, "Middleware should be called")
	assert.Equal(t, "applied", rec.Header().Get("X-Middleware"))
}
