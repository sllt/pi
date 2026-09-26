package http

import (
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/sllt/pi/pkg/pi/logging"
)

const (
	DefaultSwaggerFileName       = "openapi.json"
	staticServerNotFoundFileName = "404.html"
)

var errReadPermissionDenied = fmt.Errorf("file does not have read permission")

// Router is responsible for routing HTTP request.
type Router struct {
	mux              *chi.Mux
	RegisteredRoutes *[]string
}

type Middleware func(handler http.Handler) http.Handler

// NewRouter creates a new Router instance.
func NewRouter() *Router {
	routes := make([]string, 0)
	return &Router{
		mux:              chi.NewRouter(),
		RegisteredRoutes: &routes,
	}
}

// ServeHTTP implements [http.Handler] interface with path normalization.
func (rou *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Normalize the path before routing to handle double slashes
	originalPath := r.URL.Path
	normalizedPath := path.Clean(originalPath)

	// path.Clean returns "." for empty paths, convert to "/" for HTTP routing
	if normalizedPath == "." {
		normalizedPath = "/"
	}

	// Ensure path starts with "/" for HTTP routing
	normalizedPath = "/" + strings.TrimLeft(normalizedPath, "/")

	// Only modify if path changed
	if originalPath != normalizedPath {
		r.URL.Path = normalizedPath
		if r.URL.RawPath != "" {
			r.URL.RawPath = normalizedPath
		}
	}

	// Delegate to the underlying chi router
	rou.mux.ServeHTTP(w, r)
}

// Add adds a new route with the given HTTP method, pattern, and handler, wrapping the handler with OpenTelemetry instrumentation.
func (rou *Router) Add(method, pattern string, handler http.Handler) {
	h := otelhttp.NewHandler(handler, "pi-router")
	rou.mux.Method(method, pattern, h)
}

// Use registers middlewares to the router.
func (rou *Router) Use(middlewares ...func(http.Handler) http.Handler) {
	rou.mux.Use(middlewares...)
}

// UseMiddleware registers middlewares to the router.
func (rou *Router) UseMiddleware(mws ...Middleware) {
	for _, m := range mws {
		rou.mux.Use(m)
	}
}

// NotFound sets the handler for requests that don't match any route.
func (rou *Router) NotFound(handler http.Handler) {
	rou.mux.NotFound(handler.ServeHTTP)
}

// Walk traverses all registered routes calling the given function for each route.
func (rou *Router) Walk(fn func(method, route string) error) error {
	return chi.Walk(rou.mux, func(method, route string, handler http.Handler, middlewares ...func(http.Handler) http.Handler) error {
		return fn(method, route)
	})
}

// Handle registers a handler for the given pattern.
// This is a convenience method that delegates to chi.Mux.Handle.
func (rou *Router) Handle(pattern string, handler http.Handler) {
	rou.mux.Handle(pattern, handler)
}

// RouteGroup creates a chi sub-router with the given prefix for scoped middleware.
func (rou *Router) RouteGroup(prefix string, fn func(chi.Router)) {
	rou.mux.Route(prefix, fn)
}

// Mux returns the underlying chi.Mux.
func (rou *Router) Mux() chi.Router {
	return rou.mux
}

type staticFileConfig struct {
	directoryName string
	logger        logging.Logger
}

func (rou *Router) AddStaticFiles(logger logging.Logger, endpoint, dirName string) {
	cfg := staticFileConfig{directoryName: dirName, logger: logger}

	fileServer := http.FileServer(http.Dir(cfg.directoryName))

	if endpoint != "/" {
		endpoint += "/"
	}

	rou.mux.Handle(endpoint+"*", http.StripPrefix(endpoint, cfg.staticHandler(fileServer)))

	logger.Logf("registered static files at endpoint %v from directory %v", endpoint, dirName)
}

func (staticConfig staticFileConfig) staticHandler(fileServer http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		url := r.URL.Path

		absPath, err := filepath.Abs(filepath.Join(staticConfig.directoryName, url))
		if err != nil {
			staticConfig.respondWithError(w, "failed to resolve absolute path", url, err, http.StatusInternalServerError)
			return
		}

		// Restrict direct access to openapi.json via static routes.
		// Allow access only through /.well-known/swagger or /.well-known/openapi.json.
		if staticConfig.isRestrictedFile(url, absPath) {
			staticConfig.respondWithError(w, "unauthorized attempt to access restricted file", url, nil, http.StatusForbidden)
			return
		}

		if err := staticConfig.validateFile(absPath); err != nil {
			staticConfig.respondWithFileError(w, r, absPath, err)
			return
		}

		staticConfig.logger.Debugf("serving file: %s", absPath)

		fileServer.ServeHTTP(w, r)
	})
}

// Checks if the file is restricted.
func (staticConfig staticFileConfig) isRestrictedFile(url, absPath string) bool {
	fileName := filepath.Base(url)

	return !strings.HasPrefix(absPath, staticConfig.directoryName) || fileName == DefaultSwaggerFileName
}

// Validates file existence and permissions.
func (staticFileConfig) validateFile(absPath string) error {
	fileInfo, err := os.Stat(absPath)
	if err != nil {
		return err
	}

	// Ensure file has at least read (`r--`) permission
	if fileInfo.Mode().Perm()&0444 == 0 {
		return errReadPermissionDenied
	}

	return nil
}

// Handles different file-related errors.
func (staticConfig staticFileConfig) respondWithFileError(w http.ResponseWriter, r *http.Request, absPath string, err error) {
	if os.IsNotExist(err) {
		staticConfig.logger.Debugf("requested file not found: %s", absPath)

		w.WriteHeader(http.StatusNotFound)

		// Serve custom 404.html if available
		notFoundPath, _ := filepath.Abs(filepath.Join(staticConfig.directoryName, staticServerNotFoundFileName))
		if _, err = os.Stat(notFoundPath); err == nil {
			staticConfig.logger.Debugf("serving custom 404 page: %s", notFoundPath)

			http.ServeFile(w, r, notFoundPath)

			return
		}

		_, _ = w.Write([]byte("404 Not Found"))

		return
	}

	staticConfig.respondWithError(w, "error accessing file", absPath, err, http.StatusInternalServerError)
}

// Generic error response handler.
func (staticConfig staticFileConfig) respondWithError(w http.ResponseWriter, message, url string, err error, status int) {
	if err != nil {
		staticConfig.logger.Errorf("%s: %s, error: %v", message, url, err)
	} else {
		staticConfig.logger.Debugf("%s: %s", message, url)
	}

	w.WriteHeader(status)

	fmt.Fprintf(w, "%d %s", status, http.StatusText(status))
}
