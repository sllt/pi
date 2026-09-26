package pi

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/sllt/pi/pkg/pi/cmd/terminal"
	"github.com/sllt/pi/pkg/pi/http/middleware"
	"github.com/sllt/pi/pkg/pi/infra"
	"github.com/sllt/pi/pkg/pi/logging"
)

// New creates an HTTP Server Application and returns that App.
func New() *App {
	app := &App{}
	app.readConfig(false)
	app.container = infra.NewContainer(app.Config)

	app.initTracer()
	app.initMetricsServer()

	// HTTP Server
	address, enabled, err := endpointConfig(app.Config, "HTTP", defaultHTTPPort)
	app.initErr = errors.Join(app.initErr, err)
	app.httpDisabled = !enabled
	middlewareConfig := middleware.GetConfigs(app.Config)
	app.initErr = errors.Join(app.initErr, middleware.ValidateCORS(middlewareConfig.CorsHeaders))
	app.httpServer = newHTTPServer(app.container, defaultHTTPPort, middlewareConfig)
	app.httpServer.address = address
	app.httpServer.certFile = app.Config.GetOrDefault("CERT_FILE", "")
	app.httpServer.keyFile = app.Config.GetOrDefault("KEY_FILE", "")
	app.httpServer.staticFiles = make(map[string]string)

	// Note: Default routes (health, alive, favicon, swagger) are registered in httpServerSetup()
	// only when HTTP server actually starts. This prevents gRPC-only apps from starting HTTP server.

	// gRPC Server
	address, enabled, err = endpointConfig(app.Config, "GRPC", defaultGRPCPort)
	app.initErr = errors.Join(app.initErr, err)
	app.grpcDisabled = !enabled
	app.grpcServer, err = newGRPCServer(app.container, defaultGRPCPort, app.Config)

	// Continue without gRPC server rather than failing the entire app
	if err != nil {
		app.container.Logger.Errorf("failed to create gRPC server: %v", err)
		app.initErr = errors.Join(app.initErr, err)
	} else {
		app.grpcServer.address = address
	}

	app.subscriptionManager = newSubscriptionManager(app.container)

	// static file server
	currentWd, _ := os.Getwd()
	checkDirectory := filepath.Join(currentWd, defaultPublicStaticDir)

	if _, err = os.Stat(checkDirectory); err == nil {
		app.httpServer.staticFiles[checkDirectory] = "/static"
	}

	return app
}

// NewCMD creates a command-line application.
func NewCMD() *App {
	app := &App{}
	app.readConfig(true)
	app.container = infra.NewContainer(nil)
	app.container.Logger = logging.NewFileLogger(app.Config.Get("CMD_LOGS_FILE"))

	app.cmd = &cmd{
		out: terminal.New(),
	}

	app.container.Create(app.Config)
	app.initTracer()

	return app
}

// initMetricsServer initializes the metrics server based on configuration.
// If METRICS_PORT is explicitly set to 0, the metrics server is disabled.
func (a *App) initMetricsServer() {
	address, enabled, err := endpointConfig(a.Config, "METRICS", defaultMetricPort)
	a.initErr = errors.Join(a.initErr, err)
	if !enabled || err != nil {
		return
	}
	a.metricServer = newMetricServer(defaultMetricPort)
	a.metricServer.address = address
}
