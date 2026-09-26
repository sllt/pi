package infra

import (
	"github.com/sllt/pi/pkg/pi/config"
	"github.com/sllt/pi/pkg/pi/datasource/file"
	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/sllt/pi/pkg/pi/metrics"
	"github.com/sllt/pi/pkg/pi/websocket"
	"go.opentelemetry.io/otel/metric/noop"
)

// NewIsolated registers in-memory infrastructure without global telemetry,
// connections, remote log polling, listeners or background work.
func NewIsolated(c config.Config, l logging.Logger, m metrics.Manager) *Container {
	if l == nil {
		l = logging.NewLogger(logging.GetLevelFromString(c.Get("LOG_LEVEL")))
	}
	if m == nil {
		m = metrics.NewMetricsManager(noop.NewMeterProvider().Meter("pi"), l)
	}
	x := &Container{Logger: l, appName: c.GetOrDefault("APP_NAME", "pi-app"), appVersion: c.GetOrDefault("APP_VERSION", "dev"), metricsManager: m, WSManager: websocket.New()}
	x.File = file.NewLocalFileSystem(l)
	x.registerFrameworkMetrics()
	return x
}
