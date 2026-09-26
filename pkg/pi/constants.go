package pi

import "time"

const (
	defaultPublicStaticDir = "static"
	shutDownTimeout        = 30 * time.Second
	piTraceExporter        = "pi"
	piTracerURL            = "https://tracer.github.com/sllt/pi"
	checkPortTimeout       = 2 * time.Second
	piHost                 = "https://github.com/sllt/pi"
	startServerPing        = "/api/ping/up"
	shutServerPing         = "/api/ping/down"
	pingTimeout            = 5 * time.Second
	defaultTelemetry       = "true"
	defaultReflection      = "false"
)
