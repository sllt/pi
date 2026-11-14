package exporters

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/google/uuid"

	"github.com/sllt/pi/pkg/pi/version"
)

const (
	defaultTelemetryEndpoint = "https://github.com/sllt/pi/telemetry/v1/metrics"
	defaultAppName           = "pi-app"
	requestTimeout           = 10 * time.Second
)

// TelemetryData represents the JSON telemetry payload.
type TelemetryData struct {
	Timestamp        string `json:"timestamp"`
	EventID          string `json:"event_id"`
	Source           string `json:"source"`
	ServiceName      string `json:"service_name,omitempty"`
	ServiceVersion   string `json:"service_version,omitempty"`
	RawDataSize      int    `json:"raw_data_size"`
	FrameworkVersion string `json:"framework_version,omitempty"`
	GoVersion        string `json:"go_version,omitempty"`
	OS               string `json:"os,omitempty"`
	Architecture     string `json:"architecture,omitempty"`
	StartupTime      string `json:"startup_time,omitempty"`
}

// SendFrameworkStartupTelemetry sends telemetry data asynchronously when enabled by ambient environment.
// Deprecated for internal construction paths: framework runtime lifecycle calls SendFrameworkStartupTelemetryContext.
func SendFrameworkStartupTelemetry(appName, appVersion string) {
	if !telemetryEnabled() {
		return
	}

	go sendTelemetryData(context.Background(), appName, appVersion)
}

// SendFrameworkStartupTelemetryContext sends startup telemetry synchronously bounded by ctx.
func SendFrameworkStartupTelemetryContext(ctx context.Context, appName, appVersion string) {
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		return
	}
	sendTelemetryData(ctx, appName, appVersion)
}

func telemetryEnabled() bool {
	setting := os.Getenv("PI_TELEMETRY")
	if setting == "" {
		setting = os.Getenv("KITE_TELEMETRY")
	}
	return setting != "false"
}

func sendTelemetryData(ctx context.Context, appName, appVersion string) {
	if appName == "" {
		appName = defaultAppName
	}

	if appVersion == "" {
		appVersion = "unknown"
	}

	now := time.Now().UTC()

	data := TelemetryData{
		Timestamp:        now.Format(time.RFC3339),
		EventID:          uuid.New().String(),
		Source:           "pi-framework",
		ServiceName:      appName,
		ServiceVersion:   appVersion,
		RawDataSize:      0,
		FrameworkVersion: version.Framework,
		GoVersion:        runtime.Version(),
		OS:               runtime.GOOS,
		Architecture:     runtime.GOARCH,
		StartupTime:      now.Format(time.RFC3339),
	}

	sendToEndpointContext(ctx, &data, defaultTelemetryEndpoint)
}

func sendToEndpoint(data *TelemetryData, endpoint string) {
	sendToEndpointContext(context.Background(), data, endpoint)
}

func sendToEndpointContext(parent context.Context, data *TelemetryData, endpoint string) {
	if parent == nil {
		parent = context.Background()
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(parent, requestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		return
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: requestTimeout}

	resp, err := client.Do(req)
	if err != nil {
		return
	}

	resp.Body.Close()
}
