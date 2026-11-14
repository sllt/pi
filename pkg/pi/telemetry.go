package pi

import (
	"context"
	"fmt"
	"net/http"
)

func (a *App) hasTelemetry() bool {
	if a == nil || a.Config == nil {
		return false
	}
	fallback := defaultTelemetry
	if a.pureBuild {
		fallback = "false"
	}
	// Preserve existing opt-outs while allowing the Pi name to take precedence.
	legacy := a.Config.GetOrDefault("KITE_TELEMETRY", fallback)
	return a.Config.GetOrDefault("PI_TELEMETRY", legacy) == "true"
}

func (a *App) sendTelemetry(client *http.Client, isStart bool) {
	a.sendTelemetryContext(context.Background(), client, isStart)
}

func (a *App) sendTelemetryContext(parent context.Context, client *http.Client, isStart bool) {
	if parent == nil {
		parent = context.Background()
	}
	if parent.Err() != nil {
		return
	}

	url := fmt.Sprint(piHost, shutServerPing)

	if isStart {
		url = fmt.Sprint(piHost, startServerPing)

		if l := a.safeLogger(); l != nil {
			l.Info("Pi records the number of active servers. Set PI_TELEMETRY=false in configs to disable it.")
		}
	}

	ctx, cancel := context.WithTimeout(parent, pingTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, http.NoBody)
	if err != nil {
		return
	}

	req.Header.Set("Connection", "close")

	resp, err := client.Do(req)
	if err != nil {
		return
	}

	resp.Body.Close()
}
