package pi

import (
	"context"
	"fmt"
	"net/http"
)

func (a *App) hasTelemetry() bool {
	// Preserve existing opt-outs while allowing the Pi name to take precedence.
	legacy := a.Config.GetOrDefault("KITE_TELEMETRY", defaultTelemetry)
	return a.Config.GetOrDefault("PI_TELEMETRY", legacy) == "true"
}

func (a *App) sendTelemetry(client *http.Client, isStart bool) {
	url := fmt.Sprint(piHost, shutServerPing)

	if isStart {
		url = fmt.Sprint(piHost, startServerPing)

		a.container.Info("Pi records the number of active servers. Set PI_TELEMETRY=false in configs to disable it.")
	}

	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
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
