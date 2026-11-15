package nats

import (
	"context"

	"github.com/sllt/pi/pkg/pi/datasource"
)

const (
	natsBackend            = "Client"
	jetStreamStatusOK      = "OK"
	jetStreamStatusError   = "Error"
	jetStreamConnected     = "CONNECTED"
	jetStreamDisconnecting = "DISCONNECTED"
)

// Health checks the health of the NATS connection.
func (c *Client) Health() datasource.Health {
	if c == nil {
		return datasource.Health{Status: datasource.StatusDown}
	}

	c.connMu.RLock()
	connMgr := c.connManager
	c.connMu.RUnlock()

	if connMgr == nil {
		return datasource.Health{
			Status: datasource.StatusDown,
		}
	}

	health := connMgr.Health()
	if health.Details == nil {
		health.Details = make(map[string]any)
	}
	health.Details["backend"] = natsBackend

	js, err := connMgr.jetStream()
	if err != nil {
		health.Details["jetstream_enabled"] = false
		health.Details["jetstream_status"] = jetStreamStatusError + ": " + err.Error()

		return health
	}

	// Call AccountInfo() to get jStream status
	jetStreamStatus, err := GetJetStreamStatus(context.Background(), js)
	if err != nil {
		jetStreamStatus = jetStreamStatusError + ": " + err.Error()
	}

	health.Details["jetstream_enabled"] = true
	health.Details["jetstream_status"] = jetStreamStatus

	return health
}
