package service

import (
	"context"
	"net/http"
	"strings"
	"time"
)

const (
	serviceUp      = "UP"
	serviceDown    = "DOWN"
	defaultTimeout = 5

	StartupPath = "/.well-known/startup"
	AlivePath   = "/.well-known/alive"
	ReadyPath   = "/.well-known/ready"
	HealthPath  = "/.well-known/health"
)

type Health struct {
	Status  string         `json:"status"`
	Details map[string]any `json:"details"`
}

func (h *httpService) HealthCheck(ctx context.Context) *Health {
	return h.getHealthResponseForEndpoint(ctx, strings.TrimPrefix(AlivePath, "/"), defaultTimeout)
}

func (h *httpService) getHealthResponseForEndpoint(ctx context.Context, endpoint string, timeout int) *Health {
	var healthResponse = Health{
		Details: make(map[string]any),
	}

	if ctx == nil {
		ctx = context.Background()
	}
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()

	resp, err := h.Get(ctx, endpoint, nil)

	if err != nil || resp == nil {
		healthResponse.Status = serviceDown
		if err != nil {
			healthResponse.Details["error"] = err.Error()
		} else {
			healthResponse.Details["error"] = "empty response"
		}

		return &healthResponse
	}

	defer resp.Body.Close()

	healthResponse.Details["host"] = resp.Request.URL.Host

	if resp.StatusCode == http.StatusOK {
		healthResponse.Status = serviceUp

		return &healthResponse
	}

	healthResponse.Status = serviceDown
	healthResponse.Details["error"] = "service down"

	return &healthResponse
}
