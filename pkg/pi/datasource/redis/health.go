// Package redis provides a client for interacting with Redis key-value stores.This package allows creating and
// managing Redis clients, executing Redis commands, and handling connections to Redis databases.
package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/sllt/pi/pkg/pi/datasource"
)

const (
	healthCheckTimeout = 1 * time.Second
)

// HealthCheck returns the health status of the Redis connection.
func (r *Redis) HealthCheck() datasource.Health {
	return r.HealthCheckContext(context.Background())
}

// HealthCheckContext returns the health status of the Redis connection bounded by ctx.
func (r *Redis) HealthCheckContext(ctx context.Context) datasource.Health {
	h := datasource.Health{
		Details: make(map[string]any),
	}

	if r == nil {
		h.Status = datasource.StatusDown
		h.Details["error"] = "redis not connected"
		return h
	}

	if r.config != nil {
		h.Details["host"] = r.config.HostName + ":" + strconv.Itoa(r.config.Port)
	}

	if ctx == nil {
		ctx = context.Background()
	}
	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		ctx, cancel = context.WithTimeout(ctx, healthCheckTimeout)
		defer cancel()
	}

	if r.Client == nil {
		h.Status = datasource.StatusDown
		h.Details["error"] = "redis not connected"

		return h
	}

	info, err := r.InfoMap(ctx, "Stats").Result()
	if err != nil {
		h.Status = datasource.StatusDown
		h.Details["error"] = err.Error()

		return h
	}

	h.Status = datasource.StatusUp
	h.Details["stats"] = info["Stats"]

	return h
}

// Health returns the health status of the Redis PubSub connection.
func (ps *PubSub) Health() datasource.Health {
	return ps.HealthContext(context.Background())
}

// HealthContext returns the health status of the Redis PubSub connection bounded by ctx.
func (ps *PubSub) HealthContext(ctx context.Context) datasource.Health {
	res := datasource.Health{
		Status: datasource.StatusDown,
		Details: map[string]any{
			"backend": "REDIS",
		},
	}

	if ps == nil {
		res.Details["error"] = "redis pubsub not connected"
		return res
	}

	if ps.config != nil {
		addr := fmt.Sprintf("%s:%d", ps.config.HostName, ps.config.Port)
		res.Details["host"] = addr

		mode := ps.config.PubSubMode
		if mode == "" {
			mode = modeStreams
		}

		res.Details["mode"] = mode
	} else {
		res.Details["mode"] = modeStreams
	}

	if ps.client == nil {
		res.Details["error"] = "redis pubsub not connected"
		return res
	}

	if ctx == nil {
		ctx = context.Background()
	}
	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		ctx, cancel = context.WithTimeout(ctx, defaultRetryTimeout)
		defer cancel()
	}

	if err := ps.client.Ping(ctx).Err(); err != nil {
		if ps.logger != nil {
			ps.logger.Errorf("PubSub health check failed: %v", err)
		}
		res.Details["error"] = err.Error()
		return res
	}

	res.Status = datasource.StatusUp

	return res
}
