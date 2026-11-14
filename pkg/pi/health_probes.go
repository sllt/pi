package pi

import (
	"context"
	"net/http"
	"time"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/sllt/pi/pkg/pi/logging"
)

const defaultAppDeepHealthTimeout = 5 * time.Second

type probeUnavailableError struct {
	reason string
}

func (e probeUnavailableError) Error() string {
	if e.reason != "" {
		return e.reason
	}
	return http.StatusText(http.StatusServiceUnavailable)
}

func (probeUnavailableError) StatusCode() int {
	return http.StatusServiceUnavailable
}

func (probeUnavailableError) LogLevel() logging.Level {
	return logging.DEBUG
}

// isStartupComplete reports whether App.Start has completed successfully at least once.
// Subsequent local component restarts only affect readiness, never resetting startup to false.
func (a *App) isStartupComplete() bool {
	return a != nil && a.startupCompleted.Load()
}

func (a *App) actorTreeUnexpectedlyDead() bool {
	if a == nil {
		return true
	}
	if a.actorTreeDead.Load() {
		return true
	}
	a.runtimeMu.Lock()
	rt := a.actorRT
	a.runtimeMu.Unlock()
	if rt != nil && rt.node != nil && !a.isIntakeClosed() {
		select {
		case <-rt.node.Done():
			return true
		default:
		}
		lastBeat := rt.rootLastBeatNano.Load()
		maxAge := rt.rootHeartbeatMaxAge.Load()
		if lastBeat > 0 && maxAge > 0 && time.Now().UnixNano()-lastBeat > maxAge {
			return true
		}
	}
	return false
}

// isAlive reports process & supervision-tree liveness without performing any external DB/MQ/HTTP I/O.
func (a *App) isAlive() bool {
	if a == nil || a.actorTreeUnexpectedlyDead() {
		return false
	}
	a.runtimeMu.Lock()
	state := a.runtimeState
	a.runtimeMu.Unlock()
	return state != lifecycleStopped
}

func (a *App) startupProbeHandler(*Context) (any, error) {
	if !a.isStartupComplete() {
		return nil, probeUnavailableError{reason: "application startup not completed"}
	}
	return struct {
		Status  string `json:"status"`
		Startup bool   `json:"startup"`
	}{
		Status:  "UP",
		Startup: true,
	}, nil
}

func (a *App) aliveProbeHandler(*Context) (any, error) {
	if !a.isAlive() {
		return nil, probeUnavailableError{reason: "application liveness check failed"}
	}
	return struct {
		Status string `json:"status"`
	}{
		Status: "UP",
	}, nil
}

func (a *App) readyProbeHandler(*Context) (any, error) {
	if !a.isReady() {
		return nil, probeUnavailableError{reason: "application not ready"}
	}
	return struct {
		Status string `json:"status"`
		Ready  bool   `json:"ready"`
	}{
		Status: "UP",
		Ready:  true,
	}, nil
}

func (a *App) deepHealthHandler(c *Context) (any, error) {
	probeCtx := c.Context
	if probeCtx == nil {
		probeCtx = context.Background()
	}
	var cancel context.CancelFunc
	if _, hasDeadline := probeCtx.Deadline(); !hasDeadline {
		probeCtx, cancel = context.WithTimeout(probeCtx, defaultAppDeepHealthTimeout)
		defer cancel()
	}

	res := c.Health(probeCtx)
	if healthMap, ok := res.(map[string]any); ok && a != nil && a.needsActorRuntime() && !a.isReady() {
		healthMap["status"] = "DEGRADED"
		healthMap["runtime"] = map[string]any{
			"status":  "DOWN",
			"ready":   false,
			"startup": a.isStartupComplete(),
		}
	}
	return res, nil
}

// syncGRPCHealth synchronizes gRPC HealthCheck serving status ("" and all registered services)
// with App readiness. Never call while holding a component slot mutex or runtimeMu.
func (a *App) syncGRPCHealth() {
	if a == nil || a.grpcServer == nil || !a.grpcHealthEnabled.Load() {
		return
	}
	a.grpcServer.mu.Lock()
	h := a.grpcServer.health
	if h == nil {
		a.grpcServer.mu.Unlock()
		return
	}
	services := append([]string(nil), a.grpcServer.serviceNames...)
	a.grpcServer.mu.Unlock()

	a.grpcHealthSyncMu.Lock()
	defer a.grpcHealthSyncMu.Unlock()

	status := healthpb.HealthCheckResponse_NOT_SERVING
	if a.isReady() {
		status = healthpb.HealthCheckResponse_SERVING
	}
	h.SetServingStatus("", status)
	for _, svc := range services {
		h.SetServingStatus(svc, status)
	}
}

// notifyGRPCHealthChanged schedules an asynchronous gRPC health resync without acquiring locks.
func (a *App) notifyGRPCHealthChanged() {
	if a == nil || !a.grpcHealthEnabled.Load() {
		return
	}
	a.grpcHealthOnce.Do(func() {
		a.grpcHealthDirtyCh = make(chan struct{}, 1)
	})
	select {
	case a.grpcHealthDirtyCh <- struct{}{}:
	default:
	}
}

func (a *App) startGRPCHealthSyncWorker() {
	if a == nil || !a.grpcHealthEnabled.Load() {
		return
	}
	a.grpcHealthOnce.Do(func() {
		a.grpcHealthDirtyCh = make(chan struct{}, 1)
	})
	if !a.grpcHealthLoopStarted.CompareAndSwap(false, true) {
		return
	}
	if !a.tryAdmitRuntimeTask() {
		a.grpcHealthLoopStarted.Store(false)
		return
	}
	intakeDone := a.intakeDone()
	dirtyCh := a.grpcHealthDirtyCh
	watchHeartbeat := a.needsActorRuntime()
	go func() {
		defer a.runtimeTasks.Done()
		var tickCh <-chan time.Time
		if watchHeartbeat {
			ticker := time.NewTicker(250 * time.Millisecond)
			defer ticker.Stop()
			tickCh = ticker.C
		}
		for {
			select {
			case <-intakeDone:
				a.syncGRPCHealth()
				return
			case <-dirtyCh:
				a.syncGRPCHealth()
			case <-tickCh:
				a.syncGRPCHealth()
			}
		}
	}()
}
