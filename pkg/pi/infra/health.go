package infra

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"
	"sync"
	"time"

	"github.com/sllt/pi/pkg/pi/datasource"
	"github.com/sllt/pi/pkg/pi/service"
)

const defaultDeepHealthTimeout = 5 * time.Second

var (
	zeroContainerHealthInitMu sync.Mutex
	errHealthAdmissionClosed  = errors.New("container health check admission closed during shutdown")
)

type containerHealthState struct {
	mu             sync.Mutex
	closed         bool
	active         int
	idleCh         chan struct{}
	inFlight       map[string]*inFlightHealthCall
	activeHandlers int
	handlersIdleCh chan struct{}
}

func (hs *containerHealthState) handlersIdleChan() <-chan struct{} {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	if hs.activeHandlers == 0 || hs.handlersIdleCh == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return hs.handlersIdleCh
}

func (hs *containerHealthState) tryAcquireLocked() bool {
	if hs.closed {
		return false
	}
	if hs.active == 0 {
		hs.idleCh = make(chan struct{})
	}
	hs.active++
	return true
}

func (hs *containerHealthState) releaseLocked() {
	if hs.active > 0 {
		hs.active--
		if hs.active == 0 && hs.idleCh != nil {
			close(hs.idleCh)
			hs.idleCh = nil
		}
	}
}

func (hs *containerHealthState) idleChan() <-chan struct{} {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	if hs.active == 0 || hs.idleCh == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return hs.idleCh
}

type inFlightHealthCall struct {
	done chan struct{}
	val  any
	err  error
}

type contextSQLHealthChecker interface {
	HealthCheckContext(ctx context.Context) *datasource.Health
}

type contextRedisHealthChecker interface {
	HealthCheckContext(ctx context.Context) datasource.Health
}

type contextPubSubHealthChecker interface {
	HealthContext(ctx context.Context) datasource.Health
}

func (c *Container) getHealthState() *containerHealthState {
	zeroContainerHealthInitMu.Lock()
	defer zeroContainerHealthInitMu.Unlock()
	if c.healthState == nil {
		c.healthState = &containerHealthState{}
	}
	return c.healthState
}

// CloseHealthAdmission seals the container against starting new health check samplers during shutdown.
func (c *Container) CloseHealthAdmission() {
	if c == nil {
		return
	}
	hs := c.getHealthState()
	hs.mu.Lock()
	hs.closed = true
	hs.mu.Unlock()
}

// WaitHealthSamplers waits for all in-flight health check calls/goroutines to finish or ctx to expire.
func (c *Container) WaitHealthSamplers(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ch := c.getHealthState().idleChan()
	select {
	case <-ch:
		return nil
	default:
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WaitHealthSamplersAll blocks until every in-flight health check call/goroutine has exited.
func (c *Container) WaitHealthSamplersAll() {
	if c == nil {
		return
	}
	<-c.getHealthState().idleChan()
}

// BeginHandlerExecution tracks an in-flight HTTP handler goroutine so shutdown waits for
// timed-out or client-canceled request goroutines before closing Owned resources.
func (c *Container) BeginHandlerExecution() func() {
	if c == nil {
		return func() {}
	}
	hs := c.getHealthState()
	hs.mu.Lock()
	if hs.activeHandlers == 0 {
		hs.handlersIdleCh = make(chan struct{})
	}
	hs.activeHandlers++
	hs.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			hs.mu.Lock()
			if hs.activeHandlers > 0 {
				hs.activeHandlers--
				if hs.activeHandlers == 0 && hs.handlersIdleCh != nil {
					close(hs.handlersIdleCh)
					hs.handlersIdleCh = nil
				}
			}
			hs.mu.Unlock()
		})
	}
}

// WaitHandlers waits for all in-flight HTTP handler execution goroutines to finish or ctx to expire.
func (c *Container) WaitHandlers(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ch := c.getHealthState().handlersIdleChan()
	select {
	case <-ch:
		return nil
	default:
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WaitHandlersAll blocks until every in-flight HTTP handler execution goroutine has exited.
func (c *Container) WaitHandlersAll() {
	if c == nil {
		return
	}
	<-c.getHealthState().handlersIdleChan()
}

func runDirectHealth[T any](c *Container, key string, fn func() T) (val T, err error) {
	hs := c.getHealthState()
	hs.mu.Lock()
	if !hs.tryAcquireLocked() {
		hs.mu.Unlock()
		return val, errHealthAdmissionClosed
	}
	hs.mu.Unlock()
	defer func() {
		hs.mu.Lock()
		hs.releaseLocked()
		hs.mu.Unlock()
	}()
	defer func() {
		if r := recover(); r != nil {
			if c != nil && !isNil(c.Logger) {
				c.Logger.Errorf("health check %q panicked: %v\n%s", key, r, debug.Stack())
			}
			var zero T
			val = zero
			err = fmt.Errorf("health check %q panicked: %v", key, r)
		}
	}()
	return fn(), nil
}

func downHealthDetails(err error, timedOut bool) map[string]any {
	errMsg := "health check failed"
	if timedOut {
		errMsg = "health check timed out"
	}
	if err != nil {
		errMsg = err.Error()
	}
	details := map[string]any{
		"error": errMsg,
	}
	if timedOut {
		details["isolation"] = "singleflight_in_progress"
	}
	return details
}

func (c *Container) runBoundedHealth(key string, ctx context.Context, fn func() (any, error)) (any, error, bool) {
	hs := c.getHealthState()
	hs.mu.Lock()
	if hs.closed {
		hs.mu.Unlock()
		return nil, errHealthAdmissionClosed, false
	}
	if hs.inFlight == nil {
		hs.inFlight = make(map[string]*inFlightHealthCall)
	}
	call, exists := hs.inFlight[key]
	if !exists {
		hs.tryAcquireLocked()
		call = &inFlightHealthCall{done: make(chan struct{})}
		hs.inFlight[key] = call
		go func(hc *inFlightHealthCall) {
			defer func() {
				hs.mu.Lock()
				if hs.inFlight[key] == hc {
					delete(hs.inFlight, key)
				}
				hs.releaseLocked()
				hs.mu.Unlock()
				close(hc.done)
			}()
			defer func() {
				if r := recover(); r != nil {
					if c != nil && !isNil(c.Logger) {
						c.Logger.Errorf("health check %q panicked: %v\n%s", key, r, debug.Stack())
					}
					hc.val = nil
					hc.err = fmt.Errorf("health check %q panicked: %v", key, r)
				}
			}()
			hc.val, hc.err = fn()
		}(call)
	}
	hs.mu.Unlock()

	select {
	case <-call.done:
		return call.val, call.err, false
	case <-ctx.Done():
		return nil, ctx.Err(), true
	}
}

func (c *Container) Health(ctx context.Context) any {
	var (
		healthMap = make(map[string]any)
		downCount int
	)

	if ctx == nil {
		ctx = context.Background()
	}
	probeCtx := ctx
	var cancel context.CancelFunc
	if _, hasDeadline := probeCtx.Deadline(); !hasDeadline {
		probeCtx, cancel = context.WithTimeout(probeCtx, defaultDeepHealthTimeout)
		defer cancel()
	}

	const statusDown = "DOWN"

	if !isNil(c.SQL) {
		var health *datasource.Health
		if ctxChecker, ok := c.SQL.(contextSQLHealthChecker); ok {
			h, err := runDirectHealth(c, "sql", func() *datasource.Health {
				return ctxChecker.HealthCheckContext(probeCtx)
			})
			if err != nil {
				health = &datasource.Health{
					Status:  statusDown,
					Details: downHealthDetails(err, false),
				}
			} else {
				health = h
			}
		} else {
			val, err, timedOut := c.runBoundedHealth("sql", probeCtx, func() (any, error) {
				return c.SQL.HealthCheck(), nil
			})
			if timedOut || err != nil {
				health = &datasource.Health{
					Status:  statusDown,
					Details: downHealthDetails(err, timedOut),
				}
			} else if h, ok := val.(*datasource.Health); ok && h != nil {
				health = h
			} else {
				health = &datasource.Health{Status: statusDown}
			}
		}
		if health == nil || health.Status == statusDown {
			downCount++
		}

		healthMap["sql"] = health
	}

	if !isNil(c.Redis) {
		var health datasource.Health
		if ctxChecker, ok := c.Redis.(contextRedisHealthChecker); ok {
			h, err := runDirectHealth(c, "redis", func() datasource.Health {
				return ctxChecker.HealthCheckContext(probeCtx)
			})
			if err != nil {
				health = datasource.Health{
					Status:  statusDown,
					Details: downHealthDetails(err, false),
				}
			} else {
				health = h
			}
		} else {
			val, err, timedOut := c.runBoundedHealth("redis", probeCtx, func() (any, error) {
				return c.Redis.HealthCheck(), nil
			})
			if timedOut || err != nil {
				health = datasource.Health{
					Status:  statusDown,
					Details: downHealthDetails(err, timedOut),
				}
			} else if h, ok := val.(datasource.Health); ok {
				health = h
			} else {
				health = datasource.Health{Status: statusDown}
			}
		}
		if health.Status == statusDown {
			downCount++
		}

		healthMap["redis"] = health
	}

	if !isNil(c.PubSub) {
		var health datasource.Health
		if ctxChecker, ok := c.PubSub.(contextPubSubHealthChecker); ok {
			h, err := runDirectHealth(c, "pubsub", func() datasource.Health {
				return ctxChecker.HealthContext(probeCtx)
			})
			if err != nil {
				health = datasource.Health{
					Status:  statusDown,
					Details: downHealthDetails(err, false),
				}
			} else {
				health = h
			}
		} else {
			val, err, timedOut := c.runBoundedHealth("pubsub", probeCtx, func() (any, error) {
				return c.PubSub.Health(), nil
			})
			if timedOut || err != nil {
				health = datasource.Health{
					Status:  statusDown,
					Details: downHealthDetails(err, timedOut),
				}
			} else if h, ok := val.(datasource.Health); ok {
				health = h
			} else {
				health = datasource.Health{Status: statusDown}
			}
		}
		if health.Status == statusDown {
			downCount++
		}

		healthMap["pubsub"] = health
	}

	downCount += checkExternalDBHealth(probeCtx, c, healthMap)

	for name, svc := range c.Services {
		if isNil(svc) {
			continue
		}
		svcRef := svc
		health, err := runDirectHealth(c, "service:"+name, func() *service.Health {
			return svcRef.HealthCheck(probeCtx)
		})
		if err != nil {
			health = &service.Health{
				Status:  statusDown,
				Details: downHealthDetails(err, false),
			}
		}
		if health == nil || health.Status == statusDown {
			downCount++
		}

		healthMap[name] = health
	}

	c.appHealth(healthMap, downCount)

	return healthMap
}

func checkExternalDBHealth(ctx context.Context, c *Container, healthMap map[string]any) (downCount int) {
	services := map[string]interface {
		HealthCheck(context.Context) (any, error)
	}{
		"mongo":         c.Mongo,
		"cassandra":     c.Cassandra,
		"clickHouse":    c.Clickhouse,
		"kv-store":      c.KVStore,
		"dgraph":        c.DGraph,
		"opentsdb":      c.OpenTSDB,
		"elasticsearch": c.Elasticsearch,
		"oracle":        c.Oracle,
		"couchbase":     c.Couchbase,
		"influx":        c.InfluxDB,
	}

	for name, svc := range services {
		if !isNil(svc) {
			svcRef := svc
			health, err, timedOut := c.runBoundedHealth("ext:"+name, ctx, func() (any, error) {
				return svcRef.HealthCheck(ctx)
			})
			if timedOut || err != nil {
				downCount++
				if timedOut || health == nil {
					health = datasource.Health{
						Status:  "DOWN",
						Details: downHealthDetails(err, timedOut),
					}
				}
			} else if h, ok := health.(datasource.Health); ok && h.Status == "DOWN" {
				downCount++
			} else if hp, ok := health.(*datasource.Health); ok && hp != nil && hp.Status == "DOWN" {
				downCount++
			}

			healthMap[name] = health
		}
	}

	return downCount
}

func (c *Container) appHealth(healthMap map[string]any, downCount int) {
	healthMap["name"] = c.GetAppName()
	healthMap["version"] = c.GetAppVersion()

	if downCount == 0 {
		healthMap["status"] = "UP"
	} else {
		healthMap["status"] = "DEGRADED"
	}
}

func isNil(i any) bool {
	// Get the value of the interface
	val := reflect.ValueOf(i)

	// If the interface is not assigned or is nil, return true
	return !val.IsValid() || val.IsNil()
}
