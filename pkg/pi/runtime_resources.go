package pi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/sllt/pi/pkg/pi/datasource"
	"github.com/sllt/pi/pkg/pi/datasource/file"
	"github.com/sllt/pi/pkg/pi/datasource/pubsub"
	piRedis "github.com/sllt/pi/pkg/pi/datasource/redis"
	piSQL "github.com/sllt/pi/pkg/pi/datasource/sql"
	"github.com/sllt/pi/pkg/pi/service"
)

const (
	resUnstarted int32 = iota
	resRunning
	resStopped
	resFailed
)

func resourceStateString(state int32) string {
	switch state {
	case resUnstarted:
		return "unstarted"
	case resRunning:
		return "running"
	case resStopped:
		return "stopped"
	case resFailed:
		return "failed"
	default:
		return "unknown"
	}
}

func (o Ownership) String() string {
	switch o {
	case Owned:
		return "owned"
	case Borrowed:
		return "borrowed"
	default:
		return "unknown"
	}
}

var reservedResourceNames = map[string]bool{
	"sql":     true,
	"redis":   true,
	"pubsub":  true,
	"file":    true,
	"tracer":  true,
	"name":    true,
	"version": true,
	"status":  true,
	"runtime": true,
}

func isReservedResourceName(name string) bool {
	return reservedResourceNames[strings.TrimSpace(name)]
}

type managedResource struct {
	name      string
	kind      string // "core" | "http_service" | "adapter" | "resource"
	instance  any
	ownership Ownership
	state     atomic.Int32
	active    atomic.Bool

	lastErrMu sync.RWMutex
	lastErr   string

	startFn  func(context.Context) error
	stopFn   func(context.Context) error
	healthFn func(context.Context) (any, error)
}

func (r *managedResource) setLastError(err error) {
	if r == nil {
		return
	}
	msg := errString(err)
	r.lastErrMu.Lock()
	r.lastErr = msg
	r.lastErrMu.Unlock()
}

func (r *managedResource) getLastError() string {
	if r == nil {
		return ""
	}
	r.lastErrMu.RLock()
	msg := r.lastErr
	r.lastErrMu.RUnlock()
	return msg
}

// ResourceRuntimeSnapshot summarizes a managed core datasource, HTTP service, adapter, or custom resource.
type ResourceRuntimeSnapshot struct {
	Name      string `json:"name"`
	Kind      string `json:"kind"`
	Ownership string `json:"ownership"`
	State     string `json:"state"`
	LastError string `json:"lastError,omitempty"`
}

type adapterConfig struct {
	telemetry *bool
	startFn   func(context.Context) error
	stopFn    func(context.Context) error
	healthFn  func(context.Context) error
}

// AdapterOption customizes lifecycle hooks, health checks, or telemetry injection for a mounted adapter.
type AdapterOption func(*adapterConfig)

// WithAdapterHealthCheck overrides the health probe for a mounted adapter.
func WithAdapterHealthCheck(fn func(ctx context.Context) error) AdapterOption {
	return func(c *adapterConfig) {
		c.healthFn = fn
	}
}

// WithAdapterStart overrides the startup hook invoked during App.Start for an Owned adapter.
func WithAdapterStart(fn func(ctx context.Context) error) AdapterOption {
	return func(c *adapterConfig) {
		c.startFn = fn
	}
}

// WithAdapterStop overrides the shutdown hook invoked during App.Stop for an Owned adapter.
func WithAdapterStop(fn func(ctx context.Context) error) AdapterOption {
	return func(c *adapterConfig) {
		c.stopFn = fn
	}
}

// WithAdapterTelemetry explicitly enables or disables Logger/Metrics/Tracer injection on the adapter.
// By default, Owned adapters receive telemetry and Borrowed adapters do not.
func WithAdapterTelemetry(enabled bool) AdapterOption {
	return func(c *adapterConfig) {
		v := enabled
		c.telemetry = &v
	}
}

type loggerSetter interface{ UseLogger(any) }
type metricsSetter interface{ UseMetrics(any) }
type tracerSetter interface{ UseTracer(any) }
type tracerProviderSetter interface{ UseTracerProvider(trace.TracerProvider) }
type serviceNameSetter interface{ UseServiceName(string) }
type startedProvider interface{ IsStarted() bool }
type activeResourceProvider interface{ HasActiveResource() bool }

func evalAdapterOptions(opts []AdapterOption) adapterConfig {
	var cfg adapterConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

func shouldInjectTelemetry(ownership Ownership, cfg adapterConfig) bool {
	if cfg.telemetry != nil {
		return *cfg.telemetry
	}
	return ownership == Owned
}

func injectResourceTelemetry(k *runtimeKernel, name string, instance any, inject bool) {
	if !inject || isNilValue(instance) || k == nil {
		return
	}
	if sns, ok := instance.(serviceNameSetter); ok && name != "" {
		sns.UseServiceName(name)
	}
	if ls, ok := instance.(loggerSetter); ok {
		ls.UseLogger(k.getLogger())
	}
	if ms, ok := instance.(metricsSetter); ok && k.getMetrics() != nil {
		ms.UseMetrics(k.getMetrics())
	}
	tp := k.getTracerProvider()
	if tps, ok := instance.(tracerProviderSetter); ok && tp != nil {
		tps.UseTracerProvider(tp)
	}
	if ts, ok := instance.(tracerSetter); ok && tp != nil {
		tracerName := "pi-" + name
		if name == "pubsub" {
			tracerName = "pi-pubsub"
		}
		ts.UseTracer(tp.Tracer(tracerName))
	}
}

func bindManagedResource(
	k *runtimeKernel,
	name, kind string,
	instance any,
	ownership Ownership,
	preConnected bool,
	opts ...AdapterOption,
) *managedResource {
	cfg := evalAdapterOptions(opts)
	mr := bindManagedResourceWithConfig(name, kind, instance, ownership, preConnected, cfg)
	injectResourceTelemetry(k, name, instance, shouldInjectTelemetry(ownership, cfg))
	return mr
}

func bindManagedResourceWithConfig(
	name, kind string,
	instance any,
	ownership Ownership,
	preConnected bool,
	cfg adapterConfig,
) *managedResource {
	var startFn func(context.Context) error
	if cfg.startFn != nil {
		startFn = cfg.startFn
	} else if !preConnected && !isNilValue(instance) {
		switch s := instance.(type) {
		case interface{ Start(context.Context) error }:
			startFn = s.Start
		case interface{ ConnectContext(context.Context) error }:
			startFn = s.ConnectContext
		case interface{ Connect(context.Context) error }:
			startFn = s.Connect
		case interface{ Connect() error }:
			startFn = func(context.Context) error { return s.Connect() }
		case interface{ Connect() }:
			startFn = func(context.Context) error {
				s.Connect()
				return nil
			}
		}
	}

	var stopFn func(context.Context) error
	if cfg.stopFn != nil {
		stopFn = cfg.stopFn
	} else if !isNilValue(instance) {
		if svc, ok := instance.(service.HTTP); ok {
			stopFn = func(context.Context) error { return service.CloseService(svc) }
		} else {
			switch s := instance.(type) {
			case interface{ Stop(context.Context) error }:
				stopFn = s.Stop
			case interface{ Shutdown(context.Context) error }:
				stopFn = s.Shutdown
			case interface{ CloseContext(context.Context) error }:
				stopFn = s.CloseContext
			case interface{ Close(context.Context) error }:
				stopFn = s.Close
			case interface{ Disconnect(context.Context) error }:
				stopFn = s.Disconnect
			case io.Closer:
				stopFn = func(context.Context) error { return s.Close() }
			}
		}
	}

	var healthFn func(context.Context) (any, error)
	if cfg.healthFn != nil {
		fn := cfg.healthFn
		healthFn = func(ctx context.Context) (any, error) {
			if err := fn(ctx); err != nil {
				return datasource.Health{
					Status:  datasource.StatusDown,
					Details: downHealthDetails(err, false),
				}, err
			}
			return datasource.Health{Status: datasource.StatusUp}, nil
		}
	} else if !isNilValue(instance) {
		if svc, ok := instance.(service.HTTP); ok {
			healthFn = func(ctx context.Context) (any, error) {
				return svc.HealthCheck(ctx), nil
			}
		} else {
			switch h := instance.(type) {
			case interface {
				HealthCheckContext(context.Context) datasource.Health
			}:
				healthFn = func(ctx context.Context) (any, error) {
					return h.HealthCheckContext(ctx), nil
				}
			case interface {
				HealthCheckContext(context.Context) *datasource.Health
			}:
				healthFn = func(ctx context.Context) (any, error) {
					return h.HealthCheckContext(ctx), nil
				}
			case interface {
				HealthCheckContext(context.Context) error
			}:
				healthFn = func(ctx context.Context) (any, error) {
					if err := h.HealthCheckContext(ctx); err != nil {
						return datasource.Health{
							Status:  datasource.StatusDown,
							Details: downHealthDetails(err, false),
						}, err
					}
					return datasource.Health{Status: datasource.StatusUp}, nil
				}
			case interface {
				HealthContext(context.Context) datasource.Health
			}:
				healthFn = func(ctx context.Context) (any, error) {
					return h.HealthContext(ctx), nil
				}
			case interface {
				HealthCheck(context.Context) (any, error)
			}:
				healthFn = h.HealthCheck
			case interface {
				HealthCheck(context.Context) error
			}:
				healthFn = func(ctx context.Context) (any, error) {
					if err := h.HealthCheck(ctx); err != nil {
						return datasource.Health{
							Status:  datasource.StatusDown,
							Details: downHealthDetails(err, false),
						}, err
					}
					return datasource.Health{Status: datasource.StatusUp}, nil
				}
			case interface{ Health() datasource.Health }:
				healthFn = func(context.Context) (any, error) {
					return h.Health(), nil
				}
			case interface{ HealthCheck() datasource.Health }:
				healthFn = func(context.Context) (any, error) {
					return h.HealthCheck(), nil
				}
			case interface{ HealthCheck() *datasource.Health }:
				healthFn = func(context.Context) (any, error) {
					return h.HealthCheck(), nil
				}
			case interface{ HealthCheck() (any, error) }:
				healthFn = func(context.Context) (any, error) {
					return h.HealthCheck()
				}
			}
		}
	}

	mr := &managedResource{
		name:      name,
		kind:      kind,
		instance:  instance,
		ownership: ownership,
		startFn:   startFn,
		stopFn:    stopFn,
		healthFn:  healthFn,
	}

	if ownership == Borrowed {
		mr.state.Store(resRunning)
		mr.active.Store(false)
	} else {
		alreadyStarted := startFn == nil
		if !alreadyStarted && !isNilValue(instance) {
			if sp, ok := instance.(startedProvider); ok && sp.IsStarted() {
				alreadyStarted = true
			}
		}
		if alreadyStarted {
			// Pre-connected Owned resource is active immediately so Stop releases it even if Start is never called.
			mr.state.Store(resRunning)
			mr.active.Store(true)
		} else {
			mr.state.Store(resUnstarted)
			hasActiveUnderlying := false
			if !isNilValue(instance) {
				if ar, ok := instance.(activeResourceProvider); ok && ar.HasActiveResource() {
					hasActiveUnderlying = true
				}
			}
			mr.active.Store(hasActiveUnderlying)
		}
	}

	return mr
}

func validateOwnership(ownership Ownership) error {
	if ownership != Owned && ownership != Borrowed {
		return fmt.Errorf("ownership must be Owned or Borrowed")
	}
	return nil
}

func sameInstance(a, b any) bool {
	if isNilValue(a) || isNilValue(b) {
		return false
	}
	ta := reflect.TypeOf(a)
	tb := reflect.TypeOf(b)
	if ta != tb || !ta.Comparable() {
		return false
	}
	return a == b
}

func coreOrderRank(name string) int {
	switch name {
	case "tracer":
		return 0
	case "sql":
		return 1
	case "redis":
		return 2
	case "pubsub":
		return 3
	case "file":
		return 4
	default:
		return 100
	}
}

func insertCoreResourceLocked(resources []*managedResource, mr *managedResource) []*managedResource {
	targetRank := coreOrderRank(mr.name)
	insertIdx := len(resources)
	for i, existing := range resources {
		if existing.kind != "core" {
			insertIdx = i
			break
		}
		if coreOrderRank(existing.name) > targetRank {
			insertIdx = i
			break
		}
	}
	out := make([]*managedResource, 0, len(resources)+1)
	out = append(out, resources[:insertIdx]...)
	out = append(out, mr)
	out = append(out, resources[insertIdx:]...)
	return out
}

func (a *App) checkMutable() error {
	if a == nil || a.kernel == nil {
		return ErrNilResource
	}
	if a.atomicState.Load() != uint32(lifecycleNew) || a.kernel.frozen.Load() {
		return ErrAppAlreadyStarted
	}
	return nil
}

func (a *App) setCoreSlot(
	name string,
	instance any,
	ownership Ownership,
	preConnected bool,
	allowReplace bool,
) error {
	if err := a.checkMutable(); err != nil {
		return err
	}
	if isNilValue(instance) {
		return fmt.Errorf("%w: %s", ErrNilResource, name)
	}
	if err := validateOwnership(ownership); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}

	k := a.kernel
	var displacedStop func(context.Context) error

	k.mu.Lock()
	if a.atomicState.Load() != uint32(lifecycleNew) || k.frozen.Load() {
		k.mu.Unlock()
		return ErrAppAlreadyStarted
	}

	var oldInst any
	switch name {
	case "sql":
		oldInst = k.sql
	case "redis":
		oldInst = k.redis
	case "pubsub":
		oldInst = k.pubsub
	case "file":
		if k.explicitFile {
			oldInst = k.file
		}
	}

	existingIdx := -1
	var oldMR *managedResource
	for i, mr := range k.resources {
		if mr.name == name && mr.kind == "core" {
			existingIdx = i
			oldMR = mr
			break
		}
	}

	var shouldInject bool
	if !isNilValue(oldInst) {
		// Guard 1: self-assignment updates Ownership in place without closing.
		if sameInstance(instance, oldInst) {
			if oldMR != nil {
				wasBorrowed := oldMR.ownership == Borrowed
				oldMR.ownership = ownership
				if ownership == Borrowed {
					oldMR.active.Store(false)
					oldMR.state.Store(resRunning)
				} else {
					alreadyStarted := oldMR.startFn == nil || oldMR.active.Load()
					if !alreadyStarted {
						if sp, ok := instance.(startedProvider); ok && sp.IsStarted() {
							alreadyStarted = true
						}
					}
					if alreadyStarted {
						oldMR.active.Store(true)
						oldMR.state.Store(resRunning)
					} else {
						hasActiveUnderlying := false
						if ar, ok := instance.(activeResourceProvider); ok && ar.HasActiveResource() {
							hasActiveUnderlying = true
						}
						oldMR.active.Store(hasActiveUnderlying)
						oldMR.state.Store(resUnstarted)
					}
					if wasBorrowed {
						shouldInject = true
					}
				}
			}
			k.mu.Unlock()
			injectResourceTelemetry(k, name, instance, shouldInject)
			return nil
		}

		// Guard 2: SQL wrapper handoff (e.g. dbresolver.Resolver wrapping primary).
		isWrappedSQL := false
		if name == "sql" {
			if unwrapper, ok := instance.(interface{ UnwrapSQL() any }); ok && sameInstance(unwrapper.UnwrapSQL(), oldInst) {
				isWrappedSQL = true
			}
		}

		if !isWrappedSQL {
			if !allowReplace {
				k.mu.Unlock()
				return fmt.Errorf("%w: %q", ErrDuplicateResource, name)
			}
			// Guard 3: Legacy replacement closes active Owned displaced resource.
			if oldMR != nil && oldMR.ownership == Owned && oldMR.active.Swap(false) {
				oldMR.state.Store(resStopped)
				displacedStop = oldMR.stopFn
			}
		}
	}

	var emptyCfg adapterConfig
	mr := bindManagedResourceWithConfig(name, "core", instance, ownership, preConnected, emptyCfg)
	if name == "sql" && ownership == Owned && oldMR != nil && oldMR.active.Load() {
		if unwrapper, ok := instance.(interface{ UnwrapSQL() any }); ok && sameInstance(unwrapper.UnwrapSQL(), oldInst) {
			mr.active.Store(true)
		}
	}
	if existingIdx >= 0 {
		k.resources[existingIdx] = mr
	} else {
		k.resources = insertCoreResourceLocked(k.resources, mr)
	}

	switch name {
	case "sql":
		k.sql = instance.(piSQL.Client)
	case "redis":
		k.redis = instance.(piRedis.Client)
	case "pubsub":
		k.pubsub = instance.(pubsub.Client)
	case "file":
		k.file = instance.(file.FileSystem)
		k.explicitFile = true
	}
	k.names[name] = "core"
	k.adapters.upsertCore(name, instance, false)
	shouldInject = shouldInjectTelemetry(ownership, emptyCfg)
	k.mu.Unlock()

	injectResourceTelemetry(k, name, instance, shouldInject)
	if displacedStop != nil {
		_ = callResourceHook(name, displacedStop, context.Background())
	}
	return nil
}

// SetSQL configures or replaces the primary SQL client before App.Start.
func (a *App) SetSQL(db piSQL.Client, ownership Ownership) error {
	return a.setCoreSlot("sql", db, ownership, false, !a.pureBuild)
}

// SetRedis configures or replaces the primary Redis client before App.Start.
func (a *App) SetRedis(client piRedis.Client, ownership Ownership) error {
	return a.setCoreSlot("redis", client, ownership, false, !a.pureBuild)
}

// SetPubSub configures or replaces the primary PubSub client before App.Start.
func (a *App) SetPubSub(client pubsub.Client, ownership Ownership) error {
	return a.setCoreSlot("pubsub", client, ownership, false, !a.pureBuild)
}

// SetFile configures or replaces the primary FileSystem before App.Start.
func (a *App) SetFile(fs file.FileSystem, ownership Ownership) error {
	return a.setCoreSlot("file", fs, ownership, false, !a.pureBuild)
}

func (a *App) setHTTPServiceInternal(
	name string,
	svc service.HTTP,
	ownership Ownership,
	preConnected bool,
	allowReplace bool,
) error {
	if err := a.checkMutable(); err != nil {
		return err
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("%w: HTTP service name must not be empty", ErrDuplicateResource)
	}
	if isReservedResourceName(name) {
		return fmt.Errorf("%w: %q", ErrReservedResourceName, name)
	}
	if isNilValue(svc) {
		return fmt.Errorf("%w: HTTP service %q", ErrNilResource, name)
	}
	if err := validateOwnership(ownership); err != nil {
		return fmt.Errorf("HTTP service %q: %w", name, err)
	}

	k := a.kernel
	var displacedStop func(context.Context) error

	k.mu.Lock()
	if a.atomicState.Load() != uint32(lifecycleNew) || k.frozen.Load() {
		k.mu.Unlock()
		return ErrAppAlreadyStarted
	}

	existingKind, exists := k.names[name]
	if exists && existingKind != "http_service" {
		k.mu.Unlock()
		return fmt.Errorf("%w: %q already registered as %s", ErrDuplicateResource, name, existingKind)
	}

	k.servicesMu.Lock()
	oldSvc := k.httpServices[name]
	k.servicesMu.Unlock()

	existingIdx := -1
	var oldMR *managedResource
	for i, mr := range k.resources {
		if mr.name == name && mr.kind == "http_service" {
			existingIdx = i
			oldMR = mr
			break
		}
	}

	var shouldInject bool
	if exists {
		if sameInstance(svc, oldSvc) {
			if oldMR != nil {
				wasBorrowed := oldMR.ownership == Borrowed
				oldMR.ownership = ownership
				if ownership == Borrowed {
					oldMR.active.Store(false)
					oldMR.state.Store(resRunning)
				} else {
					alreadyStarted := oldMR.startFn == nil || oldMR.active.Load()
					if !alreadyStarted {
						if sp, ok := any(svc).(startedProvider); ok && sp.IsStarted() {
							alreadyStarted = true
						}
					}
					if alreadyStarted {
						oldMR.active.Store(true)
						oldMR.state.Store(resRunning)
					} else {
						oldMR.active.Store(false)
						oldMR.state.Store(resUnstarted)
					}
					if wasBorrowed {
						shouldInject = true
					}
				}
			}
			k.mu.Unlock()
			injectResourceTelemetry(k, name, svc, shouldInject)
			return nil
		}
		if !allowReplace {
			k.mu.Unlock()
			return fmt.Errorf("%w: %q", ErrDuplicateResource, name)
		}
		k.getLogger().Debugf("Service already registered Name: %v", name)
		if oldMR != nil && oldMR.ownership == Owned && oldMR.active.Swap(false) {
			oldMR.state.Store(resStopped)
			displacedStop = oldMR.stopFn
		}
	}

	var emptyCfg adapterConfig
	mr := bindManagedResourceWithConfig(name, "http_service", svc, ownership, preConnected, emptyCfg)
	if existingIdx >= 0 {
		k.resources[existingIdx] = mr
	} else {
		k.resources = append(k.resources, mr)
	}
	k.names[name] = "http_service"

	k.servicesMu.Lock()
	k.httpServices[name] = svc
	k.servicesMu.Unlock()

	shouldInject = shouldInjectTelemetry(ownership, emptyCfg)
	k.mu.Unlock()

	injectResourceTelemetry(k, name, svc, shouldInject)
	if displacedStop != nil {
		_ = callResourceHook(name, displacedStop, context.Background())
	}
	return nil
}

// SetHTTPService registers a pre-constructed service.HTTP instance before App.Start.
func (a *App) SetHTTPService(name string, svc service.HTTP, ownership Ownership) error {
	return a.setHTTPServiceInternal(name, svc, ownership, true, !a.pureBuild)
}

// Mount registers an external datasource or custom adapter before App.Start.
func (a *App) Mount(name string, instance any, ownership Ownership, opts ...AdapterOption) error {
	if err := a.checkMutable(); err != nil {
		return err
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("%w: adapter name must not be empty", ErrDuplicateResource)
	}
	if isReservedResourceName(name) {
		return fmt.Errorf("%w: %q", ErrReservedResourceName, name)
	}
	if isNilValue(instance) {
		return fmt.Errorf("%w: adapter %q", ErrNilResource, name)
	}
	if err := validateOwnership(ownership); err != nil {
		return fmt.Errorf("adapter %q: %w", name, err)
	}

	// Evaluate AdapterOptions before acquiring k.mu so option closures cannot deadlock on App/kernel locks.
	cfg := evalAdapterOptions(opts)
	shouldInject := shouldInjectTelemetry(ownership, cfg)

	k := a.kernel
	k.mu.Lock()
	if a.atomicState.Load() != uint32(lifecycleNew) || k.frozen.Load() {
		k.mu.Unlock()
		return ErrAppAlreadyStarted
	}
	if existingKind, exists := k.names[name]; exists {
		k.mu.Unlock()
		return fmt.Errorf("%w: %q already registered as %s", ErrDuplicateResource, name, existingKind)
	}

	mr := bindManagedResourceWithConfig(name, "adapter", instance, ownership, false, cfg)
	k.resources = append(k.resources, mr)
	k.names[name] = "adapter"
	k.adapters.upsertAdapter(name, instance)
	k.mu.Unlock()

	injectResourceTelemetry(k, name, instance, shouldInject)
	return nil
}

// MustMount registers an external adapter before App.Start or panics on error.
func (a *App) MustMount(name string, instance any, ownership Ownership, opts ...AdapterOption) {
	if err := a.Mount(name, instance, ownership, opts...); err != nil {
		panic(err)
	}
}

func (k *runtimeKernel) addCustomResource(r Resource) error {
	name := strings.TrimSpace(r.Name)
	if name == "" {
		return fmt.Errorf("%w: empty resource name", ErrDuplicateResource)
	}
	if isReservedResourceName(name) {
		return fmt.Errorf("%w: %q", ErrReservedResourceName, name)
	}
	if err := validateOwnership(r.Ownership); err != nil {
		return fmt.Errorf("resource %s: %w", name, err)
	}

	k.mu.Lock()
	defer k.mu.Unlock()
	if existingKind, exists := k.names[name]; exists {
		return fmt.Errorf("%w: %q already registered as %s", ErrDuplicateResource, name, existingKind)
	}
	mr := &managedResource{
		name:      name,
		kind:      "resource",
		ownership: r.Ownership,
		startFn:   r.Start,
		stopFn:    r.Stop,
	}
	if r.Ownership == Borrowed {
		mr.state.Store(resRunning)
		mr.active.Store(false)
	} else if r.Start == nil {
		mr.state.Store(resRunning)
		mr.active.Store(true)
	} else {
		mr.state.Store(resUnstarted)
		mr.active.Store(false)
	}
	k.resources = append(k.resources, mr)
	k.names[name] = "resource"
	return nil
}

func (a *App) startResources(ctx context.Context) error {
	if a == nil || a.kernel == nil {
		return nil
	}
	k := a.kernel
	k.freeze()

	k.mu.RLock()
	resources := append([]*managedResource(nil), k.resources...)
	k.mu.RUnlock()

	rollbackOnFailure := func() error {
		timeout, _ := a.shutdownTimeout()
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		rbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		defer cancel()
		return k.stopActiveResources(rbCtx)
	}

	for _, r := range resources {
		if r.ownership != Owned {
			continue
		}
		if err := ctx.Err(); err != nil {
			rbErr := rollbackOnFailure()
			return errors.Join(err, rbErr)
		}
		r.active.Store(true)
		if r.startFn != nil {
			if err := callResourceHook(r.name, r.startFn, ctx); err != nil {
				r.state.Store(resFailed)
				r.setLastError(err)
				rbErr := rollbackOnFailure()
				return errors.Join(err, rbErr)
			}
		}
		r.state.Store(resRunning)
		r.setLastError(nil)
	}
	return nil
}

func (a *App) stopResources(ctx context.Context) error {
	if a == nil || a.kernel == nil {
		return nil
	}
	return a.kernel.stopActiveResources(ctx)
}

func (k *runtimeKernel) stopActiveResources(ctx context.Context) error {
	if k == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	k.mu.RLock()
	resources := append([]*managedResource(nil), k.resources...)
	k.mu.RUnlock()

	var err error
	for i := len(resources) - 1; i >= 0; i-- {
		r := resources[i]
		if r.ownership == Owned && r.active.Swap(false) {
			if r.state.Load() != resFailed {
				r.state.Store(resStopped)
			}
			if r.stopFn != nil {
				if stopErr := callResourceHook(r.name, r.stopFn, ctx); stopErr != nil {
					if prevErr := r.getLastError(); prevErr != "" {
						r.setLastError(fmt.Errorf("%s; %w", prevErr, stopErr))
					} else {
						r.setLastError(stopErr)
					}
					err = errors.Join(err, stopErr)
				}
			}
		}
	}
	return err
}

func (k *runtimeKernel) snapshotResources() []ResourceRuntimeSnapshot {
	if k == nil {
		return nil
	}
	k.mu.RLock()
	resources := append([]*managedResource(nil), k.resources...)
	k.mu.RUnlock()

	if len(resources) == 0 {
		return nil
	}
	out := make([]ResourceRuntimeSnapshot, 0, len(resources))
	for _, r := range resources {
		out = append(out, ResourceRuntimeSnapshot{
			Name:      r.name,
			Kind:      r.kind,
			Ownership: r.ownership.String(),
			State:     resourceStateString(r.state.Load()),
			LastError: r.getLastError(),
		})
	}
	return out
}

func callResourceHook(name string, fn func(context.Context) error, ctx context.Context) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("resource %s panicked: %v", name, v)
		}
	}()
	if e := fn(ctx); e != nil {
		return fmt.Errorf("resource %s: %w", name, e)
	}
	return nil
}
