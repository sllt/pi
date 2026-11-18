package pi

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/sllt/pi/pkg/pi/datasource"
	"github.com/sllt/pi/pkg/pi/service"
)

const defaultDeepHealthTimeout = 5 * time.Second

var errHealthAdmissionClosed = errors.New("runtime health check admission closed during shutdown")

type inFlightHealthCall struct {
	done chan struct{}
	val  any
	err  error
}

type kernelHealthState struct {
	mu              sync.Mutex
	closed          bool
	admissionCtx    context.Context
	admissionCancel context.CancelFunc
	active          int
	idleCh          chan struct{}
	inFlight        map[string]*inFlightHealthCall
	activeHandlers  int
	handlersIdleCh  chan struct{}
	stoppingCh      chan struct{}
	stoppingClosed  bool
}

func newKernelHealthState() *kernelHealthState {
	admCtx, admCancel := context.WithCancel(context.Background())
	return &kernelHealthState{
		admissionCtx:    admCtx,
		admissionCancel: admCancel,
		inFlight:        make(map[string]*inFlightHealthCall),
		stoppingCh:      make(chan struct{}),
	}
}

func (hs *kernelHealthState) tryAcquireLocked() bool {
	if hs.closed {
		return false
	}
	if hs.active == 0 {
		hs.idleCh = make(chan struct{})
	}
	hs.active++
	return true
}

func (hs *kernelHealthState) releaseLocked() {
	if hs.active > 0 {
		hs.active--
		if hs.active == 0 && hs.idleCh != nil {
			close(hs.idleCh)
			hs.idleCh = nil
		}
	}
}

func (hs *kernelHealthState) idleChan() <-chan struct{} {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	if hs.active == 0 || hs.idleCh == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return hs.idleCh
}

func (hs *kernelHealthState) handlersIdleChan() <-chan struct{} {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	if hs.activeHandlers == 0 || hs.handlersIdleCh == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return hs.handlersIdleCh
}

func (k *runtimeKernel) CloseHealthAdmission() {
	if k == nil || k.healthState == nil {
		return
	}
	hs := k.healthState
	hs.mu.Lock()
	hs.closed = true
	cancel := hs.admissionCancel
	hs.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (k *runtimeKernel) WaitHealthSamplers(ctx context.Context) error {
	if k == nil || k.healthState == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ch := k.healthState.idleChan()
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

func (k *runtimeKernel) WaitHealthSamplersAll() {
	if k == nil || k.healthState == nil {
		return
	}
	<-k.healthState.idleChan()
}

func (k *runtimeKernel) BeginHandlerExecution() func() {
	if k == nil || k.healthState == nil {
		return func() {}
	}
	hs := k.healthState
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

func (k *runtimeKernel) WaitHandlers(ctx context.Context) error {
	if k == nil || k.healthState == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ch := k.healthState.handlersIdleChan()
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

func (k *runtimeKernel) WaitHandlersAll() {
	if k == nil || k.healthState == nil {
		return
	}
	<-k.healthState.handlersIdleChan()
}

func (k *runtimeKernel) MarkStopping() {
	if k == nil || k.healthState == nil {
		return
	}
	hs := k.healthState
	hs.mu.Lock()
	if !hs.stoppingClosed {
		if hs.stoppingCh == nil {
			hs.stoppingCh = make(chan struct{})
		}
		close(hs.stoppingCh)
		hs.stoppingClosed = true
	}
	hs.mu.Unlock()
}

func (k *runtimeKernel) ResetStoppingChan() {
	if k == nil || k.healthState == nil {
		return
	}
	hs := k.healthState
	hs.mu.Lock()
	if hs.stoppingCh == nil || hs.stoppingClosed {
		hs.stoppingCh = make(chan struct{})
		hs.stoppingClosed = false
	}
	hs.mu.Unlock()
}

func (k *runtimeKernel) StoppingChan() <-chan struct{} {
	if k == nil || k.healthState == nil {
		return nil
	}
	hs := k.healthState
	hs.mu.Lock()
	ch := hs.stoppingCh
	hs.mu.Unlock()
	return ch
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

func (k *runtimeKernel) runBoundedHealth(
	key string,
	callerCtx context.Context,
	probeTimeout time.Duration,
	fn func(context.Context) (any, error),
) (any, error, bool) {
	if k == nil || k.healthState == nil {
		return nil, errHealthAdmissionClosed, false
	}
	if err := callerCtx.Err(); err != nil {
		return nil, err, true
	}
	hs := k.healthState
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
		admCtx := hs.admissionCtx
		if admCtx == nil {
			admCtx = context.Background()
		}
		call = &inFlightHealthCall{done: make(chan struct{})}
		hs.inFlight[key] = call
		if probeTimeout <= 0 {
			probeTimeout = defaultDeepHealthTimeout
		}
		go func(hc *inFlightHealthCall) {
			execCtx, execCancel := context.WithTimeout(admCtx, probeTimeout)
			defer execCancel()
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
					k.getLogger().Errorf("health check %q panicked: %v\n%s", key, r, debug.Stack())
					hc.val = nil
					hc.err = fmt.Errorf("health check %q panicked: %v", key, r)
				}
			}()
			hc.val, hc.err = fn(execCtx)
		}(call)
	}
	hs.mu.Unlock()

	select {
	case <-call.done:
		return call.val, call.err, false
	case <-callerCtx.Done():
		return nil, callerCtx.Err(), true
	}
}

func (k *runtimeKernel) Health(ctx context.Context) map[string]any {
	healthMap := make(map[string]any)
	if k == nil {
		healthMap["name"] = ""
		healthMap["version"] = ""
		healthMap["status"] = "DOWN"
		return healthMap
	}

	if ctx == nil {
		ctx = context.Background()
	}
	callerCtx := ctx
	if _, hasDeadline := callerCtx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		callerCtx, cancel = context.WithTimeout(callerCtx, defaultDeepHealthTimeout)
		defer cancel()
	}

	k.mu.RLock()
	resources := append([]*managedResource(nil), k.resources...)
	k.mu.RUnlock()

	const statusDown = "DOWN"
	type probeOutcome struct {
		skip       bool
		notStarted bool
		val        any
		err        error
		timedOut   bool
	}
	outcomes := make([]probeOutcome, len(resources))
	var probeIndices []int
	for i, mr := range resources {
		if mr == nil || mr.healthFn == nil {
			outcomes[i].skip = true
			continue
		}
		if mr.ownership == Owned && mr.startFn != nil && mr.state.Load() != resRunning {
			outcomes[i].notStarted = true
			continue
		}
		probeIndices = append(probeIndices, i)
	}

	if len(probeIndices) == 1 {
		idx := probeIndices[0]
		mr := resources[idx]
		val, err, timedOut := k.runBoundedHealth(mr.kind+":"+mr.name, callerCtx, defaultDeepHealthTimeout, mr.healthFn)
		outcomes[idx] = probeOutcome{val: val, err: err, timedOut: timedOut}
	} else if len(probeIndices) > 1 {
		var wg sync.WaitGroup
		for _, idx := range probeIndices[:len(probeIndices)-1] {
			wg.Add(1)
			go func(slot int) {
				defer wg.Done()
				mr := resources[slot]
				val, err, timedOut := k.runBoundedHealth(mr.kind+":"+mr.name, callerCtx, defaultDeepHealthTimeout, mr.healthFn)
				outcomes[slot] = probeOutcome{val: val, err: err, timedOut: timedOut}
			}(idx)
		}
		lastIdx := probeIndices[len(probeIndices)-1]
		lastMR := resources[lastIdx]
		val, err, timedOut := k.runBoundedHealth(lastMR.kind+":"+lastMR.name, callerCtx, defaultDeepHealthTimeout, lastMR.healthFn)
		outcomes[lastIdx] = probeOutcome{val: val, err: err, timedOut: timedOut}
		wg.Wait()
	}

	var downCount int
	for i, mr := range resources {
		out := outcomes[i]
		if out.skip {
			continue
		}

		if out.notStarted {
			downCount++
			notStartedDetails := map[string]any{"error": "resource not started"}
			switch {
			case mr.name == "sql":
				healthMap[mr.name] = &datasource.Health{Status: statusDown, Details: notStartedDetails}
			case mr.kind == "http_service":
				healthMap[mr.name] = &service.Health{Status: statusDown, Details: notStartedDetails}
			default:
				healthMap[mr.name] = datasource.Health{Status: statusDown, Details: notStartedDetails}
			}
			continue
		}

		val, err, timedOut := out.val, out.err, out.timedOut
		if timedOut || err != nil {
			downCount++
			details := downHealthDetails(err, timedOut)
			switch {
			case mr.name == "sql":
				healthMap[mr.name] = &datasource.Health{Status: statusDown, Details: details}
			case mr.kind == "http_service":
				healthMap[mr.name] = &service.Health{Status: statusDown, Details: details}
			default:
				if !timedOut && val != nil {
					healthMap[mr.name] = val
				} else {
					healthMap[mr.name] = datasource.Health{Status: statusDown, Details: details}
				}
			}
			continue
		}

		switch h := val.(type) {
		case *datasource.Health:
			if h == nil {
				downCount++
				healthMap[mr.name] = &datasource.Health{Status: statusDown}
			} else {
				if h.Status == statusDown {
					downCount++
				}
				healthMap[mr.name] = h
			}
		case datasource.Health:
			if h.Status == statusDown {
				downCount++
			}
			healthMap[mr.name] = h
		case *service.Health:
			if h == nil {
				downCount++
				healthMap[mr.name] = &service.Health{Status: statusDown}
			} else {
				if h.Status == statusDown {
					downCount++
				}
				healthMap[mr.name] = h
			}
		default:
			if isNilValue(val) {
				downCount++
				healthMap[mr.name] = datasource.Health{Status: statusDown}
			} else {
				healthMap[mr.name] = val
			}
		}
	}

	healthMap["name"] = k.getAppName()
	healthMap["version"] = k.getAppVersion()
	if downCount == 0 {
		healthMap["status"] = "UP"
	} else {
		healthMap["status"] = "DEGRADED"
	}

	return healthMap
}
