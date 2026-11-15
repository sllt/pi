package pi

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sllt/tao/actor"
)

// SupervisionMode controls how Pi supervises a background worker registered via AddWorker.
type SupervisionMode uint8

const (
	// SuperviseFatal preserves App.Go error semantics: nil or context.Canceled is a graceful exit;
	// the first other error or panic immediately requests application shutdown without retry.
	SuperviseFatal SupervisionMode = iota
	// SuperviseRestart restarts abnormal exits using exponential backoff with full jitter;
	// exceeding MaxRestarts within RestartWindow triggers application shutdown.
	SuperviseRestart
	// SuperviseDegrade retries abnormal exits within the restart budget; exceeding MaxRestarts
	// stops this worker and marks it Degraded without terminating the application.
	SuperviseDegrade
)

func (m SupervisionMode) String() string {
	switch m {
	case SuperviseFatal:
		return "fatal"
	case SuperviseRestart:
		return "restart"
	case SuperviseDegrade:
		return "degrade"
	default:
		return "unknown"
	}
}

func (m SupervisionMode) MarshalText() ([]byte, error) {
	return []byte(m.String()), nil
}

func (m *SupervisionMode) UnmarshalText(text []byte) error {
	switch strings.ToLower(strings.TrimSpace(string(text))) {
	case "", "0", "fatal":
		*m = SuperviseFatal
		return nil
	case "1", "restart":
		*m = SuperviseRestart
		return nil
	case "2", "degrade":
		*m = SuperviseDegrade
		return nil
	default:
		return errInvalidWorkerSupervisionMode
	}
}

const (
	defaultWorkerBackoffInitial    = 200 * time.Millisecond
	defaultWorkerBackoffMax        = 30 * time.Second
	defaultWorkerHealthyResetAfter = time.Minute
	defaultWorkerMaxRestarts       = 5
	defaultWorkerRestartWindow     = time.Minute
)

var (
	// ErrMarkReadyInvalidContext indicates MarkReady was called outside an active worker generation.
	ErrMarkReadyInvalidContext = errors.New("MarkReady requires an active worker execution context")
	// ErrWorkerExitedBeforeReady indicates a WaitReady worker exited before calling MarkReady.
	ErrWorkerExitedBeforeReady = errors.New("background worker exited before marking ready")
	// ErrWorkerRestartBudgetExceeded indicates a worker exhausted MaxRestarts within RestartWindow.
	ErrWorkerRestartBudgetExceeded = errors.New("background worker exceeded restart budget")

	errInvalidWorkerSupervisionMode = errors.New("invalid worker supervision mode")
	errNegativeWorkerPolicyValue    = errors.New("worker policy durations and counts must be non-negative")
	errWorkerBackoffOrder           = errors.New("worker BackoffInitial cannot exceed BackoffMax")
	errControllerPreempted          = errors.New("worker generation preempted by controller restart")
)

// WorkerPolicy configures supervision, readiness gating, and retry budgets for AddWorker.
type WorkerPolicy struct {
	Mode              SupervisionMode
	Required          bool          // Controls whether unhealthy states block Readiness
	WaitReady         bool          // Requires explicit ctx.MarkReady() on each generation
	BackoffInitial    time.Duration // Default 200ms (Restart/Degrade only)
	BackoffMax        time.Duration // Default 30s with full jitter
	HealthyResetAfter time.Duration // Default 1m; resets consecutive failure backoff, not sliding window records
	MaxRestarts       int           // Default 5; excludes initial launch, negative rejected
	RestartWindow     time.Duration // Default 1m sliding window
	ShutdownTimeout   time.Duration // 0 uses App shutdown timeout; positive passed via WithShutdownTimeout
}

type workerState uint8

const (
	workerPending workerState = iota
	workerStarting
	workerRunning
	workerBackingOff
	workerCompleted
	workerDegraded
	workerFailed
	workerStopped
)

func (s workerState) String() string {
	switch s {
	case workerPending:
		return "Pending"
	case workerStarting:
		return "Starting"
	case workerRunning:
		return "Running"
	case workerBackingOff:
		return "BackingOff"
	case workerCompleted:
		return "Completed"
	case workerDegraded:
		return "Degraded"
	case workerFailed:
		return "Failed"
	case workerStopped:
		return "Stopped"
	default:
		return "Unknown"
	}
}

type workerExecHandle struct {
	slot       *workerSlot
	generation uint64
	ctx        context.Context
}

type workerReconcileMsg struct{}

type workerHealthyTimerMsg struct {
	generation uint64
}

type workerSetStoppedErrMsg struct {
	err error
}

type workerStatusSnapshot struct {
	Name                string
	ActorName           string
	State               workerState
	Ready               bool
	InFlight            bool
	Generation          uint64
	WorkerRetries       uint64
	ActorRestarts       uint64
	ConsecutiveFailures int
	WindowRestarts      int
	LastBackoff         time.Duration
	LastError           error
}

type workerSlot struct {
	app       *App
	seq       uint64
	name      string
	actorName string
	actorPath string
	actorRef  actor.Ref
	policy    WorkerPolicy
	fn        BackgroundFunc

	activeCount    atomic.Int32
	maxActiveSeen  atomic.Int32
	stoppedHookErr atomic.Pointer[error]

	mu                   sync.Mutex
	node                 *actor.Node
	controllerPID        actor.PID
	controllerLost       bool
	state                workerState
	terminal             bool
	inFlight             bool
	generation           uint64
	ready                bool
	startedAt            time.Time
	readyAt              time.Time
	execCancel           context.CancelCauseFunc
	execDone             chan struct{}
	canceledByController bool

	stateChangedCh chan struct{}

	unhandledExitGen     uint64
	unhandledExitErr     error
	unhandledExitPreempt bool
	unhandledExitHealthy bool
	pendingRetry         bool
	lastExitErr          error
	lastErr              error

	pendingReadyTimerGen uint64

	consecutiveFailures int
	restartTimes        []time.Time
	backoffUntil        time.Time
	lastBackoffDelay    time.Duration

	workerRetries uint64
	actorBoots    uint64
	actorRestarts uint64

	notifyRetrying      bool
	rootMonitorRetrying bool
	waitOldExecActive   bool
	terminalWg          waitGroup
	terminalWgDone      bool
}

func normalizeAndValidateWorkerPolicy(policy WorkerPolicy) (WorkerPolicy, error) {
	if policy.Mode > SuperviseDegrade {
		return policy, fmt.Errorf("%w: %d", errInvalidWorkerSupervisionMode, policy.Mode)
	}
	if policy.BackoffInitial < 0 ||
		policy.BackoffMax < 0 ||
		policy.HealthyResetAfter < 0 ||
		policy.MaxRestarts < 0 ||
		policy.RestartWindow < 0 ||
		policy.ShutdownTimeout < 0 {
		return policy, errNegativeWorkerPolicyValue
	}
	if policy.BackoffInitial > 0 && policy.BackoffMax > 0 && policy.BackoffInitial > policy.BackoffMax {
		return policy, fmt.Errorf("%w: initial=%s max=%s", errWorkerBackoffOrder, policy.BackoffInitial, policy.BackoffMax)
	}
	if policy.BackoffMax == 0 {
		policy.BackoffMax = defaultWorkerBackoffMax
	}
	if policy.BackoffInitial > policy.BackoffMax {
		return policy, fmt.Errorf("%w: initial=%s max=%s", errWorkerBackoffOrder, policy.BackoffInitial, policy.BackoffMax)
	}
	if policy.BackoffInitial == 0 {
		policy.BackoffInitial = min(defaultWorkerBackoffInitial, policy.BackoffMax)
	}
	if policy.HealthyResetAfter == 0 {
		policy.HealthyResetAfter = defaultWorkerHealthyResetAfter
	}
	if policy.MaxRestarts == 0 {
		policy.MaxRestarts = defaultWorkerMaxRestarts
	}
	if policy.RestartWindow == 0 {
		policy.RestartWindow = defaultWorkerRestartWindow
	}
	return policy, nil
}

func newWorkerSlot(app *App, seq uint64, name, actorName string, policy WorkerPolicy, fn BackgroundFunc) *workerSlot {
	actorPath := "pi-root/workers-sup/" + actorName
	return &workerSlot{
		app:            app,
		seq:            seq,
		name:           name,
		actorName:      actorName,
		actorPath:      actorPath,
		actorRef:       actor.Ref{Path: actorPath},
		policy:         policy,
		fn:             fn,
		state:          workerPending,
		stateChangedCh: make(chan struct{}),
	}
}

// AddWorker registers a supervised background worker before application startup.
func (a *App) AddWorker(name string, policy WorkerPolicy, fn BackgroundFunc) error {
	if a == nil {
		return errors.New("cannot register worker on nil App")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return errBackgroundWorkerNameEmpty
	}
	if fn == nil {
		return errBackgroundWorkerHandlerNil
	}
	normalized, err := normalizeAndValidateWorkerPolicy(policy)
	if err != nil {
		return err
	}

	a.runtimeMu.Lock()
	defer a.runtimeMu.Unlock()

	if a.runtimeCtx != nil || a.runtimeState != lifecycleNew {
		return fmt.Errorf("%w: %s", errBackgroundWorkerRegisterTooLate, name)
	}
	if a.hasBackgroundWorker(name) {
		return fmt.Errorf("%w: %s", errBackgroundWorkerDuplicateName, name)
	}

	a.nextWorkerSeq++
	seq := a.nextWorkerSeq
	actorName := sanitizeActorSegment("worker", seq, name)
	a.backgroundWorkers = append(a.backgroundWorkers, newWorkerSlot(a, seq, name, actorName, normalized, fn))
	return nil
}

// MarkReady signals that the current worker generation has completed initialization
// and is ready to serve. Repeated calls within the same active generation are idempotent.
// Calling MarkReady outside an active worker generation or after cancellation fails.
func (c *Context) MarkReady() error {
	if c == nil || c.workerExec == nil || c.workerExec.slot == nil {
		return ErrMarkReadyInvalidContext
	}
	if c.workerExec.ctx != nil && c.workerExec.ctx.Err() != nil {
		return fmt.Errorf("%w: %w", ErrMarkReadyInvalidContext, c.workerExec.ctx.Err())
	}
	if c.Context != nil && c.Context.Err() != nil {
		return fmt.Errorf("%w: %w", ErrMarkReadyInvalidContext, c.Context.Err())
	}
	return c.workerExec.slot.markReady(c.workerExec.generation)
}

func (s *workerSlot) bindNode(node *actor.Node) {
	s.mu.Lock()
	s.node = node
	s.mu.Unlock()
}

func (s *workerSlot) attachTerminalWaitGroup(wg waitGroup) {
	if wg == nil {
		return
	}
	s.mu.Lock()
	if s.terminal {
		s.mu.Unlock()
		return
	}
	wg.Add(1)
	s.terminalWg = wg
	s.terminalWgDone = false
	s.mu.Unlock()
}

func (s *workerSlot) currentControllerPID() actor.PID {
	s.mu.Lock()
	pid := s.controllerPID
	s.mu.Unlock()
	return pid
}

func (s *workerSlot) broadcastStateChangeLocked() {
	if s.stateChangedCh != nil {
		close(s.stateChangedCh)
	}
	s.stateChangedCh = make(chan struct{})
	if s.app != nil {
		s.app.notifyGRPCHealthChanged()
	}
}

func (s *workerSlot) checkCurrentStartupReadiness() (ready bool, terminalErr error, changed <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stateChangedCh == nil {
		s.stateChangedCh = make(chan struct{})
	}
	if s.ready && (s.state == workerRunning || s.state == workerCompleted) {
		return true, nil, s.stateChangedCh
	}
	if s.terminal && s.state != workerCompleted {
		err := s.lastErr
		if err == nil {
			err = ErrWorkerExitedBeforeReady
		}
		return false, err, s.stateChangedCh
	}
	return false, nil, s.stateChangedCh
}

func (s *workerSlot) checkInitialLaunch() (launched bool, fatalErr error, changed <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stateChangedCh == nil {
		s.stateChangedCh = make(chan struct{})
	}
	if s.terminal && (s.policy.Mode == SuperviseFatal || s.controllerLost) && s.state == workerFailed {
		err := s.lastErr
		if err == nil {
			err = errActorRuntimeTerminated
		}
		return false, err, s.stateChangedCh
	}
	if s.generation >= 1 || s.terminal {
		return true, nil, s.stateChangedCh
	}
	return false, nil, s.stateChangedCh
}

func (s *workerSlot) markTerminalLocked(state workerState, err error) waitGroup {
	s.state = state
	s.terminal = true
	s.pendingRetry = false
	s.backoffUntil = time.Time{}
	if err != nil {
		s.lastErr = err
	}
	if state != workerCompleted {
		s.ready = false
	}
	s.broadcastStateChangeLocked()
	if s.terminalWg != nil && !s.terminalWgDone {
		s.terminalWgDone = true
		return s.terminalWg
	}
	return nil
}

func (s *workerSlot) onControllerLost(cause error) error {
	if cause == nil {
		cause = errActorRuntimeTerminated
	}
	lostErr := fmt.Errorf("worker %s controller terminated: %w", s.name, cause)

	s.mu.Lock()
	if s.controllerLost {
		existing := s.lastErr
		s.mu.Unlock()
		if existing != nil {
			return existing
		}
		return lostErr
	}
	s.controllerLost = true
	s.ready = false
	s.pendingRetry = false
	s.unhandledExitGen = 0
	s.unhandledExitErr = nil
	s.unhandledExitPreempt = false
	s.unhandledExitHealthy = false
	s.backoffUntil = time.Time{}
	s.state = workerFailed
	s.terminal = true
	s.lastErr = joinShutdownErr(s.lastErr, lostErr)
	cancel := s.execCancel
	var twg waitGroup
	if !s.inFlight && s.terminalWg != nil && !s.terminalWgDone {
		s.terminalWgDone = true
		twg = s.terminalWg
	}
	s.broadcastStateChangeLocked()
	s.mu.Unlock()

	if cancel != nil {
		cancel(lostErr)
	}
	s.app.onActorTreeFailure(lostErr)
	if twg != nil {
		twg.Done()
	}
	return lostErr
}

func (s *workerSlot) markReady(gen uint64) error {
	if s.app.isIntakeClosed() {
		return fmt.Errorf("%w: application is stopping", ErrMarkReadyInvalidContext)
	}
	s.mu.Lock()
	if s.app.isIntakeClosed() {
		s.mu.Unlock()
		return fmt.Errorf("%w: application is stopping", ErrMarkReadyInvalidContext)
	}
	if !s.inFlight || s.generation != gen || s.terminal {
		s.mu.Unlock()
		return fmt.Errorf("%w: stale or finished worker generation %d", ErrMarkReadyInvalidContext, gen)
	}
	if s.ready {
		s.mu.Unlock()
		return nil
	}
	s.ready = true
	s.state = workerRunning
	s.readyAt = time.Now()
	s.pendingReadyTimerGen = gen
	s.broadcastStateChangeLocked()
	s.mu.Unlock()

	s.app.syncGRPCHealth()
	s.notifyController()
	return nil
}

func (s *workerSlot) isHealthyForReadiness() bool {
	if !s.policy.Required {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ready && (s.state == workerRunning || s.state == workerCompleted)
}

func (s *workerSlot) snapshot() workerStatusSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	windowCount := 0
	for _, t := range s.restartTimes {
		if now.Sub(t) < s.policy.RestartWindow {
			windowCount++
		}
	}
	return workerStatusSnapshot{
		Name:                s.name,
		ActorName:           s.actorName,
		State:               s.state,
		Ready:               s.ready,
		InFlight:            s.inFlight,
		Generation:          s.generation,
		WorkerRetries:       s.workerRetries,
		ActorRestarts:       s.actorRestarts,
		ConsecutiveFailures: s.consecutiveFailures,
		WindowRestarts:      windowCount,
		LastBackoff:         s.lastBackoffDelay,
		LastError:           s.lastErr,
	}
}

func (s *workerSlot) recordActiveEnter() {
	cur := s.activeCount.Add(1)
	for {
		prevMax := s.maxActiveSeen.Load()
		if cur <= prevMax || s.maxActiveSeen.CompareAndSwap(prevMax, cur) {
			break
		}
	}
}

func (s *workerSlot) recordActiveExit() {
	s.activeCount.Add(-1)
}

func (s *workerSlot) runGeneration(
	gen uint64,
	execCtx context.Context,
	execCancel context.CancelCauseFunc,
	execDone chan struct{},
) {
	var terminalWg waitGroup
	defer func() {
		s.app.runtimeTasks.Done()
		if terminalWg != nil {
			terminalWg.Done()
		}
	}()

	piCtx := newContext(nil, noopRequest{ctx: execCtx}, s.app.kernel)
	piCtx.workerExec = &workerExecHandle{
		slot:       s,
		generation: gen,
		ctx:        execCtx,
	}

	start := time.Now()
	if logger := s.app.safeLogger(); logger != nil {
		logger.Infof("Starting background worker: %s", s.name)
	}

	s.recordActiveEnter()
	var (
		err      error
		panicked bool
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				if logger := s.app.safeLogger(); logger != nil {
					panicRecovery(r, logger)
				}
				panicked = true
				err = fmt.Errorf("worker %s: %w: %v", s.name, errBackgroundWorkerPanic, r)
			}
		}()
		err = s.fn(piCtx)
	}()
	s.recordActiveExit()
	execCancel(nil)

	terminalWg = s.finishGeneration(gen, start, err, panicked, execDone)
}

func (s *workerSlot) finishGeneration(
	gen uint64,
	start time.Time,
	err error,
	panicked bool,
	execDone chan struct{},
) waitGroup {
	duration := time.Since(start)
	intakeClosing := s.app.isIntakeClosed() || s.app.workerParentContext().Err() != nil

	var (
		fatalErr         error
		shutdownErr      error
		shouldNotifyCtrl bool
		terminalWg       waitGroup
	)

	s.mu.Lock()
	if s.generation != gen {
		s.mu.Unlock()
		close(execDone)
		return nil
	}

	wasReady := s.ready
	readyAt := s.readyAt
	preempted := s.canceledByController && !intakeClosing && !panicked && (err == nil || errors.Is(err, context.Canceled))

	s.inFlight = false
	s.execCancel = nil
	s.pendingReadyTimerGen = 0

	if s.terminal {
		if panicked || (err != nil && !errors.Is(err, context.Canceled)) {
			if logger := s.app.safeLogger(); logger != nil {
				logger.Errorf("Background worker failed during shutdown: %s in %s, err: %v", s.name, duration, err)
			}
			s.lastErr = joinShutdownErr(s.lastErr, err)
			shutdownErr = err
		}
		if s.terminalWg != nil && !s.terminalWgDone {
			s.terminalWgDone = true
			terminalWg = s.terminalWg
		}
		s.broadcastStateChangeLocked()
		s.mu.Unlock()

		if shutdownErr != nil {
			s.app.recordShutdownError(shutdownErr)
		}
		close(execDone)
		return terminalWg
	}

	switch {
	case intakeClosing:
		if panicked || (err != nil && !errors.Is(err, context.Canceled)) {
			if logger := s.app.safeLogger(); logger != nil {
				logger.Errorf("Background worker failed during shutdown: %s in %s, err: %v", s.name, duration, err)
			}
			terminalWg = s.markTerminalLocked(workerFailed, err)
			shutdownErr = err
		} else {
			if logger := s.app.safeLogger(); logger != nil {
				logger.Infof("Background worker stopped gracefully: %s in %s", s.name, duration)
			}
			terminalWg = s.markTerminalLocked(workerStopped, nil)
		}

	case preempted:
		s.ready = false
		s.state = workerStarting
		s.unhandledExitGen = gen
		s.unhandledExitErr = nil
		s.unhandledExitPreempt = true
		s.unhandledExitHealthy = false
		s.broadcastStateChangeLocked()
		shouldNotifyCtrl = true

	case !panicked && (err == nil || errors.Is(err, context.Canceled)):
		if s.policy.WaitReady && !wasReady {
			preReadyErr := fmt.Errorf("worker %s: %w", s.name, ErrWorkerExitedBeforeReady)
			if err != nil {
				preReadyErr = fmt.Errorf("%w: %w", preReadyErr, err)
			}
			if logger := s.app.safeLogger(); logger != nil {
				logger.Errorf("Background worker exited before ready: %s in %s, err: %v", s.name, duration, preReadyErr)
			}
			if s.policy.Mode == SuperviseFatal {
				terminalWg = s.markTerminalLocked(workerFailed, preReadyErr)
				if s.policy.Required {
					fatalErr = preReadyErr
				}
			} else {
				s.ready = false
				s.state = workerBackingOff
				s.lastErr = preReadyErr
				s.unhandledExitGen = gen
				s.unhandledExitErr = preReadyErr
				s.unhandledExitPreempt = false
				s.unhandledExitHealthy = false
				s.broadcastStateChangeLocked()
				shouldNotifyCtrl = true
			}
		} else {
			if logger := s.app.safeLogger(); logger != nil {
				logger.Infof("Background worker stopped gracefully: %s in %s", s.name, duration)
			}
			s.ready = true
			terminalWg = s.markTerminalLocked(workerCompleted, nil)
		}

	default:
		if logger := s.app.safeLogger(); logger != nil {
			logger.Errorf("Background worker failed: %s in %s, err: %v", s.name, duration, err)
		}
		if s.policy.Mode == SuperviseFatal {
			terminalWg = s.markTerminalLocked(workerFailed, err)
			fatalErr = err
		} else {
			ranHealthy := wasReady && !readyAt.IsZero() && time.Since(readyAt) >= s.policy.HealthyResetAfter
			s.ready = false
			s.state = workerBackingOff
			s.lastErr = err
			s.unhandledExitGen = gen
			s.unhandledExitErr = err
			s.unhandledExitPreempt = false
			s.unhandledExitHealthy = ranHealthy
			s.broadcastStateChangeLocked()
			shouldNotifyCtrl = true
		}
	}
	s.mu.Unlock()

	if shutdownErr != nil {
		s.app.recordShutdownError(shutdownErr)
	}
	if fatalErr != nil {
		s.app.recordFatalCause(fatalErr)
	}

	close(execDone)

	if shouldNotifyCtrl {
		s.notifyController()
	}
	return terminalWg
}

func (s *workerSlot) needsControllerWork() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminal || s.controllerLost || s.app.isIntakeClosed() {
		return false
	}
	if s.unhandledExitGen != 0 || s.pendingReadyTimerGen != 0 {
		return true
	}
	if !s.inFlight && (s.state == workerPending || s.state == workerStarting) {
		return true
	}
	if !s.inFlight && s.state == workerBackingOff && time.Now().After(s.backoffUntil) {
		return true
	}
	return false
}

func isPermanentControllerSendErr(err error) bool {
	return errors.Is(err, actor.ErrUnresponsive) ||
		errors.Is(err, actor.ErrNoProcess) ||
		errors.Is(err, actor.ErrStalePID)
}

func (s *workerSlot) notifyController() {
	if s.app.isIntakeClosed() {
		return
	}
	s.mu.Lock()
	node := s.node
	ref := s.actorRef
	terminal := s.terminal || s.controllerLost
	s.mu.Unlock()

	if terminal || node == nil || ref.Path == "" {
		return
	}
	err := node.Send(ref, workerReconcileMsg{})
	if err == nil || s.app.isIntakeClosed() {
		return
	}
	if errors.Is(err, actor.ErrClosed) {
		s.app.onActorTreeFailure(err)
		return
	}
	if errors.Is(err, actor.ErrUnresponsive) {
		s.onControllerLost(err)
		return
	}

	s.mu.Lock()
	if s.terminal || s.controllerLost || s.notifyRetrying {
		s.mu.Unlock()
		return
	}
	if !s.app.tryAdmitRuntimeTask() {
		s.mu.Unlock()
		return
	}
	s.notifyRetrying = true
	s.mu.Unlock()

	go s.retryControllerNotify()
}

func (s *workerSlot) retryControllerNotify() {
	defer s.app.runtimeTasks.Done()
	defer func() {
		s.mu.Lock()
		s.notifyRetrying = false
		s.mu.Unlock()
	}()

	intakeDone := s.app.intakeDone()
	delay := time.Millisecond
	timer := time.NewTimer(delay)
	defer timer.Stop()

	for {
		select {
		case <-intakeDone:
			return
		case <-timer.C:
		}

		if !s.needsControllerWork() {
			return
		}

		s.mu.Lock()
		node := s.node
		ref := s.actorRef
		s.mu.Unlock()

		if node == nil || ref.Path == "" {
			return
		}
		err := node.Send(ref, workerReconcileMsg{})
		if err == nil || s.app.isIntakeClosed() {
			return
		}
		if errors.Is(err, actor.ErrClosed) {
			s.app.onActorTreeFailure(err)
			return
		}
		if isPermanentControllerSendErr(err) {
			s.onControllerLost(err)
			return
		}

		if delay < 20*time.Millisecond {
			delay *= 2
		}
		timer.Reset(delay)
	}
}

func (s *workerSlot) notifyRootMonitor(pid actor.PID) {
	if pid == (actor.PID{}) || s.app.isIntakeClosed() {
		return
	}
	s.mu.Lock()
	node := s.node
	terminal := s.terminal || s.controllerLost
	s.mu.Unlock()

	if terminal || node == nil {
		return
	}
	rootRef := actor.Ref{Path: "pi-root"}
	err := node.Send(rootRef, workerMonitorControllerMsg{slot: s, pid: pid})
	if err == nil || s.app.isIntakeClosed() {
		return
	}
	if errors.Is(err, actor.ErrClosed) || isPermanentControllerSendErr(err) {
		s.onControllerLost(err)
		return
	}

	s.mu.Lock()
	if s.terminal || s.controllerLost || s.rootMonitorRetrying {
		s.mu.Unlock()
		return
	}
	if !s.app.tryAdmitRuntimeTask() {
		s.mu.Unlock()
		return
	}
	s.rootMonitorRetrying = true
	s.mu.Unlock()

	go s.retryRootMonitorNotify()
}

func (s *workerSlot) retryRootMonitorNotify() {
	defer s.app.runtimeTasks.Done()
	defer func() {
		s.mu.Lock()
		s.rootMonitorRetrying = false
		s.mu.Unlock()
	}()

	intakeDone := s.app.intakeDone()
	rootRef := actor.Ref{Path: "pi-root"}
	delay := time.Millisecond
	timer := time.NewTimer(delay)
	defer timer.Stop()

	for {
		select {
		case <-intakeDone:
			return
		case <-timer.C:
		}

		s.mu.Lock()
		node := s.node
		pid := s.controllerPID
		stopped := s.terminal || s.controllerLost
		s.mu.Unlock()

		if stopped || node == nil || pid == (actor.PID{}) || s.app.isIntakeClosed() {
			return
		}
		err := node.Send(rootRef, workerMonitorControllerMsg{slot: s, pid: pid})
		if err == nil || s.app.isIntakeClosed() {
			return
		}
		if errors.Is(err, actor.ErrClosed) || isPermanentControllerSendErr(err) {
			s.onControllerLost(err)
			return
		}

		if delay < 20*time.Millisecond {
			delay *= 2
		}
		timer.Reset(delay)
	}
}

func (s *workerSlot) watchActorTimer(ref actor.TimerRef, onFailed func()) {
	if ref.Done() == nil {
		return
	}
	if !s.app.tryAdmitRuntimeTask() {
		ref.Cancel()
		return
	}
	intakeDone := s.app.intakeDone()
	go func() {
		defer s.app.runtimeTasks.Done()
		select {
		case <-intakeDone:
			ref.Cancel()
			return
		case <-ref.Done():
			err := ref.Err()
			if err == nil || s.app.isIntakeClosed() || errors.Is(err, context.Canceled) || errors.Is(err, actor.ErrStopped) {
				return
			}
			if errors.Is(err, actor.ErrClosed) {
				s.app.onActorTreeFailure(err)
				return
			}
			if onFailed != nil {
				onFailed()
			}
		}
	}()
}

type workerActor struct {
	actor.Actor
	app          *App
	slot         *workerSlot
	stopTracked  bool
	bootSeq      uint64
	ownedGen     uint64
	retryTimer   actor.TimerRef
	healthyTimer actor.TimerRef
}

func newWorkerActorFactory(app *App, slot *workerSlot) actor.Factory {
	return func() actor.Behavior {
		return &workerActor{
			app:  app,
			slot: slot,
		}
	}
}

func (s *workerSlot) pruneRestartTimesLocked(now time.Time) {
	keep := 0
	for _, t := range s.restartTimes {
		if now.Sub(t) < s.policy.RestartWindow {
			s.restartTimes[keep] = t
			keep++
		}
	}
	s.restartTimes = s.restartTimes[:keep]
}

func (w *workerActor) exhaustRestartBudgetLocked(exitErr error) error {
	s := w.slot
	if exitErr == nil {
		exitErr = s.lastExitErr
	}
	var budgetErr error
	if exitErr != nil {
		budgetErr = fmt.Errorf("worker %s: %w (%d in %s): %w",
			s.name, ErrWorkerRestartBudgetExceeded, s.policy.MaxRestarts, s.policy.RestartWindow, exitErr)
	} else {
		budgetErr = fmt.Errorf("worker %s: %w (%d in %s)",
			s.name, ErrWorkerRestartBudgetExceeded, s.policy.MaxRestarts, s.policy.RestartWindow)
	}
	if s.policy.Mode == SuperviseRestart {
		twg := s.markTerminalLocked(workerFailed, budgetErr)
		s.mu.Unlock()
		w.app.recordFatalCause(budgetErr)
		if twg != nil {
			twg.Done()
		}
		return nil
	}
	twg := s.markTerminalLocked(workerDegraded, budgetErr)
	s.mu.Unlock()
	if logger := w.app.safeLogger(); logger != nil {
		logger.Warnf("Background worker degraded after exhausting retries: %v", budgetErr)
	}
	if twg != nil {
		twg.Done()
	}
	return nil
}

func (w *workerActor) Receive(msg any) error {
	switch m := msg.(type) {
	case actor.Started:
		if !w.stopTracked {
			w.stopTracked = true
			w.app.controllerStopTasks.Add(1)
		}
		w.slot.mu.Lock()
		w.slot.actorBoots++
		w.bootSeq = w.slot.actorBoots
		restarted := w.bootSeq > 1
		if restarted {
			w.slot.actorRestarts = w.bootSeq - 1
		}
		w.slot.controllerPID = w.PID()
		w.slot.mu.Unlock()

		if restarted {
			w.app.recordActorRestartMetric(w.slot.name, "worker")
			w.Log().Warn("worker controller actor restarted", "worker", w.slot.name, "bootSeq", w.bootSeq)
		}

		w.slot.notifyRootMonitor(w.PID())

		if err := w.Send(w.PID(), workerReconcileMsg{}); err != nil {
			w.slot.notifyController()
		}
		return nil

	case workerReconcileMsg:
		return w.reconcile()

	case workerControllerCheckMsg:
		return nil

	case workerHealthyTimerMsg:
		w.handleHealthyReset(m.generation)
		return nil

	case workerSetStoppedErrMsg:
		w.slot.stoppedHookErr.Store(&m.err)
		return nil

	case actorInjectPanicMsg:
		panic(m.reason)

	case actor.Stopped:
		return w.handleStopped()
	}
	return nil
}

func (w *workerActor) reconcile() error {
	s := w.slot
	if w.app.isIntakeClosed() {
		w.retryTimer.Cancel()
		w.healthyTimer.Cancel()
		s.mu.Lock()
		var twg waitGroup
		if !s.inFlight && !s.terminal {
			twg = s.markTerminalLocked(workerStopped, nil)
		}
		s.mu.Unlock()
		if twg != nil {
			twg.Done()
		}
		return nil
	}

	s.mu.Lock()
	if s.terminal || s.controllerLost {
		s.mu.Unlock()
		return nil
	}

	// Hard non-overlap latch: never start a new generation while a previous generation is still running.
	if s.inFlight {
		if w.ownedGen != s.generation && w.bootSeq > 1 {
			// A replacement controller observed an older generation still running.
			// Cancel it and wait for real goroutine exit before launching any replacement.
			s.canceledByController = true
			s.ready = false
			s.state = workerStarting
			s.broadcastStateChangeLocked()
			cancel := s.execCancel
			done := s.execDone
			startWaiter := false
			if !s.waitOldExecActive && done != nil && w.app.tryAdmitRuntimeTask() {
				s.waitOldExecActive = true
				startWaiter = true
			}
			s.mu.Unlock()
			if cancel != nil {
				cancel(errControllerPreempted)
			}
			if startWaiter {
				go w.waitForOldExecExit(done)
			}
			return nil
		}

		if s.pendingReadyTimerGen == s.generation {
			gen := s.generation
			resetAfter := s.policy.HealthyResetAfter
			s.pendingReadyTimerGen = 0
			s.mu.Unlock()
			w.armHealthyTimer(gen, resetAfter)
			return nil
		}

		s.mu.Unlock()
		return nil
	}

	// Reconcile any completed/failed prior generation before launching a new one.
	if s.unhandledExitGen != 0 {
		exitErr := s.unhandledExitErr
		preempted := s.unhandledExitPreempt
		wasHealthy := s.unhandledExitHealthy
		s.unhandledExitGen = 0
		s.unhandledExitErr = nil
		s.unhandledExitPreempt = false
		s.unhandledExitHealthy = false

		if !preempted {
			s.lastExitErr = exitErr
			if wasHealthy {
				// HealthyResetAfter resets consecutive failure backoff level only;
				// sliding-window restart timestamps inside RestartWindow remain intact.
				s.consecutiveFailures = 0
			}

			now := time.Now()
			s.pruneRestartTimesLocked(now)
			if len(s.restartTimes) >= s.policy.MaxRestarts {
				return w.exhaustRestartBudgetLocked(exitErr)
			}

			// Do NOT record restartTimes or workerRetries before sleeping backoff;
			// budget is checked and deducted at actual re-execution time.
			s.pendingRetry = true
			s.consecutiveFailures++
			delay := w.app.computeWorkerBackoff(s.policy, s.consecutiveFailures)
			s.state = workerBackingOff
			s.ready = false
			s.lastBackoffDelay = delay
			s.backoffUntil = now.Add(delay)
			s.broadcastStateChangeLocked()
			s.mu.Unlock()

			w.app.recordWorkerBackoffMetric(s.name, "worker", delay)
			w.app.syncGRPCHealth()
			w.armRetryTimer(delay)
			return nil
		}
	}

	if s.state == workerBackingOff && !s.backoffUntil.IsZero() {
		if remaining := time.Until(s.backoffUntil); remaining > 0 {
			s.mu.Unlock()
			w.armRetryTimer(remaining)
			return nil
		}
		s.backoffUntil = time.Time{}
	}

	now := time.Now()
	if s.pendingRetry {
		s.pruneRestartTimesLocked(now)
		if len(s.restartTimes) >= s.policy.MaxRestarts {
			return w.exhaustRestartBudgetLocked(s.lastExitErr)
		}
	}

	if !w.app.tryAdmitRuntimeTask() {
		twg := s.markTerminalLocked(workerStopped, nil)
		s.mu.Unlock()
		if twg != nil {
			twg.Done()
		}
		return nil
	}

	didRetry := false
	if s.pendingRetry {
		s.pendingRetry = false
		s.restartTimes = append(s.restartTimes, now)
		s.workerRetries++
		didRetry = true
	}
	s.inFlight = true
	s.generation++
	gen := s.generation
	w.ownedGen = gen
	s.canceledByController = false
	s.startedAt = now
	if s.policy.WaitReady {
		s.state = workerStarting
		s.ready = false
		s.readyAt = time.Time{}
	} else {
		s.state = workerRunning
		s.ready = true
		s.readyAt = now
	}
	s.broadcastStateChangeLocked()

	execCtx, execCancel := context.WithCancelCause(w.app.workerParentContext())
	execDone := make(chan struct{})
	s.execCancel = execCancel
	s.execDone = execDone
	waitReady := s.policy.WaitReady
	resetAfter := s.policy.HealthyResetAfter
	s.mu.Unlock()

	if didRetry {
		w.app.recordWorkerRestartMetric(s.name, "worker")
	}
	w.app.syncGRPCHealth()

	if !waitReady {
		w.armHealthyTimer(gen, resetAfter)
	}

	go s.runGeneration(gen, execCtx, execCancel, execDone)
	return nil
}

func (w *workerActor) waitForOldExecExit(done <-chan struct{}) {
	defer w.app.runtimeTasks.Done()
	select {
	case <-done:
	case <-w.app.intakeDone():
	}
	w.slot.mu.Lock()
	w.slot.waitOldExecActive = false
	w.slot.mu.Unlock()
	w.slot.notifyController()
}

func (w *workerActor) armRetryTimer(delay time.Duration) {
	w.retryTimer.Cancel()
	if w.app.isIntakeClosed() {
		return
	}
	ref, err := w.SendAfter(delay, workerReconcileMsg{})
	if err != nil {
		if !w.app.isIntakeClosed() {
			if errors.Is(err, actor.ErrClosed) {
				w.app.onActorTreeFailure(err)
				return
			}
			w.slot.notifyController()
		}
		return
	}
	w.retryTimer = ref
	w.slot.watchActorTimer(ref, w.slot.notifyController)
}

func (w *workerActor) armHealthyTimer(gen uint64, resetAfter time.Duration) {
	w.healthyTimer.Cancel()
	if w.app.isIntakeClosed() || w.slot.policy.Mode == SuperviseFatal {
		return
	}
	ref, err := w.SendAfter(resetAfter, workerHealthyTimerMsg{generation: gen})
	if err != nil {
		return
	}
	w.healthyTimer = ref
	w.slot.watchActorTimer(ref, func() {
		w.handleHealthyReset(gen)
	})
}

func (w *workerActor) handleHealthyReset(gen uint64) {
	w.slot.mu.Lock()
	if w.slot.inFlight && w.slot.generation == gen && w.slot.ready {
		w.slot.consecutiveFailures = 0
	}
	w.slot.mu.Unlock()
}

func (w *workerActor) handleStopped() error {
	if w.stopTracked {
		w.stopTracked = false
		defer w.app.controllerStopTasks.Done()
	}

	w.retryTimer.Cancel()
	w.healthyTimer.Cancel()

	s := w.slot
	s.mu.Lock()
	if s.inFlight {
		if !w.app.isIntakeClosed() {
			s.canceledByController = true
			if !s.terminal {
				s.ready = false
				s.state = workerStarting
				s.broadcastStateChangeLocked()
			}
		} else if !s.terminal {
			s.ready = false
			s.broadcastStateChangeLocked()
		}
		cancel := s.execCancel
		done := s.execDone
		s.mu.Unlock()

		if cancel != nil {
			cancel(context.Canceled)
		}
		if done != nil {
			// Block until the real worker goroutine exits even if Tao's shutdown
			// watchdog expires and marks this actor ErrUnresponsive.
			<-done
		}
	} else {
		var twg waitGroup
		if w.app.isIntakeClosed() && !s.terminal {
			twg = s.markTerminalLocked(workerStopped, nil)
		} else if !s.terminal && s.ready {
			s.ready = false
			s.broadcastStateChangeLocked()
		}
		s.mu.Unlock()
		if twg != nil {
			twg.Done()
		}
	}

	if errPtr := s.stoppedHookErr.Load(); errPtr != nil && *errPtr != nil {
		return *errPtr
	}
	return nil
}

func (a *App) workerSlots() []*workerSlot {
	a.runtimeMu.Lock()
	slots := append([]*workerSlot(nil), a.backgroundWorkers...)
	a.runtimeMu.Unlock()
	return slots
}

func (a *App) buildWorkerChildSpecs(node *actor.Node) []actor.ChildSpec {
	slots := a.workerSlots()

	specs := make([]actor.ChildSpec, 0, len(slots))
	for _, slot := range slots {
		slot.bindNode(node)
		opts := []actor.SpawnOption{
			actor.WithRestart(actor.Permanent),
		}
		if slot.policy.ShutdownTimeout > 0 {
			opts = append(opts, actor.WithShutdownTimeout(slot.policy.ShutdownTimeout))
		}
		specs = append(specs, actor.ChildSpec{
			Name:    slot.actorName,
			Factory: newWorkerActorFactory(a, slot),
			Options: opts,
		})
	}
	return specs
}

func (a *App) startupAbortErr(ctx context.Context) error {
	if cause := a.fatalRuntimeCause(); cause != nil {
		return cause
	}
	if err := contextCauseOrErr(ctx); err != nil {
		return err
	}
	if a.isIntakeClosed() {
		return context.Canceled
	}
	return nil
}

func (a *App) waitForRequiredWorkersReady(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	slots := a.workerSlots()
	wsSlots := a.wsClientSlots()
	subSlots := a.subscriberSlots()

	var requiredSlots []*workerSlot
	for _, slot := range slots {
		if slot.policy.Required {
			requiredSlots = append(requiredSlots, slot)
		}
	}
	var requiredWSSlots []*wsClientSlot
	for _, slot := range wsSlots {
		if slot.policy.Required {
			requiredWSSlots = append(requiredWSSlots, slot)
		}
	}
	var requiredSubSlots []*subscriberSlot
	for _, slot := range subSlots {
		if slot.policy.Required {
			requiredSubSlots = append(requiredSubSlots, slot)
		}
	}
	if len(requiredSlots) == 0 && len(requiredWSSlots) == 0 && len(requiredSubSlots) == 0 {
		return nil
	}

	intakeDone := a.intakeDone()
	observedChs := make([]<-chan struct{}, len(requiredSlots)+len(requiredWSSlots)+len(requiredSubSlots))

	for {
		if err := a.startupAbortErr(ctx); err != nil {
			return err
		}

		var waitCh <-chan struct{}
		for i, slot := range requiredSlots {
			var (
				ok      bool
				termErr error
				ch      <-chan struct{}
			)
			if slot.policy.WaitReady {
				ok, termErr, ch = slot.checkCurrentStartupReadiness()
			} else {
				ok, termErr, ch = slot.checkInitialLaunch()
			}
			if termErr != nil {
				return termErr
			}
			if !ok {
				waitCh = ch
				break
			}
			observedChs[i] = ch
		}

		if waitCh == nil {
			baseIdx := len(requiredSlots)
			for j, wsSlot := range requiredWSSlots {
				ok, termErr, ch := wsSlot.checkStartupReadiness()
				if termErr != nil {
					return termErr
				}
				if !ok {
					waitCh = ch
					break
				}
				observedChs[baseIdx+j] = ch
			}
		}

		if waitCh == nil {
			baseIdx := len(requiredSlots) + len(requiredWSSlots)
			for k, subSlot := range requiredSubSlots {
				ok, termErr, ch := subSlot.checkStartupReadiness()
				if termErr != nil {
					return termErr
				}
				if !ok {
					waitCh = ch
					break
				}
				observedChs[baseIdx+k] = ch
			}
		}

		if waitCh == nil {
			changedDuringScan := false
			for i := range observedChs {
				select {
				case <-observedChs[i]:
					changedDuringScan = true
				default:
				}
				if changedDuringScan {
					break
				}
			}
			if !changedDuringScan {
				if err := a.startupAbortErr(ctx); err != nil {
					return err
				}
				return nil
			}
			continue
		}

		select {
		case <-waitCh:
		case <-intakeDone:
			return a.startupAbortErr(ctx)
		case <-ctx.Done():
			return contextCauseOrErr(ctx)
		}
	}
}

func (a *App) isReady() bool {
	if a == nil || a.isIntakeClosed() || a.actorTreeUnexpectedlyDead() {
		return false
	}
	a.runtimeMu.Lock()
	if a.runtimeState != lifecycleRunning {
		a.runtimeMu.Unlock()
		return false
	}
	slots := append([]*workerSlot(nil), a.backgroundWorkers...)
	wsSlots := append([]*wsClientSlot(nil), a.wsClients...)
	subSlots := append([]*subscriberSlot(nil), a.subscriberSlotsList...)
	a.runtimeMu.Unlock()

	for _, slot := range slots {
		if !slot.isHealthyForReadiness() {
			return false
		}
	}
	for _, wsSlot := range wsSlots {
		if !wsSlot.isHealthyForReadiness() {
			return false
		}
	}
	for _, subSlot := range subSlots {
		if !subSlot.isHealthyForReadiness() {
			return false
		}
	}
	return true
}

func (a *App) computeWorkerBackoff(policy WorkerPolicy, consecutiveFailures int) time.Duration {
	if consecutiveFailures < 1 {
		consecutiveFailures = 1
	}
	capDelay := policy.BackoffInitial
	for step := 1; step < consecutiveFailures && capDelay < policy.BackoffMax; step++ {
		if capDelay > policy.BackoffMax/2 {
			capDelay = policy.BackoffMax
			break
		}
		capDelay *= 2
	}
	if capDelay > policy.BackoffMax {
		capDelay = policy.BackoffMax
	}
	if capDelay <= 0 {
		return 0
	}

	a.runtimeMu.Lock()
	customJitter := a.workerJitter
	a.runtimeMu.Unlock()
	if customJitter != nil {
		d := customJitter(capDelay)
		if d < 0 {
			return 0
		}
		if d > capDelay {
			return capDelay
		}
		return d
	}

	return time.Duration(rand.Int64N(int64(capDelay) + 1))
}
