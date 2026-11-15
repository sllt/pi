package pi

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	gWebsocket "github.com/gorilla/websocket"
	"github.com/sllt/tao/actor"

	"github.com/sllt/pi/pkg/pi/websocket"
)

const (
	defaultWSHandshakeTimeout  = 10 * time.Second
	defaultWSWriteTimeout      = 10 * time.Second
	defaultWSBackoffInitial    = 200 * time.Millisecond
	defaultWSBackoffMax        = 30 * time.Second
	defaultWSHealthyResetAfter = time.Minute
	defaultWSRetryWindow       = time.Minute
	defaultLegacyWSRetryDelay  = time.Second
)

var (
	errWSServiceNameEmpty       = errors.New("websocket client service name cannot be empty")
	errWSServiceNameDuplicate   = errors.New("websocket client service name already registered")
	errWSClientInvalidURL       = errors.New("websocket client URL must use ws:// or wss:// with a valid host")
	errWSClientRegisterTooLate  = errors.New("cannot register declarative websocket client after app has started")
	errWSClientNegativeDuration = errors.New("websocket client policy durations must not be negative")
	errWSClientBackoffRange     = errors.New("websocket client BackoffInitial must not exceed BackoffMax")
	errWSClientNegativeRetries  = errors.New("websocket client MaxRetries must not be negative")
	errWSClientRetriesExhausted = errors.New("websocket client reconnection budget exhausted")
)

// WSClientPolicy configures a declarative outbound WebSocket client registered via App.AddWSClient.
// Construction only validates and stores the policy; dialing and reconnection run after App.Start.
type WSClientPolicy struct {
	Headers            http.Header
	TLSConfig          *tls.Config
	Subprotocols       []string
	HandshakeTimeout   time.Duration // Default 10s; negative rejected.
	WriteTimeout       time.Duration // Default 10s; negative rejected.
	PingInterval       time.Duration // >0 sends periodic Ping control frames to detect dead peers; negative rejected.
	EnableReconnection bool          // Reconnect with exponential backoff + full jitter on dial failure or disconnect.
	Required           bool          // Gates Start readiness and marks Readiness false while disconnected/backing off.
	FatalOnFailure     bool          // Escalate terminal connection failure to App shutdown instead of Degraded.
	BackoffInitial     time.Duration // Default 200ms (used when EnableReconnection is true).
	BackoffMax         time.Duration // Default 30s with full jitter.
	HealthyResetAfter  time.Duration // Default 1m; resets consecutive backoff level without clearing RetryWindow history.
	MaxRetries         int           // 0 allows unbounded reconnection attempts; >0 caps retries within RetryWindow.
	RetryWindow        time.Duration // Sliding window for MaxRetries (default 1m).
}

func normalizeWSClientPolicy(p WSClientPolicy) (WSClientPolicy, error) {
	if p.HandshakeTimeout < 0 ||
		p.WriteTimeout < 0 ||
		p.PingInterval < 0 ||
		p.BackoffInitial < 0 ||
		p.BackoffMax < 0 ||
		p.HealthyResetAfter < 0 ||
		p.RetryWindow < 0 {
		return p, errWSClientNegativeDuration
	}
	if p.MaxRetries < 0 {
		return p, errWSClientNegativeRetries
	}
	if p.HandshakeTimeout == 0 {
		p.HandshakeTimeout = defaultWSHandshakeTimeout
	}
	if p.WriteTimeout == 0 {
		p.WriteTimeout = defaultWSWriteTimeout
	}
	if p.BackoffInitial > 0 && p.BackoffMax > 0 && p.BackoffInitial > p.BackoffMax {
		return p, errWSClientBackoffRange
	}
	if p.BackoffInitial == 0 {
		p.BackoffInitial = defaultWSBackoffInitial
	}
	if p.BackoffMax == 0 {
		p.BackoffMax = max(defaultWSBackoffMax, p.BackoffInitial)
	}
	if p.BackoffInitial > p.BackoffMax {
		return p, errWSClientBackoffRange
	}
	if p.HealthyResetAfter == 0 {
		p.HealthyResetAfter = defaultWSHealthyResetAfter
	}
	if p.RetryWindow == 0 {
		p.RetryWindow = defaultWSRetryWindow
	}
	if p.Headers != nil {
		p.Headers = p.Headers.Clone()
	}
	if p.TLSConfig != nil {
		p.TLSConfig = p.TLSConfig.Clone()
	}
	if len(p.Subprotocols) > 0 {
		p.Subprotocols = slices.Clone(p.Subprotocols)
	}
	return p, nil
}

func validateWSClientURL(rawURL string) error {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed == nil || parsed.Host == "" {
		return fmt.Errorf("%w: %q", errWSClientInvalidURL, rawURL)
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "ws" && scheme != "wss" {
		return fmt.Errorf("%w: %q", errWSClientInvalidURL, rawURL)
	}
	return nil
}

// AddWSClient declaratively registers an outbound WebSocket client to be dialed and
// supervised under ws-sup when the application starts. Unlike legacy AddWSService,
// AddWSClient performs no network I/O or goroutine creation before App.Start.
func (a *App) AddWSClient(serviceName, rawURL string, policy WSClientPolicy) error {
	serviceName = strings.TrimSpace(serviceName)
	if serviceName == "" {
		return errWSServiceNameEmpty
	}
	if err := validateWSClientURL(rawURL); err != nil {
		return err
	}
	normalized, err := normalizeWSClientPolicy(policy)
	if err != nil {
		return err
	}

	a.runtimeMu.Lock()
	defer a.runtimeMu.Unlock()

	if a.runtimeState != lifecycleNew {
		return errWSClientRegisterTooLate
	}
	if a.hasWSClientServiceLocked(serviceName) {
		return fmt.Errorf("%w: %s", errWSClientNameErr(serviceName), serviceName)
	}

	a.nextWSClientSeq++
	seq := a.nextWSClientSeq
	actorName := sanitizeActorSegment("ws-client", seq, serviceName)
	slot := &wsClientSlot{
		app:         a,
		seq:         seq,
		serviceName: serviceName,
		actorName:   actorName,
		url:         strings.TrimSpace(rawURL),
		policy:      normalized,
		state:       workerPending,
		stateWaitCh: make(chan struct{}),
	}
	a.wsClients = append(a.wsClients, slot)
	return nil
}

func errWSClientNameErr(_ string) error {
	return errWSServiceNameDuplicate
}

func (a *App) hasWSClientServiceLocked(serviceName string) bool {
	for _, s := range a.wsClients {
		if s.serviceName == serviceName {
			return true
		}
	}
	for _, c := range a.compatWSServices {
		if c.serviceName == serviceName && !c.isStopped() {
			return true
		}
	}
	return false
}

func (a *App) wsClientSlots() []*wsClientSlot {
	a.runtimeMu.Lock()
	defer a.runtimeMu.Unlock()
	return append([]*wsClientSlot(nil), a.wsClients...)
}

type wsClientReconcileMsg struct{}

type wsClientRetryTimerMsg struct {
	epoch uint64
}

type wsClientHealthyTimerMsg struct {
	gen uint64
}

type wsClientPingTimerMsg struct {
	gen uint64
}

type wsClientSlot struct {
	app         *App
	seq         uint64
	serviceName string
	actorName   string
	actorRef    actor.Ref
	url         string
	policy      WSClientPolicy

	mu                  sync.Mutex
	node                *actor.Node
	controllerPID       actor.PID
	controllerBoots     uint64
	actorRestarts       uint64
	state               workerState
	ready               bool
	terminal            bool
	generation          uint64
	dialing             bool
	dialCancel          context.CancelFunc
	dialDone            chan struct{}
	activeConn          *websocket.Connection
	connectedAt         time.Time
	pendingConnectedGen uint64
	healthyGen          uint64
	pendingDisconnect   bool
	pendingDisconnErr   error
	pendingRetry        bool
	consecutiveFailures int
	retryTimes          []time.Time
	clientRetries       uint64
	lastBackoffDelay    time.Duration
	backoffUntil        time.Time
	lastErr             error
	notifyScheduled     bool
	stateWaitCh         chan struct{}
}

func (s *wsClientSlot) broadcastStateChangeLocked() {
	if s.stateWaitCh != nil {
		close(s.stateWaitCh)
	}
	s.stateWaitCh = make(chan struct{})
	if s.app != nil {
		s.app.notifyGRPCHealthChanged()
	}
}

func (s *wsClientSlot) currentControllerPID() actor.PID {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.controllerPID
}

func (s *wsClientSlot) isTerminal() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.terminal
}

func (s *wsClientSlot) pruneRetryTimesLocked(now time.Time) {
	if len(s.retryTimes) == 0 {
		return
	}
	cutoff := now.Add(-s.policy.RetryWindow)
	keep := 0
	for _, ts := range s.retryTimes {
		if !ts.Before(cutoff) {
			s.retryTimes[keep] = ts
			keep++
		}
	}
	s.retryTimes = s.retryTimes[:keep]
}

func (s *wsClientSlot) checkStartupReadiness() (ready bool, terminalErr error, waitCh <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	waitCh = s.stateWaitCh
	if s.ready && s.state == workerRunning && s.activeConn != nil {
		return true, nil, waitCh
	}
	if s.terminal || s.state == workerDegraded || s.state == workerFailed || s.state == workerStopped {
		err := s.lastErr
		if err == nil {
			err = fmt.Errorf("required websocket client %q stopped before becoming ready", s.serviceName)
		}
		return false, err, waitCh
	}
	return false, nil, waitCh
}

func (s *wsClientSlot) isHealthyForReadiness() bool {
	if !s.policy.Required {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.terminal && s.state == workerRunning && s.ready && s.activeConn != nil
}

func (s *wsClientSlot) closeActiveConnLocked() *websocket.Connection {
	conn := s.activeConn
	s.activeConn = nil
	s.ready = false
	return conn
}

func (s *wsClientSlot) removeAndCloseConn(conn *websocket.Connection) {
	if conn == nil {
		return
	}
	if s.app != nil {
		if hub := s.app.WebSocketHub(); hub != nil {
			if hub.CloseConnectionIfMatch(s.serviceName, conn) {
				return
			}
		}
	}
	if conn.Conn != nil {
		_ = conn.Close()
	}
}

func (s *wsClientSlot) onConnectionError(gen uint64, conn *websocket.Connection, err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	if s.generation != gen || s.activeConn != conn {
		s.mu.Unlock()
		return
	}
	wasHealthy := s.healthyGen == gen || (!s.connectedAt.IsZero() && time.Since(s.connectedAt) >= s.policy.HealthyResetAfter)
	s.activeConn = nil
	s.ready = false
	if s.terminal || s.app.isIntakeClosed() {
		s.state = workerStopped
		s.broadcastStateChangeLocked()
		s.mu.Unlock()
		s.removeAndCloseConn(conn)
		return
	}
	if wasHealthy {
		s.consecutiveFailures = 0
	}
	s.pendingDisconnect = true
	s.pendingDisconnErr = sanitizeWSDialError(err, s.url)
	s.state = workerStarting
	s.broadcastStateChangeLocked()
	s.mu.Unlock()

	s.removeAndCloseConn(conn)
	s.notifyController()
}

func sanitizeWSDialError(err error, rawURL string) error {
	if err == nil {
		return nil
	}
	orig := err.Error()
	msg := orig
	if rawURL != "" {
		redactedURL := redactEndpointURL(rawURL)
		if redactedURL != rawURL && strings.Contains(msg, rawURL) {
			msg = strings.ReplaceAll(msg, rawURL, redactedURL)
		}
	}
	msg = redactErrorString(msg)
	if msg == orig {
		return err
	}
	return errors.New(msg)
}

func (s *wsClientSlot) onControllerLost(reason error) {
	if reason == nil {
		reason = errActorRuntimeTerminated
	}
	cause := fmt.Errorf("websocket client %q controller terminated: %w", s.serviceName, reason)

	s.mu.Lock()
	s.controllerPID = actor.PID{}
	conn := s.closeActiveConnLocked()
	cancel := s.dialCancel
	if !s.terminal {
		s.terminal = true
		s.state = workerFailed
		s.lastErr = cause
		s.broadcastStateChangeLocked()
	}
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	s.removeAndCloseConn(conn)
	s.app.recordFatalCause(cause)
}

func (s *wsClientSlot) notifyController() {
	s.mu.Lock()
	if s.terminal || s.node == nil || s.app.isIntakeClosed() {
		s.mu.Unlock()
		return
	}
	node := s.node
	ref := s.actorRef
	pid := s.controllerPID
	s.mu.Unlock()

	err := node.Send(ref, wsClientReconcileMsg{})
	if err != nil && pid != (actor.PID{}) {
		_ = node.Send(pid, wsClientReconcileMsg{})
	}
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

	s.mu.Lock()
	if s.notifyScheduled || s.terminal || s.app.isIntakeClosed() {
		s.mu.Unlock()
		return
	}
	if !s.app.tryAdmitRuntimeTask() {
		s.mu.Unlock()
		return
	}
	s.notifyScheduled = true
	s.mu.Unlock()

	go func() {
		defer s.app.runtimeTasks.Done()
		intakeDone := s.app.intakeDone()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-intakeDone:
				s.mu.Lock()
				s.notifyScheduled = false
				s.mu.Unlock()
				return
			case <-ticker.C:
				s.mu.Lock()
				if s.terminal || s.node == nil || s.app.isIntakeClosed() {
					s.notifyScheduled = false
					s.mu.Unlock()
					return
				}
				n := s.node
				r := s.actorRef
				p := s.controllerPID
				s.mu.Unlock()

				sendErr := n.Send(r, wsClientReconcileMsg{})
				if sendErr != nil && p != (actor.PID{}) {
					_ = n.Send(p, wsClientReconcileMsg{})
				}
				if sendErr == nil || s.app.isIntakeClosed() {
					s.mu.Lock()
					s.notifyScheduled = false
					s.mu.Unlock()
					return
				}
				if errors.Is(sendErr, actor.ErrClosed) {
					s.mu.Lock()
					s.notifyScheduled = false
					s.mu.Unlock()
					s.app.onActorTreeFailure(sendErr)
					return
				}
				if isPermanentControllerSendErr(sendErr) {
					s.mu.Lock()
					s.notifyScheduled = false
					s.mu.Unlock()
					s.onControllerLost(sendErr)
					return
				}
			}
		}
	}()
}

type wsClientActor struct {
	actor.Actor
	app          *App
	slot         *wsClientSlot
	stopTracked  bool
	bootSeq      uint64
	armedConnGen uint64
	retryEpoch   uint64
	retryTimer   actor.TimerRef
	hlthTimer    actor.TimerRef
	pingTimer    actor.TimerRef
}

func (w *wsClientActor) cancelTimers() {
	w.retryTimer.Cancel()
	w.hlthTimer.Cancel()
	w.pingTimer.Cancel()
}

func (w *wsClientActor) node() *actor.Node {
	w.slot.mu.Lock()
	defer w.slot.mu.Unlock()
	return w.slot.node
}

func (w *wsClientActor) armRetryTimer(delay time.Duration) {
	w.retryTimer.Cancel()
	w.retryEpoch++
	epoch := w.retryEpoch
	msg := wsClientRetryTimerMsg{epoch: epoch}
	if delay <= 0 {
		if w.Send(w.PID(), msg) != nil {
			w.slot.notifyController()
		}
		return
	}
	t, err := w.SendAfter(delay, msg)
	if err != nil {
		w.slot.notifyController()
		return
	}
	w.retryTimer = t
	w.app.watchActorTimerDeliver(w.node(), w.PID(), t, msg)
}

func (w *wsClientActor) armConnectedTimers(gen uint64, healthyRemaining time.Duration) {
	w.hlthTimer.Cancel()
	w.pingTimer.Cancel()
	w.armedConnGen = gen
	node := w.node()
	if healthyRemaining > 0 {
		msg := wsClientHealthyTimerMsg{gen: gen}
		t, err := w.SendAfter(healthyRemaining, msg)
		if err == nil {
			w.hlthTimer = t
			w.app.watchActorTimerDeliver(node, w.PID(), t, msg)
		}
	}
	if p := w.slot.policy.PingInterval; p > 0 {
		msg := wsClientPingTimerMsg{gen: gen}
		t, err := w.SendAfter(p, msg)
		if err == nil {
			w.pingTimer = t
			w.app.watchActorTimerDeliver(node, w.PID(), t, msg)
		}
	}
}

func (w *wsClientActor) Receive(msg any) error {
	s := w.slot
	switch m := msg.(type) {
	case actor.Started:
		if !w.stopTracked {
			w.stopTracked = true
			w.app.controllerStopTasks.Add(1)
		}
		pid := w.PID()
		s.mu.Lock()
		s.controllerPID = pid
		s.controllerBoots++
		w.bootSeq = s.controllerBoots
		node := s.node
		restarted := w.bootSeq > 1
		if restarted {
			s.actorRestarts++
		}
		s.mu.Unlock()

		if restarted {
			w.app.recordActorRestartMetric(s.serviceName, "ws_client")
			w.Log().Warn("ws_client controller actor restarted", "service", s.serviceName, "bootSeq", w.bootSeq)
		}

		w.app.sendRootMonitorWithRetry(
			node,
			wsClientMonitorControllerMsg{slot: s, pid: pid},
			func() bool {
				s.mu.Lock()
				defer s.mu.Unlock()
				return s.terminal || s.controllerPID != pid
			},
			s.onControllerLost,
		)
		return w.reconcile()

	case wsClientReconcileMsg:
		return w.reconcile()

	case wsClientRetryTimerMsg:
		if m.epoch != w.retryEpoch {
			return nil
		}
		w.retryTimer = actor.TimerRef{}
		s.mu.Lock()
		if s.state == workerBackingOff {
			s.backoffUntil = time.Time{}
		}
		s.mu.Unlock()
		return w.reconcile()

	case wsClientHealthyTimerMsg:
		w.hlthTimer = actor.TimerRef{}
		s.mu.Lock()
		if s.generation == m.gen && s.activeConn != nil && !s.terminal {
			s.healthyGen = m.gen
			s.consecutiveFailures = 0
		}
		s.mu.Unlock()
		return nil

	case wsClientPingTimerMsg:
		w.pingTimer = actor.TimerRef{}
		s.mu.Lock()
		conn := s.activeConn
		gen := s.generation
		alive := !s.terminal && !w.app.isIntakeClosed() && conn != nil && gen == m.gen
		pingInterval := s.policy.PingInterval
		writeTimeout := s.policy.WriteTimeout
		node := s.node
		s.mu.Unlock()
		if !alive {
			return nil
		}

		deadline := time.Now().Add(min(pingInterval, writeTimeout))
		if err := conn.WriteControl(gWebsocket.PingMessage, nil, deadline); err != nil {
			s.onConnectionError(gen, conn, err)
			return nil
		}
		nextMsg := wsClientPingTimerMsg{gen: gen}
		t, err := w.SendAfter(pingInterval, nextMsg)
		if err == nil {
			w.pingTimer = t
			w.app.watchActorTimerDeliver(node, w.PID(), t, nextMsg)
		}
		return nil

	case actorInjectPanicMsg:
		panic(m.reason)

	case actor.Stopped:
		if w.stopTracked {
			w.stopTracked = false
			defer w.app.controllerStopTasks.Done()
		}
		w.cancelTimers()
		s.mu.Lock()
		closing := w.app.isIntakeClosed() || s.terminal
		var conn *websocket.Connection
		var cancel context.CancelFunc
		var done chan struct{}
		if closing {
			s.terminal = true
			if s.state != workerDegraded && s.state != workerFailed {
				s.state = workerStopped
			}
			conn = s.closeActiveConnLocked()
			cancel = s.dialCancel
			done = s.dialDone
			s.broadcastStateChangeLocked()
		}
		s.mu.Unlock()

		if cancel != nil {
			cancel()
		}
		s.removeAndCloseConn(conn)
		if done != nil {
			<-done
		}
		return nil
	}
	return nil
}

func (w *wsClientActor) failTerminalLocked(err error) error {
	s := w.slot
	if err == nil {
		err = errWSClientRetriesExhausted
	}
	s.lastErr = err
	s.terminal = true
	s.ready = false
	if s.policy.FatalOnFailure {
		s.state = workerFailed
		s.broadcastStateChangeLocked()
		s.mu.Unlock()
		w.cancelTimers()
		wrapped := fmt.Errorf("websocket client %q failed: %w", s.serviceName, err)
		w.app.recordFatalCause(wrapped)
		return nil
	}
	s.state = workerDegraded
	s.broadcastStateChangeLocked()
	s.mu.Unlock()
	w.cancelTimers()
	if l := w.app.safeLogger(); l != nil {
		l.Warnf("WebSocket client %q degraded: %v", s.serviceName, err)
	}
	return nil
}

func (w *wsClientActor) reconcile() error {
	s := w.slot
	s.mu.Lock()

	if s.terminal || w.app.isIntakeClosed() {
		conn := s.closeActiveConnLocked()
		cancel := s.dialCancel
		if !s.terminal {
			s.terminal = true
			s.state = workerStopped
			s.broadcastStateChangeLocked()
		}
		s.mu.Unlock()
		w.cancelTimers()
		if cancel != nil {
			cancel()
		}
		s.removeAndCloseConn(conn)
		return nil
	}

	// Non-overlap latch: never run two dials or overwrite a live connection.
	if s.dialing || s.activeConn != nil {
		if s.activeConn != nil && (s.pendingConnectedGen == s.generation || w.armedConnGen != s.generation) {
			gen := s.generation
			s.pendingConnectedGen = 0
			healthyRem := time.Duration(0)
			if s.policy.HealthyResetAfter > 0 && s.healthyGen != gen {
				if s.connectedAt.IsZero() {
					healthyRem = s.policy.HealthyResetAfter
				} else {
					healthyRem = max(time.Millisecond, s.policy.HealthyResetAfter-time.Since(s.connectedAt))
				}
			}
			s.mu.Unlock()
			w.armConnectedTimers(gen, healthyRem)
			return nil
		}
		s.mu.Unlock()
		return nil
	}

	if s.pendingDisconnect {
		disconnErr := s.pendingDisconnErr
		s.pendingDisconnect = false
		s.pendingDisconnErr = nil
		s.lastErr = disconnErr

		if !s.policy.EnableReconnection {
			return w.failTerminalLocked(disconnErr)
		}

		now := time.Now()
		if s.policy.MaxRetries > 0 {
			s.pruneRetryTimesLocked(now)
			if len(s.retryTimes) >= s.policy.MaxRetries {
				return w.failTerminalLocked(fmt.Errorf("%w: %w", errWSClientRetriesExhausted, disconnErr))
			}
		}

		s.pendingRetry = true
		s.consecutiveFailures++
		delay := w.app.computeWorkerBackoff(WorkerPolicy{
			BackoffInitial: s.policy.BackoffInitial,
			BackoffMax:     s.policy.BackoffMax,
		}, s.consecutiveFailures)
		s.state = workerBackingOff
		s.ready = false
		s.lastBackoffDelay = delay
		s.backoffUntil = now.Add(delay)
		s.broadcastStateChangeLocked()
		s.mu.Unlock()

		w.app.recordWorkerBackoffMetric(s.serviceName, "ws_client", delay)
		w.app.syncGRPCHealth()
		w.armRetryTimer(delay)
		return nil
	}

	if s.state == workerBackingOff && !s.backoffUntil.IsZero() {
		if rem := time.Until(s.backoffUntil); rem > 0 {
			s.mu.Unlock()
			w.armRetryTimer(rem)
			return nil
		}
		s.backoffUntil = time.Time{}
	}

	now := time.Now()
	if s.pendingRetry && s.policy.MaxRetries > 0 {
		s.pruneRetryTimesLocked(now)
		if len(s.retryTimes) >= s.policy.MaxRetries {
			return w.failTerminalLocked(fmt.Errorf("%w: %w", errWSClientRetriesExhausted, s.lastErr))
		}
	}

	if !w.app.tryAdmitRuntimeTask() {
		s.terminal = true
		s.state = workerStopped
		s.broadcastStateChangeLocked()
		s.mu.Unlock()
		w.cancelTimers()
		return nil
	}

	didRetry := false
	if s.pendingRetry {
		s.pendingRetry = false
		s.clientRetries++
		didRetry = true
		if s.policy.MaxRetries > 0 {
			s.retryTimes = append(s.retryTimes, now)
		}
	}

	s.generation++
	gen := s.generation
	dialCtx, dialCancel := context.WithTimeout(w.app.intakeContext(), s.policy.HandshakeTimeout)
	dialDone := make(chan struct{})
	s.dialing = true
	s.dialCancel = dialCancel
	s.dialDone = dialDone
	s.state = workerStarting
	s.ready = false
	s.broadcastStateChangeLocked()

	targetURL := s.url
	headers := s.policy.Headers.Clone()
	var tlsCfg *tls.Config
	if s.policy.TLSConfig != nil {
		tlsCfg = s.policy.TLSConfig.Clone()
	}
	subprotocols := slices.Clone(s.policy.Subprotocols)
	writeTimeout := s.policy.WriteTimeout
	s.mu.Unlock()

	if didRetry {
		w.app.recordWorkerRestartMetric(s.serviceName, "ws_client")
	}
	w.app.syncGRPCHealth()

	go func() {
		defer w.app.runtimeTasks.Done()
		defer close(dialDone)
		defer dialCancel()

		var (
			rawConn *gWebsocket.Conn
			resp    *http.Response
			err     error
		)
		func() {
			defer func() {
				if re := recover(); re != nil {
					if l := w.app.safeLogger(); l != nil {
						panicRecovery(re, l)
					}
					err = fmt.Errorf("websocket dial panicked: %v", re)
				}
			}()
			dialer := gWebsocket.Dialer{
				Proxy:            http.ProxyFromEnvironment,
				HandshakeTimeout: s.policy.HandshakeTimeout,
				TLSClientConfig:  tlsCfg,
				Subprotocols:     subprotocols,
			}
			rawConn, resp, err = dialer.DialContext(dialCtx, targetURL, headers)
		}()
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}

		s.mu.Lock()
		if s.generation == gen {
			s.dialing = false
			s.dialCancel = nil
			s.dialDone = nil
		}

		if err != nil {
			cleanErr := sanitizeWSDialError(err, targetURL)
			if s.terminal || w.app.isIntakeClosed() {
				s.state = workerStopped
				s.broadcastStateChangeLocked()
				s.mu.Unlock()
				return
			}
			s.pendingDisconnect = true
			s.pendingDisconnErr = cleanErr
			s.broadcastStateChangeLocked()
			s.mu.Unlock()
			if l := w.app.safeLogger(); l != nil {
				l.Debugf("WebSocket client %q dial failed: %v", s.serviceName, cleanErr)
			}
			s.notifyController()
			return
		}

		if s.terminal || w.app.isIntakeClosed() || s.generation != gen {
			s.mu.Unlock()
			if rawConn != nil {
				_ = rawConn.Close()
			}
			return
		}
		s.mu.Unlock()

		wsConn := &websocket.Connection{Conn: rawConn}
		wsConn.ConfigureClient(writeTimeout, func(connErr error) {
			s.onConnectionError(gen, wsConn, connErr)
		})
		if w.app != nil && w.app.kernel != nil {
			w.app.kernel.addWSConnection(s.serviceName, wsConn)
		}

		s.mu.Lock()
		if s.terminal || w.app.isIntakeClosed() || s.generation != gen {
			s.mu.Unlock()
			if w.app != nil {
				if hub := w.app.WebSocketHub(); hub != nil {
					hub.CloseConnectionIfMatch(s.serviceName, wsConn)
				}
			}
			_ = wsConn.ForceClose()
			return
		}
		s.activeConn = wsConn
		s.ready = true
		s.state = workerRunning
		s.connectedAt = time.Now()
		s.pendingConnectedGen = gen
		s.broadcastStateChangeLocked()
		s.mu.Unlock()

		w.app.syncGRPCHealth()
		if l := w.app.safeLogger(); l != nil {
			l.Infof("Successfully connected to WebSocket client service: %s", s.serviceName)
		}

		// Trigger timer arming on the actor mailbox.
		s.notifyController()
	}()

	return nil
}

func (a *App) buildWSClientChildSpecs(node *actor.Node) []actor.ChildSpec {
	slots := a.wsClientSlots()
	hasServerWS := a.hasWSServerRegistered()
	if len(slots) == 0 && !hasServerWS {
		return nil
	}
	specs := make([]actor.ChildSpec, 0, len(slots)+1)
	if hasServerWS {
		specs = append(specs, actor.ChildSpec{
			Name: "ws-hub",
			Factory: func() actor.Behavior {
				return &wsSessionHubActor{app: a}
			},
			Options: []actor.SpawnOption{
				actor.WithRestart(actor.Permanent),
			},
		})
	}
	for _, slot := range slots {
		s := slot
		s.mu.Lock()
		s.node = node
		s.actorRef = actor.Ref{Path: "pi-root/ws-sup/" + s.actorName}
		s.mu.Unlock()

		specs = append(specs, actor.ChildSpec{
			Name: s.actorName,
			Factory: func() actor.Behavior {
				return &wsClientActor{app: a, slot: s}
			},
			Options: []actor.SpawnOption{
				actor.WithRestart(actor.Permanent),
			},
		})
	}
	return specs
}

// compatWSService manages the lifecycle of a legacy AddWSService registration,
// supporting pre-Start retry, Stop-before-Start cancellation, and single-loop
// runtime reconnection without spawning duplicate reconnectors.
type compatWSService struct {
	app           *App
	serviceName   string
	url           string
	headers       http.Header
	retryInterval time.Duration

	mu          sync.Mutex
	ctx         context.Context
	cancel      context.CancelFunc
	stopped     bool
	loopRunning bool
	activeConn  *websocket.Connection
	wg          sync.WaitGroup
}

func newCompatWSService(app *App, serviceName, rawURL string, headers http.Header, retryInterval time.Duration) *compatWSService {
	if retryInterval <= 0 {
		retryInterval = defaultLegacyWSRetryDelay
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &compatWSService{
		app:           app,
		serviceName:   serviceName,
		url:           rawURL,
		headers:       headers.Clone(),
		retryInterval: retryInterval,
		ctx:           ctx,
		cancel:        cancel,
	}
}

func (c *compatWSService) isStopped() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stopped
}

func (c *compatWSService) attachConnected(rawConn *gWebsocket.Conn) *websocket.Connection {
	wsConn := &websocket.Connection{Conn: rawConn}
	wsConn.ConfigureClient(0, func(err error) {
		c.onDisconnect(wsConn, err)
	})

	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		_ = rawConn.Close()
		return nil
	}
	old := c.activeConn
	c.activeConn = wsConn
	c.mu.Unlock()

	if old != nil && old != wsConn {
		c.removeConn(old)
	}
	if c.app != nil && c.app.kernel != nil {
		c.app.kernel.addWSConnection(c.serviceName, wsConn)
	}
	return wsConn
}

func (c *compatWSService) removeConn(conn *websocket.Connection) {
	if conn == nil {
		return
	}
	if c.app != nil {
		if hub := c.app.WebSocketHub(); hub != nil {
			if hub.CloseConnectionIfMatch(c.serviceName, conn) {
				return
			}
		}
	}
	if conn.Conn != nil {
		_ = conn.Close()
	}
}

func (c *compatWSService) onDisconnect(conn *websocket.Connection, _ error) {
	c.mu.Lock()
	if c.activeConn == conn {
		c.activeConn = nil
	}
	shouldReconnect := !c.stopped && (c.app == nil || !c.app.isIntakeClosed())
	c.mu.Unlock()

	c.removeConn(conn)
	if shouldReconnect {
		c.ensureRetryLoop()
	}
}

func (c *compatWSService) ensureRetryLoop() {
	c.mu.Lock()
	if c.stopped || c.loopRunning || (c.app != nil && c.app.isIntakeClosed()) {
		c.mu.Unlock()
		return
	}
	c.loopRunning = true
	c.wg.Add(1)
	ctx := c.ctx
	c.mu.Unlock()

	go func() {
		defer c.wg.Done()
		c.runRetryLoop(ctx)
	}()
}

func (c *compatWSService) runRetryLoop(ctx context.Context) {
	defer func() {
		c.mu.Lock()
		c.loopRunning = false
		c.mu.Unlock()
	}()

	for {
		if ctx.Err() != nil || (c.app != nil && c.app.isIntakeClosed()) {
			return
		}

		dialCtx, dialCancel := context.WithTimeout(ctx, defaultWSHandshakeTimeout)
		rawConn, resp, err := gWebsocket.DefaultDialer.DialContext(dialCtx, c.url, c.headers.Clone())
		dialCancel()
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}

		if err == nil {
			if c.attachConnected(rawConn) != nil {
				if l := c.app.safeLogger(); l != nil {
					l.Infof("Successfully connected to WebSocket service: %s", c.serviceName)
				}
			}
			return
		}

		if ctx.Err() != nil {
			return
		}

		timer := time.NewTimer(c.retryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		if l := c.app.safeLogger(); l != nil {
			l.Debugf("Reconnecting to WebSocket service: %s. Retry interval: %v", redactEndpointURL(c.url), c.retryInterval)
		}
	}
}

func (c *compatWSService) cancelStop() {
	c.mu.Lock()
	c.stopped = true
	cancel := c.cancel
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (c *compatWSService) stopAndWait() {
	c.cancelStop()
	c.wg.Wait()

	c.mu.Lock()
	conn := c.activeConn
	c.activeConn = nil
	c.mu.Unlock()

	c.removeConn(conn)
}

func (a *App) cancelCompatWSServices() {
	a.runtimeMu.Lock()
	services := append([]*compatWSService(nil), a.compatWSServices...)
	a.runtimeMu.Unlock()

	for _, svc := range services {
		svc.cancelStop()
	}
}

func (a *App) stopCompatWSServices() {
	a.runtimeMu.Lock()
	services := append([]*compatWSService(nil), a.compatWSServices...)
	a.runtimeMu.Unlock()

	for _, svc := range services {
		svc.stopAndWait()
	}
}
