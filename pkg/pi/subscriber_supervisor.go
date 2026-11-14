package pi

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sllt/tao/actor"

	"github.com/sllt/pi/pkg/pi/datasource/pubsub"
	"github.com/sllt/pi/pkg/pi/infra"
)

const (
	defaultSubscriberConcurrency  = 1
	defaultSubscriberQueueSize    = 64
	defaultSubscriberRetryBackoff = 200 * time.Millisecond
	defaultSubscriberPullRetry    = 2 * time.Second
	maxSubscriberSendRetries      = 25
)

var (
	errSubscriberTopicEmpty                   = errors.New("subscriber topic cannot be empty")
	errSubscriberHandlerNil                   = errors.New("subscriber handler cannot be nil")
	errSubscriberDuplicateTopic               = errors.New("subscriber topic already registered")
	errSubscriberRegisterTooLate              = errors.New("cannot register subscriber after app has started")
	errSubscriberNotInitialized               = errors.New("pubsub subscriber is not initialized in container")
	errSubscriberInvalidShutdownMode          = errors.New("invalid subscriber shutdown mode")
	errSubscriberInvalidConcurrency           = errors.New("subscriber Concurrency must be >= 0")
	errSubscriberInvalidQueueSize             = errors.New("subscriber QueueSize must be >= 0")
	errSubscriberInvalidRetryPolicy           = errors.New("subscriber MaxHandlerRetries and RetryBackoff must be >= 0")
	errSubscriberDeadLetterHandlerRequired    = errors.New("AllowDeadLetterSkip requires a non-nil DeadLetterHandler")
	errSubscriberUnsupportedCapability        = errors.New("pubsub backend does not support requested strict subscriber policy")
	errSubscriberKeyExtractorForbiddenOffsets = errors.New("KeyExtractor is forbidden on cumulative-offset backends; sharding must follow broker Partition")
	errSubscriberMissingOffsetMeta            = errors.New("cumulative-offset backend delivered message without broker Partition/Offset metadata")
	errSubscriptionKeyExtractorPanic          = errors.New("subscription key extractor panicked")
	errSubscriberPullPumpPanic                = errors.New("subscriber pull pump panicked")
)

// SubscriberShutdownMode controls how admitted subscription messages behave when App.Stop begins.
type SubscriberShutdownMode uint8

const (
	// SubscriberCancel cancels in-flight handlers immediately during Phase A (legacy Subscribe default).
	SubscriberCancel SubscriberShutdownMode = iota
	// SubscriberDrain stops pulling new messages in Phase A and allows already-admitted messages
	// to finish executing and committing within the Phase B drain budget.
	SubscriberDrain
)

func (m SubscriberShutdownMode) String() string {
	switch m {
	case SubscriberCancel:
		return "cancel"
	case SubscriberDrain:
		return "drain"
	default:
		return "unknown"
	}
}

func (m SubscriberShutdownMode) MarshalText() ([]byte, error) {
	return []byte(m.String()), nil
}

func (m *SubscriberShutdownMode) UnmarshalText(text []byte) error {
	switch strings.ToLower(strings.TrimSpace(string(text))) {
	case "", "0", "cancel":
		*m = SubscriberCancel
		return nil
	case "1", "drain":
		*m = SubscriberDrain
		return nil
	default:
		return errSubscriberInvalidShutdownMode
	}
}

// SubscribePolicy configures concurrency, bounded per-worker queue size, retry/DLQ rules,
// shutdown mode, and readiness criticality for App.AddSubscriber.
type SubscribePolicy struct {
	Concurrency         int                                          // Default 1; limits active concurrent worker actors.
	QueueSize           int                                          // Per-worker in-flight + queued cap; default 64.
	KeyExtractor        func(*pubsub.Message) string                 // Optional routing key for AckModeIndependent backends.
	MaxHandlerRetries   int                                          // Handler retry attempts before failure/DLQ; default 0.
	RetryBackoff        time.Duration                                // Delay between handler retries; defaults to 200ms when MaxHandlerRetries > 0.
	DeadLetterHandler   func(*Context, *pubsub.Message, error) error // Optional DLQ callback on terminal handler failure.
	AllowDeadLetterSkip bool                                         // Default false; source message is committed after DLQ only when true.
	Shutdown            SubscriberShutdownMode
	Required            bool
}

func normalizeSubscribePolicy(p SubscribePolicy) (SubscribePolicy, error) {
	if p.Shutdown > SubscriberDrain {
		return SubscribePolicy{}, errSubscriberInvalidShutdownMode
	}
	if p.Concurrency < 0 {
		return SubscribePolicy{}, errSubscriberInvalidConcurrency
	}
	if p.Concurrency == 0 {
		p.Concurrency = defaultSubscriberConcurrency
	}
	if p.QueueSize < 0 {
		return SubscribePolicy{}, errSubscriberInvalidQueueSize
	}
	if p.QueueSize == 0 {
		p.QueueSize = defaultSubscriberQueueSize
	}
	if p.MaxHandlerRetries < 0 || p.RetryBackoff < 0 {
		return SubscribePolicy{}, errSubscriberInvalidRetryPolicy
	}
	if p.MaxHandlerRetries > 0 && p.RetryBackoff == 0 {
		p.RetryBackoff = defaultSubscriberRetryBackoff
	}
	if p.AllowDeadLetterSkip && p.DeadLetterHandler == nil {
		return SubscribePolicy{}, errSubscriberDeadLetterHandlerRequired
	}
	return p, nil
}

func validatePolicyAgainstBackend(p SubscribePolicy, sem pubsub.AckSemantics) error {
	if sem.Mode == pubsub.AckModeUnknown {
		if p.Concurrency > 1 || p.KeyExtractor != nil {
			return fmt.Errorf("%w: Concurrency > 1 and KeyExtractor require explicit AckSemanticsProvider capability evidence (got %s)", errSubscriberUnsupportedCapability, sem.Mode)
		}
	}
	if p.Shutdown == SubscriberDrain {
		if sem.Mode != pubsub.AckModeIndependent && sem.Mode != pubsub.AckModeCumulativeOffset {
			return fmt.Errorf("%w: SubscriberDrain requires manual ack backend (got %s)", errSubscriberUnsupportedCapability, sem.Mode)
		}
		if !sem.SupportsContextCommit {
			return fmt.Errorf("%w: SubscriberDrain requires ContextCommitter support", errSubscriberUnsupportedCapability)
		}
	}
	if p.AllowDeadLetterSkip {
		if sem.Mode != pubsub.AckModeIndependent && sem.Mode != pubsub.AckModeCumulativeOffset {
			return fmt.Errorf("%w: AllowDeadLetterSkip requires manual ack backend (got %s)", errSubscriberUnsupportedCapability, sem.Mode)
		}
		if !sem.SupportsContextCommit {
			return fmt.Errorf("%w: AllowDeadLetterSkip requires ContextCommitter error reporting", errSubscriberUnsupportedCapability)
		}
	}
	if sem.Mode == pubsub.AckModeCumulativeOffset {
		if p.KeyExtractor != nil {
			return errSubscriberKeyExtractorForbiddenOffsets
		}
		if !sem.SupportsContextCommit {
			return fmt.Errorf("%w: cumulative-offset backend requires ContextCommitter support", errSubscriberUnsupportedCapability)
		}
	}
	return nil
}

type subscriberLifecycleState uint8

const (
	subStatePending subscriberLifecycleState = iota
	subStateStarting
	subStateRunning
	subStateBackingOff
	subStateDegraded
	subStateFailed
	subStateStopped
)

func (s subscriberLifecycleState) String() string {
	switch s {
	case subStatePending:
		return "Pending"
	case subStateStarting:
		return "Starting"
	case subStateRunning:
		return "Running"
	case subStateBackingOff:
		return "BackingOff"
	case subStateDegraded:
		return "Degraded"
	case subStateFailed:
		return "Failed"
	case subStateStopped:
		return "Stopped"
	default:
		return "Unknown"
	}
}

type subTicketState uint8

const (
	subTicketReserved subTicketState = iota
	subTicketAdmitted
	subTicketRunning
	subTicketResolved
	subTicketAbandoned
)

type subMessageTicket struct {
	id         uint64
	slot       *subscriberSlot
	workerIdx  int
	msg        *pubsub.Message
	meta       pubsub.MessageMeta
	msgSem     pubsub.AckSemantics
	pullCancel context.CancelCauseFunc

	mu             sync.Mutex
	state          subTicketState
	recoveryAction string
	releaseOnce    sync.Once
}

func (t *subMessageTicket) markAdmitted() {
	t.mu.Lock()
	if t.state == subTicketReserved {
		t.state = subTicketAdmitted
	}
	t.mu.Unlock()
}

// releaseReturningMsg releases the worker semaphore slot, ledger entry, and runtimeTasks count
// exactly once, optionally preserving pullCancel when the coordinator retains msg for retry.
func (t *subMessageTicket) releaseInternal(cancelPull bool) {
	t.releaseOnce.Do(func() {
		s := t.slot
		if cancelPull && t.pullCancel != nil {
			t.pullCancel(nil)
		}
		<-s.sem[t.workerIdx]

		s.gateMu.Lock()
		delete(s.ledger, t.id)
		if s.inFlightTickets > 0 {
			s.inFlightTickets--
		}
		if s.inFlightTickets == 0 {
			select {
			case <-s.drainedCh:
			default:
				close(s.drainedCh)
			}
		}
		s.gateMu.Unlock()

		if s.app != nil {
			s.app.runtimeTasks.Done()
		}
	})
}

func (t *subMessageTicket) release() {
	t.releaseInternal(true)
}

func (t *subMessageTicket) failSendAndRetain() {
	t.mu.Lock()
	if t.state == subTicketReserved || t.state == subTicketAdmitted {
		t.state = subTicketAbandoned
		t.recoveryAction = "retained_for_send_retry"
	}
	t.mu.Unlock()
	t.releaseInternal(false)
}

func (t *subMessageTicket) finishResolved(action string) {
	t.mu.Lock()
	t.state = subTicketResolved
	t.recoveryAction = action
	t.mu.Unlock()
	t.release()
}

func (t *subMessageTicket) finishAbandoned(action string) {
	t.mu.Lock()
	t.state = subTicketAbandoned
	t.recoveryAction = action
	t.mu.Unlock()
	t.slot.recordAbandonedAction(action)
	t.release()
}

type subscriberSnapshot struct {
	Topic               string
	CoordActorName      string
	State               subscriberLifecycleState
	AckMode             pubsub.AckMode
	Concurrency         int
	QueueSize           int
	Required            bool
	Shutdown            SubscriberShutdownMode
	Ready               bool
	InFlightTickets     int
	FrozenPartitions    []int
	ProcessedTotal      uint64
	CommittedTotal      uint64
	FailuresTotal       uint64
	PanicsTotal         uint64
	HandlerRetriesTotal uint64
	SendRejectedTotal   uint64
	CommitFailuresTotal uint64
	CommitUnknownTotal  uint64
	DLQSentTotal        uint64
	DLQSkippedTotal     uint64
	DLQFailuresTotal    uint64
	AbandonedTotal      uint64
	CoordRestarts       uint64
	WorkerRestarts      uint64
	LastRecoveryAction  string
	LastError           error
}

type subCoordReconcileMsg struct{}

type subWorkerReconcileLedgerMsg struct{}

type subDeliverMsg struct {
	ticketID uint64
}

type subCoordMonitorMsg struct {
	slot *subscriberSlot
	pid  actor.PID
}

type subWorkerMonitorMsg struct {
	slot      *subscriberSlot
	workerIdx int
	pid       actor.PID
}

type subscriberSlot struct {
	app            *App
	seq            uint64
	topic          string
	policy         SubscribePolicy
	handler        SubscribeFunc
	isLegacy       bool
	backendSem     pubsub.AckSemantics
	coordActorName string
	coordRef       actor.Ref
	workerNames    []string
	workerRefs     []actor.Ref
	sem            []chan struct{}
	rrSeq          atomic.Uint64
	pullRetryDelay time.Duration

	mu                  sync.Mutex
	node                *actor.Node
	coordPID            actor.PID
	coordBoots          uint64
	coordRestarts       uint64
	workerPIDs          []actor.PID
	workerBoots         []uint64
	workerRestarts      uint64
	state               subscriberLifecycleState
	ready               bool
	launchedOnce        bool
	controllerLost      bool
	stateNotifyCh       chan struct{}
	pumpInFlight        bool
	pumpGen             uint64
	pumpCancel          context.CancelCauseFunc
	pumpDone            chan struct{}
	waitOldPumpActive   bool
	notifyRetrying      bool
	frozenPartitions    map[int]error
	processedTotal      uint64
	committedTotal      uint64
	failuresTotal       uint64
	panicsTotal         uint64
	handlerRetriesTotal uint64
	sendRejectedTotal   uint64
	commitFailuresTotal uint64
	commitUnknownTotal  uint64
	dlqSentTotal        uint64
	dlqSkippedTotal     uint64
	dlqFailuresTotal    uint64
	abandonedTotal      uint64
	lastRecoveryAction  string
	lastErr             error

	gateMu          sync.Mutex
	intakeStopped   bool
	nextTicketID    uint64
	activeSenders   int
	sendersIdleCh   chan struct{}
	inFlightTickets int
	drainedCh       chan struct{}
	ledger          map[uint64]*subMessageTicket
}

func newSubscriberSlot(
	app *App,
	seq uint64,
	topic string,
	policy SubscribePolicy,
	handler SubscribeFunc,
	sem pubsub.AckSemantics,
	isLegacy bool,
) *subscriberSlot {
	n := policy.Concurrency
	workerNames := make([]string, n)
	sems := make([]chan struct{}, n)
	for i := 0; i < n; i++ {
		workerNames[i] = sanitizeActorSegment(fmt.Sprintf("sub-part-%d", seq), uint64(i), topic)
		sems[i] = make(chan struct{}, policy.QueueSize)
	}
	sendersIdle := make(chan struct{})
	close(sendersIdle)
	drained := make(chan struct{})
	close(drained)

	return &subscriberSlot{
		app:              app,
		seq:              seq,
		topic:            topic,
		policy:           policy,
		handler:          handler,
		isLegacy:         isLegacy,
		backendSem:       sem,
		coordActorName:   sanitizeActorSegment("sub-topic", seq, topic),
		workerNames:      workerNames,
		workerRefs:       make([]actor.Ref, n),
		sem:              sems,
		pullRetryDelay:   defaultSubscriberPullRetry,
		workerPIDs:       make([]actor.PID, n),
		workerBoots:      make([]uint64, n),
		state:            subStatePending,
		stateNotifyCh:    make(chan struct{}),
		frozenPartitions: make(map[int]error),
		sendersIdleCh:    sendersIdle,
		drainedCh:        drained,
		ledger:           make(map[uint64]*subMessageTicket),
	}
}

// AddSubscriber registers a supervised topic consumer with explicit concurrency, backpressure,
// retry/DLQ, shutdown drain, and readiness policy.
func (a *App) AddSubscriber(topic string, policy SubscribePolicy, handler SubscribeFunc) error {
	topic = strings.TrimSpace(topic)
	if topic == "" {
		return errSubscriberTopicEmpty
	}
	if handler == nil {
		return errSubscriberHandlerNil
	}
	normPolicy, err := normalizeSubscribePolicy(policy)
	if err != nil {
		return err
	}

	var sub pubsub.Subscriber
	if a.container != nil {
		sub = a.container.GetSubscriber()
	}
	if sub == nil {
		return errSubscriberNotInitialized
	}

	sem := pubsub.ResolveAckSemantics(sub, nil)
	if err := validatePolicyAgainstBackend(normPolicy, sem); err != nil {
		return err
	}

	a.runtimeMu.Lock()
	defer a.runtimeMu.Unlock()
	if a.runtimeState != lifecycleNew {
		return errSubscriberRegisterTooLate
	}
	if a.subscriptionManager.subscriptions == nil {
		a.subscriptionManager.subscriptions = make(map[string]SubscribeFunc)
	}
	for _, existing := range a.subscriberSlotsList {
		if existing.topic == topic {
			return fmt.Errorf("%w: %s", errSubscriberDuplicateTopic, topic)
		}
	}
	if _, existsLegacy := a.subscriptionManager.subscriptions[topic]; existsLegacy {
		return fmt.Errorf("%w: %s", errSubscriberDuplicateTopic, topic)
	}

	a.nextSubscriberSeq++
	slot := newSubscriberSlot(a, a.nextSubscriberSeq, topic, normPolicy, handler, sem, false)
	a.subscriberSlotsList = append(a.subscriberSlotsList, slot)
	a.subscriptionManager.subscriptions[topic] = handler
	return nil
}

func (a *App) materializeLegacySubscriptions() {
	a.runtimeMu.Lock()
	defer a.runtimeMu.Unlock()

	if len(a.subscriptionManager.subscriptions) == 0 {
		return
	}
	registered := make(map[string]struct{}, len(a.subscriberSlotsList))
	for _, s := range a.subscriberSlotsList {
		registered[s.topic] = struct{}{}
	}

	var sub pubsub.Subscriber
	if a.container != nil {
		sub = a.container.GetSubscriber()
	}
	sem := pubsub.ResolveAckSemantics(sub, nil)

	topics := make([]string, 0, len(a.subscriptionManager.subscriptions))
	for t := range a.subscriptionManager.subscriptions {
		if _, ok := registered[t]; !ok {
			topics = append(topics, t)
		}
	}
	sort.Strings(topics)

	legacyPolicy, _ := normalizeSubscribePolicy(SubscribePolicy{
		Concurrency: 1,
		QueueSize:   defaultSubscriberQueueSize,
		Shutdown:    SubscriberCancel,
		Required:    false,
	})
	for _, topic := range topics {
		handler := a.subscriptionManager.subscriptions[topic]
		if topic == "" || handler == nil {
			continue
		}
		a.nextSubscriberSeq++
		slot := newSubscriberSlot(a, a.nextSubscriberSeq, topic, legacyPolicy, handler, sem, true)
		a.subscriberSlotsList = append(a.subscriberSlotsList, slot)
	}
}

func (a *App) subscriberSlots() []*subscriberSlot {
	a.runtimeMu.Lock()
	defer a.runtimeMu.Unlock()
	if len(a.subscriberSlotsList) == 0 {
		return nil
	}
	out := make([]*subscriberSlot, len(a.subscriberSlotsList))
	copy(out, a.subscriberSlotsList)
	return out
}

func (a *App) subscriberActorCount() int {
	a.runtimeMu.Lock()
	defer a.runtimeMu.Unlock()
	count := 0
	for _, s := range a.subscriberSlotsList {
		count += 1 + s.policy.Concurrency
	}
	for topic := range a.subscriptionManager.subscriptions {
		found := false
		for _, s := range a.subscriberSlotsList {
			if s.topic == topic {
				found = true
				break
			}
		}
		if !found {
			count += 2 // 1 coordinator + 1 worker
		}
	}
	return count
}

func (s *subscriberSlot) broadcastStateLocked() {
	close(s.stateNotifyCh)
	s.stateNotifyCh = make(chan struct{})
	if s.app != nil {
		s.app.notifyGRPCHealthChanged()
	}
}

func (s *subscriberSlot) snapshot() subscriberSnapshot {
	s.mu.Lock()
	frozen := make([]int, 0, len(s.frozenPartitions))
	for p := range s.frozenPartitions {
		frozen = append(frozen, p)
	}
	sort.Ints(frozen)
	snap := subscriberSnapshot{
		Topic:               s.topic,
		CoordActorName:      s.coordActorName,
		State:               s.state,
		AckMode:             s.backendSem.Mode,
		Concurrency:         s.policy.Concurrency,
		QueueSize:           s.policy.QueueSize,
		Required:            s.policy.Required,
		Shutdown:            s.policy.Shutdown,
		Ready:               s.ready,
		FrozenPartitions:    frozen,
		ProcessedTotal:      s.processedTotal,
		CommittedTotal:      s.committedTotal,
		FailuresTotal:       s.failuresTotal,
		PanicsTotal:         s.panicsTotal,
		HandlerRetriesTotal: s.handlerRetriesTotal,
		SendRejectedTotal:   s.sendRejectedTotal,
		CommitFailuresTotal: s.commitFailuresTotal,
		CommitUnknownTotal:  s.commitUnknownTotal,
		DLQSentTotal:        s.dlqSentTotal,
		DLQSkippedTotal:     s.dlqSkippedTotal,
		DLQFailuresTotal:    s.dlqFailuresTotal,
		AbandonedTotal:      s.abandonedTotal,
		CoordRestarts:       s.coordRestarts,
		WorkerRestarts:      s.workerRestarts,
		LastRecoveryAction:  s.lastRecoveryAction,
		LastError:           s.lastErr,
	}
	s.mu.Unlock()

	s.gateMu.Lock()
	snap.InFlightTickets = s.inFlightTickets
	s.gateMu.Unlock()
	return snap
}

func (s *subscriberSlot) recordAbandonedAction(action string) {
	s.mu.Lock()
	s.abandonedTotal++
	s.lastRecoveryAction = action
	s.mu.Unlock()
}

func (s *subscriberSlot) container() *infra.Container {
	if s.app != nil {
		return s.app.container
	}
	return nil
}

func (s *subscriberSlot) subscriberBackend() pubsub.Subscriber {
	if c := s.container(); c != nil {
		return c.GetSubscriber()
	}
	return nil
}

func (s *subscriberSlot) isHealthyForReadiness() bool {
	if !s.policy.Required {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.controllerLost || len(s.frozenPartitions) > 0 {
		return false
	}
	return s.state == subStateRunning && s.ready
}

func (s *subscriberSlot) checkStartupReadiness() (bool, error, <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch := s.stateNotifyCh
	if s.controllerLost || s.state == subStateFailed || s.state == subStateDegraded {
		err := s.lastErr
		if err == nil {
			err = fmt.Errorf("required subscriber %q failed during startup", s.topic)
		}
		return false, err, ch
	}
	if s.launchedOnce && s.ready && s.state == subStateRunning && len(s.frozenPartitions) == 0 {
		return true, nil, ch
	}
	return false, nil, ch
}

func (s *subscriberSlot) isPartitionFrozen(partition int) bool {
	s.mu.Lock()
	_, frozen := s.frozenPartitions[partition]
	s.mu.Unlock()
	return frozen
}

func (s *subscriberSlot) freezePartition(partition int, cause error) {
	if cause == nil {
		cause = errors.New("partition offset progression halted")
	}
	s.mu.Lock()
	if _, exists := s.frozenPartitions[partition]; !exists {
		s.frozenPartitions[partition] = cause
	}
	s.lastErr = cause
	s.lastRecoveryAction = "partition_frozen"
	s.broadcastStateLocked()
	s.mu.Unlock()
}

func (s *subscriberSlot) escalateOffsetFreeze(partition int, offset int64, cause error) {
	s.freezePartition(partition, cause)
	if s.app == nil || s.app.isIntakeClosed() {
		return
	}
	wrapped := fmt.Errorf("subscriber %q partition %d frozen at offset %d: %w", s.topic, partition, offset, cause)
	if s.policy.Required {
		s.mu.Lock()
		s.state = subStateFailed
		s.ready = false
		s.lastErr = wrapped
		cancel := s.pumpCancel
		s.broadcastStateLocked()
		s.mu.Unlock()
		if cancel != nil {
			cancel(wrapped)
		}
		s.app.recordFatalCause(wrapped)
		return
	}

	// Optional subscriber without broker seek capability degrades locally and stops pulling
	// rather than assuming FetchMessage would re-return the failed offset.
	s.mu.Lock()
	s.state = subStateDegraded
	s.ready = false
	s.lastErr = wrapped
	cancel := s.pumpCancel
	s.broadcastStateLocked()
	s.mu.Unlock()

	s.gateMu.Lock()
	s.intakeStopped = true
	s.gateMu.Unlock()

	if cancel != nil {
		cancel(wrapped)
	}
	if l := s.app.safeLogger(); l != nil {
		l.Errorf("Subscriber %q degraded due to frozen partition %d at offset %d: %v", s.topic, partition, offset, cause)
	}
}

func (s *subscriberSlot) escalateUnrecoverable(cause error) {
	if cause == nil {
		cause = errActorRuntimeTerminated
	}
	wrapped := fmt.Errorf("subscriber %q unrecoverable controller error: %w", s.topic, cause)
	s.mu.Lock()
	s.controllerLost = true
	s.coordPID = actor.PID{}
	for i := range s.workerPIDs {
		s.workerPIDs[i] = actor.PID{}
	}
	s.ready = false
	s.lastErr = wrapped
	if s.policy.Required {
		s.state = subStateFailed
	} else {
		s.state = subStateDegraded
	}
	cancel := s.pumpCancel
	s.broadcastStateLocked()
	s.mu.Unlock()

	s.gateMu.Lock()
	s.intakeStopped = true
	s.gateMu.Unlock()

	if cancel != nil {
		cancel(wrapped)
	}
	s.sweepUnstartedTickets("controller_lost")
	if s.app != nil && !s.app.isIntakeClosed() && s.policy.Required {
		s.app.recordFatalCause(wrapped)
	}
}

func (s *subscriberSlot) onCoordControllerLost(reason error) {
	s.escalateUnrecoverable(reason)
}

func (s *subscriberSlot) onWorkerControllerLost(workerIdx int, reason error) {
	s.escalateUnrecoverable(fmt.Errorf("partition worker %d: %w", workerIdx, reason))
}

func (s *subscriberSlot) currentCoordPID() actor.PID {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.coordPID
}

func (s *subscriberSlot) currentWorkerPID(workerIdx int) actor.PID {
	s.mu.Lock()
	defer s.mu.Unlock()
	if workerIdx < 0 || workerIdx >= len(s.workerPIDs) {
		return actor.PID{}
	}
	return s.workerPIDs[workerIdx]
}

func (s *subscriberSlot) stopIntakePhaseA() {
	s.gateMu.Lock()
	s.intakeStopped = true
	s.gateMu.Unlock()

	s.mu.Lock()
	s.ready = false
	if s.state != subStateFailed && s.state != subStateDegraded {
		s.state = subStateStopped
	}
	cancel := s.pumpCancel
	s.broadcastStateLocked()
	s.mu.Unlock()

	if cancel != nil {
		cancel(context.Canceled)
	}

	if s.policy.Shutdown == SubscriberCancel {
		s.sweepUnstartedTickets("shutdown_canceled")
	}
}

func (s *subscriberSlot) waitIntakeStopped(ctx context.Context) error {
	s.gateMu.Lock()
	sendersCh := s.sendersIdleCh
	s.gateMu.Unlock()

	s.mu.Lock()
	pumpDone := s.pumpDone
	s.mu.Unlock()

	select {
	case <-sendersCh:
	case <-ctx.Done():
		return ctx.Err()
	}
	if pumpDone != nil {
		select {
		case <-pumpDone:
		case <-ctx.Done():
			if s.app != nil {
				if l := s.app.safeLogger(); l != nil {
					l.Warnf("Subscriber %q pull pump still stopping after intake budget; resource close will wait for exit", s.topic)
				}
			}
			return ctx.Err()
		}
	}
	return nil
}

func (s *subscriberSlot) sweepUnstartedTickets(reason string) {
	s.gateMu.Lock()
	pending := make([]*subMessageTicket, 0, len(s.ledger))
	for _, t := range s.ledger {
		pending = append(pending, t)
	}
	s.gateMu.Unlock()

	for _, t := range pending {
		t.mu.Lock()
		if t.state == subTicketAdmitted {
			t.state = subTicketAbandoned
			t.recoveryAction = reason
			t.mu.Unlock()

			func(ticket *subMessageTicket) {
				defer ticket.release()
				ticketReason := reason
				if ticket.msgSem.Mode == pubsub.AckModeCumulativeOffset && ticket.meta.HasPartition {
					s.freezePartition(ticket.meta.Partition, context.Canceled)
				} else if ticket.msg != nil && ticket.msg.Committer != nil {
					if nacker, ok := ticket.msg.Committer.(pubsub.Nacker); ok && s.safeNack(nacker) {
						ticketReason = "nacked_on_shutdown"
					}
				}
				s.recordAbandonedAction(ticketReason)
			}(t)
			continue
		}
		t.mu.Unlock()
	}
}

func (s *subscriberSlot) waitDrained(ctx context.Context) error {
	if s.policy.Shutdown != SubscriberDrain {
		return nil
	}
	// First wait for any concurrent node.Send in the pull pump to finish registering/admitting.
	s.gateMu.Lock()
	sendersCh := s.sendersIdleCh
	s.gateMu.Unlock()
	select {
	case <-sendersCh:
	case <-ctx.Done():
		return ctx.Err()
	}

	s.gateMu.Lock()
	drainedCh := s.drainedCh
	inFlight := s.inFlightTickets
	s.gateMu.Unlock()
	if inFlight == 0 {
		return nil
	}

	select {
	case <-drainedCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *subscriberSlot) abandonPumpHeldMessage(msg *pubsub.Message, pullCancel context.CancelCauseFunc, reason string) {
	if pullCancel != nil {
		pullCancel(context.Canceled)
	}
	if msg == nil {
		return
	}
	msgSem := pubsub.ResolveAckSemantics(s.subscriberBackend(), msg.Committer)
	meta := pubsub.ResolveMessageMeta(msg)
	action := reason
	switch {
	case msgSem.Mode == pubsub.AckModeCumulativeOffset && meta.HasPartition:
		s.freezePartition(meta.Partition, errors.New(reason))
		action = "partition_frozen"
	case msg.Committer != nil:
		if nacker, ok := msg.Committer.(pubsub.Nacker); ok {
			if s.safeNack(nacker) {
				action = "nacked"
			} else {
				action = "nack_panicked"
			}
		} else if msgSem.Mode == pubsub.AckModeAuto {
			action = "auto_ack_unrecoverable"
		} else {
			action = "broker_redeliver"
		}
	}
	s.recordAbandonedAction(action)
}

func (s *subscriberSlot) safeNack(nacker pubsub.Nacker) (ok bool) {
	if nacker == nil {
		return false
	}
	defer func() {
		if re := recover(); re != nil {
			ok = false
			if s != nil && s.app != nil {
				if l := s.app.safeLogger(); l != nil {
					panicRecovery(re, l)
				}
				s.app.recordPubSubFailureMetric(s.topic, "nack")
			}
			if s != nil {
				s.mu.Lock()
				s.panicsTotal++
				s.lastErr = fmt.Errorf("subscriber nack panicked: %v", re)
				s.mu.Unlock()
			}
		}
	}()
	nacker.Nack()
	return true
}

func (s *subscriberSlot) safeExtractKey(msg *pubsub.Message) (key string, err error) {
	defer func() {
		if re := recover(); re != nil {
			if l := s.app.safeLogger(); l != nil {
				panicRecovery(re, l)
			}
			key = ""
			err = fmt.Errorf("%w: %v", errSubscriptionKeyExtractorPanic, re)
		}
	}()
	return s.policy.KeyExtractor(msg), nil
}

func (s *subscriberSlot) selectWorkerIndex(msg *pubsub.Message, meta pubsub.MessageMeta, msgSem pubsub.AckSemantics) (int, error) {
	n := s.policy.Concurrency
	if n <= 1 {
		if msgSem.Mode == pubsub.AckModeCumulativeOffset && (!meta.HasPartition || !meta.HasOffset) {
			return 0, errSubscriberMissingOffsetMeta
		}
		return 0, nil
	}
	if msgSem.Mode == pubsub.AckModeCumulativeOffset {
		if !meta.HasPartition || !meta.HasOffset {
			return 0, errSubscriberMissingOffsetMeta
		}
		return ((meta.Partition % n) + n) % n, nil
	}

	var keyBytes []byte
	if s.policy.KeyExtractor != nil {
		k, err := s.safeExtractKey(msg)
		if err != nil {
			return 0, err
		}
		if k != "" {
			keyBytes = []byte(k)
		}
	} else if len(meta.Key) > 0 {
		keyBytes = meta.Key
	} else if meta.HasPartition {
		return ((meta.Partition % n) + n) % n, nil
	}

	if len(keyBytes) > 0 {
		h := fnv.New32a()
		_, _ = h.Write(keyBytes)
		return int(h.Sum32() % uint32(n)), nil
	}

	idx := int((s.rrSeq.Add(1) - 1) % uint64(n))
	return idx, nil
}

func (s *subscriberSlot) handleKeyExtractorPanic(
	msg *pubsub.Message,
	pullCancel context.CancelCauseFunc,
	meta pubsub.MessageMeta,
	msgSem pubsub.AckSemantics,
	keyErr error,
) {
	s.mu.Lock()
	s.failuresTotal++
	s.panicsTotal++
	s.lastErr = keyErr
	s.mu.Unlock()
	s.app.recordPubSubFailureMetric(s.topic, "key_extractor")

	scopeCtx := s.executionScopeContext()
	msgCtx := context.Background()
	if msg != nil && msg.Context() != nil {
		msgCtx = msg.Context()
	}

	if s.policy.DeadLetterHandler != nil && scopeCtx.Err() == nil && msgCtx.Err() == nil {
		dlqHandlerCtx, cancelDLQ := context.WithCancelCause(msgCtx)
		stopOnScope := context.AfterFunc(scopeCtx, func() {
			cancelDLQ(context.Cause(scopeCtx))
		})
		dlqMsgCopy := msg.WithContext(dlqHandlerCtx)
		dlqCtx := newContext(nil, dlqMsgCopy, s.container())
		dlqCtx.Context = dlqHandlerCtx

		dlqErr := func() (dErr error) {
			defer func() {
				if re := recover(); re != nil {
					if l := s.app.safeLogger(); l != nil {
						panicRecovery(re, l)
					}
					dErr = fmt.Errorf("dead letter handler panicked: %v", re)
				}
			}()
			return s.policy.DeadLetterHandler(dlqCtx, dlqMsgCopy, keyErr)
		}()

		if dlqErr == nil {
			s.mu.Lock()
			s.dlqSentTotal++
			s.mu.Unlock()

			if s.policy.AllowDeadLetterSkip && scopeCtx.Err() == nil && dlqHandlerCtx.Err() == nil {
				pseudoTicket := &subMessageTicket{msg: msg, meta: meta, msgSem: msgSem}
				commitErr, unknown := s.commitMessage(dlqHandlerCtx, scopeCtx, pseudoTicket)
				stopOnScope()
				cancelDLQ(nil)
				if commitErr == nil {
					if pullCancel != nil {
						pullCancel(nil)
					}
					s.mu.Lock()
					s.dlqSkippedTotal++
					if msg != nil && msg.Committer != nil {
						s.committedTotal++
					}
					s.lastRecoveryAction = "dlq_skipped"
					s.mu.Unlock()
					return
				}

				s.mu.Lock()
				s.commitFailuresTotal++
				if unknown {
					s.commitUnknownTotal++
				}
				s.lastErr = commitErr
				s.mu.Unlock()
				s.app.recordPubSubFailureMetric(s.topic, "commit")

				if msgSem.Mode == pubsub.AckModeCumulativeOffset && meta.HasPartition {
					s.abandonPumpHeldMessage(msg, pullCancel, "dlq_written_source_commit_failed")
					s.escalateOffsetFreeze(meta.Partition, meta.Offset, commitErr)
					return
				}
				s.abandonPumpHeldMessage(msg, pullCancel, "dlq_written_source_commit_failed")
				return
			}

			stopOnScope()
			cancelDLQ(nil)
			if msgSem.Mode == pubsub.AckModeCumulativeOffset && meta.HasPartition {
				s.abandonPumpHeldMessage(msg, pullCancel, "dlq_written_no_skip")
				s.escalateOffsetFreeze(meta.Partition, meta.Offset, keyErr)
				return
			}
			s.abandonPumpHeldMessage(msg, pullCancel, "dlq_written_no_skip")
			return
		}

		stopOnScope()
		cancelDLQ(nil)
		s.mu.Lock()
		s.dlqFailuresTotal++
		s.lastErr = errors.Join(keyErr, dlqErr)
		s.mu.Unlock()
	}

	if msgSem.Mode == pubsub.AckModeCumulativeOffset && meta.HasPartition {
		s.abandonPumpHeldMessage(msg, pullCancel, "partition_frozen")
		s.escalateOffsetFreeze(meta.Partition, meta.Offset, keyErr)
		return
	}
	s.abandonPumpHeldMessage(msg, pullCancel, "key_extractor_panic")
}

func (s *subscriberSlot) notifyCoordinator() {
	if s.app == nil || s.app.isIntakeClosed() {
		return
	}
	s.mu.Lock()
	node := s.node
	ref := s.coordRef
	lost := s.controllerLost || s.state == subStateDegraded || s.state == subStateFailed
	s.mu.Unlock()
	if lost || node == nil || ref.Path == "" {
		return
	}
	err := node.Send(ref, subCoordReconcileMsg{})
	if err == nil || s.app.isIntakeClosed() {
		return
	}
	if errors.Is(err, actor.ErrClosed) {
		s.app.onActorTreeFailure(err)
		return
	}
	if errors.Is(err, actor.ErrUnresponsive) {
		s.onCoordControllerLost(err)
		return
	}

	s.mu.Lock()
	if s.notifyRetrying || s.controllerLost {
		s.mu.Unlock()
		return
	}
	if !s.app.tryAdmitRuntimeTask() {
		s.mu.Unlock()
		return
	}
	s.notifyRetrying = true
	s.mu.Unlock()

	go func() {
		defer s.app.runtimeTasks.Done()
		defer func() {
			s.mu.Lock()
			s.notifyRetrying = false
			s.mu.Unlock()
		}()
		intakeDone := s.app.intakeDone()
		delay := 2 * time.Millisecond
		timer := time.NewTimer(delay)
		defer timer.Stop()
		for {
			select {
			case <-intakeDone:
				return
			case <-timer.C:
			}
			s.mu.Lock()
			n := s.node
			r := s.coordRef
			done := s.controllerLost || s.state == subStateDegraded || s.state == subStateFailed
			s.mu.Unlock()
			if done || n == nil || r.Path == "" {
				return
			}
			sendErr := n.Send(r, subCoordReconcileMsg{})
			if sendErr == nil || s.app.isIntakeClosed() {
				return
			}
			if errors.Is(sendErr, actor.ErrClosed) {
				s.app.onActorTreeFailure(sendErr)
				return
			}
			if isPermanentControllerSendErr(sendErr) {
				s.onCoordControllerLost(sendErr)
				return
			}
			if delay < 20*time.Millisecond {
				delay *= 2
			}
			timer.Reset(delay)
		}
	}()
}

func (s *subscriberSlot) runPullPump(gen uint64, pullLoopCtx context.Context, pullLoopCancel context.CancelCauseFunc, pumpDone chan struct{}) {
	defer s.app.runtimeTasks.Done()
	defer close(pumpDone)
	defer pullLoopCancel(nil)

	defer func() {
		var notify bool
		s.mu.Lock()
		if s.pumpGen == gen {
			s.pumpInFlight = false
			s.pumpCancel = nil
			s.pumpDone = nil
			if !s.app.isIntakeClosed() && !s.controllerLost && s.state != subStateDegraded && s.state != subStateFailed {
				s.ready = false
				s.state = subStateStarting
				notify = true
			}
			s.broadcastStateLocked()
		}
		s.mu.Unlock()
		if notify {
			s.notifyCoordinator()
		}
	}()

	var (
		pendingMsg        *pubsub.Message
		pendingPullCancel context.CancelCauseFunc
		sendAttempts      int
	)
	defer func() {
		if pendingMsg != nil {
			s.abandonPumpHeldMessage(pendingMsg, pendingPullCancel, "pump_exited_with_pending_message")
			pendingMsg = nil
			pendingPullCancel = nil
		}
	}()
	defer func() {
		if re := recover(); re != nil {
			if l := s.app.safeLogger(); l != nil {
				panicRecovery(re, l)
			}
			panicErr := fmt.Errorf("%w: %v", errSubscriberPullPumpPanic, re)
			s.mu.Lock()
			s.failuresTotal++
			s.panicsTotal++
			s.lastErr = panicErr
			s.mu.Unlock()
			s.app.recordPubSubFailureMetric(s.topic, "pull_pump_panic")
			s.escalateUnrecoverable(panicErr)
		}
	}()

	for {
		s.gateMu.Lock()
		stopped := s.intakeStopped
		s.gateMu.Unlock()
		if stopped || pullLoopCtx.Err() != nil || s.app.isIntakeClosed() {
			return
		}

		sub := s.subscriberBackend()
		if sub == nil {
			s.escalateUnrecoverable(errSubscriberNotInitialized)
			return
		}

		var (
			msg        *pubsub.Message
			pullCancel context.CancelCauseFunc
		)

		if pendingMsg != nil {
			msg = pendingMsg
			pullCancel = pendingPullCancel
		} else {
			var (
				callPullCtx     context.Context
				stopFetchIntake func() bool
			)
			if s.policy.Shutdown == SubscriberDrain {
				callPullCtx, pullCancel = context.WithCancelCause(context.WithoutCancel(s.app.drainExecutionContext()))
				stopFetchIntake = context.AfterFunc(pullLoopCtx, func() {
					pullCancel(context.Cause(pullLoopCtx))
				})
			} else {
				callPullCtx, pullCancel = context.WithCancelCause(pullLoopCtx)
				stopFetchIntake = func() bool { return true }
			}

			rawMsg, fetchErr := sub.Subscribe(callPullCtx, s.topic)
			if fetchErr != nil {
				stopFetchIntake()
				pullCancel(fetchErr)
				if pullLoopCtx.Err() != nil || s.app.isIntakeClosed() {
					return
				}
				if l := s.app.safeLogger(); l != nil {
					l.Errorf("error while reading from topic %v, err: %v", s.topic, fetchErr)
				}
				s.mu.Lock()
				s.lastErr = fetchErr
				if len(s.frozenPartitions) == 0 && s.state != subStateDegraded && s.state != subStateFailed {
					s.state = subStateBackingOff
					s.ready = false
					s.broadcastStateLocked()
				}
				retryDelay := s.pullRetryDelay
				s.mu.Unlock()

				timer := time.NewTimer(retryDelay)
				select {
				case <-pullLoopCtx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
				continue
			}

			if !s.app.isIntakeClosed() && pullLoopCtx.Err() == nil {
				s.mu.Lock()
				if s.state == subStateBackingOff && len(s.frozenPartitions) == 0 {
					s.state = subStateRunning
					s.ready = true
					s.broadcastStateLocked()
				}
				s.mu.Unlock()
			}

			if rawMsg == nil {
				stopFetchIntake()
				pullCancel(nil)
				if pullLoopCtx.Err() != nil || s.app.isIntakeClosed() {
					return
				}
				runtime.Gosched()
				continue
			}

			if s.policy.Shutdown == SubscriberDrain {
				if !stopFetchIntake() {
					// Intake closed concurrently while Subscribe was returning; do not admit new messages.
					s.abandonPumpHeldMessage(rawMsg, pullCancel, "intake_closed_during_fetch")
					return
				}
			}

			s.gateMu.Lock()
			intakeNowStopped := s.intakeStopped
			s.gateMu.Unlock()
			if intakeNowStopped || pullLoopCtx.Err() != nil || s.app.isIntakeClosed() {
				s.abandonPumpHeldMessage(rawMsg, pullCancel, "intake_closed_after_fetch")
				return
			}

			// Own an immutable copy of Value before entering the dispatch pipeline.
			msg = rawMsg.WithContext(rawMsg.Context())
			pendingMsg = msg
			pendingPullCancel = pullCancel
		}

		msgSem := pubsub.ResolveAckSemantics(sub, msg.Committer)
		meta := pubsub.ResolveMessageMeta(msg)
		workerIdx, routeErr := s.selectWorkerIndex(msg, meta, msgSem)
		if routeErr != nil {
			if errors.Is(routeErr, errSubscriptionKeyExtractorPanic) {
				s.handleKeyExtractorPanic(msg, pullCancel, meta, msgSem, routeErr)
				pendingMsg = nil
				pendingPullCancel = nil
				continue
			}
			s.abandonPumpHeldMessage(msg, pullCancel, "invalid_offset_metadata")
			pendingMsg = nil
			pendingPullCancel = nil
			s.escalateUnrecoverable(routeErr)
			return
		}

		if msgSem.Mode == pubsub.AckModeCumulativeOffset && s.isPartitionFrozen(meta.Partition) {
			s.abandonPumpHeldMessage(msg, pullCancel, "partition_frozen")
			pendingMsg = nil
			pendingPullCancel = nil
			return
		}

		// Bounded per-worker backpressure: if workerIdx's queue is full, pause topic pulling
		// until a slot frees up or intake stops.
		select {
		case s.sem[workerIdx] <- struct{}{}:
		case <-pullLoopCtx.Done():
			s.abandonPumpHeldMessage(msg, pullCancel, "intake_closed_waiting_slot")
			pendingMsg = nil
			pendingPullCancel = nil
			return
		}

		s.gateMu.Lock()
		if s.intakeStopped || s.app.isIntakeClosed() || !s.app.tryAdmitRuntimeTask() {
			s.gateMu.Unlock()
			<-s.sem[workerIdx]
			s.abandonPumpHeldMessage(msg, pullCancel, "intake_closed_before_send")
			pendingMsg = nil
			pendingPullCancel = nil
			return
		}
		if s.activeSenders == 0 {
			s.sendersIdleCh = make(chan struct{})
		}
		s.activeSenders++
		s.nextTicketID++
		ticket := &subMessageTicket{
			id:         s.nextTicketID,
			slot:       s,
			workerIdx:  workerIdx,
			msg:        msg,
			meta:       meta,
			msgSem:     msgSem,
			pullCancel: pullCancel,
			state:      subTicketReserved,
		}
		s.ledger[ticket.id] = ticket
		if s.inFlightTickets == 0 {
			s.drainedCh = make(chan struct{})
		}
		s.inFlightTickets++
		s.gateMu.Unlock()
		pendingMsg = nil
		pendingPullCancel = nil

		s.mu.Lock()
		node := s.node
		workerRef := s.workerRefs[workerIdx]
		s.mu.Unlock()

		var sendErr error
		if node == nil || workerRef.Path == "" {
			sendErr = actor.ErrNoProcess
		} else {
			sendErr = node.Send(workerRef, subDeliverMsg{ticketID: ticket.id})
		}

		s.gateMu.Lock()
		s.activeSenders--
		if s.activeSenders == 0 {
			select {
			case <-s.sendersIdleCh:
			default:
				close(s.sendersIdleCh)
			}
		}
		s.gateMu.Unlock()

		if sendErr == nil {
			ticket.markAdmitted()
			if (s.policy.Shutdown == SubscriberCancel && s.app.isIntakeClosed()) || s.executionScopeContext().Err() != nil {
				s.sweepUnstartedTickets("shutdown_canceled")
			}
			pendingMsg = nil
			pendingPullCancel = nil
			sendAttempts = 0
			continue
		}

		// Send rejected: release slot once, do NOT commit, retain message in coordinator for retry or freeze/escalate.
		s.mu.Lock()
		s.sendRejectedTotal++
		s.lastErr = sendErr
		s.mu.Unlock()
		s.app.recordPubSubFailureMetric(s.topic, "send_rejected")

		ticket.failSendAndRetain()
		pendingMsg = msg
		pendingPullCancel = pullCancel
		sendAttempts++

		if pullLoopCtx.Err() != nil || s.app.isIntakeClosed() {
			s.abandonPumpHeldMessage(pendingMsg, pendingPullCancel, "send_rejected_on_shutdown")
			pendingMsg = nil
			pendingPullCancel = nil
			return
		}
		if errors.Is(sendErr, actor.ErrClosed) || errors.Is(sendErr, actor.ErrUnresponsive) || sendAttempts > maxSubscriberSendRetries {
			if msgSem.Mode == pubsub.AckModeCumulativeOffset && meta.HasPartition {
				s.abandonPumpHeldMessage(pendingMsg, pendingPullCancel, "partition_frozen")
				pendingMsg = nil
				pendingPullCancel = nil
				s.escalateOffsetFreeze(meta.Partition, meta.Offset, sendErr)
				return
			}
			s.abandonPumpHeldMessage(pendingMsg, pendingPullCancel, "send_retries_exhausted")
			pendingMsg = nil
			pendingPullCancel = nil
			if errors.Is(sendErr, actor.ErrClosed) || errors.Is(sendErr, actor.ErrUnresponsive) {
				s.escalateUnrecoverable(sendErr)
				return
			}
			continue
		}

		backoff := min(50*time.Millisecond, time.Duration(sendAttempts)*5*time.Millisecond)
		timer := time.NewTimer(backoff)
		select {
		case <-pullLoopCtx.Done():
			timer.Stop()
			s.abandonPumpHeldMessage(pendingMsg, pendingPullCancel, "intake_closed_during_send_retry")
			pendingMsg = nil
			pendingPullCancel = nil
			return
		case <-timer.C:
		}
	}
}

func (s *subscriberSlot) executionScopeContext() context.Context {
	if s.policy.Shutdown == SubscriberDrain {
		return s.app.drainExecutionContext()
	}
	return s.app.intakeContext()
}

func (s *subscriberSlot) commitMessage(handlerCtx, scopeCtx context.Context, ticket *subMessageTicket) (error, bool) {
	if ticket.msg == nil || ticket.msg.Committer == nil {
		return nil, false
	}
	if err := scopeCtx.Err(); err != nil {
		return err, false
	}
	if err := handlerCtx.Err(); err != nil {
		return err, false
	}
	if cc, ok := ticket.msg.Committer.(pubsub.ContextCommitter); ok {
		var (
			commitErr   error
			commitPanic any
		)
		func() {
			defer func() {
				if re := recover(); re != nil {
					commitPanic = re
					if s != nil && s.app != nil {
						if l := s.app.safeLogger(); l != nil {
							panicRecovery(re, l)
						}
					}
				}
			}()
			commitErr = cc.CommitContext(handlerCtx)
		}()
		if commitPanic != nil {
			if s != nil {
				s.mu.Lock()
				s.panicsTotal++
				s.mu.Unlock()
			}
			return fmt.Errorf("subscriber commit panicked: %v", commitPanic), true
		}
		if commitErr != nil {
			unknown := scopeCtx.Err() != nil || handlerCtx.Err() != nil
			return commitErr, unknown
		}
		return nil, false
	}
	var commitPanic any
	func() {
		defer func() {
			if re := recover(); re != nil {
				commitPanic = re
				if s != nil && s.app != nil {
					if l := s.app.safeLogger(); l != nil {
						panicRecovery(re, l)
					}
				}
			}
		}()
		ticket.msg.Commit()
	}()
	if commitPanic != nil {
		if s != nil {
			s.mu.Lock()
			s.panicsTotal++
			s.mu.Unlock()
		}
		return fmt.Errorf("subscriber commit panicked: %v", commitPanic), true
	}
	return nil, false
}

func (s *subscriberSlot) executeTicketByID(ticketID uint64) {
	s.gateMu.Lock()
	ticket := s.ledger[ticketID]
	s.gateMu.Unlock()
	if ticket == nil {
		return
	}
	s.executeTicket(ticket)
}

func (s *subscriberSlot) executeTicket(ticket *subMessageTicket) {
	scopeCtx := s.executionScopeContext()

	ticket.mu.Lock()
	if ticket.state != subTicketReserved && ticket.state != subTicketAdmitted {
		ticket.mu.Unlock()
		return
	}
	// Ensure slot & runtimeTasks release once on every exit path, even if a committer panics.
	defer ticket.release()

	if err := scopeCtx.Err(); err != nil {
		ticket.state = subTicketAbandoned
		ticket.recoveryAction = "scope_canceled_before_run"
		ticket.mu.Unlock()
		if ticket.msgSem.Mode == pubsub.AckModeCumulativeOffset && ticket.meta.HasPartition {
			s.freezePartition(ticket.meta.Partition, err)
		} else if ticket.msg != nil && ticket.msg.Committer != nil {
			if nacker, ok := ticket.msg.Committer.(pubsub.Nacker); ok {
				s.safeNack(nacker)
			}
		}
		s.recordAbandonedAction("scope_canceled_before_run")
		return
	}
	if ticket.msgSem.Mode == pubsub.AckModeCumulativeOffset && s.isPartitionFrozen(ticket.meta.Partition) {
		ticket.state = subTicketAbandoned
		ticket.recoveryAction = "partition_frozen"
		ticket.mu.Unlock()
		s.recordAbandonedAction("partition_frozen")
		return
	}
	if err := ticket.msg.Context().Err(); err != nil {
		ticket.state = subTicketAbandoned
		ticket.recoveryAction = "message_canceled_before_run"
		ticket.mu.Unlock()
		if ticket.msgSem.Mode == pubsub.AckModeCumulativeOffset && ticket.meta.HasPartition {
			s.escalateOffsetFreeze(ticket.meta.Partition, ticket.meta.Offset, err)
		}
		s.recordAbandonedAction("message_canceled_before_run")
		return
	}
	ticket.state = subTicketRunning
	ticket.mu.Unlock()

	handlerCtx, cancelHandler := context.WithCancelCause(ticket.msg.Context())
	stopOnScope := context.AfterFunc(scopeCtx, func() {
		cancelHandler(context.Cause(scopeCtx))
	})
	defer stopOnScope()
	defer cancelHandler(nil)

	if err := scopeCtx.Err(); err != nil {
		if ticket.msgSem.Mode == pubsub.AckModeCumulativeOffset && ticket.meta.HasPartition {
			s.freezePartition(ticket.meta.Partition, err)
		}
		ticket.finishAbandoned("scope_canceled_before_handler")
		return
	}
	if err := handlerCtx.Err(); err != nil {
		if ticket.msgSem.Mode == pubsub.AckModeCumulativeOffset && ticket.meta.HasPartition {
			s.escalateOffsetFreeze(ticket.meta.Partition, ticket.meta.Offset, err)
		}
		ticket.finishAbandoned("message_canceled_before_handler")
		return
	}

	var (
		lastErr  error
		panicked bool
	)

	for attempt := 0; attempt <= s.policy.MaxHandlerRetries; attempt++ {
		if attempt > 0 {
			s.mu.Lock()
			s.handlerRetriesTotal++
			s.mu.Unlock()

			timer := time.NewTimer(s.policy.RetryBackoff)
			select {
			case <-scopeCtx.Done():
				timer.Stop()
				lastErr = scopeCtx.Err()
			case <-handlerCtx.Done():
				timer.Stop()
				lastErr = handlerCtx.Err()
			case <-timer.C:
			}
			if scopeCtx.Err() != nil || handlerCtx.Err() != nil {
				break
			}
		}

		panicked = false
		msgAttemptCopy := ticket.msg.WithContext(handlerCtx)
		msgCtx := newContext(nil, msgAttemptCopy, s.container())
		msgCtx.Context = handlerCtx

		lastErr = func(c *Context) (hErr error) {
			defer func() {
				if re := recover(); re != nil {
					panicked = true
					if l := s.app.safeLogger(); l != nil {
						panicRecovery(re, l)
					}
					hErr = fmt.Errorf("%w: %v", errSubscriptionHandlerPanic, re)
				}
			}()
			return s.handler(c)
		}(msgCtx)

		if lastErr == nil {
			break
		}
		if l := s.app.safeLogger(); l != nil && !(s.app.isIntakeClosed() && errors.Is(lastErr, context.Canceled)) {
			l.Errorf("error in handler for topic %s (attempt %d/%d): %v", s.topic, attempt+1, s.policy.MaxHandlerRetries+1, lastErr)
		}
		if scopeCtx.Err() != nil || handlerCtx.Err() != nil {
			break
		}
	}

	if lastErr == nil {
		// Never initiate Commit if execution cancellation has been observed!
		if scopeErr := scopeCtx.Err(); scopeErr != nil {
			if ticket.msgSem.Mode == pubsub.AckModeCumulativeOffset && ticket.meta.HasPartition {
				s.freezePartition(ticket.meta.Partition, scopeErr)
			} else if ticket.msg.Committer != nil {
				if nacker, ok := ticket.msg.Committer.(pubsub.Nacker); ok {
					s.safeNack(nacker)
				}
			}
			ticket.finishAbandoned("scope_canceled_before_commit")
			return
		}
		if hErr := handlerCtx.Err(); hErr != nil {
			if ticket.msgSem.Mode == pubsub.AckModeCumulativeOffset && ticket.meta.HasPartition {
				s.escalateOffsetFreeze(ticket.meta.Partition, ticket.meta.Offset, hErr)
			} else if ticket.msg.Committer != nil {
				if nacker, ok := ticket.msg.Committer.(pubsub.Nacker); ok {
					s.safeNack(nacker)
				}
			}
			ticket.finishAbandoned("message_canceled_before_commit")
			return
		}

		commitErr, unknown := s.commitMessage(handlerCtx, scopeCtx, ticket)
		if commitErr != nil {
			s.mu.Lock()
			s.commitFailuresTotal++
			if unknown {
				s.commitUnknownTotal++
			}
			s.lastErr = commitErr
			s.mu.Unlock()
			s.app.recordPubSubFailureMetric(s.topic, "commit")

			if ticket.msgSem.Mode == pubsub.AckModeCumulativeOffset && ticket.meta.HasPartition {
				s.escalateOffsetFreeze(ticket.meta.Partition, ticket.meta.Offset, commitErr)
			}
			if unknown {
				ticket.finishAbandoned("commit_outcome_unknown")
			} else {
				ticket.finishAbandoned("commit_failed")
			}
			return
		}

		s.mu.Lock()
		s.processedTotal++
		if ticket.msg.Committer != nil {
			s.committedTotal++
		}
		s.mu.Unlock()
		ticket.finishResolved("committed")
		return
	}

	// Handler failed after exhausting retries.
	s.mu.Lock()
	s.failuresTotal++
	if panicked {
		s.panicsTotal++
	}
	s.lastErr = lastErr
	s.mu.Unlock()
	s.app.recordPubSubFailureMetric(s.topic, "handler")

	if s.policy.DeadLetterHandler != nil && scopeCtx.Err() == nil && handlerCtx.Err() == nil {
		dlqMsgCopy := ticket.msg.WithContext(handlerCtx)
		dlqCtx := newContext(nil, dlqMsgCopy, s.container())
		dlqCtx.Context = handlerCtx

		dlqErr := func() (dErr error) {
			defer func() {
				if re := recover(); re != nil {
					if l := s.app.safeLogger(); l != nil {
						panicRecovery(re, l)
					}
					dErr = fmt.Errorf("dead letter handler panicked: %v", re)
				}
			}()
			return s.policy.DeadLetterHandler(dlqCtx, dlqMsgCopy, lastErr)
		}()

		if dlqErr == nil {
			s.mu.Lock()
			s.dlqSentTotal++
			s.mu.Unlock()

			if s.policy.AllowDeadLetterSkip && scopeCtx.Err() == nil && handlerCtx.Err() == nil {
				commitErr, unknown := s.commitMessage(handlerCtx, scopeCtx, ticket)
				if commitErr == nil {
					s.mu.Lock()
					s.dlqSkippedTotal++
					if ticket.msg.Committer != nil {
						s.committedTotal++
					}
					s.mu.Unlock()
					ticket.finishResolved("dlq_skipped")
					return
				}

				s.mu.Lock()
				s.commitFailuresTotal++
				if unknown {
					s.commitUnknownTotal++
				}
				s.lastErr = commitErr
				s.mu.Unlock()
				s.app.recordPubSubFailureMetric(s.topic, "commit")

				if ticket.msgSem.Mode == pubsub.AckModeCumulativeOffset && ticket.meta.HasPartition {
					s.escalateOffsetFreeze(ticket.meta.Partition, ticket.meta.Offset, commitErr)
				}
				ticket.finishAbandoned("dlq_written_source_commit_failed")
				return
			}

			// DLQ succeeded, bit AllowDeadLetterSkip is false (or canceled before source commit).
			if ticket.msgSem.Mode == pubsub.AckModeCumulativeOffset && ticket.meta.HasPartition {
				s.escalateOffsetFreeze(ticket.meta.Partition, ticket.meta.Offset, lastErr)
			} else if ticket.msg.Committer != nil {
				if nacker, ok := ticket.msg.Committer.(pubsub.Nacker); ok {
					s.safeNack(nacker)
				}
			}
			ticket.finishAbandoned("dlq_written_no_skip")
			return
		}

		s.mu.Lock()
		s.dlqFailuresTotal++
		s.lastErr = errors.Join(lastErr, dlqErr)
		s.mu.Unlock()
	}

	if ticket.msgSem.Mode == pubsub.AckModeCumulativeOffset && ticket.meta.HasPartition {
		s.escalateOffsetFreeze(ticket.meta.Partition, ticket.meta.Offset, lastErr)
	} else if ticket.msg != nil && ticket.msg.Committer != nil {
		if nacker, ok := ticket.msg.Committer.(pubsub.Nacker); ok {
			if s.safeNack(nacker) {
				ticket.finishAbandoned("nacked")
				return
			}
			ticket.finishAbandoned("nack_panicked")
			return
		}
	}
	ticket.finishAbandoned("handler_failed")
}

type subTopicCoordinatorActor struct {
	actor.Actor
	app         *App
	slot        *subscriberSlot
	stopTracked bool
	ownedGen    uint64
	bootSeq     uint64
}

func (c *subTopicCoordinatorActor) Receive(msg any) error {
	s := c.slot
	switch m := msg.(type) {
	case actor.Started:
		if !c.stopTracked {
			c.stopTracked = true
			c.app.controllerStopTasks.Add(1)
		}
		pid := c.PID()
		s.mu.Lock()
		s.coordPID = pid
		s.coordBoots++
		c.bootSeq = s.coordBoots
		node := s.node
		restarted := s.coordBoots > 1
		if restarted {
			s.coordRestarts = s.coordBoots - 1
		}
		s.mu.Unlock()

		if restarted {
			c.app.recordActorRestartMetric(s.topic, "subscriber_coord")
			c.Log().Warn("subscriber coordinator actor restarted", "topic", s.topic, "bootSeq", c.bootSeq)
		}

		c.app.sendRootMonitorWithRetry(
			node,
			subCoordMonitorMsg{slot: s, pid: pid},
			func() bool {
				s.mu.Lock()
				defer s.mu.Unlock()
				return s.controllerLost || s.coordPID != pid
			},
			s.onCoordControllerLost,
		)
		return c.reconcile()

	case subCoordReconcileMsg:
		return c.reconcile()

	case actorInjectPanicMsg:
		panic(m.reason)

	case actor.Stopped:
		if c.stopTracked {
			c.stopTracked = false
			defer c.app.controllerStopTasks.Done()
		}
		s.mu.Lock()
		cancel := s.pumpCancel
		done := s.pumpDone
		s.mu.Unlock()

		if cancel != nil {
			cancel(context.Canceled)
		}
		if done != nil {
			<-done
		}
		return nil
	}
	return nil
}

func (c *subTopicCoordinatorActor) reconcile() error {
	s := c.slot
	if c.app.isIntakeClosed() {
		s.stopIntakePhaseA()
		return nil
	}

	s.mu.Lock()
	if s.controllerLost || s.state == subStateDegraded || s.state == subStateFailed {
		s.mu.Unlock()
		return nil
	}

	if s.pumpInFlight {
		if c.ownedGen != s.pumpGen && c.bootSeq > 1 {
			cancel := s.pumpCancel
			done := s.pumpDone
			startWaiter := false
			if !s.waitOldPumpActive && done != nil && c.app.tryAdmitRuntimeTask() {
				s.waitOldPumpActive = true
				startWaiter = true
			}
			s.mu.Unlock()
			if cancel != nil {
				cancel(errControllerPreempted)
			}
			if startWaiter {
				go func() {
					defer c.app.runtimeTasks.Done()
					select {
					case <-done:
					case <-c.app.intakeDone():
					}
					s.mu.Lock()
					s.waitOldPumpActive = false
					s.mu.Unlock()
					s.notifyCoordinator()
				}()
			}
			return nil
		}
		s.mu.Unlock()
		return nil
	}

	if !c.app.tryAdmitRuntimeTask() {
		s.state = subStateStopped
		s.ready = false
		s.broadcastStateLocked()
		s.mu.Unlock()
		return nil
	}

	s.pumpInFlight = true
	s.pumpGen++
	gen := s.pumpGen
	c.ownedGen = gen
	s.launchedOnce = true
	s.state = subStateRunning
	s.ready = true
	pullCtx, pullCancel := context.WithCancelCause(c.app.intakeContext())
	pumpDone := make(chan struct{})
	s.pumpCancel = pullCancel
	s.pumpDone = pumpDone
	s.broadcastStateLocked()
	s.mu.Unlock()

	go s.runPullPump(gen, pullCtx, pullCancel, pumpDone)
	return nil
}

type subPartitionWorkerActor struct {
	actor.Actor
	app         *App
	slot        *subscriberSlot
	workerIdx   int
	stopTracked bool
}

func (w *subPartitionWorkerActor) Receive(msg any) error {
	s := w.slot
	switch m := msg.(type) {
	case actor.Started:
		if !w.stopTracked {
			w.stopTracked = true
			w.app.controllerStopTasks.Add(1)
		}
		pid := w.PID()
		var restarted bool
		s.mu.Lock()
		s.workerPIDs[w.workerIdx] = pid
		s.workerBoots[w.workerIdx]++
		node := s.node
		if s.workerBoots[w.workerIdx] > 1 {
			s.workerRestarts++
			restarted = true
		}
		s.mu.Unlock()

		if restarted {
			w.app.recordActorRestartMetric(s.topic, "subscriber_worker")
			w.Log().Warn("subscriber worker actor restarted", "topic", s.topic, "workerIdx", w.workerIdx)
		}

		idx := w.workerIdx
		w.app.sendRootMonitorWithRetry(
			node,
			subWorkerMonitorMsg{
				slot:      s,
				workerIdx: idx,
				pid:       pid,
			},
			func() bool {
				s.mu.Lock()
				defer s.mu.Unlock()
				return s.controllerLost || s.workerPIDs[idx] != pid
			},
			func(err error) {
				s.onWorkerControllerLost(idx, err)
			},
		)

		if restarted {
			_ = w.Send(pid, subWorkerReconcileLedgerMsg{})
		}
		return nil

	case subDeliverMsg:
		s.executeTicketByID(m.ticketID)
		return nil

	case subWorkerReconcileLedgerMsg:
		w.reconcilePendingLedgerTickets()
		return nil

	case subWorkerCheckMsg:
		return nil

	case actorInjectPanicMsg:
		panic(m.reason)

	case actor.Stopped:
		if w.stopTracked {
			w.stopTracked = false
			defer w.app.controllerStopTasks.Done()
		}
		return nil
	}
	return nil
}

func (w *subPartitionWorkerActor) reconcilePendingLedgerTickets() {
	s := w.slot
	s.gateMu.Lock()
	ids := make([]uint64, 0, len(s.ledger))
	for id, t := range s.ledger {
		if t.workerIdx != w.workerIdx {
			continue
		}
		t.mu.Lock()
		admitted := t.state == subTicketAdmitted
		t.mu.Unlock()
		if admitted {
			ids = append(ids, id)
		}
	}
	s.gateMu.Unlock()
	if len(ids) == 0 {
		return
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		if w.app.isIntakeClosed() && s.policy.Shutdown == SubscriberCancel {
			return
		}
		s.executeTicketByID(id)
	}
}

func (a *App) buildSubscriberChildSpecs(node *actor.Node) []actor.ChildSpec {
	a.materializeLegacySubscriptions()
	slots := a.subscriberSlots()
	if len(slots) == 0 {
		return nil
	}

	var specs []actor.ChildSpec
	for _, slot := range slots {
		s := slot
		s.mu.Lock()
		s.node = node
		s.coordRef = actor.Ref{Path: "pi-root/subs-sup/" + s.coordActorName}
		for i, wName := range s.workerNames {
			s.workerRefs[i] = actor.Ref{Path: "pi-root/subs-sup/" + wName}
		}
		s.mu.Unlock()

		// Spawn partition workers BEFORE topic coordinator so workers are ready before pump sends,
		// and stop in reverse order (coordinator stops first, then workers).
		for i, wName := range s.workerNames {
			workerIdx := i
			specs = append(specs, actor.ChildSpec{
				Name: wName,
				Factory: func() actor.Behavior {
					return &subPartitionWorkerActor{
						app:       a,
						slot:      s,
						workerIdx: workerIdx,
					}
				},
				Options: []actor.SpawnOption{
					actor.WithRestart(actor.Permanent),
					actor.WithRestartMailbox(actor.PreserveRef),
					actor.WithMailboxSize(max(s.policy.QueueSize*2, 64)),
				},
			})
		}

		specs = append(specs, actor.ChildSpec{
			Name: s.coordActorName,
			Factory: func() actor.Behavior {
				return &subTopicCoordinatorActor{
					app:  a,
					slot: s,
				}
			},
			Options: []actor.SpawnOption{
				actor.WithRestart(actor.Permanent),
			},
		})
	}
	return specs
}

func (a *App) isSubscribersSupervised() bool {
	a.runtimeMu.Lock()
	rt := a.actorRT
	a.runtimeMu.Unlock()
	return rt != nil && rt.subsSupPID != (actor.PID{})
}

func (a *App) cancelSubscribersIntake() {
	for _, s := range a.subscriberSlots() {
		s.stopIntakePhaseA()
	}
}

func (a *App) hasDrainSubscribers() bool {
	for _, s := range a.subscriberSlots() {
		if s.policy.Shutdown == SubscriberDrain {
			return true
		}
	}
	return false
}

func (a *App) waitSubscribersIntakeStopped(ctx context.Context) error {
	var err error
	for _, s := range a.subscriberSlots() {
		if waitErr := s.waitIntakeStopped(ctx); waitErr != nil {
			err = joinShutdownErr(err, waitErr)
		}
	}
	return err
}

func (a *App) waitSubscribersDrained(ctx context.Context) error {
	var err error
	for _, s := range a.subscriberSlots() {
		if waitErr := s.waitDrained(ctx); waitErr != nil {
			err = joinShutdownErr(err, waitErr)
		}
	}
	return err
}

func (a *App) sweepSubscribersUnstarted(reason string) {
	for _, s := range a.subscriberSlots() {
		s.sweepUnstartedTickets(reason)
	}
}
