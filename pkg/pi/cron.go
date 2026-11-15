package pi

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sllt/tao/actor"
	"go.opentelemetry.io/otel"

	"github.com/sllt/pi/pkg/pi/version"
)

const (
	seconds                 = 59
	minutes                 = 59
	hrs                     = 23
	days                    = 31
	months                  = 12
	dayOfWeek               = 6
	scheduleParts           = 5
	schedulePartsWithSecond = 6
	maxCronCatchupSeconds   = 5
)

var (
	errBadScheduleFormat = errors.New("schedule string must have five components like * * * * *")

	errCronJobNameEmpty       = errors.New("cron job name cannot be empty")
	errCronJobHandlerNil      = errors.New("cron job handler cannot be nil")
	errCronJobDuplicateName   = errors.New("cron job name already registered")
	errCronRegisterTooLate    = errors.New("cannot register cron job after app has started")
	errCronInvalidOverlap     = errors.New("invalid cron overlap policy")
	errCronInvalidConcurrency = errors.New("cron MaxConcurrency must be >= 0 (and <= 1 unless CronAllowConcurrent is set)")
	errCronNegativeTimeout    = errors.New("cron Timeout must not be negative")
	errCronMultipleOptions    = errors.New("at most one CronJobOptions value may be provided")
)

// CronFunc is the legacy cron callback signature used by AddCronJob and Crontab.AddJob.
type CronFunc func(ctx *Context)

// CronOverlapPolicy controls how a scheduled job behaves when a new tick fires
// while a previous execution is still running.
type CronOverlapPolicy uint8

const (
	// CronSkipIfRunning skips the new trigger and increments the skipped counter
	// if an execution of this job is still in flight.
	CronSkipIfRunning CronOverlapPolicy = iota
	// CronQueueNext coalesces at most one pending trigger while an execution is
	// in flight; pending triggers are dropped and counted if shutdown begins first.
	CronQueueNext
	// CronAllowConcurrent allows up to MaxConcurrency concurrent executions of the job.
	CronAllowConcurrent
)

func (p CronOverlapPolicy) String() string {
	switch p {
	case CronSkipIfRunning:
		return "skip_if_running"
	case CronQueueNext:
		return "queue_next"
	case CronAllowConcurrent:
		return "allow_concurrent"
	default:
		return "unknown"
	}
}

func (p CronOverlapPolicy) MarshalText() ([]byte, error) {
	return []byte(p.String()), nil
}

func (p *CronOverlapPolicy) UnmarshalText(text []byte) error {
	switch strings.ToLower(strings.TrimSpace(string(text))) {
	case "", "0", "skip_if_running":
		*p = CronSkipIfRunning
		return nil
	case "1", "queue_next":
		*p = CronQueueNext
		return nil
	case "2", "allow_concurrent":
		*p = CronAllowConcurrent
		return nil
	default:
		return errCronInvalidOverlap
	}
}

// CronJobOptions configures overlap handling, concurrency limits, per-run timeout,
// and schedule timezone for App.AddCron.
type CronJobOptions struct {
	Overlap        CronOverlapPolicy
	MaxConcurrency int            // Effective when Overlap == CronAllowConcurrent; 0 defaults to 1.
	Timeout        time.Duration  // 0 disables per-run deadline (still canceled on App shutdown).
	Location       *time.Location // Nil defaults to time.Local.
}

func normalizeCronJobOptions(opts ...CronJobOptions) (CronJobOptions, error) {
	if len(opts) > 1 {
		return CronJobOptions{}, errCronMultipleOptions
	}
	var o CronJobOptions
	if len(opts) == 1 {
		o = opts[0]
	}
	if o.Overlap > CronAllowConcurrent {
		return CronJobOptions{}, errCronInvalidOverlap
	}
	if o.MaxConcurrency < 0 {
		return CronJobOptions{}, errCronInvalidConcurrency
	}
	if o.Overlap == CronAllowConcurrent {
		if o.MaxConcurrency == 0 {
			o.MaxConcurrency = 1
		}
	} else {
		if o.MaxConcurrency > 1 {
			return CronJobOptions{}, errCronInvalidConcurrency
		}
		o.MaxConcurrency = 1
	}
	if o.Timeout < 0 {
		return CronJobOptions{}, errCronNegativeTimeout
	}
	if o.Location == nil {
		o.Location = time.Local
	}
	return o, nil
}

// Crontab stores registered cron job specifications.
// Construction performs no background work; supervised scheduling starts in App.Start.
type Crontab struct {
	ticker         *time.Ticker
	jobs           []*job
	rt             *runtimeKernel
	app            *App
	done           chan struct{}
	nextSeq        uint64
	lastTickUnix   atomic.Int64
	schedulerBoots atomic.Uint64

	mu sync.RWMutex
}

type job struct {
	sec       map[int]struct{}
	min       map[int]struct{}
	hour      map[int]struct{}
	day       map[int]struct{}
	month     map[int]struct{}
	dayOfWeek map[int]struct{}

	name string
	fn   CronFunc
	slot *cronJobSlot
}

type tick struct {
	sec       int
	min       int
	hour      int
	day       int
	month     int
	dayOfWeek int
}

// newCron initializes and returns an inert Crontab without starting background goroutines.
func newCron(rt *runtimeKernel) *Crontab {
	return &Crontab{
		rt:   rt,
		jobs: make([]*job, 0),
		done: make(chan struct{}),
	}
}

func newAppCron(app *App) *Crontab {
	var rt *runtimeKernel
	if app != nil {
		rt = app.kernel
	}
	c := newCron(rt)
	c.app = app
	return c
}

// Cron returns the App's Crontab instance, initializing it lazily without starting any goroutine.
func (a *App) Cron() *Crontab {
	a.runtimeMu.Lock()
	defer a.runtimeMu.Unlock()
	if a.cron == nil {
		a.cron = newAppCron(a)
	} else if a.cron.app == nil {
		a.cron.app = a
	}
	return a.cron
}

// AddCron registers a supervised cron job to run under cron-sup after App.Start.
func (a *App) AddCron(schedule, name string, fn func(*Context) error, opts ...CronJobOptions) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return errCronJobNameEmpty
	}
	if fn == nil {
		return errCronJobHandlerNil
	}
	normOpts, err := normalizeCronJobOptions(opts...)
	if err != nil {
		return err
	}
	parsed, err := parseSchedule(schedule)
	if err != nil {
		return err
	}

	a.runtimeMu.Lock()
	if a.runtimeState != lifecycleNew {
		a.runtimeMu.Unlock()
		return errCronRegisterTooLate
	}
	if a.cron == nil {
		a.cron = newAppCron(a)
	} else if a.cron.app == nil {
		a.cron.app = a
	}
	cronTab := a.cron
	a.runtimeMu.Unlock()

	cronTab.mu.Lock()
	defer cronTab.mu.Unlock()
	for _, existing := range cronTab.jobs {
		if existing.name == name {
			return fmt.Errorf("%w: %s", errCronJobDuplicateName, name)
		}
	}

	cronTab.nextSeq++
	seq := cronTab.nextSeq
	parsed.name = name
	parsed.fn = func(ctx *Context) {
		_ = fn(ctx)
	}
	parsed.slot = newCronJobSlot(a, cronTab, seq, schedule, name, parsed, fn, normOpts)
	cronTab.jobs = append(cronTab.jobs, parsed)
	return nil
}

// AddJob validates and registers a legacy CronFunc on the Crontab without starting goroutines.
func (c *Crontab) AddJob(schedule, jobName string, fn CronFunc) error {
	j, err := parseSchedule(schedule)
	if err != nil {
		return err
	}

	if c.app != nil {
		c.app.runtimeMu.Lock()
		started := c.app.runtimeState != lifecycleNew
		c.app.runtimeMu.Unlock()
		if started {
			return errCronRegisterTooLate
		}
	}

	j.name = jobName
	j.fn = fn

	normOpts, _ := normalizeCronJobOptions()
	var errFn func(*Context) error
	if fn != nil {
		errFn = func(ctx *Context) error {
			fn(ctx)
			return nil
		}
	}

	c.mu.Lock()
	c.nextSeq++
	seq := c.nextSeq
	j.slot = newCronJobSlot(c.app, c, seq, schedule, jobName, j, errFn, normOpts)
	c.jobs = append(c.jobs, j)
	c.mu.Unlock()

	return nil
}

// runScheduled executes matching jobs once for direct unit-test verification.
func (c *Crontab) runScheduled(t time.Time) {
	c.mu.RLock()
	jb := make([]*job, len(c.jobs))
	copy(jb, c.jobs)
	rt := c.rt
	if c.app != nil && c.app.kernel != nil {
		rt = c.app.kernel
	}
	c.mu.RUnlock()

	for _, j := range jb {
		loc := time.Local
		if j.slot != nil && j.slot.opts.Location != nil {
			loc = j.slot.opts.Location
		}
		if j.tick(getTick(t.In(loc))) {
			go j.run(rt)
		}
	}
}

func (j *job) run(rt *runtimeKernel) {
	tp := otel.GetTracerProvider()
	if rt != nil {
		tp = rt.getTracerProvider()
	}
	ctx, span := tp.Tracer("pi-"+version.Framework).
		Start(context.Background(), j.name)
	defer span.End()

	c := newContext(nil, noopRequest{ctx: ctx}, rt)
	if rt != nil && rt.logger != nil {
		c.Infof("Starting cron job: %s", j.name)
	}

	start := time.Now()
	defer func() {
		if r := recover(); r != nil && rt != nil && rt.logger != nil {
			c.Errorf("Panic in cron job %s: %v", j.name, r)
		}
		if rt != nil && rt.logger != nil {
			c.Infof("Finished cron job: %s in %s", j.name, time.Since(start))
		}
	}()

	if j.fn != nil {
		j.fn(c)
	}
}

// Stop prevents the cron scheduler from firing new jobs and drops queued triggers.
func (c *Crontab) Stop() {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.ticker != nil {
		c.ticker.Stop()
	}
	select {
	case <-c.done:
	default:
		close(c.done)
	}
	jobs := append([]*job(nil), c.jobs...)
	c.mu.Unlock()

	for _, j := range jobs {
		if j != nil && j.slot != nil {
			j.slot.cancelIntake()
		}
	}
}

type cronJobSnapshot struct {
	Name              string
	ActorName         string
	Schedule          string
	Overlap           CronOverlapPolicy
	MaxConcurrency    int
	ActiveRuns        int
	QueuedNext        bool
	RunsTotal         uint64
	SkippedTotal      uint64
	DroppedQueueTotal uint64
	FailuresTotal     uint64
	PanicsTotal       uint64
	ActorRestarts     uint64
	LastDuration      time.Duration
	LastError         error
}

type cronSchedulerTickMsg struct{}
type cronSchedulerStopMsg struct{}
type cronJobReconcileMsg struct{}

type cronSchedulerMonitorMsg struct {
	pid actor.PID
}

type cronJobMonitorControllerMsg struct {
	slot *cronJobSlot
	pid  actor.PID
}

type cronJobSlot struct {
	app       *App
	crontab   *Crontab
	seq       uint64
	schedule  string
	name      string
	actorName string
	actorRef  actor.Ref
	parsed    *job
	fn        func(*Context) error
	opts      CronJobOptions

	mu                sync.Mutex
	node              *actor.Node
	controllerPID     actor.PID
	controllerBoots   uint64
	actorRestarts     uint64
	stopping          bool
	controllerLost    bool
	pendingStarts     int
	activeCount       int
	maxActiveObserved int
	queuedNext        bool
	runsTotal         uint64
	skippedTotal      uint64
	droppedQueueTotal uint64
	failuresTotal     uint64
	panicsTotal       uint64
	lastDuration      time.Duration
	lastErr           error
	notifyRetrying    bool
	idleCh            chan struct{}
}

func newCronJobSlot(
	app *App,
	crontab *Crontab,
	seq uint64,
	schedule, name string,
	parsed *job,
	fn func(*Context) error,
	opts CronJobOptions,
) *cronJobSlot {
	idleCh := make(chan struct{})
	close(idleCh)
	return &cronJobSlot{
		app:       app,
		crontab:   crontab,
		seq:       seq,
		schedule:  schedule,
		name:      name,
		actorName: sanitizeActorSegment("cron-job", seq, name),
		parsed:    parsed,
		fn:        fn,
		opts:      opts,
		idleCh:    idleCh,
	}
}

func (s *cronJobSlot) snapshot() cronJobSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cronJobSnapshot{
		Name:              s.name,
		ActorName:         s.actorName,
		Schedule:          s.schedule,
		Overlap:           s.opts.Overlap,
		MaxConcurrency:    s.opts.MaxConcurrency,
		ActiveRuns:        s.activeCount,
		QueuedNext:        s.queuedNext,
		RunsTotal:         s.runsTotal,
		SkippedTotal:      s.skippedTotal,
		DroppedQueueTotal: s.droppedQueueTotal,
		FailuresTotal:     s.failuresTotal,
		PanicsTotal:       s.panicsTotal,
		ActorRestarts:     s.actorRestarts,
		LastDuration:      s.lastDuration,
		LastError:         s.lastErr,
	}
}

func (s *cronJobSlot) currentControllerPID() actor.PID {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.controllerPID
}

func (s *cronJobSlot) concurrencyLimitLocked() int {
	if s.opts.Overlap == CronAllowConcurrent && s.opts.MaxConcurrency > 0 {
		return s.opts.MaxConcurrency
	}
	return 1
}

func (s *cronJobSlot) cancelIntake() {
	s.mu.Lock()
	s.stopping = true
	if s.queuedNext {
		s.queuedNext = false
		s.droppedQueueTotal++
	}
	if s.pendingStarts > 0 {
		s.droppedQueueTotal += uint64(s.pendingStarts)
		s.pendingStarts = 0
	}
	s.mu.Unlock()
}

func (s *cronJobSlot) waitIdle(ctx context.Context) error {
	s.mu.Lock()
	ch := s.idleCh
	s.mu.Unlock()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *cronJobSlot) onControllerLost(reason error) {
	if reason == nil {
		reason = errActorRuntimeTerminated
	}
	cause := fmt.Errorf("cron job %q controller terminated: %w", s.name, reason)
	s.mu.Lock()
	s.controllerLost = true
	s.stopping = true
	if s.queuedNext {
		s.queuedNext = false
		s.droppedQueueTotal++
	}
	s.pendingStarts = 0
	s.lastErr = cause
	s.mu.Unlock()

	if s.app != nil {
		s.app.recordFatalCause(cause)
	}
}

func (s *cronJobSlot) enqueueTrigger() {
	if s.app != nil && s.app.isIntakeClosed() {
		s.cancelIntake()
		return
	}

	s.mu.Lock()
	if s.stopping || s.controllerLost || (s.app != nil && s.app.isIntakeClosed()) {
		if s.queuedNext {
			s.queuedNext = false
			s.droppedQueueTotal++
		}
		s.mu.Unlock()
		return
	}

	limit := s.concurrencyLimitLocked()
	inUse := s.activeCount + s.pendingStarts
	shouldNotify := false
	didSkip := false

	if inUse < limit {
		s.pendingStarts++
		shouldNotify = true
	} else {
		switch s.opts.Overlap {
		case CronSkipIfRunning:
			s.skippedTotal++
			didSkip = true
		case CronQueueNext:
			if !s.queuedNext {
				s.queuedNext = true
			} else {
				s.skippedTotal++
				didSkip = true
			}
		case CronAllowConcurrent:
			s.skippedTotal++
			didSkip = true
		}
	}
	s.mu.Unlock()

	if didSkip {
		s.app.recordCronSkippedMetric(s.name, "overlap")
	}
	if shouldNotify {
		s.notifyController()
	}
}

func (s *cronJobSlot) needsControllerWork() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping || s.controllerLost || (s.app != nil && s.app.isIntakeClosed()) {
		return false
	}
	return s.pendingStarts > 0
}

func (s *cronJobSlot) notifyController() {
	if s.app == nil || s.app.isIntakeClosed() {
		return
	}
	s.mu.Lock()
	node := s.node
	ref := s.actorRef
	stopped := s.stopping || s.controllerLost
	s.mu.Unlock()

	if stopped || node == nil || ref.Path == "" {
		return
	}
	err := node.Send(ref, cronJobReconcileMsg{})
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
	if s.notifyRetrying || s.stopping || s.controllerLost {
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
			n := s.node
			r := s.actorRef
			s.mu.Unlock()
			if n == nil || r.Path == "" {
				return
			}
			sendErr := n.Send(r, cronJobReconcileMsg{})
			if sendErr == nil || s.app.isIntakeClosed() {
				return
			}
			if errors.Is(sendErr, actor.ErrClosed) {
				s.app.onActorTreeFailure(sendErr)
				return
			}
			if isPermanentControllerSendErr(sendErr) {
				s.onControllerLost(sendErr)
				return
			}
			if delay < 20*time.Millisecond {
				delay *= 2
			}
			timer.Reset(delay)
		}
	}()
}

func (s *cronJobSlot) runExecution() {
	defer s.app.runtimeTasks.Done()

	baseCtx := s.app.intakeContext()
	runCtx := baseCtx
	var runCancel context.CancelFunc
	if s.opts.Timeout > 0 {
		runCtx, runCancel = context.WithTimeout(baseCtx, s.opts.Timeout)
	}
	if runCancel != nil {
		defer runCancel()
	}

	var rt *runtimeKernel
	if s.app != nil && s.app.kernel != nil {
		rt = s.app.kernel
	} else if s.crontab != nil {
		rt = s.crontab.rt
	}

	tp := otel.GetTracerProvider()
	if rt != nil {
		tp = rt.getTracerProvider()
	}
	spanCtx, span := tp.Tracer("pi-"+version.Framework).
		Start(runCtx, s.name)
	defer span.End()

	piCtx := newContext(nil, noopRequest{ctx: spanCtx}, rt)

	if l := s.app.safeLogger(); l != nil {
		l.Debugf("Starting cron job: %s", s.name)
	}

	start := time.Now()
	var (
		runErr   error
		panicked bool
	)

	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
				runErr = fmt.Errorf("cron job %q panicked: %v", s.name, r)
				if l := s.app.safeLogger(); l != nil {
					l.Errorf("Panic in cron job %s: %v", s.name, r)
				}
			}
		}()
		if s.fn != nil {
			runErr = s.fn(piCtx)
		}
	}()

	dur := time.Since(start)
	if !panicked && runErr != nil && !(s.app.isIntakeClosed() && errors.Is(runErr, context.Canceled)) {
		if l := s.app.safeLogger(); l != nil {
			l.Errorf("Cron job %s failed: %v", s.name, runErr)
		}
	} else if l := s.app.safeLogger(); l != nil {
		l.Debugf("Finished cron job: %s in %s", s.name, dur)
	}

	metricStatus := "ok"
	s.mu.Lock()
	s.activeCount--
	s.runsTotal++
	s.lastDuration = dur
	if panicked {
		s.panicsTotal++
		s.failuresTotal++
		s.lastErr = runErr
		metricStatus = "panic"
	} else if runErr != nil && !(s.app.isIntakeClosed() && errors.Is(runErr, context.Canceled)) {
		s.failuresTotal++
		s.lastErr = runErr
		metricStatus = "error"
	}

	shouldNotify := false
	if s.stopping || s.controllerLost || s.app.isIntakeClosed() {
		if s.queuedNext {
			s.queuedNext = false
			s.droppedQueueTotal++
		}
		if s.pendingStarts > 0 {
			s.droppedQueueTotal += uint64(s.pendingStarts)
			s.pendingStarts = 0
		}
	} else if s.queuedNext && (s.activeCount+s.pendingStarts) < s.concurrencyLimitLocked() {
		s.queuedNext = false
		s.pendingStarts++
		shouldNotify = true
	}

	if s.activeCount == 0 {
		select {
		case <-s.idleCh:
		default:
			close(s.idleCh)
		}
	}
	s.mu.Unlock()

	s.app.recordCronRunMetric(s.name, metricStatus, dur)

	if shouldNotify {
		s.notifyController()
	}
}

type cronJobActor struct {
	actor.Actor
	app         *App
	slot        *cronJobSlot
	stopTracked bool
}

func (c *cronJobActor) Receive(msg any) error {
	s := c.slot
	switch m := msg.(type) {
	case actor.Started:
		if !c.stopTracked {
			c.stopTracked = true
			c.app.controllerStopTasks.Add(1)
		}
		s.mu.Lock()
		s.controllerPID = c.PID()
		s.controllerBoots++
		node := s.node
		restarted := s.controllerBoots > 1
		if restarted {
			s.actorRestarts = s.controllerBoots - 1
		}
		s.mu.Unlock()

		if restarted {
			c.app.recordActorRestartMetric(s.name, "cron_job")
			c.Log().Warn("cron_job controller actor restarted", "job", s.name)
		}

		pid := c.PID()
		c.app.sendRootMonitorWithRetry(
			node,
			cronJobMonitorControllerMsg{slot: s, pid: pid},
			func() bool {
				s.mu.Lock()
				defer s.mu.Unlock()
				return s.stopping || s.controllerLost || s.controllerPID != pid
			},
			s.onControllerLost,
		)
		return c.reconcile()

	case cronJobReconcileMsg:
		return c.reconcile()

	case actorInjectPanicMsg:
		panic(m.reason)

	case actor.Stopped:
		if c.stopTracked {
			c.stopTracked = false
			defer c.app.controllerStopTasks.Done()
		}
		s.mu.Lock()
		closing := c.app.isIntakeClosed() || s.stopping || s.controllerLost
		var idleCh chan struct{}
		if closing {
			s.stopping = true
			if s.queuedNext {
				s.queuedNext = false
				s.droppedQueueTotal++
			}
			if s.pendingStarts > 0 {
				s.droppedQueueTotal += uint64(s.pendingStarts)
				s.pendingStarts = 0
			}
			idleCh = s.idleCh
		}
		s.mu.Unlock()

		if idleCh != nil {
			<-idleCh
		}
		return nil
	}
	return nil
}

func (c *cronJobActor) reconcile() error {
	s := c.slot
	for {
		if c.app.isIntakeClosed() {
			s.cancelIntake()
			return nil
		}

		s.mu.Lock()
		if s.stopping || s.controllerLost || s.pendingStarts <= 0 {
			s.mu.Unlock()
			return nil
		}
		if s.activeCount >= s.concurrencyLimitLocked() {
			s.pendingStarts = 0
			s.mu.Unlock()
			return nil
		}
		if !c.app.tryAdmitRuntimeTask() {
			s.stopping = true
			s.droppedQueueTotal += uint64(s.pendingStarts)
			s.pendingStarts = 0
			if s.queuedNext {
				s.queuedNext = false
				s.droppedQueueTotal++
			}
			s.mu.Unlock()
			return nil
		}

		s.pendingStarts--
		if s.activeCount == 0 {
			s.idleCh = make(chan struct{})
		}
		s.activeCount++
		if s.activeCount > s.maxActiveObserved {
			s.maxActiveObserved = s.activeCount
		}
		s.mu.Unlock()

		go s.runExecution()
	}
}

type cronSchedulerActor struct {
	actor.Actor
	app         *App
	crontab     *Crontab
	slots       []*cronJobSlot
	stopTracked bool
	timer       actor.TimerRef
}

func (s *cronSchedulerActor) now() time.Time {
	if s.app != nil && s.app.cronNow != nil {
		return s.app.cronNow()
	}
	return time.Now()
}

func (s *cronSchedulerActor) loadLastTick() int64 {
	if s.crontab != nil {
		return s.crontab.lastTickUnix.Load()
	}
	return 0
}

func (s *cronSchedulerActor) storeLastTick(sec int64) {
	if s.crontab != nil {
		s.crontab.lastTickUnix.Store(sec)
	}
}

func (s *cronSchedulerActor) armTickTimer(delay time.Duration) {
	s.timer.Cancel()
	if s.app.isIntakeClosed() {
		return
	}
	ref, err := s.SendAfter(max(time.Millisecond, delay), cronSchedulerTickMsg{})
	if err != nil {
		if !s.app.isIntakeClosed() && errors.Is(err, actor.ErrClosed) {
			s.app.onActorTreeFailure(err)
		}
		return
	}
	s.timer = ref
	s.app.runtimeMu.Lock()
	rt := s.app.actorRT
	s.app.runtimeMu.Unlock()
	if rt != nil && rt.node != nil {
		s.app.watchActorTimerDeliver(rt.node, s.PID(), ref, cronSchedulerTickMsg{})
	}
}

func (s *cronSchedulerActor) armNextSecond(now time.Time) {
	nextSec := now.Truncate(time.Second).Add(time.Second)
	s.armTickTimer(nextSec.Sub(now))
}

func (s *cronSchedulerActor) dispatchUnixSecond(sec int64) {
	if s.app.isIntakeClosed() {
		return
	}
	ts := time.Unix(sec, 0)
	for _, slot := range s.slots {
		if slot == nil || slot.parsed == nil {
			continue
		}
		loc := time.Local
		if slot.opts.Location != nil {
			loc = slot.opts.Location
		}
		if slot.parsed.tick(getTick(ts.In(loc))) {
			slot.enqueueTrigger()
		}
	}
}

func (s *cronSchedulerActor) Receive(msg any) error {
	switch m := msg.(type) {
	case actor.Started:
		if !s.stopTracked {
			s.stopTracked = true
			s.app.controllerStopTasks.Add(1)
		}
		if s.crontab != nil && s.crontab.schedulerBoots.Add(1) > 1 {
			s.app.recordActorRestartMetric("cron-scheduler", "cron_scheduler")
			s.Log().Warn("cron-scheduler actor restarted")
		}
		pid := s.PID()
		s.app.runtimeMu.Lock()
		rt := s.app.actorRT
		var node *actor.Node
		if rt != nil {
			rt.cronSchedPID = pid
			node = rt.node
		}
		s.app.runtimeMu.Unlock()

		s.app.sendRootMonitorWithRetry(
			node,
			cronSchedulerMonitorMsg{pid: pid},
			func() bool {
				s.app.runtimeMu.Lock()
				defer s.app.runtimeMu.Unlock()
				return s.app.actorRT == nil || s.app.actorRT.cronSchedPID != pid
			},
			func(err error) {
				s.app.onActorTreeFailure(fmt.Errorf("monitor cron-scheduler %s: %w", pid, err))
			},
		)

		now := s.now()
		currUnix := now.Unix()
		prevTick := s.loadLastTick()
		if prevTick == 0 {
			s.storeLastTick(currUnix)
			s.armNextSecond(now)
			return nil
		}
		if currUnix > prevTick {
			startSec := currUnix
			if currUnix-prevTick <= maxCronCatchupSeconds {
				startSec = prevTick + 1
			}
			for sec := startSec; sec <= currUnix; sec++ {
				if s.app.isIntakeClosed() {
					return nil
				}
				s.storeLastTick(sec)
				s.dispatchUnixSecond(sec)
			}
		}
		s.armNextSecond(s.now())
		return nil

	case cronSchedulerTickMsg:
		s.timer = actor.TimerRef{}
		if s.app.isIntakeClosed() {
			return nil
		}
		now := s.now()
		currUnix := now.Unix()
		lastTick := s.loadLastTick()
		if currUnix <= lastTick {
			target := time.Unix(lastTick+1, 0)
			s.armTickTimer(target.Sub(now))
			return nil
		}

		startSec := currUnix
		if lastTick > 0 && currUnix-lastTick <= maxCronCatchupSeconds {
			startSec = lastTick + 1
		}
		for sec := startSec; sec <= currUnix; sec++ {
			if s.app.isIntakeClosed() {
				return nil
			}
			s.storeLastTick(sec)
			s.dispatchUnixSecond(sec)
		}
		s.armNextSecond(s.now())
		return nil

	case cronSchedulerStopMsg:
		s.timer.Cancel()
		return nil

	case actorInjectPanicMsg:
		panic(m.reason)

	case actor.Stopped:
		if s.stopTracked {
			s.stopTracked = false
			defer s.app.controllerStopTasks.Done()
		}
		s.timer.Cancel()
		return nil
	}
	return nil
}

func (a *App) cronJobSlots() []*cronJobSlot {
	a.runtimeMu.Lock()
	cronTab := a.cron
	a.runtimeMu.Unlock()
	if cronTab == nil {
		return nil
	}
	cronTab.mu.RLock()
	defer cronTab.mu.RUnlock()
	if len(cronTab.jobs) == 0 {
		return nil
	}
	slots := make([]*cronJobSlot, 0, len(cronTab.jobs))
	for _, j := range cronTab.jobs {
		if j != nil && j.slot != nil {
			j.slot.mu.Lock()
			if j.slot.app == nil {
				j.slot.app = a
			}
			j.slot.mu.Unlock()
			slots = append(slots, j.slot)
		}
	}
	return slots
}

func (a *App) cronActorCount() int {
	slots := a.cronJobSlots()
	if len(slots) == 0 {
		return 0
	}
	return 1 + len(slots)
}

func (a *App) buildCronChildSpecs(node *actor.Node) []actor.ChildSpec {
	slots := a.cronJobSlots()
	if len(slots) == 0 {
		return nil
	}
	a.runtimeMu.Lock()
	cronTab := a.cron
	a.runtimeMu.Unlock()
	specs := make([]actor.ChildSpec, 0, 1+len(slots))
	specs = append(specs, actor.ChildSpec{
		Name: "cron-scheduler",
		Factory: func() actor.Behavior {
			return &cronSchedulerActor{
				app:     a,
				crontab: cronTab,
				slots:   slots,
			}
		},
		Options: []actor.SpawnOption{
			actor.WithRestart(actor.Permanent),
		},
	})
	for _, slot := range slots {
		s := slot
		s.mu.Lock()
		s.node = node
		s.actorRef = actor.Ref{Path: "pi-root/cron-sup/" + s.actorName}
		s.mu.Unlock()

		specs = append(specs, actor.ChildSpec{
			Name: s.actorName,
			Factory: func() actor.Behavior {
				return &cronJobActor{app: a, slot: s}
			},
			Options: []actor.SpawnOption{
				actor.WithRestart(actor.Permanent),
			},
		})
	}
	return specs
}

func (a *App) cancelCronIntake() {
	slots := a.cronJobSlots()
	for _, s := range slots {
		s.cancelIntake()
	}

	a.runtimeMu.Lock()
	rt := a.actorRT
	a.runtimeMu.Unlock()
	if rt != nil && rt.node != nil {
		_ = rt.node.Send(actor.Ref{Path: "pi-root/cron-sup/cron-scheduler"}, cronSchedulerStopMsg{})
	}
}

func (a *App) waitCronInFlight(ctx context.Context) error {
	slots := a.cronJobSlots()
	var err error
	for _, s := range slots {
		if waitErr := s.waitIdle(ctx); waitErr != nil {
			err = joinShutdownErr(err, waitErr)
		}
	}
	return err
}

// errOutOfRange denotes the errors that occur when a range in schedule is out of scope for the particular time unit.
type errOutOfRange struct {
	rangeVal any
	input    string
	min, max int
}

func (e errOutOfRange) Error() string {
	return fmt.Sprintf("out of range for %s in %s. %s must be in "+
		"range %d-%d", e.rangeVal, e.input, e.rangeVal, e.min, e.max)
}

type errParsing struct {
	invalidPart string
	base        string
}

func (e errParsing) Error() string {
	if e.base != "" {
		return fmt.Sprintf("unable to parse %s part in %s", e.invalidPart, e.base)
	}

	return fmt.Sprintf("unable to parse %s", e.invalidPart)
}

// noopRequest is a non-operating implementation of Request interface
// this is required to prevent panics while executing cron jobs.
type noopRequest struct {
	ctx context.Context
}

func (n noopRequest) Context() context.Context {
	if n.ctx != nil {
		return n.ctx
	}

	return context.Background()
}

func (noopRequest) Param(string) string {
	return ""
}

func (noopRequest) PathParam(string) string {
	return ""
}

func (noopRequest) HostName() string {
	return "pi"
}

func (noopRequest) Bind(any) error {
	return nil
}

func (noopRequest) Params(string) []string {
	return nil
}
