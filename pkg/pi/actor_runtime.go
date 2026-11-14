package pi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sllt/tao/actor"

	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/sllt/pi/pkg/pi/version"
)

const maxActorSegmentLen = 128

var (
	actorNodeSeq              atomic.Uint64
	errActorRuntimeTerminated = errors.New("actor supervision tree terminated unexpectedly")
)

type actorInjectPanicMsg struct {
	reason string
}

type workerMonitorControllerMsg struct {
	slot *workerSlot
	pid  actor.PID
}

type wsClientMonitorControllerMsg struct {
	slot *wsClientSlot
	pid  actor.PID
}

type workerControllerCheckMsg struct{}
type subWorkerCheckMsg struct{}
type piRootHeartbeatMsg struct{}

const (
	defaultPiRootHeartbeatInterval = 2 * time.Second
	defaultPiRootHeartbeatMaxAge   = 20 * time.Second
)

// sanitizeActorSegment converts an arbitrary component identifier into a valid
// tao/actor path segment ([a-zA-Z0-9._-], length 1..128, never "." or "..").
// Including monotonic seq prevents post-sanitization collisions while keeping
// the raw identifier isolated in App-private metadata.
func sanitizeActorSegment(prefix string, seq uint64, rawName string) string {
	cleanPrefix := cleanSegmentToken(prefix)
	if cleanPrefix == "" {
		cleanPrefix = "pi"
	}

	head := cleanPrefix + "-" + strconv.FormatUint(seq, 10)
	if len(head) >= maxActorSegmentLen {
		head = strings.TrimRight(head[:maxActorSegmentLen], "._-")
		if !isValidActorSegment(head) {
			return "pi-" + strconv.FormatUint(seq, 10)
		}
		return head
	}

	slug := cleanSegmentToken(rawName)
	if slug == "" {
		return head
	}

	avail := maxActorSegmentLen - len(head) - 1
	if avail <= 0 {
		return head
	}
	if len(slug) > avail {
		slug = strings.TrimRight(slug[:avail], "._-")
	}
	if slug == "" {
		return head
	}

	candidate := head + "-" + slug
	if !isValidActorSegment(candidate) {
		return head
	}
	return candidate
}

func cleanSegmentToken(raw string) string {
	if raw == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(min(len(raw), maxActorSegmentLen))
	lastUnderscore := false
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		valid := (c >= 'a' && c <= 'z') ||
			(c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') ||
			c == '.' || c == '_' || c == '-'
		if valid {
			if c == '_' {
				if lastUnderscore {
					continue
				}
				lastUnderscore = true
			} else {
				lastUnderscore = false
			}
			b.WriteByte(c)
			continue
		}
		if !lastUnderscore && b.Len() > 0 {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	res := strings.Trim(b.String(), "._-")
	if res == "." || res == ".." {
		return ""
	}
	return res
}

func isValidActorSegment(s string) bool {
	if len(s) == 0 || len(s) > maxActorSegmentLen || s == "." || s == ".." {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= 'a' && c <= 'z') ||
			(c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') ||
			c == '.' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

// piSlogHandler adapts logging.Logger to log/slog without touching slog.Default().
type piSlogHandler struct {
	logger logging.Logger
	groups []string
	attrs  []slog.Attr
}

func newActorSlogLogger(l logging.Logger) *slog.Logger {
	return slog.New(&piSlogHandler{logger: l})
}

func (h *piSlogHandler) Enabled(_ context.Context, _ slog.Level) bool {
	return h != nil && h.logger != nil
}

func (h *piSlogHandler) Handle(_ context.Context, r slog.Record) error {
	if h == nil || h.logger == nil {
		return nil
	}
	var b strings.Builder
	b.WriteString("[actor] ")
	b.WriteString(r.Message)

	prefix := strings.Join(h.groups, ".")
	for _, a := range h.attrs {
		appendSlogAttr(&b, prefix, a)
	}
	r.Attrs(func(a slog.Attr) bool {
		appendSlogAttr(&b, prefix, a)
		return true
	})

	msg := b.String()
	switch {
	case r.Level >= slog.LevelError:
		h.logger.Errorf("%s", msg)
	case r.Level >= slog.LevelWarn:
		h.logger.Warnf("%s", msg)
	case r.Level >= slog.LevelInfo:
		h.logger.Infof("%s", msg)
	default:
		h.logger.Debugf("%s", msg)
	}
	return nil
}

func appendSlogAttr(b *strings.Builder, groupPrefix string, a slog.Attr) {
	a.Value = a.Value.Resolve()
	if a.Equal(slog.Attr{}) {
		return
	}
	if a.Value.Kind() == slog.KindGroup {
		nextPrefix := a.Key
		if groupPrefix != "" && nextPrefix != "" {
			nextPrefix = groupPrefix + "." + nextPrefix
		} else if groupPrefix != "" {
			nextPrefix = groupPrefix
		}
		for _, sub := range a.Value.Group() {
			appendSlogAttr(b, nextPrefix, sub)
		}
		return
	}
	key := a.Key
	if groupPrefix != "" && key != "" {
		key = groupPrefix + "." + key
	}
	b.WriteByte(' ')
	b.WriteString(key)
	b.WriteByte('=')
	b.WriteString(a.Value.String())
}

func (h *piSlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	combined := make([]slog.Attr, 0, len(h.attrs)+len(attrs))
	combined = append(combined, h.attrs...)
	combined = append(combined, attrs...)
	return &piSlogHandler{
		logger: h.logger,
		groups: h.groups,
		attrs:  combined,
	}
}

func (h *piSlogHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	groups := make([]string, 0, len(h.groups)+1)
	groups = append(groups, h.groups...)
	groups = append(groups, name)
	return &piSlogHandler{
		logger: h.logger,
		groups: groups,
		attrs:  h.attrs,
	}
}

type actorRuntime struct {
	app           *App
	node          *actor.Node
	rootPID       actor.PID
	workersSupPID actor.PID
	cronSupPID    actor.PID
	cronSchedPID  actor.PID
	wsSupPID      actor.PID
	wsHubPID      actor.PID
	subsSupPID    actor.PID

	rootLastBeatNano    atomic.Int64
	rootHeartbeatMaxAge atomic.Int64

	watchStopOnce sync.Once
	watchStopCh   chan struct{}
	watchDoneCh   chan struct{}

	stopOnce  sync.Once
	stoppedCh chan struct{}
	stopErr   error
}

func (rt *actorRuntime) closeWatch() {
	rt.watchStopOnce.Do(func() {
		close(rt.watchStopCh)
	})
	<-rt.watchDoneCh
}

func (rt *actorRuntime) stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	rt.stopOnce.Do(func() {
		defer close(rt.stoppedCh)
		rt.closeWatch()
		if rt.node != nil {
			rt.stopErr = rt.node.Stop(ctx)
			select {
			case <-rt.node.Done():
			case <-ctx.Done():
			}
		}
	})
	select {
	case <-rt.stoppedCh:
		return rt.stopErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *App) sendRootMonitorWithRetry(
	node *actor.Node,
	msg any,
	shouldAbort func() bool,
	onLost func(error),
) {
	if a == nil || node == nil || a.isIntakeClosed() {
		return
	}
	if shouldAbort != nil && shouldAbort() {
		return
	}
	rootRef := actor.Ref{Path: "pi-root"}
	err := node.Send(rootRef, msg)
	if err == nil || a.isIntakeClosed() {
		return
	}
	if errors.Is(err, actor.ErrClosed) || isPermanentControllerSendErr(err) {
		if onLost != nil {
			onLost(err)
		}
		return
	}
	if !a.tryAdmitRuntimeTask() {
		return
	}
	intakeDone := a.intakeDone()
	go func() {
		defer a.runtimeTasks.Done()
		delay := time.Millisecond
		timer := time.NewTimer(delay)
		defer timer.Stop()
		for {
			select {
			case <-intakeDone:
				return
			case <-timer.C:
			}
			if a.isIntakeClosed() || (shouldAbort != nil && shouldAbort()) {
				return
			}
			sendErr := node.Send(rootRef, msg)
			if sendErr == nil || a.isIntakeClosed() {
				return
			}
			if errors.Is(sendErr, actor.ErrClosed) || isPermanentControllerSendErr(sendErr) {
				if onLost != nil {
					onLost(sendErr)
				}
				return
			}
			if delay < 20*time.Millisecond {
				delay *= 2
			}
			timer.Reset(delay)
		}
	}()
}

func (a *App) watchActorTimerDeliver(node *actor.Node, target actor.PID, ref actor.TimerRef, msg any) {
	if a == nil || node == nil || target == (actor.PID{}) || ref.Done() == nil {
		return
	}
	if !a.tryAdmitRuntimeTask() {
		ref.Cancel()
		return
	}
	intakeDone := a.intakeDone()
	go func() {
		defer a.runtimeTasks.Done()
		select {
		case <-intakeDone:
			ref.Cancel()
			return
		case <-ref.Done():
			err := ref.Err()
			if err == nil || a.isIntakeClosed() || errors.Is(err, context.Canceled) || errors.Is(err, actor.ErrStopped) {
				return
			}
			if errors.Is(err, actor.ErrClosed) {
				a.onActorTreeFailure(err)
				return
			}
			delay := time.Millisecond
			timer := time.NewTimer(delay)
			defer timer.Stop()
			for {
				if a.isIntakeClosed() {
					return
				}
				sendErr := node.Send(target, msg)
				if sendErr == nil || a.isIntakeClosed() {
					return
				}
				if errors.Is(sendErr, actor.ErrClosed) {
					a.onActorTreeFailure(sendErr)
					return
				}
				if isPermanentControllerSendErr(sendErr) || errors.Is(sendErr, actor.ErrStopped) {
					return
				}
				select {
				case <-intakeDone:
					return
				case <-timer.C:
				}
				if delay < 20*time.Millisecond {
					delay *= 2
				}
				timer.Reset(delay)
			}
		}
	}()
}

type subWorkerMonEntry struct {
	slot      *subscriberSlot
	workerIdx int
}

type piRootActor struct {
	actor.Actor
	app            *App
	rt             *actorRuntime
	stopTracked    bool
	hbTimer        actor.TimerRef
	supervisors    map[actor.PID]string
	workerByPID    map[actor.PID]*workerSlot
	cronSchedPIDs  map[actor.PID]struct{}
	cronByPID      map[actor.PID]*cronJobSlot
	wsHubPIDs      map[actor.PID]struct{}
	wsClientByPID  map[actor.PID]*wsClientSlot
	subCoordByPID  map[actor.PID]*subscriberSlot
	subWorkerByPID map[actor.PID]subWorkerMonEntry
}

func (r *piRootActor) spawnChildSupervisor(name string, children []actor.ChildSpec) (actor.PID, error) {
	if len(children) == 0 {
		return actor.PID{}, nil
	}
	supFactory := actor.Supervisor(actor.SupervisorSpec{
		Strategy:    actor.OneForOne,
		MaxRestarts: 5,
		Window:      10 * time.Second,
		Children:    children,
	})
	supPID, err := r.Spawn(
		supFactory,
		actor.WithName(name),
		actor.WithRestart(actor.Temporary),
	)
	if err != nil {
		return actor.PID{}, err
	}
	if _, err := r.Monitor(supPID); err != nil {
		return actor.PID{}, err
	}
	if r.supervisors == nil {
		r.supervisors = make(map[actor.PID]string)
	}
	r.supervisors[supPID] = name
	return supPID, nil
}

func (r *piRootActor) monitorWorkerController(slot *workerSlot, pid actor.PID) {
	if slot == nil || pid == (actor.PID{}) || r.app.isIntakeClosed() {
		return
	}
	if r.workerByPID == nil {
		r.workerByPID = make(map[actor.PID]*workerSlot)
	}
	if _, exists := r.workerByPID[pid]; exists {
		return
	}
	if _, err := r.Monitor(pid); err != nil {
		if !r.app.isIntakeClosed() && slot.currentControllerPID() == pid {
			slot.onControllerLost(fmt.Errorf("monitor controller %s: %w", pid, err))
		}
		return
	}
	r.workerByPID[pid] = slot
}

func (r *piRootActor) monitorCronScheduler(pid actor.PID) {
	if pid == (actor.PID{}) || r.app.isIntakeClosed() {
		return
	}
	if r.cronSchedPIDs == nil {
		r.cronSchedPIDs = make(map[actor.PID]struct{})
	}
	if _, exists := r.cronSchedPIDs[pid]; exists {
		return
	}
	if _, err := r.Monitor(pid); err != nil {
		if !r.app.isIntakeClosed() {
			r.app.runtimeMu.Lock()
			curr := r.rt.cronSchedPID
			r.app.runtimeMu.Unlock()
			if curr == pid {
				r.app.onActorTreeFailure(fmt.Errorf("monitor cron-scheduler %s: %w", pid, err))
			}
		}
		return
	}
	r.cronSchedPIDs[pid] = struct{}{}
}

func (r *piRootActor) monitorCronJobController(slot *cronJobSlot, pid actor.PID) {
	if slot == nil || pid == (actor.PID{}) || r.app.isIntakeClosed() {
		return
	}
	if r.cronByPID == nil {
		r.cronByPID = make(map[actor.PID]*cronJobSlot)
	}
	if _, exists := r.cronByPID[pid]; exists {
		return
	}
	if _, err := r.Monitor(pid); err != nil {
		if !r.app.isIntakeClosed() && slot.currentControllerPID() == pid {
			slot.onControllerLost(fmt.Errorf("monitor cron-job controller %s: %w", pid, err))
		}
		return
	}
	r.cronByPID[pid] = slot
}

func (r *piRootActor) monitorWSHub(pid actor.PID) {
	if pid == (actor.PID{}) || r.app.isIntakeClosed() {
		return
	}
	if r.wsHubPIDs == nil {
		r.wsHubPIDs = make(map[actor.PID]struct{})
	}
	if _, exists := r.wsHubPIDs[pid]; exists {
		return
	}
	if _, err := r.Monitor(pid); err != nil {
		if !r.app.isIntakeClosed() {
			r.app.runtimeMu.Lock()
			curr := r.rt.wsHubPID
			r.app.runtimeMu.Unlock()
			if curr == pid {
				r.app.onActorTreeFailure(fmt.Errorf("monitor ws-hub %s: %w", pid, err))
			}
		}
		return
	}
	r.wsHubPIDs[pid] = struct{}{}
}

func (r *piRootActor) monitorWSClientController(slot *wsClientSlot, pid actor.PID) {
	if slot == nil || pid == (actor.PID{}) || r.app.isIntakeClosed() {
		return
	}
	if r.wsClientByPID == nil {
		r.wsClientByPID = make(map[actor.PID]*wsClientSlot)
	}
	if _, exists := r.wsClientByPID[pid]; exists {
		return
	}
	if _, err := r.Monitor(pid); err != nil {
		if !r.app.isIntakeClosed() && slot.currentControllerPID() == pid {
			slot.onControllerLost(fmt.Errorf("monitor ws-client controller %s: %w", pid, err))
		}
		return
	}
	r.wsClientByPID[pid] = slot
}

func (r *piRootActor) monitorSubscriberCoordinator(slot *subscriberSlot, pid actor.PID) {
	if slot == nil || pid == (actor.PID{}) || r.app.isIntakeClosed() {
		return
	}
	if r.subCoordByPID == nil {
		r.subCoordByPID = make(map[actor.PID]*subscriberSlot)
	}
	if _, exists := r.subCoordByPID[pid]; exists {
		return
	}
	if _, err := r.Monitor(pid); err != nil {
		if !r.app.isIntakeClosed() && slot.currentCoordPID() == pid {
			slot.onCoordControllerLost(fmt.Errorf("monitor subscriber coordinator %s: %w", pid, err))
		}
		return
	}
	r.subCoordByPID[pid] = slot
}

func (r *piRootActor) monitorSubscriberWorker(slot *subscriberSlot, workerIdx int, pid actor.PID) {
	if slot == nil || pid == (actor.PID{}) || r.app.isIntakeClosed() {
		return
	}
	if r.subWorkerByPID == nil {
		r.subWorkerByPID = make(map[actor.PID]subWorkerMonEntry)
	}
	if _, exists := r.subWorkerByPID[pid]; exists {
		return
	}
	if _, err := r.Monitor(pid); err != nil {
		if !r.app.isIntakeClosed() && slot.currentWorkerPID(workerIdx) == pid {
			slot.onWorkerControllerLost(workerIdx, fmt.Errorf("monitor subscriber worker %s: %w", pid, err))
		}
		return
	}
	r.subWorkerByPID[pid] = subWorkerMonEntry{slot: slot, workerIdx: workerIdx}
}

func (r *piRootActor) armHeartbeatTimer() {
	r.hbTimer.Cancel()
	if r.app.isIntakeClosed() {
		return
	}
	t, err := r.SendAfter(defaultPiRootHeartbeatInterval, piRootHeartbeatMsg{})
	if err != nil {
		return
	}
	r.hbTimer = t
	if r.rt != nil && r.rt.node != nil {
		r.app.watchActorTimerDeliver(r.rt.node, r.PID(), t, piRootHeartbeatMsg{})
	}
}

func (r *piRootActor) Receive(msg any) error {
	switch m := msg.(type) {
	case actor.Started:
		if !r.stopTracked {
			r.stopTracked = true
			r.app.controllerStopTasks.Add(1)
		}
		if r.rt != nil {
			r.app.runtimeMu.Lock()
			r.rt.rootPID = r.PID()
			r.app.runtimeMu.Unlock()
		}
		if workerChildren := r.app.buildWorkerChildSpecs(r.rt.node); len(workerChildren) > 0 {
			supPID, err := r.spawnChildSupervisor("workers-sup", workerChildren)
			if err != nil {
				return err
			}
			r.rt.workersSupPID = supPID
			for _, slot := range r.app.workerSlots() {
				r.monitorWorkerController(slot, slot.currentControllerPID())
			}
		}
		if cronChildren := r.app.buildCronChildSpecs(r.rt.node); len(cronChildren) > 0 {
			supPID, err := r.spawnChildSupervisor("cron-sup", cronChildren)
			if err != nil {
				return err
			}
			r.rt.cronSupPID = supPID
			r.app.runtimeMu.Lock()
			schedPID := r.rt.cronSchedPID
			r.app.runtimeMu.Unlock()
			r.monitorCronScheduler(schedPID)
			for _, slot := range r.app.cronJobSlots() {
				r.monitorCronJobController(slot, slot.currentControllerPID())
			}
		}
		if wsChildren := r.app.buildWSClientChildSpecs(r.rt.node); len(wsChildren) > 0 {
			supPID, err := r.spawnChildSupervisor("ws-sup", wsChildren)
			if err != nil {
				return err
			}
			r.rt.wsSupPID = supPID
			r.app.runtimeMu.Lock()
			hubPID := r.rt.wsHubPID
			r.app.runtimeMu.Unlock()
			r.monitorWSHub(hubPID)
			for _, slot := range r.app.wsClientSlots() {
				r.monitorWSClientController(slot, slot.currentControllerPID())
			}
		}
		if subChildren := r.app.buildSubscriberChildSpecs(r.rt.node); len(subChildren) > 0 {
			supPID, err := r.spawnChildSupervisor("subs-sup", subChildren)
			if err != nil {
				return err
			}
			r.rt.subsSupPID = supPID
			for _, slot := range r.app.subscriberSlots() {
				r.monitorSubscriberCoordinator(slot, slot.currentCoordPID())
				for i := range slot.workerNames {
					r.monitorSubscriberWorker(slot, i, slot.currentWorkerPID(i))
				}
			}
		}
		if r.rt != nil {
			r.rt.rootLastBeatNano.Store(time.Now().UnixNano())
		}
		r.armHeartbeatTimer()
		return nil

	case piRootHeartbeatMsg:
		r.hbTimer = actor.TimerRef{}
		if r.rt != nil {
			r.rt.rootLastBeatNano.Store(time.Now().UnixNano())
		}
		if !r.app.isIntakeClosed() {
			r.armHeartbeatTimer()
		}
		return nil

	case workerMonitorControllerMsg:
		r.monitorWorkerController(m.slot, m.pid)
		return nil

	case cronSchedulerMonitorMsg:
		r.monitorCronScheduler(m.pid)
		return nil

	case cronJobMonitorControllerMsg:
		r.monitorCronJobController(m.slot, m.pid)
		return nil

	case wsClientMonitorControllerMsg:
		r.monitorWSClientController(m.slot, m.pid)
		return nil

	case wsHubMonitorMsg:
		r.monitorWSHub(m.pid)
		return nil

	case subCoordMonitorMsg:
		r.monitorSubscriberCoordinator(m.slot, m.pid)
		return nil

	case subWorkerMonitorMsg:
		r.monitorSubscriberWorker(m.slot, m.workerIdx, m.pid)
		return nil

	case actor.Down:
		if supName, isSup := r.supervisors[m.PID]; isSup {
			delete(r.supervisors, m.PID)
			if !r.app.isIntakeClosed() || (m.Reason != nil && !errors.Is(m.Reason, actor.ErrStopped)) {
				cause := m.Reason
				if cause == nil {
					cause = errActorRuntimeTerminated
				}
				wrapped := fmt.Errorf("%s terminated: %w", supName, cause)
				r.app.onActorTreeFailure(wrapped)
				r.Stop(wrapped)
			}
			return nil
		}
		if slot, ok := r.workerByPID[m.PID]; ok {
			delete(r.workerByPID, m.PID)
			if r.app.isIntakeClosed() {
				return nil
			}
			if errors.Is(m.Reason, actor.ErrUnresponsive) {
				slot.onControllerLost(m.Reason)
				return nil
			}
			if !errors.Is(m.Reason, actor.ErrProcessCrashed) &&
				slot.currentControllerPID() == m.PID &&
				r.rt != nil && r.rt.node != nil {
				probeErr := r.rt.node.Send(slot.actorRef, workerControllerCheckMsg{})
				if errors.Is(probeErr, actor.ErrUnresponsive) ||
					errors.Is(probeErr, actor.ErrNoProcess) ||
					errors.Is(probeErr, actor.ErrStalePID) ||
					errors.Is(probeErr, actor.ErrClosed) {
					cause := m.Reason
					if cause == nil {
						cause = probeErr
					} else {
						cause = errors.Join(cause, probeErr)
					}
					slot.onControllerLost(cause)
					return nil
				}
			}
			slot.notifyController()
			return nil
		}
		if _, ok := r.cronSchedPIDs[m.PID]; ok {
			delete(r.cronSchedPIDs, m.PID)
			if r.app.isIntakeClosed() {
				return nil
			}
			if errors.Is(m.Reason, actor.ErrUnresponsive) {
				r.app.onActorTreeFailure(fmt.Errorf("cron-scheduler unresponsive: %w", m.Reason))
				return nil
			}
			r.app.runtimeMu.Lock()
			currSched := r.rt.cronSchedPID
			r.app.runtimeMu.Unlock()
			if !errors.Is(m.Reason, actor.ErrProcessCrashed) &&
				(currSched == (actor.PID{}) || currSched == m.PID) &&
				r.rt != nil && r.rt.node != nil {
				probeErr := r.rt.node.Send(actor.Ref{Path: "pi-root/cron-sup/cron-scheduler"}, cronSchedulerTickMsg{})
				if isPermanentControllerSendErr(probeErr) || errors.Is(probeErr, actor.ErrClosed) {
					cause := m.Reason
					if cause == nil {
						cause = probeErr
					} else {
						cause = errors.Join(cause, probeErr)
					}
					r.app.onActorTreeFailure(fmt.Errorf("cron-scheduler terminated: %w", cause))
				}
			}
			return nil
		}
		if cronSlot, ok := r.cronByPID[m.PID]; ok {
			delete(r.cronByPID, m.PID)
			if r.app.isIntakeClosed() {
				return nil
			}
			if errors.Is(m.Reason, actor.ErrUnresponsive) {
				cronSlot.onControllerLost(m.Reason)
				return nil
			}
			if !errors.Is(m.Reason, actor.ErrProcessCrashed) &&
				cronSlot.currentControllerPID() == m.PID &&
				r.rt != nil && r.rt.node != nil {
				probeErr := r.rt.node.Send(cronSlot.actorRef, cronJobReconcileMsg{})
				if isPermanentControllerSendErr(probeErr) || errors.Is(probeErr, actor.ErrClosed) {
					cause := m.Reason
					if cause == nil {
						cause = probeErr
					} else {
						cause = errors.Join(cause, probeErr)
					}
					cronSlot.onControllerLost(cause)
					return nil
				}
			}
			cronSlot.notifyController()
			return nil
		}
		if _, ok := r.wsHubPIDs[m.PID]; ok {
			delete(r.wsHubPIDs, m.PID)
			if r.app.isIntakeClosed() {
				return nil
			}
			if errors.Is(m.Reason, actor.ErrUnresponsive) {
				r.app.onActorTreeFailure(fmt.Errorf("ws-hub unresponsive: %w", m.Reason))
				return nil
			}
			r.app.runtimeMu.Lock()
			currHub := r.rt.wsHubPID
			r.app.runtimeMu.Unlock()
			if !errors.Is(m.Reason, actor.ErrProcessCrashed) &&
				(currHub == (actor.PID{}) || currHub == m.PID) &&
				r.rt != nil && r.rt.node != nil {
				probeErr := r.rt.node.Send(actor.Ref{Path: wsHubActorPath}, wsHubCheckMsg{})
				if isPermanentControllerSendErr(probeErr) || errors.Is(probeErr, actor.ErrClosed) {
					cause := m.Reason
					if cause == nil {
						cause = probeErr
					} else {
						cause = errors.Join(cause, probeErr)
					}
					r.app.onActorTreeFailure(fmt.Errorf("ws-hub terminated: %w", cause))
				}
			}
			return nil
		}
		if wsSlot, ok := r.wsClientByPID[m.PID]; ok {
			delete(r.wsClientByPID, m.PID)
			if r.app.isIntakeClosed() || wsSlot.isTerminal() {
				return nil
			}
			if errors.Is(m.Reason, actor.ErrUnresponsive) {
				wsSlot.onControllerLost(m.Reason)
				return nil
			}
			if !errors.Is(m.Reason, actor.ErrProcessCrashed) &&
				wsSlot.currentControllerPID() == m.PID &&
				r.rt != nil && r.rt.node != nil {
				probeErr := r.rt.node.Send(wsSlot.actorRef, wsClientReconcileMsg{})
				if errors.Is(probeErr, actor.ErrUnresponsive) ||
					errors.Is(probeErr, actor.ErrNoProcess) ||
					errors.Is(probeErr, actor.ErrStalePID) ||
					errors.Is(probeErr, actor.ErrClosed) {
					cause := m.Reason
					if cause == nil {
						cause = probeErr
					} else {
						cause = errors.Join(cause, probeErr)
					}
					wsSlot.onControllerLost(cause)
					return nil
				}
			}
			wsSlot.notifyController()
			return nil
		}
		if subSlot, ok := r.subCoordByPID[m.PID]; ok {
			delete(r.subCoordByPID, m.PID)
			if r.app.isIntakeClosed() {
				return nil
			}
			if errors.Is(m.Reason, actor.ErrUnresponsive) {
				subSlot.onCoordControllerLost(m.Reason)
				return nil
			}
			if !errors.Is(m.Reason, actor.ErrProcessCrashed) &&
				subSlot.currentCoordPID() == m.PID &&
				r.rt != nil && r.rt.node != nil {
				probeErr := r.rt.node.Send(subSlot.coordRef, subCoordReconcileMsg{})
				if isPermanentControllerSendErr(probeErr) || errors.Is(probeErr, actor.ErrClosed) {
					cause := m.Reason
					if cause == nil {
						cause = probeErr
					} else {
						cause = errors.Join(cause, probeErr)
					}
					subSlot.onCoordControllerLost(cause)
					return nil
				}
			}
			subSlot.notifyCoordinator()
			return nil
		}
		if entry, ok := r.subWorkerByPID[m.PID]; ok {
			delete(r.subWorkerByPID, m.PID)
			if r.app.isIntakeClosed() {
				return nil
			}
			if errors.Is(m.Reason, actor.ErrUnresponsive) {
				entry.slot.onWorkerControllerLost(entry.workerIdx, m.Reason)
				return nil
			}
			if !errors.Is(m.Reason, actor.ErrProcessCrashed) &&
				entry.slot.currentWorkerPID(entry.workerIdx) == m.PID &&
				r.rt != nil && r.rt.node != nil {
				probeErr := r.rt.node.Send(entry.slot.workerRefs[entry.workerIdx], subWorkerCheckMsg{})
				if isPermanentControllerSendErr(probeErr) || errors.Is(probeErr, actor.ErrClosed) {
					cause := m.Reason
					if cause == nil {
						cause = probeErr
					} else {
						cause = errors.Join(cause, probeErr)
					}
					entry.slot.onWorkerControllerLost(entry.workerIdx, cause)
					return nil
				}
			}
			return nil
		}
		return nil

	case actorInjectPanicMsg:
		panic(m.reason)

	case actor.Stopped:
		if r.stopTracked {
			r.stopTracked = false
			defer r.app.controllerStopTasks.Done()
		}
		r.hbTimer.Cancel()
		if !r.app.isIntakeClosed() {
			cause := m.Reason
			if cause == nil {
				cause = errActorRuntimeTerminated
			}
			r.app.onActorTreeFailure(fmt.Errorf("pi-root stopped: %w", cause))
		}
		return nil
	}
	return nil
}

func (a *App) needsActorRuntime() bool {
	a.runtimeMu.Lock()
	hasBase := len(a.backgroundWorkers) > 0 || len(a.wsClients) > 0 || a.wsServerRegistered
	a.runtimeMu.Unlock()
	return hasBase || a.cronActorCount() > 0 || a.subscriberActorCount() > 0
}

func (a *App) startActorRuntime(startupCtx context.Context) error {
	if startupCtx == nil {
		startupCtx = context.Background()
	}
	if err := contextCauseOrErr(startupCtx); err != nil {
		return err
	}

	cronCount := a.cronActorCount()
	subCount := a.subscriberActorCount()

	a.runtimeMu.Lock()
	if a.actorRT != nil {
		a.runtimeMu.Unlock()
		return nil
	}
	wsServerCount := 0
	if a.wsServerRegistered {
		wsServerCount = 1
	}
	componentCount := len(a.backgroundWorkers) + len(a.wsClients) + wsServerCount + cronCount + subCount
	maxWorkerStop := time.Duration(0)
	for _, w := range a.backgroundWorkers {
		if w.policy.ShutdownTimeout > maxWorkerStop {
			maxWorkerStop = w.policy.ShutdownTimeout
		}
	}
	a.runtimeMu.Unlock()

	if componentCount == 0 {
		return nil
	}

	a.ensureSupervisionMetrics()

	appStopTimeout := 30 * time.Second
	if a.Config != nil {
		if cfgTimeout, err := a.shutdownTimeout(); err == nil && cfgTimeout > 0 {
			appStopTimeout = cfgTimeout
		}
	}
	nodeShutdownTimeout := max(appStopTimeout, maxWorkerStop)

	nodeStartupTimeout := max(30*time.Second, nodeShutdownTimeout)
	if deadline, ok := startupCtx.Deadline(); ok {
		if rem := time.Until(deadline); rem > nodeStartupTimeout {
			nodeStartupTimeout = rem
		}
	}

	rawAppName := "app"
	var logger logging.Logger
	if a.container != nil {
		if name := a.container.GetAppName(); name != "" {
			rawAppName = name
		}
		logger = a.container.Logger
	}
	nodeName := sanitizeActorSegment("pi-app", actorNodeSeq.Add(1), rawAppName)

	node, err := actor.NewNode(actor.NodeOptions{
		Name:                    nodeName,
		Version:                 version.Framework,
		Logger:                  newActorSlogLogger(logger),
		MaxObservations:         max(65536, componentCount*8+64),
		MaxObservationsPerActor: max(256, componentCount*4+16),
		StartupTimeout:          nodeStartupTimeout,
		ShutdownTimeout:         nodeShutdownTimeout,
	})
	if err != nil {
		return fmt.Errorf("create actor node: %w", err)
	}

	rt := &actorRuntime{
		app:         a,
		node:        node,
		watchStopCh: make(chan struct{}),
		watchDoneCh: make(chan struct{}),
		stoppedCh:   make(chan struct{}),
	}
	rt.rootLastBeatNano.Store(time.Now().UnixNano())
	rt.rootHeartbeatMaxAge.Store(int64(defaultPiRootHeartbeatMaxAge))

	go func() {
		defer close(rt.watchDoneCh)
		select {
		case <-rt.watchStopCh:
			return
		case <-node.Done():
			if !a.isIntakeClosed() {
				cause := node.Err()
				if cause == nil {
					cause = errActorRuntimeTerminated
				}
				a.onActorTreeFailure(fmt.Errorf("actor node terminated: %w", cause))
			}
		}
	}()

	a.runtimeMu.Lock()
	if a.isIntakeClosed() || a.runtimeState == lifecycleStopping || a.runtimeState == lifecycleStopped {
		a.runtimeMu.Unlock()
		cause := a.startupAbortCause(startupCtx)
		a.beginStopIntake(cause)
		_ = rt.stop(context.Background())
		return cause
	}
	a.actorRT = rt
	a.runtimeMu.Unlock()

	// Bound Spawn by startupCtx as well if startupCtx cancels during boot.
	stopSpawnOnCancel := context.AfterFunc(startupCtx, func() {
		a.beginStopIntake(a.startupAbortCause(startupCtx))
		_ = rt.stop(context.Background())
	})
	rootPID, spawnErr := node.Spawn(
		func() actor.Behavior {
			return &piRootActor{app: a, rt: rt}
		},
		actor.WithName("pi-root"),
		actor.WithRestart(actor.Temporary),
	)
	if !stopSpawnOnCancel() {
		// startupCtx canceled concurrently while spawning.
		cause := a.startupAbortCause(startupCtx)
		a.beginStopIntake(cause)
		_ = rt.stop(context.Background())
		return cause
	}
	if spawnErr != nil {
		a.beginStopIntake(spawnErr)
		_ = rt.stop(context.Background())
		if startupErr := contextCauseOrErr(startupCtx); startupErr != nil {
			return startupErr
		}
		return fmt.Errorf("start pi-root supervisor: %w", spawnErr)
	}
	a.runtimeMu.Lock()
	rt.rootPID = rootPID
	a.runtimeMu.Unlock()

	if a.isIntakeClosed() {
		_ = rt.stop(context.Background())
		return a.startupAbortCause(startupCtx)
	}

	return nil
}

func (a *App) startupAbortCause(startupCtx context.Context) error {
	if err := contextCauseOrErr(startupCtx); err != nil {
		return err
	}
	if err := contextCauseOrErr(a.runtimeContext()); err != nil {
		return err
	}
	if err := contextCauseOrErr(a.intakeContext()); err != nil {
		return err
	}
	return context.Canceled
}

func (a *App) stopActorRuntime(ctx context.Context) error {
	a.runtimeMu.Lock()
	rt := a.actorRT
	a.runtimeMu.Unlock()
	if rt == nil {
		return nil
	}
	return rt.stop(ctx)
}

func (a *App) onActorTreeFailure(cause error) {
	if cause == nil {
		cause = errActorRuntimeTerminated
	}
	a.actorTreeDead.Store(true)
	a.recordFatalCause(cause)
}
