package pi

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	gWebsocket "github.com/gorilla/websocket"
	"github.com/sllt/tao/actor"

	"github.com/sllt/pi/pkg/pi/websocket"
)

const wsHubActorPath = "pi-root/ws-sup/ws-hub"

type wsHubMonitorMsg struct {
	pid actor.PID
}

type wsHubSpawnSessionMsg struct {
	session     *wsServerSession
	mailboxSize int
	replyCh     chan wsHubSpawnSessionReply
}

type wsHubSpawnSessionReply struct {
	pid actor.PID
	err error
}

type wsOutboundFrameMsg struct {
	messageType  int
	payload      []byte
	writeTimeout time.Duration
}

type wsSessionCloseMsg struct{}
type wsHubCheckMsg struct{}

type wsServerSession struct {
	app       *App
	seq       uint64
	connID    string
	actorName string
	conn      *websocket.Connection
	sem       chan struct{}
	doneCh    chan struct{}
	doneOnce  sync.Once

	mu       sync.Mutex
	actorPID actor.PID
	closed   bool

	enqueuedTotal    atomic.Uint64
	droppedSlowTotal atomic.Uint64
	writeErrorsTotal atomic.Uint64
	spawnSeq         atomic.Uint64
}

func (s *wsServerSession) setActorPID(pid actor.PID) {
	s.mu.Lock()
	if !s.closed {
		s.actorPID = pid
	}
	s.mu.Unlock()
}

func (s *wsServerSession) getActorPID() actor.PID {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.actorPID
}

func (s *wsServerSession) markClosed() actor.PID {
	s.mu.Lock()
	s.closed = true
	pid := s.actorPID
	s.actorPID = actor.PID{}
	s.mu.Unlock()
	return pid
}

func (s *wsServerSession) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *wsServerSession) releaseSlot() {
	select {
	case <-s.sem:
	default:
	}
}

func (s *wsServerSession) markHandlerDone() {
	s.doneOnce.Do(func() {
		close(s.doneCh)
	})
}

type wsSessionHubActor struct {
	actor.Actor
	app         *App
	stopTracked bool
}

func (a *App) spawnServerWSSessionActor(sess *wsServerSession, mailboxSize int) (actor.PID, error) {
	if a == nil || sess == nil || a.isIntakeClosed() || sess.isClosed() {
		return actor.PID{}, errAppAlreadyStopped
	}
	a.runtimeMu.Lock()
	rt := a.actorRT
	a.runtimeMu.Unlock()
	if rt == nil || rt.node == nil {
		return actor.PID{}, errAppAlreadyStopped
	}
	if !a.tryAdmitRuntimeTask() {
		return actor.PID{}, errAppAlreadyStopped
	}

	var releaseOnce sync.Once
	releaseTask := func() {
		releaseOnce.Do(a.runtimeTasks.Done)
	}

	mbSize := mailboxSize
	if mbSize <= 0 {
		mbSize = websocket.DefaultHubMailboxSize
	}
	actorName := sess.actorName
	if attempt := sess.spawnSeq.Add(1); attempt > 1 {
		actorName = fmt.Sprintf("%s-g%d", sess.actorName, attempt)
	}
	pid, err := rt.node.Spawn(
		func() actor.Behavior {
			return &wsSessionActor{
				app:         a,
				session:     sess,
				releaseTask: releaseTask,
			}
		},
		actor.WithName(actorName),
		actor.WithMailboxSize(mbSize),
		actor.WithRestart(actor.Temporary),
	)
	if err != nil {
		releaseTask()
		return actor.PID{}, err
	}
	sess.setActorPID(pid)
	if sess.isClosed() || a.isIntakeClosed() {
		a.sendWSSessionCloseReliable(rt.node, pid)
		return actor.PID{}, errAppAlreadyStopped
	}
	return pid, nil
}

func (a *App) sendWSSessionCloseReliable(node *actor.Node, pid actor.PID) {
	if node == nil || pid == (actor.PID{}) {
		return
	}
	err := node.Send(pid, wsSessionCloseMsg{})
	if err == nil || !errors.Is(err, actor.ErrMailboxFull) {
		return
	}
	select {
	case <-node.Done():
		return
	default:
	}
	if a != nil {
		a.controllerStopTasks.Add(1)
	}
	go func() {
		if a != nil {
			defer a.controllerStopTasks.Done()
		}
		timer := time.NewTimer(time.Millisecond)
		defer timer.Stop()
		for {
			select {
			case <-node.Done():
				return
			case <-timer.C:
			}
			sendErr := node.Send(pid, wsSessionCloseMsg{})
			if sendErr == nil || !errors.Is(sendErr, actor.ErrMailboxFull) {
				return
			}
			timer.Reset(2 * time.Millisecond)
		}
	}()
}

func (h *wsSessionHubActor) Receive(msg any) error {
	switch m := msg.(type) {
	case actor.Started:
		if !h.stopTracked {
			h.stopTracked = true
			h.app.controllerStopTasks.Add(1)
		}
		restarted := h.app.wsHubBoots.Add(1) > 1
		if restarted {
			h.app.recordActorRestartMetric("ws-hub", "ws_hub")
			h.Log().Warn("ws-hub controller actor restarted")
		}
		pid := h.PID()
		h.app.runtimeMu.Lock()
		rt := h.app.actorRT
		var node *actor.Node
		if rt != nil {
			rt.wsHubPID = pid
			node = rt.node
		}
		h.app.runtimeMu.Unlock()

		h.app.sendRootMonitorWithRetry(
			node,
			wsHubMonitorMsg{pid: pid},
			func() bool {
				h.app.runtimeMu.Lock()
				defer h.app.runtimeMu.Unlock()
				return h.app.actorRT == nil || h.app.actorRT.wsHubPID != pid
			},
			func(err error) {
				h.app.onActorTreeFailure(fmt.Errorf("monitor ws-hub %s: %w", pid, err))
			},
		)

		if restarted && !h.app.isIntakeClosed() {
			mbSize := websocket.DefaultHubMailboxSize
			if h.app.httpServer != nil && h.app.httpServer.ws != nil {
				mbSize = h.app.httpServer.ws.HubPolicy().MailboxSize
			}
			for _, sess := range h.app.snapshotActiveWSSessions() {
				if sess != nil && !sess.isClosed() && sess.getActorPID() == (actor.PID{}) {
					_, _ = h.app.spawnServerWSSessionActor(sess, mbSize)
				}
			}
		}
		return nil

	case wsHubCheckMsg:
		return nil

	case wsHubSpawnSessionMsg:
		pid, err := h.app.spawnServerWSSessionActor(m.session, m.mailboxSize)
		if m.replyCh != nil {
			m.replyCh <- wsHubSpawnSessionReply{pid: pid, err: err}
		}
		return nil

	case actorInjectPanicMsg:
		panic(m.reason)

	case actor.Stopped:
		if h.stopTracked {
			h.stopTracked = false
			defer h.app.controllerStopTasks.Done()
		}
		return nil
	}
	return nil
}

type wsSessionActor struct {
	actor.Actor
	app         *App
	session     *wsServerSession
	releaseTask func()
	stopTracked bool
}

func (w *wsSessionActor) Receive(msg any) error {
	switch m := msg.(type) {
	case actor.Started:
		if !w.stopTracked {
			w.stopTracked = true
			w.app.controllerStopTasks.Add(1)
		}
		if w.session == nil || w.session.isClosed() {
			w.Stop(nil)
			return nil
		}
		w.session.setActorPID(w.PID())
		if w.session.isClosed() {
			w.Stop(nil)
		}
		return nil

	case wsOutboundFrameMsg:
		if w.session != nil {
			defer w.session.releaseSlot()
		}
		if w.session == nil || w.session.isClosed() || w.session.conn == nil || w.session.conn.IsClosed() {
			if w.session == nil || w.session.isClosed() {
				w.Stop(nil)
			}
			return nil
		}
		if m.writeTimeout > 0 {
			w.session.conn.SetWriteTimeout(m.writeTimeout)
		}
		if err := w.session.conn.WriteMessage(m.messageType, m.payload); err != nil {
			w.session.writeErrorsTotal.Add(1)
			w.app.recordWSWriteErrorMetric()
		}
		if w.session.isClosed() {
			w.Stop(nil)
		}
		return nil

	case wsSessionCloseMsg:
		w.Stop(nil)
		return nil

	case actor.Stopped:
		if w.stopTracked {
			w.stopTracked = false
			defer w.app.controllerStopTasks.Done()
		}
		if w.session != nil {
			w.session.mu.Lock()
			if w.session.actorPID == w.PID() {
				w.session.actorPID = actor.PID{}
			}
			w.session.mu.Unlock()
			// Reclaim any semaphore slots held by unexecuted mailbox messages when this actor stops.
			for {
				select {
				case <-w.session.sem:
				default:
					goto semDrained
				}
			}
		semDrained:
		}
		if w.releaseTask != nil {
			w.releaseTask()
		}
		return nil

	default:
		if w.session == nil || w.session.isClosed() {
			w.Stop(nil)
		}
		return nil
	}
}

// ConfigureWebSocketHub sets the bounded outbound queue, message size limit, write timeout,
// and slow-consumer eviction policy for server WebSocket connections managed by App.
func (a *App) ConfigureWebSocketHub(policy websocket.HubPolicy) {
	if a == nil {
		return
	}
	a.bindWSManagerHooks()
	if a.httpServer != nil && a.httpServer.ws != nil {
		a.httpServer.ws.ConfigureHub(policy)
	} else if a.container != nil && a.container.WSManager != nil {
		a.container.WSManager.ConfigureHub(policy)
	}
}

func (a *App) bindWSManagerHooks() {
	if a == nil {
		return
	}
	var mgr *websocket.Manager
	if a.httpServer != nil && a.httpServer.ws != nil {
		mgr = a.httpServer.ws
	} else if a.container != nil {
		mgr = a.container.WSManager
	}
	if mgr == nil {
		return
	}
	mgr.SetAdmissionCheck(a.isIntakeClosed)
	mgr.SetHubDispatcher(a.dispatchHubOutbound)
}

func (a *App) markWSServerRegistered() {
	a.ensureSupervisionMetrics()
	a.bindWSManagerHooks()
	a.runtimeMu.Lock()
	a.wsServerRegistered = true
	a.runtimeMu.Unlock()

	a.wsDrainOnce.Do(func() {
		a.registerPhaseBDrain(a.drainServerWebSockets)
	})
}

func (a *App) hasWSServerRegistered() bool {
	a.runtimeMu.Lock()
	defer a.runtimeMu.Unlock()
	return a.wsServerRegistered
}

func (a *App) registerServerWSSession(reqCtx context.Context, connID string, conn *websocket.Connection) (*wsServerSession, error) {
	var policy websocket.HubPolicy
	if a.httpServer != nil && a.httpServer.ws != nil {
		policy = a.httpServer.ws.HubPolicy()
	} else {
		policy = websocket.HubPolicy{
			MailboxSize:     websocket.DefaultHubMailboxSize,
			MaxMessageBytes: websocket.DefaultHubMaxMessageBytes,
			WriteTimeout:    websocket.DefaultHubWriteTimeout,
		}
	}
	if policy.WriteTimeout > 0 {
		conn.SetWriteTimeout(policy.WriteTimeout)
	}

	seq := atomic.AddUint64(&a.nextWSSessionSeq, 1)
	actorName := sanitizeActorSegment("ws-conn", seq, "")
	sess := &wsServerSession{
		app:       a,
		seq:       seq,
		connID:    connID,
		actorName: actorName,
		conn:      conn,
		sem:       make(chan struct{}, max(1, policy.MailboxSize)),
		doneCh:    make(chan struct{}),
	}

	// Attach supervised session actor directly on App's actor node before publishing in
	// wsServerSessions so no actorless session is ever exposed while the actor runtime is live.
	a.runtimeMu.Lock()
	rt := a.actorRT
	a.runtimeMu.Unlock()

	if rt != nil && rt.node != nil {
		if _, spawnErr := a.spawnServerWSSessionActor(sess, policy.MailboxSize); spawnErr != nil {
			sess.markClosed()
			sess.markHandlerDone()
			if reqCtx.Err() != nil {
				return nil, reqCtx.Err()
			}
			return nil, spawnErr
		}
	}

	a.wsServerSessionsMu.Lock()
	if a.isIntakeClosed() {
		a.wsServerSessionsMu.Unlock()
		a.unregisterServerWSSession(sess)
		return nil, errAppAlreadyStopped
	}
	if a.wsServerSessions == nil {
		a.wsServerSessions = make(map[string]*wsServerSession)
	}
	prevSess := a.wsServerSessions[connID]
	a.wsServerSessions[connID] = sess
	a.wsServerSessionsMu.Unlock()

	if prevSess != nil && prevSess != sess {
		a.unregisterServerWSSession(prevSess)
	}

	return sess, nil
}

func (a *App) unregisterServerWSSession(sess *wsServerSession) {
	if sess == nil {
		return
	}
	pid := sess.markClosed()
	sess.markHandlerDone()

	a.wsServerSessionsMu.Lock()
	if curr, ok := a.wsServerSessions[sess.connID]; ok && curr == sess {
		delete(a.wsServerSessions, sess.connID)
	}
	a.wsServerSessionsMu.Unlock()

	if a.httpServer != nil && a.httpServer.ws != nil {
		a.httpServer.ws.CloseConnectionIfMatch(sess.connID, sess.conn)
	} else if sess.conn != nil {
		_ = sess.conn.Close()
	}

	if pid != (actor.PID{}) {
		a.runtimeMu.Lock()
		rt := a.actorRT
		a.runtimeMu.Unlock()
		if rt != nil && rt.node != nil {
			a.sendWSSessionCloseReliable(rt.node, pid)
		}
	}
}

func (a *App) dispatchHubOutbound(
	connID string,
	conn *websocket.Connection,
	messageType int,
	ownedPayload []byte,
	policy websocket.HubPolicy,
) (bool, error) {
	a.wsServerSessionsMu.RLock()
	sess := a.wsServerSessions[connID]
	a.wsServerSessionsMu.RUnlock()

	if sess == nil || sess.conn != conn {
		return false, nil
	}
	if sess.isClosed() || a.isIntakeClosed() {
		return true, websocket.ErrConnectionClosed
	}

	pid := sess.getActorPID()
	a.runtimeMu.Lock()
	rt := a.actorRT
	a.runtimeMu.Unlock()
	if rt == nil || rt.node == nil {
		return false, nil
	}
	if pid == (actor.PID{}) {
		return true, websocket.ErrConnectionClosed
	}

	select {
	case sess.sem <- struct{}{}:
	default:
		sess.droppedSlowTotal.Add(1)
		a.recordWSSlowDropMetric()
		return true, fmt.Errorf("%w: %w", websocket.ErrSlowConsumer, actor.ErrMailboxFull)
	}

	sendErr := rt.node.Send(pid, wsOutboundFrameMsg{
		messageType:  messageType,
		payload:      ownedPayload,
		writeTimeout: policy.WriteTimeout,
	})
	if sendErr != nil {
		sess.releaseSlot()
		if errors.Is(sendErr, actor.ErrMailboxFull) {
			sess.droppedSlowTotal.Add(1)
			a.recordWSSlowDropMetric()
			return true, fmt.Errorf("%w: %w", websocket.ErrSlowConsumer, sendErr)
		}
		return true, websocket.ErrConnectionClosed
	}

	sess.enqueuedTotal.Add(1)
	a.wsServerEnqueuedTotal.Add(1)
	return true, nil
}

func (a *App) snapshotActiveWSSessions() []*wsServerSession {
	a.wsServerSessionsMu.RLock()
	defer a.wsServerSessionsMu.RUnlock()
	if len(a.wsServerSessions) == 0 {
		return nil
	}
	out := make([]*wsServerSession, 0, len(a.wsServerSessions))
	for _, s := range a.wsServerSessions {
		out = append(out, s)
	}
	return out
}

func (a *App) forceCloseServerWebSockets() {
	for _, s := range a.snapshotActiveWSSessions() {
		if s.conn != nil {
			_ = s.conn.ForceClose()
		}
	}
}

func (a *App) drainServerWebSockets(drainCtx context.Context) error {
	sessions := a.snapshotActiveWSSessions()
	if len(sessions) == 0 {
		return nil
	}

	// Signal BeginDrain on all active sessions first so idle readers wake immediately,
	// while mid-turn wsHandlers can still write their final response before sending CloseGoingAway.
	for _, sess := range sessions {
		if sess != nil && sess.conn != nil {
			sess.conn.BeginDrain()
		}
	}

	closePayload := gWebsocket.FormatCloseMessage(gWebsocket.CloseGoingAway, "server shutting down")
	for _, sess := range sessions {
		select {
		case <-sess.doneCh:
		case <-drainCtx.Done():
			// Interrupt any stalled network writes or blocked handlers before Phase C cancels drainExecCtx.
			for _, rem := range sessions {
				if rem == nil || rem.conn == nil {
					continue
				}
				select {
				case <-rem.doneCh:
				default:
					_ = rem.conn.WriteControl(gWebsocket.CloseMessage, closePayload, time.Now().Add(50*time.Millisecond))
					_ = rem.conn.ForceClose()
				}
			}
			return drainCtx.Err()
		}
	}

	return nil
}
