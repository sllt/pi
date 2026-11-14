package websocket

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// WSKey defines the key type for WSConnectionKey.
type WSKey string

// WSConnectionKey is a key constant that stores the connection id in the request context.
const WSConnectionKey WSKey = "ws-connection-key"

const (
	defaultWriteLockTimeout    = 10 * time.Second
	defaultControlWriteTimeout = 2 * time.Second

	// DefaultHubMailboxSize is the default per-session outbound async queue size.
	DefaultHubMailboxSize = 64
	// DefaultHubMaxMessageBytes is the default maximum payload size accepted by Hub.Send / SendJSON (1 MiB).
	DefaultHubMaxMessageBytes = 1 << 20
	// DefaultHubWriteTimeout is the default per-frame write deadline and write-token acquisition timeout.
	DefaultHubWriteTimeout = 10 * time.Second
)

// PreparedMessage aliases gorilla/websocket.PreparedMessage for callers using Connection.WritePreparedMessage.
type PreparedMessage = websocket.PreparedMessage

// NewPreparedMessage constructs a PreparedMessage compatible with Connection.WritePreparedMessage.
var NewPreparedMessage = websocket.NewPreparedMessage

// ErrorConnection is the connection error that occurs when websocket connection cannot be established.
var ErrorConnection = errors.New("couldn't establish connection to web socket")

var (
	// ErrConnectionNotFound indicates the target connection ID is not registered in the hub.
	ErrConnectionNotFound = errors.New("websocket connection not found")
	// ErrConnectionClosed indicates the connection has been closed or is shutting down.
	ErrConnectionClosed = fmt.Errorf("websocket connection closed: %w", net.ErrClosed)
	// ErrMessageTooLarge indicates an outbound async payload exceeds HubPolicy.MaxMessageBytes.
	ErrMessageTooLarge = errors.New("websocket message exceeds maximum allowed size")
	// ErrSlowConsumer indicates the per-connection outbound async mailbox is full.
	ErrSlowConsumer = errors.New("websocket session outbound mailbox full")
	// ErrWriteLockTimeout indicates acquiring exclusive write ownership timed out (e.g. unclosed NextWriter or stalled write).
	ErrWriteLockTimeout error = writeLockTimeoutError{}
)

type writeLockTimeoutError struct{}

func (writeLockTimeoutError) Error() string   { return "websocket write lock acquisition timed out" }
func (writeLockTimeoutError) Timeout() bool   { return true }
func (writeLockTimeoutError) Temporary() bool { return true }

// The message types are defined in RFC 6455, section 11.8.
const (
	// TextMessage denotes a text data message. The text message payload is
	// interpreted as UTF-8 encoded text data.
	TextMessage = websocket.TextMessage
	// BinaryMessage denotes a binary data message.
	BinaryMessage = websocket.BinaryMessage
	// CloseMessage denotes a close control message.
	CloseMessage = websocket.CloseMessage
	// PingMessage denotes a ping control message.
	PingMessage = websocket.PingMessage
	// PongMessage denotes a pong control message.
	PongMessage = websocket.PongMessage
)

// HubPolicy configures bounded async enqueue, payload limits, write timeouts, and slow-consumer handling.
type HubPolicy struct {
	MailboxSize      int           // Default 64; bounded outbound async queue capacity per connection.
	MaxMessageBytes  int           // Default 1 MiB; max payload bytes accepted by Hub.Send / SendJSON.
	WriteTimeout     time.Duration // Default 10s; per-frame write deadline and write-token acquisition timeout.
	DisconnectOnSlow bool          // Close and evict the connection when its outbound mailbox is full.
}

func normalizeHubPolicy(p HubPolicy) HubPolicy {
	if p.MailboxSize <= 0 {
		p.MailboxSize = DefaultHubMailboxSize
	}
	if p.MaxMessageBytes <= 0 {
		p.MaxMessageBytes = DefaultHubMaxMessageBytes
	}
	if p.WriteTimeout <= 0 {
		p.WriteTimeout = DefaultHubWriteTimeout
	}
	return p
}

// HubDispatcher allows App's supervised actor runtime to service Hub.Send / SendJSON
// via per-connection session actors under ws-sup. When handled is false, ConnectionHub
// falls back to its standalone bounded per-connection writer queue.
type HubDispatcher func(connID string, conn *Connection, messageType int, ownedPayload []byte, policy HubPolicy) (handled bool, err error)

type standaloneOutboundFrame struct {
	messageType int
	payload     []byte
	writeTimout time.Duration
}

// Connection is a wrapper for gorilla websocket connection.
//
// Embedded *websocket.Conn is retained for struct-literal and promoted-method compatibility.
// Direct calls to conn.Conn.* bypass Connection's serialized write ownership and are treated
// as an caller-managed escape hatch; application and framework code should call Connection's
// methods or ConnectionHub.Send / SendJSON.
type Connection struct {
	*websocket.Conn

	initOnce     sync.Once
	writeToken   chan struct{}
	abortOnce    sync.Once
	abortWriteCh chan struct{}

	stateMu               sync.Mutex
	writeTimeout          time.Duration
	explicitWriteDeadline time.Time
	closed                bool
	disconnectOnce        sync.Once
	onDisconnect          func(error)

	inRead   atomic.Int32
	draining atomic.Bool

	standaloneMu     sync.Mutex
	standaloneQueue  chan standaloneOutboundFrame
	standaloneStopCh chan struct{}
	standaloneDoneCh chan struct{}
}

func (w *Connection) ensureInit() {
	if w == nil {
		return
	}
	w.initOnce.Do(func() {
		w.writeToken = make(chan struct{}, 1)
		w.writeToken <- struct{}{}
		w.abortWriteCh = make(chan struct{})
	})
}

// ConfigureClient attaches optional write deadline and disconnect notification
// hooks for managed outbound WebSocket clients.
func (w *Connection) ConfigureClient(writeTimeout time.Duration, onDisconnect func(error)) {
	if w == nil {
		return
	}
	w.ensureInit()
	w.stateMu.Lock()
	w.writeTimeout = writeTimeout
	w.onDisconnect = onDisconnect
	w.stateMu.Unlock()
}

// SetWriteTimeout sets the per-write deadline and write-lock acquisition budget
// used when no explicit SetWriteDeadline is active.
func (w *Connection) SetWriteTimeout(writeTimeout time.Duration) {
	if w == nil {
		return
	}
	w.ensureInit()
	w.stateMu.Lock()
	w.writeTimeout = writeTimeout
	w.stateMu.Unlock()
}

// IsClosed reports whether Close or ForceClose has been initiated on Connection.
func (w *Connection) IsClosed() bool {
	if w == nil {
		return true
	}
	w.ensureInit()
	select {
	case <-w.abortWriteCh:
		return true
	default:
		return false
	}
}

// IsReading reports whether a caller is currently inside Connection.ReadMessage or Connection.Bind.
func (w *Connection) IsReading() bool {
	if w == nil {
		return false
	}
	return w.inRead.Load() > 0
}

// BeginDrain marks the connection as draining for graceful shutdown: new ReadMessage/Bind turns
// return immediately, and any goroutine currently blocked in ReadMessage is woken via SetReadDeadline,
// while an already mid-turn handler may still complete its final synchronous response write.
func (w *Connection) BeginDrain() {
	if w == nil {
		return
	}
	w.ensureInit()
	w.draining.Store(true)
	if w.Conn != nil && w.Conn.UnderlyingConn() != nil {
		_ = w.Conn.SetReadDeadline(time.Now())
	}
}

func (w *Connection) abortWrites() {
	if w == nil {
		return
	}
	w.ensureInit()
	w.abortOnce.Do(func() {
		w.stateMu.Lock()
		w.closed = true
		w.stateMu.Unlock()
		close(w.abortWriteCh)
	})
	w.stopStandalonePump()
}

func (w *Connection) writeLockBudget(targetDeadline time.Time) (time.Duration, bool) {
	if !targetDeadline.IsZero() {
		rem := time.Until(targetDeadline)
		if rem <= 0 {
			return 0, true
		}
		return rem, false
	}
	w.stateMu.Lock()
	explicit := w.explicitWriteDeadline
	timeout := w.writeTimeout
	w.stateMu.Unlock()

	if !explicit.IsZero() {
		rem := time.Until(explicit)
		if rem <= 0 {
			return 0, true
		}
		return rem, false
	}
	if timeout > 0 {
		return timeout, false
	}
	return defaultWriteLockTimeout, false
}

func (w *Connection) acquireWrite(targetDeadline time.Time) error {
	if w == nil || w.Conn == nil {
		return ErrorConnection
	}
	w.ensureInit()

	select {
	case <-w.abortWriteCh:
		return ErrConnectionClosed
	default:
	}

	budget, expired := w.writeLockBudget(targetDeadline)
	if expired {
		return ErrWriteLockTimeout
	}

	// Fast path when uncontended.
	select {
	case <-w.abortWriteCh:
		return ErrConnectionClosed
	case <-w.writeToken:
		select {
		case <-w.abortWriteCh:
			w.releaseWrite()
			return ErrConnectionClosed
		default:
			return nil
		}
	default:
	}

	timer := time.NewTimer(budget)
	defer timer.Stop()

	select {
	case <-w.abortWriteCh:
		return ErrConnectionClosed
	case <-w.writeToken:
		select {
		case <-w.abortWriteCh:
			w.releaseWrite()
			return ErrConnectionClosed
		default:
			return nil
		}
	case <-timer.C:
		return ErrWriteLockTimeout
	}
}

func (w *Connection) acquireWriteConfig() error {
	if w == nil || w.Conn == nil {
		return ErrorConnection
	}
	w.ensureInit()

	select {
	case <-w.abortWriteCh:
		return ErrConnectionClosed
	default:
	}

	// Fast path when uncontended.
	select {
	case <-w.abortWriteCh:
		return ErrConnectionClosed
	case <-w.writeToken:
		select {
		case <-w.abortWriteCh:
			w.releaseWrite()
			return ErrConnectionClosed
		default:
			return nil
		}
	default:
	}

	w.stateMu.Lock()
	budget := w.writeTimeout
	w.stateMu.Unlock()
	if budget <= 0 {
		budget = defaultWriteLockTimeout
	}

	timer := time.NewTimer(budget)
	defer timer.Stop()

	select {
	case <-w.abortWriteCh:
		return ErrConnectionClosed
	case <-w.writeToken:
		select {
		case <-w.abortWriteCh:
			w.releaseWrite()
			return ErrConnectionClosed
		default:
			return nil
		}
	case <-timer.C:
		return ErrWriteLockTimeout
	}
}

func (w *Connection) releaseWrite() {
	if w == nil {
		return
	}
	select {
	case w.writeToken <- struct{}{}:
	default:
	}
}

func (w *Connection) applyAutoWriteDeadlineLocked() {
	w.stateMu.Lock()
	explicit := w.explicitWriteDeadline
	timeout := w.writeTimeout
	w.stateMu.Unlock()

	if explicit.IsZero() && timeout > 0 && w.Conn != nil && w.Conn.UnderlyingConn() != nil {
		_ = w.Conn.SetWriteDeadline(time.Now().Add(timeout))
	}
}

func isSerializationOnlyError(err error) bool {
	if err == nil {
		return false
	}
	var unsupportedType *json.UnsupportedTypeError
	var unsupportedVal *json.UnsupportedValueError
	var marshalerErr *json.MarshalerError
	return errors.As(err, &unsupportedType) ||
		errors.As(err, &unsupportedVal) ||
		errors.As(err, &marshalerErr)
}

func (w *Connection) reportTransportError(err error) {
	if w == nil || err == nil || isSerializationOnlyError(err) {
		return
	}
	w.disconnectOnce.Do(func() {
		w.stateMu.Lock()
		cb := w.onDisconnect
		w.stateMu.Unlock()
		if cb != nil {
			func() {
				defer func() { _ = recover() }()
				cb(err)
			}()
		}
	})
}

type WSUpgrader struct {
	Upgrader Upgrader
}

// NewWSUpgrader initialize a new websocket upgrader that upgrades an incoming http request
// to a websocket connection. It takes in Options that can be used to customize the upgraded connections.
func NewWSUpgrader(opts ...Options) *WSUpgrader {
	defaultUpgrader := &websocket.Upgrader{}
	for _, opt := range opts {
		opt(defaultUpgrader)
	}

	return &WSUpgrader{
		Upgrader: defaultUpgrader,
	}
}

func (*Connection) Context() context.Context {
	return context.TODO() // Implement proper context handling if needed
}

func (*Connection) Param(_ string) string {
	return "" // Not applicable for WebSocket, can be implemented if needed
}

func (*Connection) PathParam(_ string) string {
	return "" // Not applicable for WebSocket, can be implemented if needed
}

func (w *Connection) Bind(v any) error {
	if w == nil || w.Conn == nil {
		return ErrorConnection
	}
	if w.draining.Load() {
		return ErrConnectionClosed
	}

	var (
		message []byte
		err     error
	)
	if !func() bool {
		w.inRead.Add(1)
		defer w.inRead.Add(-1)
		if w.draining.Load() {
			return false
		}
		_, message, err = w.Conn.ReadMessage()
		return true
	}() {
		return ErrConnectionClosed
	}
	if err != nil {
		w.reportTransportError(err)
		return err
	}

	switch v := v.(type) {
	case *string:
		*v = string(message)
	default:
		return json.Unmarshal(message, v)
	}

	return nil
}

// WriteMessage writes the data on the underlying ws connection synchronously under exclusive write ownership.
func (w *Connection) WriteMessage(messageType int, data []byte) error {
	if err := w.acquireWrite(time.Time{}); err != nil {
		return err
	}
	var err error
	func() {
		defer w.releaseWrite()
		w.applyAutoWriteDeadlineLocked()
		err = w.Conn.WriteMessage(messageType, data)
	}()

	if err != nil {
		w.reportTransportError(err)
	}
	return err
}

// WriteJSON marshals v as JSON and writes it synchronously under exclusive write ownership.
func (w *Connection) WriteJSON(v any) error {
	if err := w.acquireWrite(time.Time{}); err != nil {
		return err
	}
	var err error
	func() {
		defer w.releaseWrite()
		var buf bytes.Buffer
		if encErr := json.NewEncoder(&buf).Encode(v); encErr != nil {
			err = encErr
			return
		}
		w.applyAutoWriteDeadlineLocked()
		err = w.Conn.WriteMessage(TextMessage, buf.Bytes())
	}()

	if err != nil {
		w.reportTransportError(err)
	}
	return err
}

// WritePreparedMessage writes pm synchronously under exclusive write ownership.
func (w *Connection) WritePreparedMessage(pm *PreparedMessage) error {
	if err := w.acquireWrite(time.Time{}); err != nil {
		return err
	}
	var err error
	func() {
		defer w.releaseWrite()
		w.applyAutoWriteDeadlineLocked()
		err = w.Conn.WritePreparedMessage(pm)
	}()

	if err != nil {
		w.reportTransportError(err)
	}
	return err
}

// managedFrameWriter wraps Gorilla's NextWriter frame writer and holds Connection's
// exclusive write token until Close completes, preventing interleaved frame corruption.
type managedFrameWriter struct {
	conn      *Connection
	mu        sync.Mutex
	inner     io.WriteCloser
	closed    bool
	closeOnce sync.Once
	closeErr  error
}

func (mw *managedFrameWriter) Write(p []byte) (int, error) {
	mw.mu.Lock()
	defer mw.mu.Unlock()
	if mw.closed || mw.inner == nil {
		return 0, io.ErrClosedPipe
	}
	if mw.conn.IsClosed() {
		return 0, ErrConnectionClosed
	}
	mw.conn.applyAutoWriteDeadlineLocked()
	n, err := mw.inner.Write(p)
	if err != nil {
		mw.conn.reportTransportError(err)
	}
	return n, err
}

func (mw *managedFrameWriter) Close() error {
	mw.closeOnce.Do(func() {
		mw.mu.Lock()
		mw.closed = true
		inner := mw.inner
		mw.inner = nil
		mw.mu.Unlock()

		var err error
		func() {
			defer mw.conn.releaseWrite()
			if inner != nil {
				mw.conn.applyAutoWriteDeadlineLocked()
				err = inner.Close()
			}
		}()
		if err != nil {
			mw.conn.reportTransportError(err)
		}
		mw.closeErr = err
	})
	return mw.closeErr
}

// NextWriter returns a managed frame writer holding exclusive write ownership until Close is called.
func (w *Connection) NextWriter(messageType int) (io.WriteCloser, error) {
	if err := w.acquireWrite(time.Time{}); err != nil {
		return nil, err
	}
	var (
		writer io.WriteCloser
		err    error
		ok     bool
	)
	func() {
		defer func() {
			if !ok {
				w.releaseWrite()
			}
		}()
		w.applyAutoWriteDeadlineLocked()
		writer, err = w.Conn.NextWriter(messageType)
		if err == nil {
			ok = true
		}
	}()
	if err != nil {
		w.reportTransportError(err)
		return nil, err
	}
	return &managedFrameWriter{
		conn:  w,
		inner: writer,
	}, nil
}

// WriteControl writes a control frame (Close, Ping, Pong) with an independent short deadline.
// Gorilla permits WriteControl concurrently with data writes, so it never blocks behind writeToken.
func (w *Connection) WriteControl(messageType int, data []byte, deadline time.Time) error {
	if w == nil || w.Conn == nil || w.Conn.UnderlyingConn() == nil {
		return ErrorConnection
	}
	if deadline.IsZero() {
		deadline = time.Now().Add(defaultControlWriteTimeout)
	}
	err := w.Conn.WriteControl(messageType, data, deadline)
	if err != nil && !errors.Is(err, websocket.ErrCloseSent) {
		w.reportTransportError(err)
	}
	return err
}

// EnableWriteCompression serializes compression state changes with active data writes.
func (w *Connection) EnableWriteCompression(enable bool) {
	if err := w.acquireWriteConfig(); err != nil {
		return
	}
	defer w.releaseWrite()
	w.Conn.EnableWriteCompression(enable)
}

// SetCompressionLevel serializes compression level updates with active data writes.
func (w *Connection) SetCompressionLevel(level int) error {
	if err := w.acquireWriteConfig(); err != nil {
		return err
	}
	defer w.releaseWrite()
	return w.Conn.SetCompressionLevel(level)
}

// ReadMessage reads the next message from the websocket connection.
//
// This method is thread-safe and can be called concurrently with WriteMessage.
func (w *Connection) ReadMessage() (messageType int, p []byte, err error) {
	if w == nil || w.Conn == nil {
		return -1, nil, ErrorConnection
	}
	if w.draining.Load() {
		return -1, nil, ErrConnectionClosed
	}

	if !func() bool {
		w.inRead.Add(1)
		defer w.inRead.Add(-1)
		if w.draining.Load() {
			return false
		}
		messageType, p, err = w.Conn.ReadMessage()
		return true
	}() {
		return -1, nil, ErrConnectionClosed
	}
	if err != nil {
		w.reportTransportError(err)
	}
	return messageType, p, err
}

// SetReadDeadline sets the read deadline for the websocket connection.
func (w *Connection) SetReadDeadline(t time.Time) error {
	if w == nil || w.Conn == nil {
		return ErrorConnection
	}
	return w.Conn.SetReadDeadline(t)
}

// SetWriteDeadline sets the write deadline for the websocket connection under exclusive write ownership.
func (w *Connection) SetWriteDeadline(t time.Time) error {
	if err := w.acquireWriteConfig(); err != nil {
		return err
	}
	defer w.releaseWrite()

	w.stateMu.Lock()
	w.explicitWriteDeadline = t
	w.stateMu.Unlock()

	return w.Conn.SetWriteDeadline(t)
}

// Close aborts queued write-lock waiters and closes the underlying WebSocket connection.
func (w *Connection) Close() error {
	if w == nil {
		return ErrorConnection
	}
	w.abortWrites()
	if w.Conn == nil || w.Conn.UnderlyingConn() == nil {
		return ErrorConnection
	}
	return w.Conn.Close()
}

// ForceClose immediately expires read/write deadlines on the underlying transport socket
// and closes the connection, interrupting any stalled network write or blocked ReadMessage.
func (w *Connection) ForceClose() error {
	if w == nil {
		return ErrorConnection
	}
	w.draining.Store(true)
	w.abortWrites()
	if w.Conn == nil || w.Conn.UnderlyingConn() == nil {
		return ErrorConnection
	}
	now := time.Now()
	_ = w.Conn.UnderlyingConn().SetDeadline(now)
	if nc := w.Conn.UnderlyingConn(); nc != nil {
		_ = nc.Close()
	}
	return w.Conn.Close()
}

func (w *Connection) enqueueStandalone(frame standaloneOutboundFrame, mailboxSize int) error {
	w.ensureInit()
	if w.IsClosed() {
		return ErrConnectionClosed
	}

	w.standaloneMu.Lock()
	if w.IsClosed() {
		w.standaloneMu.Unlock()
		return ErrConnectionClosed
	}
	if w.standaloneQueue == nil {
		if mailboxSize <= 0 {
			mailboxSize = DefaultHubMailboxSize
		}
		q := make(chan standaloneOutboundFrame, mailboxSize)
		stopCh := make(chan struct{})
		doneCh := make(chan struct{})
		w.standaloneQueue = q
		w.standaloneStopCh = stopCh
		w.standaloneDoneCh = doneCh

		go func() {
			defer close(doneCh)
			for {
				select {
				case <-stopCh:
					return
				case <-w.abortWriteCh:
					return
				case item := <-q:
					if item.writeTimout > 0 {
						w.stateMu.Lock()
						if w.writeTimeout == 0 {
							w.writeTimeout = item.writeTimout
						}
						w.stateMu.Unlock()
					}
					if err := w.WriteMessage(item.messageType, item.payload); err != nil {
						return
					}
				}
			}
		}()
	}
	q := w.standaloneQueue
	stopCh := w.standaloneStopCh
	w.standaloneMu.Unlock()

	select {
	case <-stopCh:
		return ErrConnectionClosed
	case <-w.abortWriteCh:
		return ErrConnectionClosed
	default:
	}

	select {
	case <-stopCh:
		return ErrConnectionClosed
	case <-w.abortWriteCh:
		return ErrConnectionClosed
	case q <- frame:
		return nil
	default:
		return ErrSlowConsumer
	}
}

func (w *Connection) stopStandalonePump() {
	w.standaloneMu.Lock()
	stopCh := w.standaloneStopCh
	w.standaloneStopCh = nil
	w.standaloneMu.Unlock()
	if stopCh != nil {
		close(stopCh)
	}
}

// WaitAsyncWrites waits for any standalone async write pump goroutine on this connection to exit.
func (w *Connection) WaitAsyncWrites(ctx context.Context) error {
	if w == nil {
		return nil
	}
	w.standaloneMu.Lock()
	doneCh := w.standaloneDoneCh
	w.standaloneMu.Unlock()
	if doneCh == nil {
		return nil
	}
	if ctx == nil {
		<-doneCh
		return nil
	}
	select {
	case <-doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (*Connection) HostName() string {
	return "" // Not applicable for WebSocket, can be implemented if needed
}

// Manager is a websocket manager that handles the upgrader and manages all
// active connections through ConnectionHub.
type Manager struct {
	ConnectionHub
	WebSocketUpgrader *WSUpgrader
}

// ConnectionHub stores and provides functionality to work with
// all active connections with websocket clients.
type ConnectionHub struct {
	mu                   sync.RWMutex
	WebSocketConnections map[string]*Connection
	policy               HubPolicy
	policyConfigured     bool
	admissionClosedFn    func() bool
	dispatcher           HubDispatcher
}

// New initializes a new websocket manager with default websocket upgrader.
func New() *Manager {
	return &Manager{
		WebSocketUpgrader: NewWSUpgrader(),
		ConnectionHub: ConnectionHub{
			mu:                   sync.RWMutex{},
			WebSocketConnections: make(map[string]*Connection),
			policy:               normalizeHubPolicy(HubPolicy{}),
			policyConfigured:     true,
		},
	}
}

// ConfigureHub updates the bounded outbound policy for Hub.Send and Hub.SendJSON.
func (h *ConnectionHub) ConfigureHub(policy HubPolicy) {
	if h == nil {
		return
	}
	normalized := normalizeHubPolicy(policy)
	h.mu.Lock()
	h.policy = normalized
	h.policyConfigured = true
	h.mu.Unlock()
}

// HubPolicy returns the normalized HubPolicy active on ConnectionHub.
func (h *ConnectionHub) HubPolicy() HubPolicy {
	if h == nil {
		return normalizeHubPolicy(HubPolicy{})
	}
	h.mu.RLock()
	configured := h.policyConfigured
	policy := h.policy
	h.mu.RUnlock()
	if !configured {
		return normalizeHubPolicy(HubPolicy{})
	}
	return policy
}

// SetAdmissionCheck binds an intake-closed predicate so new upgrades and async sends
// stop immediately when application shutdown begins.
func (h *ConnectionHub) SetAdmissionCheck(fn func() bool) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.admissionClosedFn = fn
	h.mu.Unlock()
}

// IsAdmissionClosed reports whether WebSocket intake has been closed for shutdown.
func (h *ConnectionHub) IsAdmissionClosed() bool {
	if h == nil {
		return false
	}
	h.mu.RLock()
	fn := h.admissionClosedFn
	h.mu.RUnlock()
	return fn != nil && fn()
}

// SetHubDispatcher attaches App's actor-backed session dispatcher for Hub.Send / SendJSON.
func (h *ConnectionHub) SetHubDispatcher(fn HubDispatcher) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.dispatcher = fn
	h.mu.Unlock()
}

// Send performs a bounded asynchronous enqueue of a copy of payload to connID.
// A nil return indicates only that the message was accepted into the session's bounded
// outbound queue, NOT that the remote peer received it; callers needing synchronous network
// write completion must call conn.WriteMessage directly.
func (h *ConnectionHub) Send(connID string, messageType int, payload []byte) error {
	if h == nil {
		return ErrConnectionNotFound
	}
	if len(payload) > h.HubPolicy().MaxMessageBytes {
		return ErrMessageTooLarge
	}
	var owned []byte
	if payload != nil {
		owned = bytes.Clone(payload)
	}
	return h.sendOwned(connID, messageType, owned)
}

// SendJSON marshals v to JSON and performs a bounded asynchronous enqueue to connID.
// Like Send, returning nil indicates bounded queue admission rather than remote delivery.
func (h *ConnectionHub) SendJSON(connID string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return h.sendOwned(connID, TextMessage, data)
}

func (h *ConnectionHub) sendOwned(connID string, messageType int, ownedPayload []byte) error {
	if h == nil {
		return ErrConnectionNotFound
	}
	if h.IsAdmissionClosed() {
		return ErrConnectionClosed
	}

	h.mu.RLock()
	policy := h.policy
	if !h.policyConfigured {
		policy = normalizeHubPolicy(HubPolicy{})
	}
	conn, exists := h.WebSocketConnections[connID]
	dispatcher := h.dispatcher
	h.mu.RUnlock()

	if len(ownedPayload) > policy.MaxMessageBytes {
		return ErrMessageTooLarge
	}
	if !exists || conn == nil || conn.Conn == nil {
		return ErrConnectionNotFound
	}
	if conn.IsClosed() {
		return ErrConnectionClosed
	}

	if dispatcher != nil {
		handled, err := dispatcher(connID, conn, messageType, ownedPayload, policy)
		if handled {
			if errors.Is(err, ErrSlowConsumer) && policy.DisconnectOnSlow {
				h.evictSlowConnection(connID, conn)
			}
			return err
		}
	}

	err := conn.enqueueStandalone(standaloneOutboundFrame{
		messageType: messageType,
		payload:     ownedPayload,
		writeTimout: policy.WriteTimeout,
	}, policy.MailboxSize)
	if errors.Is(err, ErrSlowConsumer) && policy.DisconnectOnSlow {
		h.evictSlowConnection(connID, conn)
	}
	return err
}

func (h *ConnectionHub) evictSlowConnection(connID string, target *Connection) {
	if h == nil || target == nil {
		return
	}
	h.mu.Lock()
	curr, ok := h.WebSocketConnections[connID]
	if ok && curr == target {
		delete(h.WebSocketConnections, connID)
	}
	h.mu.Unlock()

	closeFrame := websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "slow consumer")
	_ = target.WriteControl(CloseMessage, closeFrame, time.Now().Add(200*time.Millisecond))
	_ = target.ForceClose()
}

// Upgrade calls the upgrader to upgrade an http connection to a websocket connection.
func (u *WSUpgrader) Upgrade(w http.ResponseWriter, r *http.Request,
	responseHeader http.Header) (*websocket.Conn, error) {
	return u.Upgrader.Upgrade(w, r, responseHeader)
}

// GetWebsocketConnection returns a websocket connection which has been initialized in the middleware.
func (ws *Manager) GetWebsocketConnection(connID string) *Connection {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	return ws.WebSocketConnections[connID]
}

// ListConnections returns a list of all active WebSocket connection IDs.
func (ws *Manager) ListConnections() []string {
	ws.mu.RLock()
	defer ws.mu.RUnlock()

	connections := make([]string, 0, len(ws.WebSocketConnections))
	for connID := range ws.WebSocketConnections {
		connections = append(connections, connID)
	}

	return connections
}

// GetConnectionByServiceName retrieves a WebSocket connection by its service name.
func (ws *Manager) GetConnectionByServiceName(serviceName string) *Connection {
	ws.mu.RLock()
	defer ws.mu.RUnlock()

	return ws.WebSocketConnections[serviceName]
}

// AddWebsocketConnection add a new connection with the connection id key.
func (ws *Manager) AddWebsocketConnection(connID string, conn *Connection) {
	ws.mu.Lock()
	prev := ws.WebSocketConnections[connID]
	ws.WebSocketConnections[connID] = conn
	ws.mu.Unlock()

	if prev != nil && prev != conn {
		_ = prev.Close()
	}
}

// CloseConnection closes a websocket connection and then removes it from the connection hub.
func (ws *Manager) CloseConnection(connID string) {
	ws.mu.Lock()
	conn, ok := ws.WebSocketConnections[connID]
	if ok {
		delete(ws.WebSocketConnections, connID)
	}
	ws.mu.Unlock()

	if ok && conn != nil {
		_ = conn.Close()
	}
}

// CloseConnectionIfMatch closes and deletes connID only if it currently maps to target.
func (ws *Manager) CloseConnectionIfMatch(connID string, target *Connection) bool {
	if ws == nil || target == nil {
		return false
	}
	ws.mu.Lock()
	curr, ok := ws.WebSocketConnections[connID]
	if !ok || curr != target {
		ws.mu.Unlock()
		return false
	}
	delete(ws.WebSocketConnections, connID)
	ws.mu.Unlock()

	_ = target.Close()
	return true
}

func (*Connection) Params(string) []string {
	return nil
}
