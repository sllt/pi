package websocket

import (
	"bufio"
	"compress/flate"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gWebsocket "github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newPairedWSConnections(t *testing.T) (serverConn *Connection, clientRaw *gWebsocket.Conn, cleanup func()) {
	t.Helper()

	serverReady := make(chan *Connection, 1)
	doneHold := make(chan struct{})
	upgrader := gWebsocket.Upgrader{
		CheckOrigin:       func(*http.Request) bool { return true },
		EnableCompression: true,
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		serverReady <- &Connection{Conn: raw}
		<-doneHold
	}))

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	dialer := gWebsocket.Dialer{EnableCompression: true}
	client, resp, err := dialer.Dial(wsURL, nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	require.NoError(t, err)

	sc := <-serverReady
	var once sync.Once
	cleanup = func() {
		once.Do(func() {
			close(doneHold)
			_ = client.Close()
			_ = sc.ForceClose()
			srv.Close()
		})
	}
	t.Cleanup(cleanup)
	return sc, client, cleanup
}

func TestConnection_ConcurrentWritesAndPreparedMessage(t *testing.T) {
	serverConn, clientRaw, _ := newPairedWSConnections(t)
	serverConn.SetWriteTimeout(5 * time.Second)

	pm, err := NewPreparedMessage(TextMessage, []byte("prepared-payload"))
	require.NoError(t, err)

	const workers = 16
	const perWorker = 8
	expectedFrames := workers * perWorker * 3 // WriteMessage + WriteJSON + WritePreparedMessage

	var pongCount atomic.Int32
	clientRaw.SetPingHandler(func(appData string) error {
		pongCount.Add(1)
		return clientRaw.WriteControl(PongMessage, []byte(appData), time.Now().Add(time.Second))
	})

	readDone := make(chan error, 1)
	var received atomic.Int32
	go func() {
		for int(received.Load()) < expectedFrames {
			_ = clientRaw.SetReadDeadline(time.Now().Add(5 * time.Second))
			_, msg, rErr := clientRaw.ReadMessage()
			if rErr != nil {
				readDone <- rErr
				return
			}
			if len(msg) == 0 {
				readDone <- errors.New("empty frame")
				return
			}
			received.Add(1)
		}
		readDone <- nil
	}()

	var wg sync.WaitGroup
	errCh := make(chan error, workers*4)
	for i := range workers {
		id := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range perWorker {
				if id%2 == 0 && j == 0 {
					serverConn.EnableWriteCompression(true)
					_ = serverConn.SetCompressionLevel(flate.BestSpeed)
				}
				if wErr := serverConn.SetWriteDeadline(time.Now().Add(5 * time.Second)); wErr != nil {
					errCh <- wErr
					return
				}
				if wErr := serverConn.WriteMessage(TextMessage, []byte(fmt.Sprintf("msg-%d-%d", id, j))); wErr != nil {
					errCh <- wErr
					return
				}
				if wErr := serverConn.WriteJSON(map[string]int{"w": id, "n": j}); wErr != nil {
					errCh <- wErr
					return
				}
				if wErr := serverConn.WritePreparedMessage(pm); wErr != nil {
					errCh <- wErr
					return
				}
				if j == 0 {
					if cErr := serverConn.WriteControl(PingMessage, []byte("ping"), time.Now().Add(time.Second)); cErr != nil {
						errCh <- cErr
						return
					}
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for wErr := range errCh {
		require.NoError(t, wErr)
	}

	require.NoError(t, <-readDone)
	assert.Equal(t, int32(expectedFrames), received.Load())
}

func TestConnection_NextWriterExclusiveOwnershipAndUnclosedTimeout(t *testing.T) {
	t.Run("interleaved WriteMessage waits until NextWriter closes", func(t *testing.T) {
		serverConn, clientRaw, _ := newPairedWSConnections(t)
		serverConn.SetWriteTimeout(3 * time.Second)

		frames := make(chan string, 2)
		go func() {
			for range 2 {
				_, data, err := clientRaw.ReadMessage()
				if err != nil {
					return
				}
				frames <- string(data)
			}
		}()

		w, err := serverConn.NextWriter(TextMessage)
		require.NoError(t, err)
		_, err = w.Write([]byte("part1-"))
		require.NoError(t, err)

		secondWriteStarted := make(chan struct{})
		secondWriteDone := make(chan error, 1)
		go func() {
			close(secondWriteStarted)
			secondWriteDone <- serverConn.WriteMessage(TextMessage, []byte("second-complete-frame"))
		}()

		<-secondWriteStarted
		time.Sleep(25 * time.Millisecond)
		select {
		case err := <-secondWriteDone:
			t.Fatalf("WriteMessage must wait for NextWriter.Close(), finished early with %v", err)
		default:
		}

		_, err = w.Write([]byte("part2"))
		require.NoError(t, err)
		require.NoError(t, w.Close())
		// Idempotent second Close must not release a second write token.
		require.NoError(t, w.Close())

		require.NoError(t, <-secondWriteDone)
		assert.Equal(t, "part1-part2", <-frames)
		assert.Equal(t, "second-complete-frame", <-frames)
	})

	t.Run("unclosed NextWriter times out contended writer and Close unblocks waiter", func(t *testing.T) {
		serverConn, _, _ := newPairedWSConnections(t)
		serverConn.SetWriteTimeout(60 * time.Millisecond)

		unclosedWriter, err := serverConn.NextWriter(TextMessage)
		require.NoError(t, err)
		_, err = unclosedWriter.Write([]byte("dangling"))
		require.NoError(t, err)

		err = serverConn.WriteMessage(TextMessage, []byte("blocked"))
		require.ErrorIs(t, err, ErrWriteLockTimeout)

		// Now start a waiter with a long timeout and verify Close() immediately unblocks it.
		serverConn.SetWriteTimeout(5 * time.Second)
		waitErrCh := make(chan error, 1)
		go func() {
			waitErrCh <- serverConn.WriteMessage(TextMessage, []byte("aborted-by-close"))
		}()
		time.Sleep(20 * time.Millisecond)
		require.NoError(t, serverConn.Close())

		select {
		case waitErr := <-waitErrCh:
			require.ErrorIs(t, waitErr, ErrConnectionClosed)
		case <-time.After(time.Second):
			t.Fatal("Close() did not unblock queued WriteMessage waiter")
		}
	})
}

func TestConnection_StalledWriteInterruptedByForceCloseAndControlFrame(t *testing.T) {
	// net.Pipe has zero kernel buffer, so any write without a reader stalls immediately!
	srvSide, cliSide := net.Pipe()
	defer srvSide.Close()
	defer cliSide.Close()

	// Perform HTTP WebSocket handshake over net.Pipe.
	upgrader := gWebsocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srvConnCh := make(chan *Connection, 1)
	go func() {
		br := bufio.NewReader(srvSide)
		req, err := http.ReadRequest(br)
		if err != nil {
			return
		}
		hijackRW := &pipeHijackResponseWriter{conn: srvSide, brw: bufio.NewReadWriter(br, bufio.NewWriter(srvSide))}
		raw, err := upgrader.Upgrade(hijackRW, req, nil)
		if err != nil {
			return
		}
		srvConnCh <- &Connection{Conn: raw}
	}()

	dialer := gWebsocket.Dialer{
		NetDialContext: func(context.Context, string, string) (net.Conn, error) {
			return cliSide, nil
		},
	}
	cliRaw, resp, err := dialer.Dial("ws://pipe.local/ws", nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	require.NoError(t, err)
	defer cliRaw.Close()

	srvConn := <-srvConnCh
	srvConn.SetWriteTimeout(10 * time.Second)

	// Stop reading on cliRaw completely so srvConn.WriteMessage blocks inside net.Pipe.Write!
	stalledWriteErr := make(chan error, 1)
	go func() {
		stalledWriteErr <- srvConn.WriteMessage(BinaryMessage, make([]byte, 4096))
	}()

	blockedReadErr := make(chan error, 1)
	go func() {
		_, _, rErr := srvConn.ReadMessage()
		blockedReadErr <- rErr
	}()

	time.Sleep(30 * time.Millisecond)

	// WriteControl must NOT wait for srvConn's 10s writeToken; it uses its own short 30ms deadline.
	ctrlStart := time.Now()
	_ = srvConn.WriteControl(CloseMessage, gWebsocket.FormatCloseMessage(gWebsocket.CloseGoingAway, "bye"), time.Now().Add(30*time.Millisecond))
	assert.Less(t, time.Since(ctrlStart), 500*time.Millisecond, "WriteControl must not block behind 10s writeToken")

	// ForceClose must interrupt BOTH the stalled WriteMessage and the blocked ReadMessage immediately.
	_ = srvConn.ForceClose()

	select {
	case wErr := <-stalledWriteErr:
		require.Error(t, wErr)
	case <-time.After(time.Second):
		t.Fatal("ForceClose did not interrupt stalled WriteMessage")
	}

	select {
	case rErr := <-blockedReadErr:
		require.Error(t, rErr)
	case <-time.After(time.Second):
		t.Fatal("ForceClose did not interrupt blocked ReadMessage")
	}
}

func TestConnectionHub_StandaloneSendOwnershipLimitsAndSlowEviction(t *testing.T) {
	mgr := New()
	mgr.ConfigureHub(HubPolicy{
		MailboxSize:      2,
		MaxMessageBytes:  32,
		WriteTimeout:     2 * time.Second,
		DisconnectOnSlow: false,
	})

	serverConn, clientRaw, _ := newPairedWSConnections(t)
	mgr.AddWebsocketConnection("c1", serverConn)

	// 1. Payload ownership: mutating caller buffer after Send must not corrupt received frame.
	callerBuf := []byte("immutable-hello")
	require.NoError(t, mgr.Send("c1", TextMessage, callerBuf))
	callerBuf[0] = 'X'

	_, got, err := clientRaw.ReadMessage()
	require.NoError(t, err)
	assert.Equal(t, "immutable-hello", string(got))

	// 2. MaxMessageBytes enforcement before allocation/enqueue.
	err = mgr.Send("c1", BinaryMessage, make([]byte, 33))
	require.ErrorIs(t, err, ErrMessageTooLarge)

	// 3. Slow consumer saturation & DisconnectOnSlow eviction using a stalled net.Pipe connection.
	srvSide, cliSide := net.Pipe()
	defer srvSide.Close()
	defer cliSide.Close()

	upgrader := gWebsocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	stalledConnCh := make(chan *Connection, 1)
	go func() {
		br := bufio.NewReader(srvSide)
		req, err := http.ReadRequest(br)
		if err != nil {
			return
		}
		hijackRW := &pipeHijackResponseWriter{conn: srvSide, brw: bufio.NewReadWriter(br, bufio.NewWriter(srvSide))}
		raw, err := upgrader.Upgrade(hijackRW, req, nil)
		if err == nil {
			stalledConnCh <- &Connection{Conn: raw}
		}
	}()
	dialer := gWebsocket.Dialer{
		NetDialContext: func(context.Context, string, string) (net.Conn, error) { return cliSide, nil },
	}
	cliPipe, resp, err := dialer.Dial("ws://pipe.local/slow", nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	require.NoError(t, err)
	defer cliPipe.Close()

	stalledConn := <-stalledConnCh
	mgr.ConfigureHub(HubPolicy{
		MailboxSize:      1,
		MaxMessageBytes:  64,
		WriteTimeout:     5 * time.Second,
		DisconnectOnSlow: true,
	})
	mgr.AddWebsocketConnection("slow-1", stalledConn)

	var sawSlowErr bool
	for range 10 {
		sErr := mgr.Send("slow-1", TextMessage, []byte("fill"))
		if errors.Is(sErr, ErrSlowConsumer) {
			sawSlowErr = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.True(t, sawSlowErr, "expected ErrSlowConsumer when standalone queue saturates")
	assert.Nil(t, mgr.GetWebsocketConnection("slow-1"), "DisconnectOnSlow must evict connection from Hub")
	assert.True(t, stalledConn.IsClosed(), "DisconnectOnSlow must close slow connection")
}

type pipeHijackResponseWriter struct {
	header http.Header
	conn   net.Conn
	brw    *bufio.ReadWriter
}

func (p *pipeHijackResponseWriter) Header() http.Header {
	if p.header == nil {
		p.header = make(http.Header)
	}
	return p.header
}

func (p *pipeHijackResponseWriter) Write(b []byte) (int, error) {
	return p.conn.Write(b)
}

func (p *pipeHijackResponseWriter) WriteHeader(int) {}

func (p *pipeHijackResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return p.conn, p.brw, nil
}

var _ io.WriteCloser = (*managedFrameWriter)(nil)

type stage4PanickingJSON struct{}

func (stage4PanickingJSON) MarshalJSON() ([]byte, error) {
	panic("stage4-marshal-json-panic")
}

func TestConnection_WriteJSONPanicReleasesWriteTokenAndPreservesStream(t *testing.T) {
	server, clientRaw, _ := newPairedWSConnections(t)
	server.SetWriteTimeout(30 * time.Millisecond)

	var recovered any
	func() {
		defer func() {
			recovered = recover()
		}()
		_ = server.WriteJSON(stage4PanickingJSON{})
	}()
	require.Equal(t, "stage4-marshal-json-panic", recovered)

	// Exclusive write ownership must have been released cleanly, and no partial frame
	// should have been opened on the underlying WebSocket stream.
	require.NoError(t, server.SetWriteDeadline(time.Time{}))
	require.NoError(t, server.WriteMessage(TextMessage, []byte("still-healthy")))

	mt, got, err := clientRaw.ReadMessage()
	require.NoError(t, err)
	assert.Equal(t, TextMessage, mt)
	assert.Equal(t, "still-healthy", string(got))
}
