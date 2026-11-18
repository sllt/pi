package response

import (
	"bytes"
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type lockedBuffer struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	flushes atomic.Int32
	closed  atomic.Bool
	writeCB func()
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed.Load() {
		return 0, errors.New("write after stream Run returned")
	}
	if b.writeCB != nil {
		b.writeCB()
	}
	return b.buf.Write(p)
}

func (b *lockedBuffer) Flush() {
	b.flushes.Add(1)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type mockFlusherRW struct {
	hdr     http.Header
	buf     bytes.Buffer
	flushes int
}

func (m *mockFlusherRW) Header() http.Header {
	if m.hdr == nil {
		m.hdr = make(http.Header)
	}
	return m.hdr
}

func (m *mockFlusherRW) Write(p []byte) (int, error) {
	return m.buf.Write(p)
}

func (m *mockFlusherRW) WriteHeader(int) {}

func (m *mockFlusherRW) Flush() {
	m.flushes++
}

func TestSSEWriter_SendFormatsAndValidation(t *testing.T) {
	t.Run("all_fields_and_data_types", func(t *testing.T) {
		var buf bytes.Buffer
		w := &SSEWriter{w: &buf}

		require.NoError(t, w.Send(SSEEvent{
			ID:    "evt-1",
			Event: "update",
			Retry: 2500 * time.Millisecond,
			Data:  "hello",
		}))
		assert.Equal(t, "id: evt-1\nevent: update\nretry: 2500\ndata: hello\n\n", buf.String())

		buf.Reset()
		require.NoError(t, w.Send(SSEEvent{Data: nil}))
		assert.Equal(t, "data:\n\n", buf.String())

		buf.Reset()
		require.NoError(t, w.SendData("line1\r\nline2\rline3\nline4"))
		assert.Equal(t, "data: line1\ndata: line2\ndata: line3\ndata: line4\n\n", buf.String())

		buf.Reset()
		require.NoError(t, w.SendData([]byte("byte1\r\nbyte2")))
		assert.Equal(t, "data: byte1\ndata: byte2\n\n", buf.String())

		buf.Reset()
		require.NoError(t, w.SendEvent("metrics", struct {
			Count int `json:"count"`
		}{Count: 42}))
		assert.Equal(t, "event: metrics\ndata: {\"count\":42}\n\n", buf.String())

		buf.Reset()
		require.NoError(t, w.Send(SSEEvent{
			Retry: 500 * time.Microsecond, // sub-millisecond ignored
			Data:  "",
		}))
		assert.Equal(t, "data: \n\n", buf.String())
	})

	t.Run("rejects_cr_lf_in_id_and_event", func(t *testing.T) {
		var buf bytes.Buffer
		w := &SSEWriter{w: &buf}

		for _, tc := range []SSEEvent{
			{ID: "bad\nid", Data: "x"},
			{ID: "bad\rid", Data: "x"},
			{Event: "bad\nevent", Data: "x"},
			{Event: "bad\r\nevent", Data: "x"},
		} {
			err := w.Send(tc)
			require.EqualError(t, err, "sse: event and id must not contain CR or LF")
		}
		assert.Empty(t, buf.String())
	})

	t.Run("json_marshal_error_writes_nothing", func(t *testing.T) {
		var buf bytes.Buffer
		w := &SSEWriter{w: &buf}

		err := w.SendData(math.NaN())
		require.Error(t, err)
		assert.Empty(t, buf.String())
	})

	t.Run("comment_multiline_and_flush", func(t *testing.T) {
		rw := &mockFlusherRW{}
		w := &SSEWriter{w: rw}

		require.NoError(t, w.Comment("keepalive"))
		assert.Equal(t, ": keepalive\n\n", rw.buf.String())
		assert.Equal(t, 1, rw.flushes)

		rw.buf.Reset()
		require.NoError(t, w.Comment("a\r\nb\rc"))
		assert.Equal(t, ": a\n: b\n: c\n\n", rw.buf.String())
		assert.Equal(t, 2, rw.flushes)

		require.NoError(t, w.Flush())
		assert.Equal(t, 3, rw.flushes)

		rec := httptest.NewRecorder()
		wRec := &SSEWriter{w: rec}
		require.NoError(t, wRec.SendData("ping"))
		assert.True(t, rec.Flushed)
		assert.Equal(t, "data: ping\n\n", rec.Body.String())
	})
}

func TestNewSSE_OptionsHeadersAndKeepAlive(t *testing.T) {
	t.Run("default_headers_and_custom_merge_without_clobbering_content_type", func(t *testing.T) {
		callerHdrs := map[string]string{
			"Content-Type":  "text/plain",
			"content-type":  "application/json",
			"cache-control": "no-store",
			"X-Stream-ID":   "abc-123",
		}
		s := NewSSE(
			func(ctx context.Context, sse *SSEWriter) error {
				return sse.SendData("ok")
			},
			WithSSEHeaders(callerHdrs),
			WithSSEKeepAlive(0),
		)
		// Mutating caller map after NewSSE must not affect SSE.
		callerHdrs["X-Stream-ID"] = "mutated"

		assert.Equal(t, "text/event-stream; charset=utf-8", s.Headers["Content-Type"])
		assert.Equal(t, "no-store", s.Headers["cache-control"])
		assert.NotContains(t, s.Headers, "Cache-Control")
		assert.NotContains(t, s.Headers, "Connection")
		assert.NotContains(t, s.Headers, "connection")
		assert.Equal(t, "no", s.Headers["X-Accel-Buffering"])
		assert.Equal(t, "abc-123", s.Headers["X-Stream-ID"])
		assert.NotContains(t, s.Headers, "content-type")
	})

	t.Run("nil_callback_returns_sse_with_nil_run", func(t *testing.T) {
		s := NewSSE(nil)
		require.NotNil(t, s)
		assert.Nil(t, s.Run)
		assert.Equal(t, "text/event-stream; charset=utf-8", s.Headers["Content-Type"])
		assert.Equal(t, "no-cache, no-transform", s.Headers["Cache-Control"])
		assert.Equal(t, "no", s.Headers["X-Accel-Buffering"])
		assert.NotContains(t, s.Headers, "Connection")
	})

	t.Run("keepalive_ticker_concurrent_send_and_clean_stop", func(t *testing.T) {
		lb := &lockedBuffer{}
		s := NewSSE(
			func(ctx context.Context, sse *SSEWriter) error {
				var wg sync.WaitGroup
				for i := range 4 {
					wg.Add(1)
					go func(id int) {
						defer wg.Done()
						for range 10 {
							_ = sse.SendEvent("tick", map[string]int{"worker": id})
							time.Sleep(2 * time.Millisecond)
						}
					}(i)
				}
				time.Sleep(35 * time.Millisecond)
				wg.Wait()
				return nil
			},
			WithSSEKeepAlive(8*time.Millisecond),
		)

		require.NoError(t, s.Run(t.Context(), lb, lb))
		lb.closed.Store(true)

		// Give any hypothetical leaked goroutine time to try writing after return.
		time.Sleep(25 * time.Millisecond)

		out := lb.String()
		assert.Contains(t, out, ": keepalive\n\n")
		assert.Contains(t, out, "event: tick\n")
		assert.Greater(t, strings.Count(out, "event: tick\n"), 0)
		assert.Greater(t, lb.flushes.Load(), int32(1))
	})

	t.Run("keepalive_write_failure_cancels_ctx_passed_to_fn", func(t *testing.T) {
		fw := &failingWriter{err: errors.New("broken pipe")}
		var fnCtxErr error
		s := NewSSE(
			func(ctx context.Context, sse *SSEWriter) error {
				select {
				case <-ctx.Done():
					fnCtxErr = ctx.Err()
					return ctx.Err()
				case <-time.After(2 * time.Second):
					return errors.New("timed out waiting for keepalive cancellation")
				}
			},
			WithSSEKeepAlive(5*time.Millisecond),
		)

		err := s.Run(t.Context(), fw, nil)
		require.ErrorIs(t, err, context.Canceled)
		require.ErrorIs(t, fnCtxErr, context.Canceled)
	})
}

type failingWriter struct {
	err error
}

func (f *failingWriter) Write([]byte) (int, error) {
	return 0, f.err
}
