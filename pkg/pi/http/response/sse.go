package response

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultSSEKeepAlive        = 15 * time.Second
	defaultSSEKeepAliveComment = "keepalive"
	defaultSSEContentType      = "text/event-stream; charset=utf-8"
)

var errSSEInvalidField = errors.New("sse: event and id must not contain CR or LF")

// SSEFunc writes Server-Sent Events to writer until completion or context cancellation.
type SSEFunc func(ctx context.Context, sse *SSEWriter) error

// SSE represents a Server-Sent Events response stream (HTTP 200).
type SSE struct {
	Headers map[string]string
	Run     func(ctx context.Context, w io.Writer, flusher http.Flusher) error
}

// SSEEvent represents a single Server-Sent Events frame.
type SSEEvent struct {
	ID    string
	Event string
	Data  any
	Retry time.Duration
}

type sseConfig struct {
	keepAlive        time.Duration
	keepAliveComment string
	headers          map[string]string
}

// SSEOption configures an *SSE returned by NewSSE.
type SSEOption func(*sseConfig)

// WithSSEKeepAlive sets the interval for automatic SSE keepalive comment frames.
// Passing interval <= 0 disables the keepalive ticker.
func WithSSEKeepAlive(interval time.Duration) SSEOption {
	return func(cfg *sseConfig) {
		if interval <= 0 {
			cfg.keepAlive = 0
			return
		}
		cfg.keepAlive = interval
	}
}

// WithSSEHeaders merges custom HTTP headers onto the default SSE headers.
// Content-Type cannot be overridden via WithSSEHeaders.
func WithSSEHeaders(headers map[string]string) SSEOption {
	return func(cfg *sseConfig) {
		if len(headers) == 0 {
			return
		}
		if cfg.headers == nil {
			cfg.headers = make(map[string]string, len(headers))
		}
		for k, v := range headers {
			if k == "" || strings.EqualFold(k, "Content-Type") {
				continue
			}
			for existing := range cfg.headers {
				if strings.EqualFold(existing, k) && existing != k {
					delete(cfg.headers, existing)
				}
			}
			cfg.headers[k] = v
		}
	}
}

// SSEWriter writes thread-safe, flushed Server-Sent Events frames to an io.Writer.
type SSEWriter struct {
	mu      sync.Mutex
	w       io.Writer
	flusher http.Flusher
}

// Send formats and flushes a single SSEEvent frame.
func (s *SSEWriter) Send(event SSEEvent) error {
	if s == nil || s.w == nil {
		return errors.New("sse: writer is nil")
	}
	if strings.ContainsAny(event.ID, "\r\n") || strings.ContainsAny(event.Event, "\r\n") {
		return errSSEInvalidField
	}

	var buf bytes.Buffer
	if event.ID != "" {
		buf.WriteString("id: ")
		buf.WriteString(event.ID)
		buf.WriteByte('\n')
	}
	if event.Event != "" {
		buf.WriteString("event: ")
		buf.WriteString(event.Event)
		buf.WriteByte('\n')
	}
	if event.Retry > 0 {
		if ms := event.Retry.Milliseconds(); ms > 0 {
			buf.WriteString("retry: ")
			buf.WriteString(strconv.FormatInt(ms, 10))
			buf.WriteByte('\n')
		}
	}

	switch v := event.Data.(type) {
	case nil:
		buf.WriteString("data:\n")
	case string:
		writeSSEDataLines(&buf, v)
	case []byte:
		writeSSEDataLines(&buf, string(v))
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		buf.WriteString("data: ")
		buf.Write(b)
		buf.WriteByte('\n')
	}
	buf.WriteByte('\n')

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.w.Write(buf.Bytes()); err != nil {
		return err
	}
	return s.flushLocked()
}

// SendData sends an unnamed SSE event containing only data.
func (s *SSEWriter) SendData(data any) error {
	return s.Send(SSEEvent{Data: data})
}

// SendEvent sends a named SSE event with data.
func (s *SSEWriter) SendEvent(event string, data any) error {
	return s.Send(SSEEvent{Event: event, Data: data})
}

// Comment writes an SSE comment frame (prefixed with ": ") and flushes immediately.
func (s *SSEWriter) Comment(text string) error {
	if s == nil || s.w == nil {
		return errors.New("sse: writer is nil")
	}
	var buf bytes.Buffer
	lines := splitSSELines(text)
	for _, line := range lines {
		buf.WriteString(": ")
		buf.WriteString(line)
		buf.WriteByte('\n')
	}
	buf.WriteByte('\n')

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.w.Write(buf.Bytes()); err != nil {
		return err
	}
	return s.flushLocked()
}

// Flush flushes buffered SSE output to the underlying ResponseWriter or http.Flusher.
func (s *SSEWriter) Flush() error {
	if s == nil || s.w == nil {
		return errors.New("sse: writer is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushLocked()
}

func (s *SSEWriter) flushLocked() error {
	if rw, ok := s.w.(http.ResponseWriter); ok {
		err := http.NewResponseController(rw).Flush()
		if err != nil && !errors.Is(err, http.ErrNotSupported) {
			return err
		}
		if err == nil {
			return nil
		}
	}
	if s.flusher != nil {
		s.flusher.Flush()
		return nil
	}
	if f, ok := s.w.(http.Flusher); ok {
		f.Flush()
	}
	return nil
}

func writeSSEDataLines(buf *bytes.Buffer, text string) {
	for _, line := range splitSSELines(text) {
		buf.WriteString("data: ")
		buf.WriteString(line)
		buf.WriteByte('\n')
	}
}

func splitSSELines(text string) []string {
	normalized := strings.ReplaceAll(text, "\r\n", "\n")
	normalized = strings.ReplaceAll(normalized, "\r", "\n")
	return strings.Split(normalized, "\n")
}

// NewSSE constructs an *SSE configured for Server-Sent Events with optional
// keepalive comments and custom headers.
func NewSSE(fn SSEFunc, opts ...SSEOption) *SSE {
	cfg := sseConfig{
		keepAlive:        defaultSSEKeepAlive,
		keepAliveComment: defaultSSEKeepAliveComment,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	hdrs := map[string]string{
		"Content-Type":      defaultSSEContentType,
		"Cache-Control":     "no-cache, no-transform",
		"X-Accel-Buffering": "no",
	}
	for k, v := range cfg.headers {
		if k == "" || strings.EqualFold(k, "Content-Type") {
			continue
		}
		for existing := range hdrs {
			if strings.EqualFold(existing, k) && existing != k {
				delete(hdrs, existing)
			}
		}
		if v == "" {
			delete(hdrs, k)
			continue
		}
		hdrs[k] = v
	}

	if fn == nil {
		return &SSE{
			Headers: hdrs,
			Run:     nil,
		}
	}

	keepAlive := cfg.keepAlive
	keepAliveComment := cfg.keepAliveComment
	return &SSE{
		Headers: hdrs,
		Run: func(ctx context.Context, w io.Writer, flusher http.Flusher) error {
			if ctx == nil {
				ctx = context.Background()
			}
			sseCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			if flusher == nil {
				flusher, _ = w.(http.Flusher)
			}
			writer := &SSEWriter{w: w, flusher: flusher}
			if keepAlive > 0 {
				ticker := time.NewTicker(keepAlive)
				done := make(chan struct{})
				stopped := make(chan struct{})
				go func() {
					defer close(stopped)
					defer ticker.Stop()
					for {
						select {
						case <-done:
							return
						case <-sseCtx.Done():
							return
						case <-ticker.C:
							if err := writer.Comment(keepAliveComment); err != nil {
								cancel()
								return
							}
						}
					}
				}()
				defer func() {
					close(done)
					<-stopped
				}()
			}
			return fn(sseCtx, writer)
		},
	}
}
