package logging_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/sllt/pi/pkg/pi/logging/remotelogger"
	"github.com/sllt/pi/pkg/pi/testutil"
)

type testLogValuer struct {
	name string
}

func (v testLogValuer) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("name", v.name),
		slog.Int("len", len(v.name)),
	)
}

type nestedLogValuer struct {
	inner slog.LogValuer
}

func (v nestedLogValuer) LogValue() slog.Value {
	return slog.AnyValue(v.inner)
}

func tracedCtx(t *testing.T, hexTrace string) context.Context {
	t.Helper()
	tid, err := trace.TraceIDFromHex(hexTrace)
	require.NoError(t, err)
	sid, err := trace.SpanIDFromHex("2222222222222222")
	require.NoError(t, err)
	return trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
	}))
}

func TestNewSlogHandler_OrderedGroupAndAttrFrames(t *testing.T) {
	var normal, errOut bytes.Buffer
	base := logging.NewWriterLogger(logging.DEBUG, &normal, &errOut)

	// WithAttrs -> WithGroup -> WithAttrs must keep outer attrs ungrouped and nest inner attrs.
	h := logging.NewSlogHandler(base).
		WithAttrs([]slog.Attr{slog.String("a", "1")}).
		WithGroup("g1").
		WithAttrs([]slog.Attr{slog.String("b", "2")}).
		WithGroup("g2")

	sl := slog.New(h)
	ctx := tracedCtx(t, "0123456789abcdef0123456789abcdef")
	sl.InfoContext(ctx, "ordered test",
		slog.String("c", "3"),
		slog.Group("inline_grp", slog.Int("n", 9)),
		slog.Group("", slog.Bool("inlined", true)),
		slog.Any("user", nestedLogValuer{inner: testLogValuer{name: "alice"}}),
		slog.Any("err", errors.New("boom")),
		slog.Float64("nan", math.NaN()),
		slog.Duration("dur", 5*time.Second),
	)

	var entry struct {
		Level   string         `json:"level"`
		TraceID string         `json:"trace_id"`
		Message string         `json:"message"`
		Fields  map[string]any `json:"fields"`
	}
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(normal.Bytes()), &entry))
	assert.Equal(t, "INFO", entry.Level)
	assert.Equal(t, "0123456789abcdef0123456789abcdef", entry.TraceID)
	assert.Equal(t, "ordered test", entry.Message)
	assert.Equal(t, "1", entry.Fields["a"])
	assert.Equal(t, "2", entry.Fields["g1.b"])
	assert.Equal(t, "3", entry.Fields["g1.g2.c"])
	assert.Equal(t, float64(9), entry.Fields["g1.g2.inline_grp.n"])
	assert.Equal(t, true, entry.Fields["g1.g2.inlined"])
	assert.Equal(t, "alice", entry.Fields["g1.g2.user.name"])
	assert.Equal(t, float64(5), entry.Fields["g1.g2.user.len"])
	assert.Equal(t, "boom", entry.Fields["g1.g2.err"])
	assert.Equal(t, "NaN", entry.Fields["g1.g2.nan"])
	assert.Equal(t, float64(5*time.Second), entry.Fields["g1.g2.dur"])
}

func TestNewSlogHandler_EnabledAndLevelRouting(t *testing.T) {
	var normal, errOut bytes.Buffer
	base := logging.NewWriterLogger(logging.WARN, &normal, &errOut)
	h := logging.NewSlogHandler(base)
	ctx := context.Background()

	assert.False(t, h.Enabled(ctx, slog.LevelDebug))
	assert.False(t, h.Enabled(ctx, slog.LevelInfo))
	assert.True(t, h.Enabled(ctx, slog.LevelWarn))
	assert.True(t, h.Enabled(ctx, slog.LevelError))

	sl := slog.New(h)
	sl.Debug("skip debug")
	sl.Info("skip info")
	sl.Warn("kept warn", "k", 1)
	sl.Error("kept error", "k", 2)

	assert.Contains(t, normal.String(), `"kept warn"`)
	assert.NotContains(t, normal.String(), "skip")
	assert.Contains(t, errOut.String(), `"kept error"`)

	base.ChangeLevel(logging.DEBUG)
	assert.True(t, h.Enabled(ctx, slog.LevelDebug))

	assert.False(t, logging.NewSlogHandler(nil).Enabled(ctx, slog.LevelError))
	assert.False(t, logging.NewSlogHandler(logging.Noop()).Enabled(ctx, slog.LevelError))
	assert.True(t, logging.NewSlogHandler(logging.NewMockLogger(logging.INFO)).Enabled(ctx, slog.LevelDebug))
}

func TestNewSlogHandler_PreservesContextLoggerFieldsAndEmptyGroups(t *testing.T) {
	var normal, errOut bytes.Buffer
	base := logging.NewWriterLogger(logging.DEBUG, &normal, &errOut)
	cl := logging.WithContext(context.Background(), base).With("svc", "actor")

	h := logging.NewSlogHandler(cl).WithGroup("empty")
	slog.New(h).Info("no record attrs")

	var entry struct {
		Message string         `json:"message"`
		Fields  map[string]any `json:"fields"`
	}
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(normal.Bytes()), &entry))
	assert.Equal(t, "no record attrs", entry.Message)
	assert.Equal(t, map[string]any{"svc": "actor"}, entry.Fields)
}

func TestRemoteLogger_UnwrapAndStructuredForwarding(t *testing.T) {
	ctx := tracedCtx(t, "abcdef0123456789abcdef0123456789")

	out := testutil.StdoutOutputForFunc(func() {
		rl := remotelogger.New(logging.DEBUG, "", 0)
		if closer, ok := rl.(io.Closer); ok {
			defer closer.Close()
		}

		unwrapper, ok := rl.(interface{ Unwrap() logging.Logger })
		require.True(t, ok, "remoteLogger must implement Unwrap() logging.Logger")
		require.NotNil(t, unwrapper.Unwrap())

		sl, ok := rl.(logging.StructuredLogger)
		require.True(t, ok, "remoteLogger must implement StructuredLogger")

		sl.Infow("direct remote", "k", "v")
		logging.WithContext(ctx, rl).With("req", "r1").Infow("ctx remote", "ok", true)
		slog.New(logging.NewSlogHandler(rl)).WarnContext(ctx, "slog remote", "code", 429)
	})

	lines := bytes.Split(bytes.TrimSpace([]byte(out)), []byte("\n"))
	require.Len(t, lines, 3)

	var direct, viaCtx, viaSlog struct {
		Level   string         `json:"level"`
		TraceID string         `json:"trace_id"`
		Message string         `json:"message"`
		Fields  map[string]any `json:"fields"`
	}
	require.NoError(t, json.Unmarshal(lines[0], &direct))
	assert.Equal(t, "INFO", direct.Level)
	assert.Equal(t, "direct remote", direct.Message)
	assert.Equal(t, map[string]any{"k": "v"}, direct.Fields)

	require.NoError(t, json.Unmarshal(lines[1], &viaCtx))
	assert.Equal(t, "abcdef0123456789abcdef0123456789", viaCtx.TraceID)
	assert.Equal(t, "ctx remote", viaCtx.Message)
	assert.Equal(t, map[string]any{"req": "r1", "ok": true}, viaCtx.Fields)

	require.NoError(t, json.Unmarshal(lines[2], &viaSlog))
	assert.Equal(t, "WARN", viaSlog.Level)
	assert.Equal(t, "abcdef0123456789abcdef0123456789", viaSlog.TraceID)
	assert.Equal(t, "slog remote", viaSlog.Message)
	assert.Equal(t, map[string]any{"code": float64(429)}, viaSlog.Fields)
}

func TestNewSlogHandler_CallerAccuracy(t *testing.T) {
	var normal, errOut bytes.Buffer
	base := logging.NewWriterLogger(logging.DEBUG, &normal, &errOut)
	if cc, ok := base.(logging.CallerConfigurer); ok {
		cc.SetShowCaller(true)
	}

	sl := slog.New(logging.NewSlogHandler(base).WithGroup("actor"))
	_, _, lineBefore, ok := runtime.Caller(0)
	require.True(t, ok)
	sl.Info("slog caller test", "pid", "p1")

	var entry struct {
		Caller  string         `json:"caller"`
		Message string         `json:"message"`
		Fields  map[string]any `json:"fields"`
	}
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(normal.Bytes()), &entry))
	assert.Equal(t, fmt.Sprintf("slog_handler_test.go:%d", lineBefore+2), entry.Caller)
	assert.Equal(t, "slog caller test", entry.Message)
	assert.Equal(t, map[string]any{"actor.pid": "p1"}, entry.Fields)
}
