package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/sllt/pi/pkg/pi/testutil"
)

type structuredEmbeddedCtxHolder struct {
	ContextLogger
}

type typedNilError struct {
	msg string
}

func (e *typedNilError) Error() string {
	return e.msg
}

type unwrappingLogger struct {
	Logger
}

func (u *unwrappingLogger) Unwrap() Logger {
	return u.Logger
}

type mockPlainLogger struct {
	mu    sync.Mutex
	level Level
	calls []string
}

func (m *mockPlainLogger) record(level string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, fmt.Sprintf("%s:%v", level, fmt.Sprint(args...)))
}

func (m *mockPlainLogger) Debug(args ...any)          { m.record("DEBUG", args...) }
func (m *mockPlainLogger) Debugf(f string, a ...any)  { m.record("DEBUG", fmt.Sprintf(f, a...)) }
func (m *mockPlainLogger) Log(args ...any)            { m.record("INFO", args...) }
func (m *mockPlainLogger) Logf(f string, a ...any)    { m.record("INFO", fmt.Sprintf(f, a...)) }
func (m *mockPlainLogger) Info(args ...any)           { m.record("INFO", args...) }
func (m *mockPlainLogger) Infof(f string, a ...any)   { m.record("INFO", fmt.Sprintf(f, a...)) }
func (m *mockPlainLogger) Notice(args ...any)         { m.record("NOTICE", args...) }
func (m *mockPlainLogger) Noticef(f string, a ...any) { m.record("NOTICE", fmt.Sprintf(f, a...)) }
func (m *mockPlainLogger) Warn(args ...any)           { m.record("WARN", args...) }
func (m *mockPlainLogger) Warnf(f string, a ...any)   { m.record("WARN", fmt.Sprintf(f, a...)) }
func (m *mockPlainLogger) Error(args ...any)          { m.record("ERROR", args...) }
func (m *mockPlainLogger) Errorf(f string, a ...any)  { m.record("ERROR", fmt.Sprintf(f, a...)) }
func (m *mockPlainLogger) Fatal(args ...any)          { m.record("FATAL", args...) }
func (m *mockPlainLogger) Fatalf(f string, a ...any)  { m.record("FATAL", fmt.Sprintf(f, a...)) }
func (m *mockPlainLogger) ChangeLevel(l Level)        { m.level = l }

func tracedCtxForTest(t *testing.T, hexTrace string) context.Context {
	t.Helper()
	tid, err := trace.TraceIDFromHex(hexTrace)
	require.NoError(t, err)
	sid, err := trace.SpanIDFromHex("1111111111111111")
	require.NoError(t, err)
	return trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
	}))
}

func TestStructuredLogger_JSONAndUnchangedLegacyOutput(t *testing.T) {
	var normal, errOut bytes.Buffer
	base := NewWriterLogger(INFO, &normal, &errOut)
	if cc, ok := base.(CallerConfigurer); ok {
		cc.SetShowCaller(true)
	}

	// Legacy Info/Infof must not emit "fields".
	base.Info("legacy info")
	base.Infof("legacy %s", "infof")

	sl, ok := base.(StructuredLogger)
	require.True(t, ok, "built-in logger must implement StructuredLogger")

	sl.Debugw("filtered debug", "k", "v")
	_, _, lineBefore, ok := runtime.Caller(0)
	require.True(t, ok)
	sl.Infow("user login", "user_id", 42, "method", "oauth")
	sl.Warnw("slow query", "ms", 250)
	sl.Errorw("db failed", "err", errors.New("connection refused"))

	normalLines := bytes.Split(bytes.TrimSpace(normal.Bytes()), []byte("\n"))
	require.Len(t, normalLines, 4)

	assert.NotContains(t, string(normalLines[0]), `"fields"`)
	assert.NotContains(t, string(normalLines[1]), `"fields"`)

	var infoEntry struct {
		Level   string         `json:"level"`
		Caller  string         `json:"caller"`
		Message string         `json:"message"`
		Fields  map[string]any `json:"fields"`
	}
	require.NoError(t, json.Unmarshal(normalLines[2], &infoEntry))
	assert.Equal(t, "INFO", infoEntry.Level)
	assert.Equal(t, "user login", infoEntry.Message)
	assert.Equal(t, fmt.Sprintf("structured_test.go:%d", lineBefore+2), infoEntry.Caller)
	assert.Equal(t, map[string]any{"user_id": float64(42), "method": "oauth"}, infoEntry.Fields)

	var warnEntry struct {
		Level   string         `json:"level"`
		Message string         `json:"message"`
		Fields  map[string]any `json:"fields"`
	}
	require.NoError(t, json.Unmarshal(normalLines[3], &warnEntry))
	assert.Equal(t, "WARN", warnEntry.Level)
	assert.Equal(t, "slow query", warnEntry.Message)
	assert.Equal(t, map[string]any{"ms": float64(250)}, warnEntry.Fields)

	var errEntry struct {
		Level   string         `json:"level"`
		Message string         `json:"message"`
		Fields  map[string]any `json:"fields"`
	}
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(errOut.Bytes()), &errEntry))
	assert.Equal(t, "ERROR", errEntry.Level)
	assert.Equal(t, "db failed", errEntry.Message)
	assert.Equal(t, map[string]any{"err": "connection refused"}, errEntry.Fields)
}

func TestStructuredLogger_NormalizeFieldsSafety(t *testing.T) {
	var normal, errOut bytes.Buffer
	base := NewWriterLogger(DEBUG, &normal, &errOut)
	sl, ok := base.(StructuredLogger)
	require.True(t, ok)

	var nilErr *typedNilError
	ch := make(chan int)
	fn := func() {}

	sl.Infow("edge cases",
		123, "non-string-key",
		"err", errors.New("boom"),
		"nil_err", nilErr,
		"nan", math.NaN(),
		"inf", math.Inf(1),
		"ninf32", float32(math.Inf(-1)),
		"fn", fn,
		"ch", ch,
		"cplx", complex(1, 2),
		"slice", []any{errors.New("nested-err"), math.NaN()},
		"nested", map[string]any{"e": errors.New("map-err"), "inf": math.Inf(-1)},
		"bad_slice", []float64{1.5, math.NaN()},
		"dangling_odd_value",
	)

	var entry struct {
		Message string         `json:"message"`
		Fields  map[string]any `json:"fields"`
	}
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(normal.Bytes()), &entry))
	assert.Equal(t, "edge cases", entry.Message)
	assert.Equal(t, "non-string-key", entry.Fields["123"])
	assert.Equal(t, "boom", entry.Fields["err"])
	assert.Nil(t, entry.Fields["nil_err"])
	assert.Equal(t, "NaN", entry.Fields["nan"])
	assert.Equal(t, "+Inf", entry.Fields["inf"])
	assert.Equal(t, "-Inf", entry.Fields["ninf32"])
	assert.NotEmpty(t, entry.Fields["fn"])
	assert.NotEmpty(t, entry.Fields["ch"])
	assert.Equal(t, "(1+2i)", entry.Fields["cplx"])
	assert.Equal(t, []any{"nested-err", "NaN"}, entry.Fields["slice"])
	assert.Equal(t, map[string]any{"e": "map-err", "inf": "-Inf"}, entry.Fields["nested"])
	assert.NotEmpty(t, entry.Fields["bad_slice"])
	assert.Equal(t, "dangling_odd_value", entry.Fields["!BADKEY"])
}

func TestStructuredLogger_PrettyPrintSortedFields(t *testing.T) {
	out := &bytes.Buffer{}
	l := &logger{isTerminal: true, lock: make(chan struct{}, 1)}

	l.prettyPrint(&logEntry{
		Level:   INFO,
		Time:    time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		TraceID: "0123456789abcdef0123456789abcdef",
		Message: "request done",
		Fields: map[string]any{
			"z": "last",
			"a": 1,
			"m": "mid",
		},
	}, out)

	line := out.String()
	assert.Contains(t, line, "request done a=1 m=mid z=last\n")
	assert.Contains(t, line, "0123456789abcdef0123456789abcdef")
}

func TestContextLogger_WithTraceFieldsCallerAndConcurrency(t *testing.T) {
	var normal, errOut bytes.Buffer
	base := NewWriterLogger(DEBUG, &normal, &errOut)
	if cc, ok := base.(CallerConfigurer); ok {
		cc.SetShowCaller(true)
	}

	ctx1 := tracedCtxForTest(t, "0123456789abcdef0123456789abcdef")
	ctx2 := tracedCtxForTest(t, "fedcba9876543210fedcba9876543210")

	parent := WithContext(ctx1, base).With("service", "auth", "attempt", 1)
	child := parent.With("attempt", 2, "user", "alice")

	// Parent must remain immutable.
	parent.Infow("parent event")
	child.Infow("child event", "extra", true)

	// Re-wrapping ContextLogger preserves fields while updating traceID.
	rebound := NewContextLogger(ctx2, parent)
	rebound.Infof("rebound %s", "msg")

	// Embedded ContextLogger (mirrors pi.Context embedding logging.ContextLogger).
	holder := &structuredEmbeddedCtxHolder{
		ContextLogger: *WithContext(ctx1, base),
	}
	_, _, lineBefore, ok := runtime.Caller(0)
	require.True(t, ok)
	holder.With("component", "handler").Infow("embedded call", "status", 200)

	lines := bytes.Split(bytes.TrimSpace(normal.Bytes()), []byte("\n"))
	require.Len(t, lines, 4)

	var e0, e1, e2, e3 struct {
		Caller  string         `json:"caller"`
		TraceID string         `json:"trace_id"`
		Message string         `json:"message"`
		Fields  map[string]any `json:"fields"`
	}
	require.NoError(t, json.Unmarshal(lines[0], &e0))
	assert.Equal(t, "0123456789abcdef0123456789abcdef", e0.TraceID)
	assert.Equal(t, map[string]any{"service": "auth", "attempt": float64(1)}, e0.Fields)

	require.NoError(t, json.Unmarshal(lines[1], &e1))
	assert.Equal(t, "0123456789abcdef0123456789abcdef", e1.TraceID)
	assert.Equal(t, map[string]any{"service": "auth", "attempt": float64(2), "user": "alice", "extra": true}, e1.Fields)

	require.NoError(t, json.Unmarshal(lines[2], &e2))
	assert.Equal(t, "fedcba9876543210fedcba9876543210", e2.TraceID)
	assert.Equal(t, "rebound msg", e2.Message)
	assert.Equal(t, map[string]any{"service": "auth", "attempt": float64(1)}, e2.Fields)

	require.NoError(t, json.Unmarshal(lines[3], &e3))
	assert.Equal(t, "0123456789abcdef0123456789abcdef", e3.TraceID)
	assert.Equal(t, fmt.Sprintf("structured_test.go:%d", lineBefore+2), e3.Caller)
	assert.Equal(t, map[string]any{"component": "handler", "status": float64(200)}, e3.Fields)

	// Concurrent With + Infow on shared parent must be race-free.
	normal.Reset()
	const goroutines = 16
	const iters = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			gLog := parent.With("worker", gid)
			for i := 0; i < iters; i++ {
				gLog.Infow("work", "iter", i)
			}
		}(g)
	}
	wg.Wait()

	concLines := bytes.Split(bytes.TrimSpace(normal.Bytes()), []byte("\n"))
	require.Len(t, concLines, goroutines*iters)
}

func TestContextLogger_CustomMockFallbackAndUnwrap(t *testing.T) {
	ctx := tracedCtxForTest(t, "0123456789abcdef0123456789abcdef")

	t.Run("custom plain Logger fallback formats sorted key=val", func(t *testing.T) {
		mock := &mockPlainLogger{level: DEBUG}
		cl := WithContext(ctx, mock).With("z", "last", "a", 1)
		cl.Infow("hello", "m", "mid")
		cl.Warnw("careful", "err", errors.New("oops"))
		cl.Errorw("failed", "code", 500)
		cl.Debugw("trace", "ok", true)

		require.Len(t, mock.calls, 4)
		assert.Equal(t, "INFO:hello a=1 m=mid z=last", mock.calls[0])
		assert.Equal(t, "WARN:careful a=1 err=oops z=last", mock.calls[1])
		assert.Equal(t, "ERROR:failed a=1 code=500 z=last", mock.calls[2])
		assert.Equal(t, "DEBUG:trace a=1 ok=true z=last", mock.calls[3])
	})

	t.Run("MockLogger fallback works cleanly", func(t *testing.T) {
		out := testutil.StdoutOutputForFunc(func() {
			mock := NewMockLogger(DEBUG)
			WithContext(ctx, mock).With("a", 1).Infow("mocked", "b", 2)
		})
		assert.Equal(t, "mocked a=1 b=2\n", out)
	})

	t.Run("Unwrap reaches structuredLoggerWithSkip", func(t *testing.T) {
		var buf bytes.Buffer
		inner := NewWriterLogger(DEBUG, &buf, &buf)
		if cc, ok := inner.(CallerConfigurer); ok {
			cc.SetShowCaller(true)
		}
		wrapped := &unwrappingLogger{Logger: inner}

		_, _, lineBefore, ok := runtime.Caller(0)
		require.True(t, ok)
		WithContext(ctx, wrapped).With("env", "prod").Infow("unwrapped", "ok", true)

		var entry struct {
			Caller  string         `json:"caller"`
			TraceID string         `json:"trace_id"`
			Message string         `json:"message"`
			Fields  map[string]any `json:"fields"`
		}
		require.NoError(t, json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &entry))
		assert.Equal(t, "0123456789abcdef0123456789abcdef", entry.TraceID)
		assert.Equal(t, "unwrapped", entry.Message)
		assert.Equal(t, fmt.Sprintf("structured_test.go:%d", lineBefore+2), entry.Caller)
		assert.Equal(t, map[string]any{"env": "prod", "ok": true}, entry.Fields)
	})

	t.Run("Noop and nil ContextLogger structured methods do not panic", func(t *testing.T) {
		require.NotPanics(t, func() {
			var nilCL *ContextLogger
			nilCL.With("k", "v").Infow("ignored")
			nilCL.Debugw("d")
			nilCL.Warnw("w")
			nilCL.Errorw("e")

			n := Noop()
			if sl, ok := n.(StructuredLogger); ok {
				sl.With("k", "v").Infow("ignored")
				sl.Debugw("d")
				sl.Infow("i")
				sl.Warnw("w")
				sl.Errorw("e")
			}
		})
	})
}

type countingError struct {
	calls *atomic.Int32
}

func (e countingError) Error() string {
	e.calls.Add(1)
	return "counted"
}

type cyclicStruct struct {
	Next *cyclicStruct
}

func wrapperLogInfow(cl *ContextLogger, msg string, kv ...any) {
	cl.WithCallerSkip(1).Infow(msg, kv...)
}

func wrapperLogInfo(cl *ContextLogger, args ...any) {
	cl.WithCallerSkip(1).Info(args...)
}

func TestContextLogger_WithCallerSkip(t *testing.T) {
	var normal, errOut bytes.Buffer
	base := NewWriterLogger(DEBUG, &normal, &errOut)
	if cc, ok := base.(CallerConfigurer); ok {
		cc.SetShowCaller(true)
	}

	parent := WithContext(context.Background(), base).With("svc", "edge")
	// Clamping negative skip to 0 must keep parent immutable.
	clamped := parent.WithCallerSkip(-3)
	assert.Equal(t, 0, clamped.callerSkip)
	assert.Equal(t, 0, parent.callerSkip)

	_, _, lineBeforeInfow, ok := runtime.Caller(0)
	require.True(t, ok)
	wrapperLogInfow(parent, "from wrapper", "ok", true)

	_, _, lineBeforeInfo, ok := runtime.Caller(0)
	require.True(t, ok)
	wrapperLogInfo(WithContext(context.Background(), base), "plain wrapper")

	lines := bytes.Split(bytes.TrimSpace(normal.Bytes()), []byte("\n"))
	require.Len(t, lines, 2)

	var e1, e2 struct {
		Caller  string         `json:"caller"`
		Message string         `json:"message"`
		Fields  map[string]any `json:"fields"`
	}
	require.NoError(t, json.Unmarshal(lines[0], &e1))
	assert.Equal(t, fmt.Sprintf("structured_test.go:%d", lineBeforeInfow+2), e1.Caller)
	assert.Equal(t, "from wrapper", e1.Message)
	assert.Equal(t, map[string]any{"svc": "edge", "ok": true}, e1.Fields)

	require.NoError(t, json.Unmarshal(lines[1], &e2))
	assert.Equal(t, fmt.Sprintf("structured_test.go:%d", lineBeforeInfo+2), e2.Caller)
	assert.Equal(t, "plain wrapper", e2.Message)
}

func TestStructuredLogger_CyclicGuardAndDisabledZeroNormalize(t *testing.T) {
	var normal, errOut bytes.Buffer
	base := NewWriterLogger(INFO, &normal, &errOut)
	sl := base.(StructuredLogger)
	cl := WithContext(context.Background(), base)

	var calls atomic.Int32
	ce := countingError{calls: &calls}
	sl.Debugw("disabled direct", "err", ce)
	cl.Debugw("disabled ctx", "err", ce)
	assert.Zero(t, calls.Load(), "disabled Debugw must not normalize fields")

	cyclicMap := map[string]any{}
	cyclicMap["self"] = cyclicMap

	cyclicSlice := make([]any, 1)
	cyclicSlice[0] = cyclicSlice

	cs := &cyclicStruct{}
	cs.Next = cs

	require.NotPanics(t, func() {
		cl.Infow("cyclic values", "cmap", cyclicMap, "cslice", cyclicSlice, "cstruct", cs)
	})

	var entry struct {
		Message string         `json:"message"`
		Fields  map[string]any `json:"fields"`
	}
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(normal.Bytes()), &entry))
	assert.Equal(t, "cyclic values", entry.Message)
	assert.Contains(t, normal.String(), "[max-depth]")
	assert.Contains(t, entry.Fields["cstruct"], "<cyclic")
}

func TestTypedNilLoggerSafety(t *testing.T) {
	var nilLogger *logger
	var nilCtx *ContextLogger
	var nilMock *mockPlainLogger
	var nilUnwrap *unwrappingLogger
	wrappedNil := &unwrappingLogger{Logger: nilLogger}

	require.NotPanics(t, func() {
		nilLogger.ChangeLevel(ERROR)
		nilLogger.SetShowCaller(true)
		nilLogger.Infow("safe")
		nilLogger.With("a", 1).Infow("safe")
	})

	for _, candidate := range []Logger{nilLogger, nilCtx, nilMock, nilUnwrap, wrappedNil} {
		require.NotPanics(t, func() {
			cl := NewContextLogger(context.Background(), candidate)
			assert.Nil(t, cl.base)
			cl.With("k", "v").WithCallerSkip(1).Infow("safe")
			cl.Info("safe")
			cl.Infof("%s", "safe")
			cl.ChangeLevel(ERROR)

			h := NewSlogHandler(candidate)
			assert.False(t, h.Enabled(context.Background(), 0))
		})
	}
}
