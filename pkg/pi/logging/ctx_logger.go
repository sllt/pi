package logging

import (
	"context"
	"fmt"
	"maps"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

// loggerWithSkip is an internal interface for loggers that support custom caller skip
// and direct trace ID propagation without allocating metadata wrappers.
type loggerWithSkip interface {
	logArgsWithSkip(skip int, level Level, traceID string, args ...any)
	logfWithSkip(skip int, level Level, traceID string, format string, args ...any)
}

type loggerWithCallerSkip interface {
	logArgsWithExtraSkip(skip, extraSkip int, level Level, traceID string, args ...any)
	logfWithExtraSkip(skip, extraSkip int, level Level, traceID string, format string, args ...any)
}

type structuredLoggerWithSkip interface {
	logStructuredWithSkip(skip int, level Level, traceID string, msg string, fields map[string]any)
}

type structuredLoggerWithCaller interface {
	logStructuredWithCaller(skip, extraSkip int, callerPC uintptr, level Level, traceID string, msg string, fields map[string]any, normalized bool)
}

// ContextLogger wraps a base Logger and associates an OpenTelemetry trace ID
// extracted from a context.Context with subsequent log calls.
//
// For built-in loggers implementing loggerWithSkip, the trace ID is propagated
// directly into log entries with zero extra allocations. Custom Logger
// implementations receive the original log arguments unmodified.
type ContextLogger struct {
	base       Logger
	traceID    string
	fields     map[string]any
	callerSkip int
}

var _ StructuredLogger = (*ContextLogger)(nil)

func isNilLogger(l Logger) bool {
	return l == nil || isNilValue(l)
}

// NewContextLogger creates a *ContextLogger that wraps base and captures any valid
// OpenTelemetry trace ID present on ctx.
//
// If base is already a *ContextLogger, it unwraps to the underlying base logger so
// repeated wrapping never nests ContextLogger instances or leaks stale trace IDs,
// while preserving any structured fields and caller skip attached via With/WithCallerSkip.
func NewContextLogger(ctx context.Context, base Logger) *ContextLogger {
	var inheritedFields map[string]any
	inheritedSkip := 0
	for !isNilLogger(base) {
		existing, ok := base.(*ContextLogger)
		if !ok {
			break
		}
		if len(existing.fields) > 0 {
			if inheritedFields == nil {
				inheritedFields = maps.Clone(existing.fields)
			} else {
				for k, v := range existing.fields {
					if _, exists := inheritedFields[k]; !exists {
						inheritedFields[k] = v
					}
				}
			}
		}
		if inheritedSkip == 0 && existing.callerSkip > 0 {
			inheritedSkip = existing.callerSkip
		}
		base = existing.base
	}
	if isNilLogger(base) || unwrapBaseLogger(base) == nil {
		base = nil
	}
	cl := &ContextLogger{
		base:       base,
		fields:     inheritedFields,
		callerSkip: inheritedSkip,
	}
	cl.UpdateContext(ctx)
	return cl
}

// WithContext returns a trace-aware *ContextLogger bound to ctx, suitable for
// Service and Repository layers receiving a standard context.Context.
// When base is nil, the returned *ContextLogger safely discards log calls.
func WithContext(ctx context.Context, base Logger) *ContextLogger {
	return NewContextLogger(ctx, base)
}

// UpdateContext refreshes the cached OpenTelemetry TraceID from ctx so child spans
// created via Context.Trace or transport supervisors immediately propagate to subsequent logs.
func (l *ContextLogger) UpdateContext(ctx context.Context) {
	if l == nil {
		return
	}
	if ctx == nil {
		l.traceID = ""
		return
	}
	sc := trace.SpanFromContext(ctx).SpanContext()
	if sc.IsValid() {
		l.traceID = sc.TraceID().String()
	} else {
		l.traceID = ""
	}
}

// With returns an immutable copy of ContextLogger with the given key-value fields attached.
func (l *ContextLogger) With(kv ...any) *ContextLogger {
	extra := normalizeFields(kv...)
	if l == nil {
		return &ContextLogger{fields: extra}
	}
	var merged map[string]any
	if len(l.fields) > 0 || len(extra) > 0 {
		merged = make(map[string]any, len(l.fields)+len(extra))
		for k, v := range l.fields {
			merged[k] = v
		}
		for k, v := range extra {
			merged[k] = v
		}
	}
	return &ContextLogger{
		base:       l.base,
		traceID:    l.traceID,
		fields:     merged,
		callerSkip: l.callerSkip,
	}
}

// WithCallerSkip returns an immutable copy of ContextLogger with extra caller frames skipped.
func (l *ContextLogger) WithCallerSkip(delta int) *ContextLogger {
	if l == nil {
		skip := max(delta, 0)
		return &ContextLogger{callerSkip: skip}
	}
	nextSkip := max(l.callerSkip+delta, 0)
	return &ContextLogger{
		base:       l.base,
		traceID:    l.traceID,
		fields:     maps.Clone(l.fields),
		callerSkip: nextSkip,
	}
}

const maxLoggerUnwrapDepth = 16

func unwrapBaseLogger(base Logger) Logger {
	for depth := 0; depth < maxLoggerUnwrapDepth && !isNilLogger(base); depth++ {
		switch u := base.(type) {
		case *ContextLogger:
			base = u.base
		case interface{ Unwrap() Logger }:
			next := u.Unwrap()
			if isNilLogger(next) {
				return nil
			}
			base = next
		default:
			return base
		}
	}
	if isNilLogger(base) {
		return nil
	}
	return base
}

func unwrapLoggerAs[T any](base Logger) (T, bool) {
	var zero T
	for depth := 0; depth < maxLoggerUnwrapDepth && !isNilLogger(base); depth++ {
		if target, ok := base.(T); ok {
			return target, true
		}
		switch u := base.(type) {
		case *ContextLogger:
			base = u.base
		case interface{ Unwrap() Logger }:
			next := u.Unwrap()
			if isNilLogger(next) {
				return zero, false
			}
			base = next
		default:
			return zero, false
		}
	}
	return zero, false
}

func (l *ContextLogger) isLevelEnabled(level Level) bool {
	if l == nil || isNilLogger(l.base) {
		return false
	}
	unwrapped := unwrapBaseLogger(l.base)
	if unwrapped == nil {
		return false
	}
	if lg, ok := unwrapped.(levelGetter); ok {
		if level == FATAL {
			if _, isNoop := unwrapped.(noopLogger); isNoop {
				return false
			}
			return true
		}
		return level >= lg.getLevel()
	}
	return true
}

func formatStructuredFallback(msg string, fields map[string]any) string {
	if len(fields) == 0 {
		return msg
	}
	kvStr := formatSortedFields(fields)
	if msg == "" {
		return kvStr
	}
	return msg + " " + kvStr
}

func (l *ContextLogger) logStructured(level Level, msg string, kv ...any) {
	if !l.isLevelEnabled(level) {
		return
	}
	l.logStructuredMapWithCaller(level, 0, msg, normalizeFields(kv...), true)
}

func (l *ContextLogger) logStructuredMap(level Level, msg string, extra map[string]any) {
	l.logStructuredMapWithCaller(level, 0, msg, extra, true)
}

func (l *ContextLogger) logStructuredMapWithCaller(level Level, callerPC uintptr, msg string, extra map[string]any, normalized bool) {
	if !l.isLevelEnabled(level) {
		return
	}
	var merged map[string]any
	switch {
	case len(l.fields) > 0 && len(extra) > 0:
		merged = make(map[string]any, len(l.fields)+len(extra))
		for k, v := range l.fields {
			merged[k] = v
		}
		for k, v := range extra {
			if normalized {
				merged[k] = v
			} else {
				merged[k] = normalizeFieldValue(v)
			}
		}
		normalized = true
	case len(l.fields) > 0:
		merged = l.fields
		normalized = true
	case len(extra) > 0:
		merged = extra
	}

	if lc, ok := unwrapLoggerAs[structuredLoggerWithCaller](l.base); ok {
		lc.logStructuredWithCaller(2, l.callerSkip, callerPC, level, l.traceID, msg, merged, normalized)
		return
	}
	if ls, ok := unwrapLoggerAs[structuredLoggerWithSkip](l.base); ok {
		ls.logStructuredWithSkip(2+l.callerSkip, level, l.traceID, msg, merged)
		return
	}

	formatted := formatStructuredFallback(msg, merged)
	switch level {
	case DEBUG:
		l.base.Debug(formatted)
	case INFO:
		l.base.Info(formatted)
	case NOTICE:
		l.base.Notice(formatted)
	case WARN:
		l.base.Warn(formatted)
	case ERROR:
		l.base.Error(formatted)
	case FATAL:
		l.base.Fatal(formatted)
	}
}

// logArgs logs unformatted arguments using the correct caller skip value for ContextLogger.
func (l *ContextLogger) logArgs(level Level, args ...any) {
	if !l.isLevelEnabled(level) {
		return
	}
	if len(l.fields) > 0 {
		var msg string
		if len(args) == 1 {
			msg = fmt.Sprint(args[0])
		} else {
			msg = strings.TrimSuffix(fmt.Sprintln(args...), "\n")
		}
		l.logStructuredMapWithCaller(level, 0, msg, nil, true)
		return
	}
	if lc, ok := unwrapLoggerAs[loggerWithCallerSkip](l.base); ok {
		lc.logArgsWithExtraSkip(3, l.callerSkip, level, l.traceID, args...)
		return
	}
	if ls, ok := unwrapLoggerAs[loggerWithSkip](l.base); ok {
		// skip=3: runtime.Caller(0) -> logArgsWithSkip(1) -> logArgs(2) -> Debug/Info(3) -> user code
		ls.logArgsWithSkip(3+l.callerSkip, level, l.traceID, args...)
		return
	}
	// Fallback for custom Logger implementations: forward args untouched.
	switch level {
	case DEBUG:
		l.base.Debug(args...)
	case INFO:
		l.base.Info(args...)
	case NOTICE:
		l.base.Notice(args...)
	case WARN:
		l.base.Warn(args...)
	case ERROR:
		l.base.Error(args...)
	case FATAL:
		l.base.Fatal(args...)
	}
}

// logf logs formatted output using the correct caller skip value for ContextLogger.
func (l *ContextLogger) logf(level Level, format string, args ...any) {
	if !l.isLevelEnabled(level) {
		return
	}
	if len(l.fields) > 0 {
		l.logStructuredMapWithCaller(level, 0, fmt.Sprintf(format, args...), nil, true)
		return
	}
	if lc, ok := unwrapLoggerAs[loggerWithCallerSkip](l.base); ok {
		lc.logfWithExtraSkip(3, l.callerSkip, level, l.traceID, format, args...)
		return
	}
	if ls, ok := unwrapLoggerAs[loggerWithSkip](l.base); ok {
		// skip=3: runtime.Caller(0) -> logfWithSkip(1) -> logf(2) -> Debugf/Infof(3) -> user code
		ls.logfWithSkip(3+l.callerSkip, level, l.traceID, format, args...)
		return
	}
	// Fallback for custom Logger implementations: forward args untouched.
	switch level {
	case DEBUG:
		l.base.Debugf(format, args...)
	case INFO:
		l.base.Infof(format, args...)
	case NOTICE:
		l.base.Noticef(format, args...)
	case WARN:
		l.base.Warnf(format, args...)
	case ERROR:
		l.base.Errorf(format, args...)
	case FATAL:
		l.base.Fatalf(format, args...)
	}
}

func (l *ContextLogger) Debug(args ...any)             { l.logArgs(DEBUG, args...) }
func (l *ContextLogger) Debugf(f string, args ...any)  { l.logf(DEBUG, f, args...) }
func (l *ContextLogger) Debugw(msg string, kv ...any)  { l.logStructured(DEBUG, msg, kv...) }
func (l *ContextLogger) Log(args ...any)               { l.logArgs(INFO, args...) }
func (l *ContextLogger) Logf(f string, args ...any)    { l.logf(INFO, f, args...) }
func (l *ContextLogger) Info(args ...any)              { l.logArgs(INFO, args...) }
func (l *ContextLogger) Infof(f string, args ...any)   { l.logf(INFO, f, args...) }
func (l *ContextLogger) Infow(msg string, kv ...any)   { l.logStructured(INFO, msg, kv...) }
func (l *ContextLogger) Notice(args ...any)            { l.logArgs(NOTICE, args...) }
func (l *ContextLogger) Noticef(f string, args ...any) { l.logf(NOTICE, f, args...) }
func (l *ContextLogger) Warn(args ...any)              { l.logArgs(WARN, args...) }
func (l *ContextLogger) Warnf(f string, args ...any)   { l.logf(WARN, f, args...) }
func (l *ContextLogger) Warnw(msg string, kv ...any)   { l.logStructured(WARN, msg, kv...) }
func (l *ContextLogger) Error(args ...any)             { l.logArgs(ERROR, args...) }
func (l *ContextLogger) Errorf(f string, args ...any)  { l.logf(ERROR, f, args...) }
func (l *ContextLogger) Errorw(msg string, kv ...any)  { l.logStructured(ERROR, msg, kv...) }
func (l *ContextLogger) Fatal(args ...any)             { l.logArgs(FATAL, args...) }
func (l *ContextLogger) Fatalf(f string, args ...any)  { l.logf(FATAL, f, args...) }
func (l *ContextLogger) ChangeLevel(level Level) {
	if l == nil || isNilLogger(l.base) || unwrapBaseLogger(l.base) == nil {
		return
	}
	l.base.ChangeLevel(level)
}
