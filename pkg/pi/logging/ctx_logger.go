package logging

import (
	"context"

	"go.opentelemetry.io/otel/trace"
)

// loggerWithSkip is an internal interface for loggers that support custom caller skip
// and direct trace ID propagation without allocating metadata wrappers.
type loggerWithSkip interface {
	logArgsWithSkip(skip int, level Level, traceID string, args ...any)
	logfWithSkip(skip int, level Level, traceID string, format string, args ...any)
}

// ContextLogger wraps a base Logger and associates an OpenTelemetry trace ID
// extracted from a context.Context with subsequent log calls.
//
// For built-in loggers implementing loggerWithSkip, the trace ID is propagated
// directly into log entries with zero extra allocations. Custom Logger
// implementations receive the original log arguments unmodified.
type ContextLogger struct {
	base    Logger
	traceID string
}

// NewContextLogger creates a *ContextLogger that wraps base and captures any valid
// OpenTelemetry trace ID present on ctx.
//
// If base is already a *ContextLogger, it unwraps to the underlying base logger so
// repeated wrapping never nests ContextLogger instances or leaks stale trace IDs.
func NewContextLogger(ctx context.Context, base Logger) *ContextLogger {
	for {
		existing, ok := base.(*ContextLogger)
		if !ok {
			break
		}
		if existing == nil {
			base = nil
			break
		}
		base = existing.base
	}
	cl := &ContextLogger{base: base}
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

// logArgs logs unformatted arguments using the correct caller skip value for ContextLogger.
func (l *ContextLogger) logArgs(level Level, args ...any) {
	if l == nil || l.base == nil {
		return
	}
	if ls, ok := l.base.(loggerWithSkip); ok {
		// skip=3: runtime.Caller(0) -> logArgsWithSkip(1) -> logArgs(2) -> Debug/Info(3) -> user code
		ls.logArgsWithSkip(3, level, l.traceID, args...)
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
	if l == nil || l.base == nil {
		return
	}
	if ls, ok := l.base.(loggerWithSkip); ok {
		// skip=3: runtime.Caller(0) -> logfWithSkip(1) -> logf(2) -> Debugf/Infof(3) -> user code
		ls.logfWithSkip(3, level, l.traceID, format, args...)
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
func (l *ContextLogger) Log(args ...any)               { l.logArgs(INFO, args...) }
func (l *ContextLogger) Logf(f string, args ...any)    { l.logf(INFO, f, args...) }
func (l *ContextLogger) Info(args ...any)              { l.logArgs(INFO, args...) }
func (l *ContextLogger) Infof(f string, args ...any)   { l.logf(INFO, f, args...) }
func (l *ContextLogger) Notice(args ...any)            { l.logArgs(NOTICE, args...) }
func (l *ContextLogger) Noticef(f string, args ...any) { l.logf(NOTICE, f, args...) }
func (l *ContextLogger) Warn(args ...any)              { l.logArgs(WARN, args...) }
func (l *ContextLogger) Warnf(f string, args ...any)   { l.logf(WARN, f, args...) }
func (l *ContextLogger) Error(args ...any)             { l.logArgs(ERROR, args...) }
func (l *ContextLogger) Errorf(f string, args ...any)  { l.logf(ERROR, f, args...) }
func (l *ContextLogger) Fatal(args ...any)             { l.logArgs(FATAL, args...) }
func (l *ContextLogger) Fatalf(f string, args ...any)  { l.logf(FATAL, f, args...) }
func (l *ContextLogger) ChangeLevel(level Level) {
	if l == nil || l.base == nil {
		return
	}
	l.base.ChangeLevel(level)
}
