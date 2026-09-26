package logging

import (
	"context"

	"go.opentelemetry.io/otel/trace"
)

// loggerWithSkip is an internal interface for loggers that support custom caller skip.
type loggerWithSkip interface {
	logfWithSkip(skip int, level Level, format string, args ...any)
}

// ContextLogger is a wrapper around a base Logger that injects the current
// trace ID (if present in the context) into log messages automatically.
//
// It is intended for use within request-scoped contexts where OpenTelemetry
// trace information is available.
type ContextLogger struct {
	base    Logger
	traceID string
}

// NewContextLogger creates a new ContextLogger that wraps the provided base logger
// and automatically appends OpenTelemetry trace information (trace ID) to log output
// when available in the context.
func NewContextLogger(ctx context.Context, base Logger) *ContextLogger {
	var traceID string

	sc := trace.SpanFromContext(ctx).SpanContext()

	if sc.IsValid() {
		traceID = sc.TraceID().String()
	}

	return &ContextLogger{base: base, traceID: traceID}
}

// withTraceInfo appends the trace ID from the context (if available).
// This allows trace IDs to be extracted later during formatting or filtering.
func (l *ContextLogger) withTraceInfo(args ...any) []any {
	if l.traceID != "" {
		return append(args, map[string]any{"__trace_id__": l.traceID})
	}

	return args
}

// logWithSkip logs using the correct caller skip value for ContextLogger.
func (l *ContextLogger) logWithSkip(level Level, format string, args ...any) {
	if ls, ok := l.base.(loggerWithSkip); ok {
		// skip=3: runtime.Caller(0) -> logfWithSkip(1) -> logWithSkip(2) -> Debug/Info(3) -> user code
		ls.logfWithSkip(3, level, format, l.withTraceInfo(args...)...)
	} else {
		// Fallback for other logger implementations
		switch level {
		case DEBUG:
			if format == "" {
				l.base.Debug(l.withTraceInfo(args...)...)
			} else {
				l.base.Debugf(format, l.withTraceInfo(args...)...)
			}
		case INFO:
			if format == "" {
				l.base.Info(l.withTraceInfo(args...)...)
			} else {
				l.base.Infof(format, l.withTraceInfo(args...)...)
			}
		case NOTICE:
			if format == "" {
				l.base.Notice(l.withTraceInfo(args...)...)
			} else {
				l.base.Noticef(format, l.withTraceInfo(args...)...)
			}
		case WARN:
			if format == "" {
				l.base.Warn(l.withTraceInfo(args...)...)
			} else {
				l.base.Warnf(format, l.withTraceInfo(args...)...)
			}
		case ERROR:
			if format == "" {
				l.base.Error(l.withTraceInfo(args...)...)
			} else {
				l.base.Errorf(format, l.withTraceInfo(args...)...)
			}
		case FATAL:
			if format == "" {
				l.base.Fatal(l.withTraceInfo(args...)...)
			} else {
				l.base.Fatalf(format, l.withTraceInfo(args...)...)
			}
		}
	}
}

func (l *ContextLogger) Debug(args ...any)             { l.logWithSkip(DEBUG, "", args...) }
func (l *ContextLogger) Debugf(f string, args ...any)  { l.logWithSkip(DEBUG, f, args...) }
func (l *ContextLogger) Log(args ...any)               { l.logWithSkip(INFO, "", args...) }
func (l *ContextLogger) Logf(f string, args ...any)    { l.logWithSkip(INFO, f, args...) }
func (l *ContextLogger) Info(args ...any)              { l.logWithSkip(INFO, "", args...) }
func (l *ContextLogger) Infof(f string, args ...any)   { l.logWithSkip(INFO, f, args...) }
func (l *ContextLogger) Notice(args ...any)            { l.logWithSkip(NOTICE, "", args...) }
func (l *ContextLogger) Noticef(f string, args ...any) { l.logWithSkip(NOTICE, f, args...) }
func (l *ContextLogger) Warn(args ...any)              { l.logWithSkip(WARN, "", args...) }
func (l *ContextLogger) Warnf(f string, args ...any)   { l.logWithSkip(WARN, f, args...) }
func (l *ContextLogger) Error(args ...any)             { l.logWithSkip(ERROR, "", args...) }
func (l *ContextLogger) Errorf(f string, args ...any)  { l.logWithSkip(ERROR, f, args...) }
func (l *ContextLogger) Fatal(args ...any)             { l.logWithSkip(FATAL, "", args...) }
func (l *ContextLogger) Fatalf(f string, args ...any)  { l.logWithSkip(FATAL, f, args...) }
func (l *ContextLogger) ChangeLevel(level Level)       { l.base.ChangeLevel(level) }
