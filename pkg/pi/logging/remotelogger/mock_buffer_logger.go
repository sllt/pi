package remotelogger

import (
	"fmt"
	"strings"
	"sync"

	"github.com/sllt/pi/pkg/pi/logging"
)

// testBufferLogger is a simple logger that writes to a buffer.
// It's primarily used in dynamic_level_logger_test.go for testing HTTPLogFilter and other log-related functionality,
// where direct capture of stdout would be insufficient.
// Unlike testutil.StdoutOutputForFunc, this logger provides isolation for component-specific testing and respects the logging levels.
type testBufferLogger struct {
	mu    sync.Mutex
	buf   *strings.Builder
	level logging.Level
}

func (l *testBufferLogger) Debug(args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.level <= logging.DEBUG {
		fmt.Fprintln(l.buf, args...)
	}
}

func (l *testBufferLogger) Logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(l.buf, format+"\n", args...)
}

func (l *testBufferLogger) Info(args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.level <= logging.INFO {
		fmt.Fprintln(l.buf, args...)
	}
}

func (l *testBufferLogger) Notice(args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.level <= logging.NOTICE {
		fmt.Fprintln(l.buf, args...)
	}
}

func (l *testBufferLogger) Warn(args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.level <= logging.WARN {
		fmt.Fprintln(l.buf, args...)
	}
}

func (l *testBufferLogger) Error(args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.level <= logging.ERROR {
		fmt.Fprintln(l.buf, args...)
	}
}

func (l *testBufferLogger) Fatal(args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.level <= logging.FATAL {
		fmt.Fprintln(l.buf, args...)
	}
}

func (l *testBufferLogger) Log(args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintln(l.buf, args...)
}

func (l *testBufferLogger) Infof(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(l.buf, format+"\n", args...)
}

func (l *testBufferLogger) Debugf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(l.buf, format+"\n", args...)
}

func (l *testBufferLogger) Warnf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(l.buf, format+"\n", args...)
}

func (l *testBufferLogger) Errorf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(l.buf, format+"\n", args...)
}

func (l *testBufferLogger) Fatalf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(l.buf, format+"\n", args...)
}

func (l *testBufferLogger) Noticef(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(l.buf, format+"\n", args...)
}

func (l *testBufferLogger) ChangeLevel(level logging.Level) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}
