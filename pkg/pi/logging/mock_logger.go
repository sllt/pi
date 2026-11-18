package logging

import (
	"fmt"
	"io"
	"os"
	"sync"
)

type MockLogger struct {
	mu     sync.RWMutex
	level  Level
	out    io.Writer
	errOut io.Writer
}

func NewMockLogger(level Level) Logger {
	return &MockLogger{
		level:  level,
		out:    os.Stdout,
		errOut: os.Stderr,
	}
}

func (m *MockLogger) targetWriter(level Level) (io.Writer, bool) {
	m.mu.RLock()
	currLevel := m.level
	out := m.out
	errOut := m.errOut
	m.mu.RUnlock()

	if level < currLevel {
		return nil, false
	}

	if level >= ERROR {
		out = errOut
	}
	return out, true
}

func (m *MockLogger) writeMessage(out io.Writer, message any) {
	m.mu.Lock()
	fmt.Fprintf(out, "%v\n", message)
	m.mu.Unlock()
}

func (m *MockLogger) logArgs(level Level, args ...any) {
	out, ok := m.targetWriter(level)
	if !ok {
		return
	}
	var message any
	if len(args) == 1 {
		message = args[0]
	} else {
		message = args
	}
	m.writeMessage(out, message)
}

func (m *MockLogger) logf(level Level, format string, args ...any) {
	out, ok := m.targetWriter(level)
	if !ok {
		return
	}
	m.writeMessage(out, fmt.Sprintf(format, args...))
}

func (m *MockLogger) Debug(args ...any) {
	m.logArgs(DEBUG, args...)
}

func (m *MockLogger) Debugf(format string, args ...any) {
	m.logf(DEBUG, format, args...)
}

func (m *MockLogger) Info(args ...any) {
	m.logArgs(INFO, args...)
}

func (m *MockLogger) Infof(format string, args ...any) {
	m.logf(INFO, format, args...)
}

func (m *MockLogger) Notice(args ...any) {
	m.logArgs(NOTICE, args...)
}

func (m *MockLogger) Noticef(format string, args ...any) {
	m.logf(NOTICE, format, args...)
}

func (m *MockLogger) Warn(args ...any) {
	m.logArgs(WARN, args...)
}

func (m *MockLogger) Warnf(format string, args ...any) {
	m.logf(WARN, format, args...)
}

func (m *MockLogger) Error(args ...any) {
	m.logArgs(ERROR, args...)
}

func (m *MockLogger) Errorf(format string, args ...any) {
	m.logf(ERROR, format, args...)
}

func (m *MockLogger) Fatal(args ...any) {
	m.logArgs(FATAL, args...)
}

func (m *MockLogger) Fatalf(format string, args ...any) {
	m.logf(FATAL, format, args...)
}

func (m *MockLogger) Log(args ...any) {
	m.logArgs(INFO, args...)
}

func (m *MockLogger) Logf(format string, args ...any) {
	m.logf(INFO, format, args...)
}

func (m *MockLogger) ChangeLevel(level Level) {
	m.mu.Lock()
	m.level = level
	m.mu.Unlock()
}
