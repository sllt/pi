package remotelogger

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/sllt/pi/pkg/pi/service"
)

const (
	requestTimeout = 5 * time.Second
	// ANSI color codes for terminal output.
	colorBlue   = 34  // For successful responses (2xx)
	colorYellow = 220 // For client errors (4xx)
	colorRed    = 202 // For server errors (5xx)
)

// httpDebugMsg represents a structured HTTP debug log entry.
// It implements PrettyPrint for colored output and json.Marshaler for JSON logs.
type httpDebugMsg struct {
	CorrelationID string `json:"correlation_id"`
	ResponseCode  int    `json:"response_code"`
	ResponseTime  int64  `json:"response_time_us"`
	HTTPMethod    string `json:"http_method"`
	URI           string `json:"uri"`
}

func (m httpDebugMsg) PrettyPrint(w io.Writer) {
	colorCode := colorForResponseCode(m.ResponseCode)
	fmt.Fprintf(w,
		"\u001B[38;5;8m%s \u001B[38;5;%dm%-6d\u001B[0m %8dμs\u001B[0m %s %s\n",
		m.CorrelationID,
		colorCode,
		m.ResponseCode,
		m.ResponseTime,
		m.HTTPMethod,
		m.URI,
	)
}

func (m httpDebugMsg) MarshalJSON() ([]byte, error) {
	type alias httpDebugMsg
	return json.Marshal(alias(m))
}

// httpLogFilter filters HTTP logs from remote logger to reduce noise.
type httpLogFilter struct {
	logging.Logger
	mu                 sync.Mutex
	firstSuccessfulHit bool
	initLogged         bool
}

// Log implements a simplified filtering strategy with consistent formatting.
func (f *httpLogFilter) Log(args ...any) {
	if len(args) == 0 || args[0] == nil {
		f.Logger.Log(args...)
		return
	}

	// Handle HTTP logs.
	httpLog, ok := args[0].(*service.Log)
	if !ok {
		f.Logger.Log(args...)
		return
	}

	f.handleHTTPLog(httpLog, args)
}

func (f *httpLogFilter) handleHTTPLog(httpLog *service.Log, args []any) {
	// Log initialization message if not already logged
	f.mu.Lock()
	notLoggedYet := !f.initLogged

	if notLoggedYet {
		f.initLogged = true
	}

	f.mu.Unlock()

	if notLoggedYet {
		f.Logger.Infof("Initializing remote logger connection to %s", httpLog.URI)
	}

	isSuccessful := httpLog.ResponseCode >= 200 && httpLog.ResponseCode < 300

	f.mu.Lock()
	isFirstHit := !f.firstSuccessfulHit
	f.mu.Unlock()

	switch {
	// First successful hit - log at INFO level
	case isSuccessful && isFirstHit:
		f.mu.Lock()
		f.firstSuccessfulHit = true
		f.mu.Unlock()
		f.Logger.Log(args...)

	// Subsequent successful hits - log at DEBUG level with consistent format
	case isSuccessful:
		msg := httpDebugMsg{
			CorrelationID: httpLog.CorrelationID,
			ResponseCode:  httpLog.ResponseCode,
			ResponseTime:  httpLog.ResponseTime,
			HTTPMethod:    httpLog.HTTPMethod,
			URI:           httpLog.URI,
		}
		f.Logger.Debug(msg)

	// Error responses - pass through to original logger
	default:
		f.Logger.Log(args...)
	}
}

func colorForResponseCode(status int) int {
	switch {
	case status >= 200 && status < 300:
		return colorBlue
	case status >= 400 && status < 500:
		return colorYellow
	case status >= 500 && status < 600:
		return colorRed
	}

	return 0
}

/*
New creates a new RemoteLogger instance with the provided level, remote configuration URL, and level fetch interval.
The remote configuration URL is expected to be a JSON endpoint that returns the desired log level for the service.
The level fetch interval determines how often the logger checks for updates to the remote configuration.
*/
func New(level logging.Level, remoteConfigURL string, loggerFetchInterval time.Duration) logging.Logger {
	l := &remoteLogger{
		remoteURL:          remoteConfigURL,
		Logger:             logging.NewLogger(level),
		levelFetchInterval: loggerFetchInterval,
		currentLevel:       level,
	}

	if remoteConfigURL != "" {
		if ctx, ok := l.acquireLoopContext(); ok {
			go func() {
				defer l.wg.Done()
				l.runUpdateLoop(ctx)
			}()
		}
	}

	return l
}

type remoteLogger struct {
	remoteURL          string
	levelFetchInterval time.Duration
	mu                 sync.RWMutex
	currentLevel       logging.Level
	logging.Logger

	lifecycleMu sync.Mutex
	ctx         context.Context
	cancel      context.CancelFunc
	closed      bool
	wg          sync.WaitGroup
}

var _ logging.StructuredLogger = (*remoteLogger)(nil)

// Unwrap returns the underlying logging.Logger wrapped by this remoteLogger.
func (r *remoteLogger) Unwrap() logging.Logger {
	if r == nil {
		return nil
	}
	return r.Logger
}

type infoEnabledChecker interface {
	IsInfoEnabled() bool
}

// IsInfoEnabled reports whether INFO-level logs are enabled on the underlying logger.
func (r *remoteLogger) IsInfoEnabled() bool {
	if r == nil || r.Logger == nil {
		return false
	}
	if checker, ok := r.Logger.(infoEnabledChecker); ok {
		return checker.IsInfoEnabled()
	}
	r.mu.RLock()
	lvl := r.currentLevel
	r.mu.RUnlock()
	return lvl <= logging.INFO
}

func (r *remoteLogger) structuredTarget() logging.StructuredLogger {
	if r == nil || r.Logger == nil {
		return nil
	}
	if sl, ok := r.Logger.(logging.StructuredLogger); ok {
		return sl
	}
	return logging.WithContext(nil, r.Logger)
}

// With returns an immutable *logging.ContextLogger deriving from this logger with the given fields.
func (r *remoteLogger) With(kv ...any) *logging.ContextLogger {
	if r == nil {
		return logging.WithContext(nil, nil).With(kv...)
	}
	return logging.WithContext(nil, r).With(kv...)
}

// Debugw logs a structured debug message with key-value fields.
func (r *remoteLogger) Debugw(msg string, kv ...any) {
	if sl := r.structuredTarget(); sl != nil {
		sl.Debugw(msg, kv...)
	}
}

// Infow logs a structured info message with key-value fields.
func (r *remoteLogger) Infow(msg string, kv ...any) {
	if sl := r.structuredTarget(); sl != nil {
		sl.Infow(msg, kv...)
	}
}

// Warnw logs a structured warn message with key-value fields.
func (r *remoteLogger) Warnw(msg string, kv ...any) {
	if sl := r.structuredTarget(); sl != nil {
		sl.Warnw(msg, kv...)
	}
}

// Errorw logs a structured error message with key-value fields.
func (r *remoteLogger) Errorw(msg string, kv ...any) {
	if sl := r.structuredTarget(); sl != nil {
		sl.Errorw(msg, kv...)
	}
}

func (r *remoteLogger) acquireLoopContext() (context.Context, bool) {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()

	if r.closed {
		return nil, false
	}
	if r.ctx == nil {
		r.ctx, r.cancel = context.WithCancel(context.Background())
	}
	r.wg.Add(1)
	return r.ctx, true
}

// Close stops background remote log-level polling and waits for any active
// polling goroutines to exit. The underlying local logger remains usable for
// final shutdown diagnostics.
func (r *remoteLogger) Close() error {
	if r == nil {
		return nil
	}
	r.lifecycleMu.Lock()
	r.closed = true
	cancel := r.cancel
	r.lifecycleMu.Unlock()

	if cancel != nil {
		cancel()
	}
	r.wg.Wait()
	return nil
}

// SetShowCaller delegates to the underlying logger if it supports CallerConfigurer.
func (r *remoteLogger) SetShowCaller(show bool) {
	if r == nil {
		return
	}
	if cc, ok := r.Logger.(logging.CallerConfigurer); ok {
		cc.SetShowCaller(show)
	}
}

// UpdateLogLevel continuously fetches the log level from the remote configuration URL at the specified interval
// and updates the underlying log level if it has changed.
func (r *remoteLogger) UpdateLogLevel() {
	ctx, ok := r.acquireLoopContext()
	if !ok {
		return
	}
	defer r.wg.Done()
	r.runUpdateLoop(ctx)
}

func (r *remoteLogger) runUpdateLoop(ctx context.Context) {
	const defaultFetchInterval = 15 * time.Second

	interval := r.levelFetchInterval
	if interval <= 0 {
		interval = defaultFetchInterval
	}

	// Create filtered logger with proper initialization
	filteredLogger := &httpLogFilter{
		Logger:             r.Logger,
		firstSuccessfulHit: false,
		initLogged:         false,
	}

	remoteService := service.NewHTTPService(r.remoteURL, filteredLogger, nil)

	r.Infof("Remote logger monitoring initialized with URL: %s, interval: %s",
		r.remoteURL, interval)

	checkAndUpdateLevel := func() {
		if ctx.Err() != nil {
			return
		}

		r.mu.RLock()
		currentLevel := r.currentLevel
		r.mu.RUnlock()

		newLevel, err := fetchAndUpdateLogLevelContext(ctx, remoteService, currentLevel)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			r.Warnf("Failed to fetch log level: %v", err)
			return
		}

		r.mu.Lock()

		if r.currentLevel != newLevel {
			oldLevel := r.currentLevel
			r.currentLevel = newLevel
			r.mu.Unlock()

			logLevelChange(r, oldLevel, newLevel)
			r.ChangeLevel(newLevel)
		} else {
			r.mu.Unlock()
		}
	}

	// Perform initial check immediately
	checkAndUpdateLevel()
	if ctx.Err() != nil {
		return
	}

	// Setup ticker for periodic checks
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			checkAndUpdateLevel()
		}
	}
}

// Helper function to log level changes at appropriate level.
func logLevelChange(r *remoteLogger, oldLevel, newLevel logging.Level) {
	// Use the higher level to ensure visibility
	logLevel := oldLevel
	if newLevel > oldLevel {
		logLevel = newLevel
	}

	message := fmt.Sprintf("LOG_LEVEL updated from %v to %v", oldLevel, newLevel)

	switch logLevel {
	case logging.FATAL:
		r.Warnf("%s", message)
	case logging.ERROR:
		r.Errorf("%s", message)
	case logging.WARN:
		r.Warnf("%s", message)
	case logging.NOTICE:
		r.Noticef("%s", message)
	case logging.INFO:
		r.Infof("%s", message)
	case logging.DEBUG:
		r.Infof("%s", message) // Using Info for DEBUG to ensure visibility
	}
}

func fetchAndUpdateLogLevel(remoteService service.HTTP, currentLevel logging.Level) (logging.Level, error) {
	return fetchAndUpdateLogLevelContext(context.Background(), remoteService, currentLevel)
}

func fetchAndUpdateLogLevelContext(parent context.Context, remoteService service.HTTP, currentLevel logging.Level) (logging.Level, error) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, requestTimeout) // Set timeout for 5 seconds
	defer cancel()

	resp, err := remoteService.Get(ctx, "", nil)
	if err != nil {
		return currentLevel, err
	}
	defer resp.Body.Close()

	var response struct {
		Data struct {
			ServiceName string `json:"serviceName"`
			Level       string `json:"logLevel"`
		} `json:"data"`
	}

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return currentLevel, err
	}

	err = json.Unmarshal(responseBody, &response)
	if err != nil {
		return currentLevel, err
	}

	logLevels := []string{"DEBUG", "INFO", "NOTICE", "WARN", "ERROR", "FATAL"}

	if slices.Contains(logLevels, response.Data.Level) && response.Data.ServiceName != "" {
		newLevel := logging.GetLevelFromString(response.Data.Level)
		return newLevel, nil
	}

	return currentLevel, nil
}
