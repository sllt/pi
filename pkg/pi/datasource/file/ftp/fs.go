package ftp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sllt/pi/pkg/pi/datasource/file"
)

var (
	errInvalidConfig   = errors.New("invalid FTP configuration: host and port are required")
	errInvalidProvider = errors.New("invalid FTP provider")
)

const defaultTimeout = 10 * time.Second

type fileSystem struct {
	*file.CommonFileSystem

	retryMu   sync.Mutex
	stopCh    chan struct{}
	retryDone chan struct{}
	closed    bool
}

// New creates and validates a new FTP file system.
// Returns error if connection fails or configuration is invalid.
func New(config *Config) file.FileSystemProvider {
	var cfgCopy Config
	if config != nil {
		cfgCopy = *config
	}

	// Set default dial timeout if not specified
	if cfgCopy.DialTimeout == 0 {
		cfgCopy.DialTimeout = 5 * time.Second
	}

	adapter := &storageAdapter{cfg: &cfgCopy}

	location := buildLocation(&cfgCopy)

	fs := &fileSystem{
		CommonFileSystem: &file.CommonFileSystem{
			Provider:     adapter,
			Location:     location,
			ProviderName: "FTP",
		},
		stopCh: make(chan struct{}),
	}

	return fs
}

// FromEnv constructs an unstarted FTP FileSystemProvider from an environment lookup function.
func FromEnv(get func(string) string) (file.FileSystemProvider, error) {
	if get == nil {
		return nil, errInvalidConfig
	}

	host := strings.TrimSpace(get("FTP_HOST"))
	if host == "" {
		return nil, errInvalidConfig
	}

	port := 21
	if rawPort := strings.TrimSpace(get("FTP_PORT")); rawPort != "" {
		parsed, err := strconv.Atoi(rawPort)
		if err != nil || parsed <= 0 || parsed > 65535 {
			return nil, fmt.Errorf("ftp: invalid FTP_PORT %q", rawPort)
		}
		port = parsed
	}

	var dialTimeout time.Duration
	if rawTimeout := strings.TrimSpace(get("FTP_DIAL_TIMEOUT")); rawTimeout != "" {
		parsed, err := time.ParseDuration(rawTimeout)
		if err != nil || parsed <= 0 {
			return nil, fmt.Errorf("ftp: invalid FTP_DIAL_TIMEOUT %q", rawTimeout)
		}
		dialTimeout = parsed
	}

	return New(&Config{
		Host:        host,
		Port:        port,
		User:        strings.TrimSpace(get("FTP_USER")),
		Password:    get("FTP_PASSWORD"),
		RemoteDir:   strings.TrimSpace(get("FTP_REMOTE_DIR")),
		DialTimeout: dialTimeout,
	}), nil
}

// ConnectContext establishes the FTP connection within ctx and fails fast without background retries.
func (f *fileSystem) ConnectContext(ctx context.Context) error {
	if f == nil || f.CommonFileSystem == nil {
		return errInvalidConfig
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.CommonFileSystem.IsConnected() {
		return nil
	}
	if err := f.validateConfig(); err != nil {
		return err
	}
	return f.CommonFileSystem.Connect(ctx)
}

// buildLocation creates the location string for metrics/logging.
func buildLocation(config *Config) string {
	if config.Host == "" {
		return "ftp://unconfigured"
	}

	port := config.Port
	if port == 0 {
		port = 21 // Default FTP port
	}

	location := fmt.Sprintf("%s:%d", config.Host, port)

	if config.RemoteDir != "" && config.RemoteDir != "/" {
		location = fmt.Sprintf("%s:%d%s", config.Host, port, config.RemoteDir)
	}

	return location
}

// Connect tries a single immediate connect via provider; on failure it starts a background retry.
func (f *fileSystem) Connect() {
	if f.CommonFileSystem.IsConnected() {
		return
	}

	// Validate configuration before attempting connection
	if err := f.validateConfig(); err != nil {
		if f.CommonFileSystem.Logger != nil {
			f.CommonFileSystem.Logger.Errorf("Invalid FTP configuration: %v", err)
		}

		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	err := f.CommonFileSystem.Connect(ctx)
	if err != nil {
		// Log warning if logger is available
		if f.CommonFileSystem.Logger != nil {
			f.CommonFileSystem.Logger.Warnf("FTP server %s not available, starting background retry: %v",
				f.CommonFileSystem.Location, err)
		}

		// Start background retry
		f.launchRetryConnect()

		return
	}

	// Connected successfully
	if f.CommonFileSystem.Logger != nil {
		f.CommonFileSystem.Logger.Infof("FTP connection established to server %s", f.CommonFileSystem.Location)
	}
}

func (f *fileSystem) ensureStopCh() <-chan struct{} {
	f.retryMu.Lock()
	defer f.retryMu.Unlock()

	if f.stopCh == nil {
		f.stopCh = make(chan struct{})
	}

	return f.stopCh
}

func (f *fileSystem) launchRetryConnect() {
	f.retryMu.Lock()
	if f.closed || f.retryDone != nil {
		f.retryMu.Unlock()
		return
	}

	if f.stopCh == nil {
		f.stopCh = make(chan struct{})
	}

	done := make(chan struct{})
	f.retryDone = done
	f.retryMu.Unlock()

	go func() {
		defer func() {
			close(done)
			f.retryMu.Lock()
			if f.retryDone == done {
				f.retryDone = nil
			}
			f.retryMu.Unlock()
		}()

		f.startRetryConnect()
	}()
}

// Close stops background connection retries and closes the underlying provider if closable.
func (f *fileSystem) Close() error {
	if f == nil {
		return nil
	}

	f.retryMu.Lock()
	if !f.closed {
		f.closed = true
		if f.stopCh == nil {
			f.stopCh = make(chan struct{})
		}
		close(f.stopCh)
	}
	done := f.retryDone
	f.retryMu.Unlock()

	if done != nil {
		<-done
	}

	if f.CommonFileSystem != nil {
		f.CommonFileSystem.SetDisableRetry(true)
		f.CommonFileSystem.SetConnected(false)
	}

	if f.CommonFileSystem != nil && f.CommonFileSystem.Provider != nil {
		if closer, ok := f.CommonFileSystem.Provider.(io.Closer); ok {
			return closer.Close()
		}
	}

	return nil
}

// startRetryConnect retries connection every minute until success or shutdown.
func (f *fileSystem) startRetryConnect() {
	if f.CommonFileSystem.IsConnected() || f.CommonFileSystem.IsRetryDisabled() {
		return
	}

	stopCh := f.ensureStopCh()

	select {
	case <-stopCh:
		return
	default:
	}

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			if f.CommonFileSystem.IsConnected() || f.CommonFileSystem.IsRetryDisabled() {
				return
			}

			baseCtx, stopCancel := context.WithCancel(context.Background())
			attemptDone := make(chan struct{})
			go func() {
				select {
				case <-stopCh:
					stopCancel()
				case <-attemptDone:
				}
			}()

			ctx, cancel := context.WithTimeout(baseCtx, defaultTimeout)
			err := f.CommonFileSystem.Connect(ctx)
			cancel()
			close(attemptDone)
			stopCancel()

			if err == nil {
				// Success - exit retry loop
				if f.CommonFileSystem.Logger != nil {
					f.CommonFileSystem.Logger.Infof("FTP connection restored to server %s", f.CommonFileSystem.Location)
				}

				return
			}

			// Still failing - log and continue retrying
			if f.CommonFileSystem.Logger != nil {
				f.CommonFileSystem.Logger.Debugf("FTP retry failed, will try again: %v", err)
			}
		}
	}
}

// validateConfig checks if the configuration is valid.
func (f *fileSystem) validateConfig() error {
	adapter, ok := f.CommonFileSystem.Provider.(*storageAdapter)
	if !ok {
		return errInvalidProvider
	}

	cfg := adapter.cfg
	if cfg == nil || cfg.Host == "" || cfg.Port <= 0 || cfg.Port > 65535 || cfg.DialTimeout < 0 {
		return errInvalidConfig
	}

	return nil
}
