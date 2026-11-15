package azure

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/sllt/pi/pkg/pi/datasource/file"
)

var (
	errInvalidConfig       = errors.New("invalid Azure configuration: share name is required")
	errAccountNameRequired = errors.New("invalid Azure configuration: account name is required")
	errAccountKeyRequired  = errors.New("invalid Azure configuration: account key is required")
)

const defaultTimeout = 10 * time.Second

type azureFileSystem struct {
	*file.CommonFileSystem

	retryMu   sync.Mutex
	stopCh    chan struct{}
	retryDone chan struct{}
	closed    bool
}

// Config represents the Azure File Storage configuration.
type Config struct {
	AccountName string // Azure Storage Account name
	AccountKey  string // Azure Storage Account key
	ShareName   string // Azure File Share name
	Endpoint    string // Azure Storage endpoint (optional, defaults to core.windows.net)
}

// New creates and validates a new Azure File Storage file system.
// Returns error if configuration is invalid.
// Connection will be established when Connect() is called.
func New(config *Config) (file.FileSystemProvider, error) {
	if config == nil {
		return nil, errInvalidConfig
	}

	if config.ShareName == "" {
		return nil, errInvalidConfig
	}

	if config.AccountName == "" {
		return nil, errAccountNameRequired
	}

	if config.AccountKey == "" {
		return nil, errAccountKeyRequired
	}

	adapter := &storageAdapter{cfg: config}

	fs := &azureFileSystem{
		CommonFileSystem: &file.CommonFileSystem{
			Provider:     adapter,
			Location:     config.ShareName,
			ProviderName: "Azure", // Set provider name for observability
		},
		stopCh: make(chan struct{}),
	}

	return fs, nil
}

// FromEnv constructs an unstarted Azure File Storage provider from an environment lookup function.
func FromEnv(get func(string) string) (file.FileSystemProvider, error) {
	if get == nil {
		return nil, errInvalidConfig
	}

	firstNonEmpty := func(keys ...string) string {
		for _, k := range keys {
			if v := strings.TrimSpace(get(k)); v != "" {
				return v
			}
		}
		return ""
	}

	return New(&Config{
		AccountName: firstNonEmpty("AZURE_STORAGE_ACCOUNT", "AZURE_ACCOUNT_NAME"),
		AccountKey:  firstNonEmpty("AZURE_STORAGE_KEY", "AZURE_ACCOUNT_KEY"),
		ShareName:   firstNonEmpty("AZURE_STORAGE_SHARE", "AZURE_SHARE_NAME"),
		Endpoint:    firstNonEmpty("AZURE_STORAGE_ENDPOINT", "AZURE_ENDPOINT"),
	})
}

// ConnectContext initializes the Azure File Share connection within ctx and fails fast without background retries.
func (f *azureFileSystem) ConnectContext(ctx context.Context) error {
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
	return f.CommonFileSystem.Connect(ctx)
}

// Connect tries a single immediate connect via provider; on failure it starts a background retry.
func (f *azureFileSystem) Connect() {
	if f.CommonFileSystem.IsConnected() {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	if f.CommonFileSystem.Logger != nil {
		f.CommonFileSystem.Logger.Debugf("Attempting to connect to Azure File Share %s (timeout: %v)",
			f.CommonFileSystem.Location, defaultTimeout)
	}

	// Use CommonFileSystem.Connect for bookkeeping
	if err := f.CommonFileSystem.Connect(ctx); err != nil {
		if f.CommonFileSystem.Logger != nil {
			f.CommonFileSystem.Logger.Warnf("Azure File Share %s not available, starting background retry: %v", f.CommonFileSystem.Location, err)
		}

		f.launchRetryConnect()

		return
	}

	// Connected successfully
	if f.CommonFileSystem.Logger != nil {
		f.CommonFileSystem.Logger.Debugf("Successfully connected to Azure File Share %s", f.CommonFileSystem.Location)
	}
}

func (f *azureFileSystem) ensureStopCh() <-chan struct{} {
	f.retryMu.Lock()
	defer f.retryMu.Unlock()

	if f.stopCh == nil {
		f.stopCh = make(chan struct{})
	}

	return f.stopCh
}

func (f *azureFileSystem) launchRetryConnect() {
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
func (f *azureFileSystem) Close() error {
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

// startRetryConnect repeatedly calls provider.Connect until success or shutdown.
func (f *azureFileSystem) startRetryConnect() {
	stopCh := f.ensureStopCh()

	select {
	case <-stopCh:
		return
	default:
	}

	f.logRetryStart()

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	retryCount := 0

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			if f.shouldExitRetry() {
				f.logRetryExit()
				return
			}

			retryCount++

			f.logRetryAttempt(retryCount)

			if f.attemptConnectionWithStop(retryCount, stopCh) {
				return
			}
		}
	}
}

// logRetryStart logs the start of background retry.
func (f *azureFileSystem) logRetryStart() {
	if f.CommonFileSystem.Logger != nil {
		f.CommonFileSystem.Logger.Debugf(
			"Starting background retry for Azure File Share %s (retry interval: 1 minute)",
			f.CommonFileSystem.Location,
		)
	}
}

// shouldExitRetry checks if retry loop should exit.
func (f *azureFileSystem) shouldExitRetry() bool {
	return f.CommonFileSystem.IsConnected() || f.CommonFileSystem.IsRetryDisabled()
}

// logRetryExit logs the exit reason from retry loop.
func (f *azureFileSystem) logRetryExit() {
	if f.CommonFileSystem.Logger == nil {
		return
	}

	if f.CommonFileSystem.IsConnected() {
		f.CommonFileSystem.Logger.Debugf(
			"Retry loop exiting: Azure File Share %s is now connected",
			f.CommonFileSystem.Location,
		)
	} else {
		f.CommonFileSystem.Logger.Debugf(
			"Retry loop exiting: retry disabled for Azure File Share %s",
			f.CommonFileSystem.Location,
		)
	}
}

// logRetryAttempt logs a retry attempt.
func (f *azureFileSystem) logRetryAttempt(retryCount int) {
	if f.CommonFileSystem.Logger != nil {
		f.CommonFileSystem.Logger.Debugf(
			"Retry attempt #%d: attempting to connect to Azure File Share %s (timeout: %v)",
			retryCount,
			f.CommonFileSystem.Location,
			defaultTimeout,
		)
	}
}

// attemptConnection attempts to connect and returns true if successful.
func (f *azureFileSystem) attemptConnection(retryCount int) bool {
	return f.attemptConnectionWithStop(retryCount, f.ensureStopCh())
}

func (f *azureFileSystem) attemptConnectionWithStop(retryCount int, stopCh <-chan struct{}) bool {
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
		f.logRetrySuccess(retryCount)
		return true
	}

	f.logRetryFailure(retryCount, err)

	return false
}

// logRetrySuccess logs successful retry.
func (f *azureFileSystem) logRetrySuccess(_ int) {
	if f.CommonFileSystem.Logger != nil {
		f.CommonFileSystem.Logger.Infof("Azure connection restored to share %s", f.CommonFileSystem.Location)
	}
}

// logRetryFailure logs failed retry attempt.
func (f *azureFileSystem) logRetryFailure(retryCount int, err error) {
	if f.CommonFileSystem.Logger != nil {
		f.CommonFileSystem.Logger.Debugf(
			"Retry attempt #%d failed for Azure File Share %s: %v (will retry in 1 minute)",
			retryCount,
			f.CommonFileSystem.Location,
			err,
		)
	}
}
