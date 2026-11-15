package gcs

import (
	"context"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/sllt/pi/pkg/pi/datasource/file"
)

const defaultTimeout = 10 * time.Second

type fileSystem struct {
	*file.CommonFileSystem

	retryMu   sync.Mutex
	stopCh    chan struct{}
	retryDone chan struct{}
	closed    bool
}

// Config represents the gcs configuration.
type Config struct {
	EndPoint        string
	BucketName      string
	CredentialsJSON string
	ProjectID       string
}

// New creates and validates a new GCS file system.
func New(config *Config) file.FileSystemProvider {
	if config == nil {
		config = &Config{}
	}

	adapter := &storageAdapter{cfg: config}

	fs := &fileSystem{
		CommonFileSystem: &file.CommonFileSystem{
			Provider:     adapter,
			Location:     config.BucketName,
			ProviderName: "GCS", // Set provider name for observability
		},
		stopCh: make(chan struct{}),
	}

	return fs
}

// FromEnv constructs an unstarted GCS FileSystemProvider from an environment lookup function.
func FromEnv(get func(string) string) (file.FileSystemProvider, error) {
	if get == nil {
		return nil, errGCSConfigNil
	}

	firstNonEmpty := func(keys ...string) string {
		for _, k := range keys {
			if v := strings.TrimSpace(get(k)); v != "" {
				return v
			}
		}
		return ""
	}

	bucket := firstNonEmpty("GCS_BUCKET", "GCS_BUCKET_NAME")
	if bucket == "" {
		return nil, errGCSBucketRequired
	}

	return New(&Config{
		BucketName:      bucket,
		EndPoint:        firstNonEmpty("GCS_ENDPOINT"),
		CredentialsJSON: get("GCS_CREDENTIALS_JSON"),
		ProjectID:       firstNonEmpty("GCS_PROJECT_ID", "GOOGLE_CLOUD_PROJECT"),
	}), nil
}

// ConnectContext establishes the GCS bucket connection within ctx and fails fast without background retries.
func (f *fileSystem) ConnectContext(ctx context.Context) error {
	if f == nil || f.CommonFileSystem == nil {
		return errGCSConfigNil
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
	if adapter, ok := f.CommonFileSystem.Provider.(*storageAdapter); ok {
		if adapter.cfg == nil {
			return errGCSConfigNil
		}
		if strings.TrimSpace(adapter.cfg.BucketName) == "" {
			return errGCSBucketRequired
		}
	}
	return f.CommonFileSystem.Connect(ctx)
}

// Connect tries a single immediate connect via provider; on failure it starts a background retry.
func (f *fileSystem) Connect() {
	if f.CommonFileSystem.IsConnected() {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	err := f.CommonFileSystem.Connect(ctx)
	if err != nil {
		if f.CommonFileSystem.Logger != nil {
			f.CommonFileSystem.Logger.Warnf("GCS bucket %s not available, starting background retry: %v",
				f.CommonFileSystem.Location, err)
		}

		// Start background retry
		f.launchRetryConnect()

		return
	}

	// Connected successfully
	if f.CommonFileSystem.Logger != nil {
		f.CommonFileSystem.Logger.Infof("GCS connection established to bucket %s", f.CommonFileSystem.Location)
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
					f.CommonFileSystem.Logger.Infof("GCS connection restored to bucket %s", f.CommonFileSystem.Location)
				}

				return
			}

			// Still failing - log and continue retrying
			if f.CommonFileSystem.Logger != nil {
				f.CommonFileSystem.Logger.Debugf("GCS retry failed, will try again: %v", err)
			}
		}
	}
}
