package ftp

import (
	"context"
	"testing"
	"time"

	"github.com/sllt/pi/pkg/pi/datasource/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestNew_NilConfig(t *testing.T) {
	fs := New(nil)

	require.NotNil(t, fs)
	assert.Equal(t, "ftp://unconfigured", fs.(*fileSystem).CommonFileSystem.Location)
}

func TestNew_EmptyHost(t *testing.T) {
	config := &Config{Host: "", Port: 2121}

	fs := New(config)

	require.NotNil(t, fs)
	defer func() { _ = fs.(*fileSystem).Close() }()

	assert.Equal(t, "ftp://unconfigured", fs.(*fileSystem).CommonFileSystem.Location)
}

func TestNew_PortZero(t *testing.T) {
	config := &Config{Host: "localhost", Port: 0}

	fs := New(config)

	require.NotNil(t, fs)
	defer func() { _ = fs.(*fileSystem).Close() }()
	// Port 0 will default to 21 in buildLocation
	assert.Equal(t, "localhost:21", fs.(*fileSystem).CommonFileSystem.Location)
}

func TestNew_ConnectionFailure_StartsRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockLogger := file.NewMockLogger(ctrl)
	mockMetrics := file.NewMockMetrics(ctrl)

	config := &Config{
		Host:     "non-existent-host",
		Port:     9999,
		User:     "testuser",
		Password: "testpass",
	}

	fs := New(config)
	require.NotNil(t, fs)
	defer func() { _ = fs.(*fileSystem).Close() }()

	// Inject logger and metrics (mimicking WithFile / SetFile behavior)
	fs.UseLogger(mockLogger)
	fs.UseMetrics(mockMetrics)

	mockLogger.EXPECT().Warnf(
		"FTP server %s not available, starting background retry: %v",
		gomock.Any(),
		gomock.Any(),
	)

	mockMetrics.EXPECT().NewHistogram(file.AppFileStats, gomock.Any(), gomock.Any())
	mockLogger.EXPECT().Debug(gomock.Any()).AnyTimes()
	mockMetrics.EXPECT().RecordHistogram(gomock.Any(), file.AppFileStats, gomock.Any(), gomock.Any()).AnyTimes()

	fs.Connect()

	time.Sleep(100 * time.Millisecond)

	fs.(*fileSystem).CommonFileSystem.SetDisableRetry(true)
}

func TestNew_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockLogger := file.NewMockLogger(ctrl)
	mockMetrics := file.NewMockMetrics(ctrl)

	config := &Config{
		Host:     "localhost",
		Port:     2121,
		User:     "testuser",
		Password: "testpass",
	}

	fs := New(config)
	require.NotNil(t, fs)
	defer func() { _ = fs.(*fileSystem).Close() }()

	assert.Equal(t, "localhost:2121", fs.(*fileSystem).CommonFileSystem.Location)

	// Inject logger and metrics
	fs.UseLogger(mockLogger)
	fs.UseMetrics(mockMetrics)

	mockMetrics.EXPECT().NewHistogram(file.AppFileStats, gomock.Any(), gomock.Any())

	mockLogger.EXPECT().Infof("connected to %s", "localhost:2121").MaxTimes(1)
	mockLogger.EXPECT().Warnf(
		"FTP server %s not available, starting background retry: %v",
		gomock.Any(),
		gomock.Any(),
	).MaxTimes(1)

	mockLogger.EXPECT().Debug(gomock.Any()).AnyTimes()
	mockMetrics.EXPECT().RecordHistogram(gomock.Any(), file.AppFileStats, gomock.Any(), gomock.Any()).AnyTimes()

	fs.Connect()
}

func TestNew_WithRemoteDir(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockLogger := file.NewMockLogger(ctrl)
	mockMetrics := file.NewMockMetrics(ctrl)

	config := &Config{
		Host:      "localhost",
		Port:      2121,
		User:      "testuser",
		Password:  "testpass",
		RemoteDir: "/uploads",
	}

	fs := New(config)

	require.NotNil(t, fs)
	defer func() { _ = fs.(*fileSystem).Close() }()

	assert.Equal(t, "localhost:2121/uploads", fs.(*fileSystem).CommonFileSystem.Location)

	// Inject logger and metrics
	fs.UseLogger(mockLogger)
	fs.UseMetrics(mockMetrics)

	mockMetrics.EXPECT().NewHistogram(file.AppFileStats, gomock.Any(), gomock.Any())

	mockLogger.EXPECT().Infof("connected to %s", "localhost:2121/uploads").MaxTimes(1)
	mockLogger.EXPECT().Warnf(
		"FTP server %s not available, starting background retry: %v",
		gomock.Any(),
		gomock.Any(),
	).MaxTimes(1)

	mockLogger.EXPECT().Debug(gomock.Any()).AnyTimes()
	mockMetrics.EXPECT().RecordHistogram(gomock.Any(), file.AppFileStats, gomock.Any(), gomock.Any()).AnyTimes()

	fs.Connect()
}

func TestNew_WithRootRemoteDir(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockLogger := file.NewMockLogger(ctrl)
	mockMetrics := file.NewMockMetrics(ctrl)

	config := &Config{
		Host:      "localhost",
		Port:      2121,
		User:      "testuser",
		Password:  "testpass",
		RemoteDir: "/",
	}

	fs := New(config)
	require.NotNil(t, fs)
	defer func() { _ = fs.(*fileSystem).Close() }()

	assert.Equal(t, "localhost:2121", fs.(*fileSystem).CommonFileSystem.Location)

	// Inject logger and metrics
	fs.UseLogger(mockLogger)
	fs.UseMetrics(mockMetrics)

	mockMetrics.EXPECT().NewHistogram(file.AppFileStats, gomock.Any(), gomock.Any())

	mockLogger.EXPECT().Infof("connected to %s", "localhost:2121").MaxTimes(1)
	mockLogger.EXPECT().Warnf(
		"FTP server %s not available, starting background retry: %v",
		gomock.Any(),
		gomock.Any(),
	).MaxTimes(1)

	mockLogger.EXPECT().Debug(gomock.Any()).AnyTimes()
	mockMetrics.EXPECT().RecordHistogram(gomock.Any(), file.AppFileStats, gomock.Any(), gomock.Any()).AnyTimes()

	fs.Connect()
}

func TestNew_WithCustomDialTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	config := &Config{
		Host:        "localhost",
		Port:        2121,
		User:        "testuser",
		Password:    "testpass",
		DialTimeout: 3 * time.Second,
	}

	fs := New(config)

	require.NotNil(t, fs)
	defer func() { _ = fs.(*fileSystem).Close() }()

	adapter := fs.(*fileSystem).CommonFileSystem.Provider.(*storageAdapter)
	assert.Equal(t, 3*time.Second, adapter.cfg.DialTimeout)
}

func TestConnect_AlreadyConnected(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockLogger := file.NewMockLogger(ctrl)
	mockMetrics := file.NewMockMetrics(ctrl)

	config := &Config{
		Host:     "localhost",
		Port:     2121,
		User:     "testuser",
		Password: "testpass",
	}

	adapter := &storageAdapter{cfg: config}

	fs := &fileSystem{
		CommonFileSystem: &file.CommonFileSystem{
			Provider: adapter,
			Location: "localhost:2121",
			Logger:   mockLogger,
			Metrics:  mockMetrics,
		},
	}

	fs.CommonFileSystem.SetConnected(true)

	fs.Connect()

	assert.True(t, fs.CommonFileSystem.IsConnected())
}

func TestStartRetryConnect_ExitWhenRetryDisabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockLogger := file.NewMockLogger(ctrl)
	mockMetrics := file.NewMockMetrics(ctrl)

	config := &Config{
		Host:     "localhost",
		Port:     2121,
		User:     "testuser",
		Password: "testpass",
	}

	adapter := &storageAdapter{cfg: config}

	fs := &fileSystem{
		CommonFileSystem: &file.CommonFileSystem{
			Provider: adapter,
			Location: "localhost:2121",
			Logger:   mockLogger,
			Metrics:  mockMetrics,
		},
	}

	// Disable retry before starting
	fs.CommonFileSystem.SetDisableRetry(true)

	// Should exit immediately without any expectations
	done := make(chan bool)

	go func() {
		fs.startRetryConnect()

		done <- true
	}()

	select {
	case <-done:
		// Success: function exited immediately
	case <-time.After(100 * time.Millisecond):
		t.Fatal("startRetryConnect did not exit when retry is disabled")
	}
}

func TestFTP_ConnectContext_Health_Close_FromEnv(t *testing.T) {
	_, err := FromEnv(func(string) string { return "" })
	require.ErrorIs(t, err, errInvalidConfig)

	server, _, cleanup := setupTestFTPServer(t)
	defer cleanup()

	fsProvider, err := FromEnv(func(key string) string {
		switch key {
		case "FTP_HOST":
			return "127.0.0.1"
		case "FTP_PORT":
			return "2121"
		case "FTP_USER":
			return "test"
		case "FTP_PASSWORD":
			return "test"
		default:
			return ""
		}
	})
	require.NoError(t, err)
	require.NotNil(t, fsProvider)

	// Cancelled context fails fast without spawning background retry
	ftpFS := fsProvider.(*fileSystem)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, ftpFS.ConnectContext(ctx), context.Canceled)
	assert.Nil(t, ftpFS.retryDone)

	// ConnectContext against embedded test FTP server + Health + idempotent Close
	liveFS := New(getTestConfig(server.Port)).(*fileSystem)
	require.NoError(t, liveFS.ConnectContext(context.Background()))
	adapter := liveFS.CommonFileSystem.Provider.(*storageAdapter)
	require.NoError(t, adapter.Health(context.Background()))
	require.NoError(t, liveFS.Close())
	require.NoError(t, liveFS.Close())
	require.ErrorIs(t, adapter.Health(context.Background()), errFTPClientNotInitialized)
}
