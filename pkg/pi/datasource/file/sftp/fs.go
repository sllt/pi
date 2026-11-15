package sftp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/sftp"
	"github.com/sllt/pi/pkg/pi/datasource/file"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

const (
	statusSuccess   = "SUCCESS"
	statusError     = "ERROR"
	defaultSFTPPort = 22
)

var (
	errSFTPConfigInvalid   = errors.New("sftp: host and port (> 0) are required")
	errSFTPHostKeyRequired = errors.New("sftp: HostKeyCallBack is required")
	errSFTPNotConnected    = errors.New("sftp: client is not connected")
)

type FileSystem struct {
	mu      sync.RWMutex
	logger  Logger
	metrics Metrics
	config  Config
	client  sftpClient
	sshConn io.Closer
}

type Config struct {
	User            string
	Password        string
	Host            string
	Port            int
	HostKeyCallBack ssh.HostKeyCallback
}

func New(cfg Config) *FileSystem {
	return &FileSystem{config: cfg}
}

// FromEnv constructs an unstarted SFTP FileSystem from an environment lookup function.
func FromEnv(get func(string) string) (*FileSystem, error) {
	if get == nil {
		return nil, errSFTPConfigInvalid
	}

	host := strings.TrimSpace(get("SFTP_HOST"))
	if host == "" {
		return nil, errSFTPConfigInvalid
	}

	port := defaultSFTPPort
	if rawPort := strings.TrimSpace(get("SFTP_PORT")); rawPort != "" {
		parsed, err := strconv.Atoi(rawPort)
		if err != nil || parsed <= 0 || parsed > 65535 {
			return nil, fmt.Errorf("sftp: invalid SFTP_PORT %q", rawPort)
		}
		port = parsed
	}

	var hostKeyCb ssh.HostKeyCallback
	if khFile := strings.TrimSpace(get("SFTP_KNOWN_HOSTS_FILE")); khFile != "" {
		cb, err := knownhosts.New(khFile)
		if err != nil {
			return nil, fmt.Errorf("sftp: failed to load SFTP_KNOWN_HOSTS_FILE: %w", err)
		}
		hostKeyCb = cb
	} else if rawIgnore := strings.TrimSpace(get("SFTP_INSECURE_IGNORE_HOST_KEY")); rawIgnore != "" {
		ignore, err := strconv.ParseBool(rawIgnore)
		if err != nil {
			return nil, fmt.Errorf("sftp: invalid SFTP_INSECURE_IGNORE_HOST_KEY %q: %w", rawIgnore, err)
		}
		if ignore {
			//nolint:gosec // Explicitly opted into by environment configuration for non-production/internal hosts.
			hostKeyCb = ssh.InsecureIgnoreHostKey()
		}
	}
	if hostKeyCb == nil {
		return nil, errSFTPHostKeyRequired
	}

	return New(Config{
		Host:            host,
		Port:            port,
		User:            strings.TrimSpace(get("SFTP_USER")),
		Password:        get("SFTP_PASSWORD"),
		HostKeyCallBack: hostKeyCb,
	}), nil
}

// UseLogger sets the logger for the FileSystem client.
func (f *FileSystem) UseLogger(logger any) {
	if l, ok := logger.(Logger); ok {
		f.logger = l
	}
}

// UseMetrics sets the metrics for the FileSystem client.
func (f *FileSystem) UseMetrics(metrics any) {
	if m, ok := metrics.(Metrics); ok {
		f.metrics = m
	}
}

func (f *FileSystem) logDebugf(pattern string, args ...any) {
	if f != nil && f.logger != nil {
		f.logger.Debugf(pattern, args...)
	}
}

func (f *FileSystem) logLogf(pattern string, args ...any) {
	if f != nil && f.logger != nil {
		f.logger.Logf(pattern, args...)
	}
}

func (f *FileSystem) logErrorf(pattern string, args ...any) {
	if f != nil && f.logger != nil {
		f.logger.Errorf(pattern, args...)
	}
}

// Connect establishes a connection to FileSystem and registers metrics using the provided configuration when the client was Created.
func (f *FileSystem) Connect() {
	if f == nil {
		return
	}
	f.logDebugf("connecting to SFTP server with host `%v` and port `%v`", f.config.Host, f.config.Port)

	if f.config.HostKeyCallBack == nil {
		f.logErrorf("failed to connect with sftp with err %v", errSFTPHostKeyRequired)
		return
	}

	addr := net.JoinHostPort(f.config.Host, strconv.Itoa(f.config.Port))

	config := &ssh.ClientConfig{
		User:            f.config.User,
		Auth:            []ssh.AuthMethod{ssh.Password(f.config.Password)},
		HostKeyCallback: f.config.HostKeyCallBack,
	}

	conn, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		f.logErrorf("failed to connect with sftp with err %v", err)
		return
	}

	client, err := sftp.NewClient(conn)
	if err != nil {
		_ = conn.Close()
		f.logErrorf("failed to create sftp client with err %v", err)
		return
	}

	f.mu.Lock()
	f.client = client
	f.sshConn = conn
	f.mu.Unlock()

	f.logLogf("connected to SFTP client successfully")
}

// ConnectContext dials the SSH/SFTP server within ctx and fails fast on error.
func (f *FileSystem) ConnectContext(ctx context.Context) error {
	if f == nil {
		return errSFTPConfigInvalid
	}
	if strings.TrimSpace(f.config.Host) == "" || f.config.Port <= 0 || f.config.Port > 65535 {
		return errSFTPConfigInvalid
	}
	if f.config.HostKeyCallBack == nil {
		return errSFTPHostKeyRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	f.mu.RLock()
	existing := f.client
	f.mu.RUnlock()
	if existing != nil {
		return nil
	}

	addr := net.JoinHostPort(f.config.Host, strconv.Itoa(f.config.Port))
	var d net.Dialer
	rawConn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("sftp: dial %s failed: %w", addr, err)
	}

	if dl, ok := ctx.Deadline(); ok {
		_ = rawConn.SetDeadline(dl)
	}
	stopCancel := context.AfterFunc(ctx, func() {
		_ = rawConn.SetDeadline(time.Now())
	})

	sshCfg := &ssh.ClientConfig{
		User:            f.config.User,
		Auth:            []ssh.AuthMethod{ssh.Password(f.config.Password)},
		HostKeyCallback: f.config.HostKeyCallBack,
	}

	c, chans, reqs, err := ssh.NewClientConn(rawConn, addr, sshCfg)
	if err != nil {
		stopCancel()
		_ = rawConn.Close()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("sftp: ssh handshake %s failed: %w", addr, err)
	}

	sshClient := ssh.NewClient(c, chans, reqs)
	client, err := sftp.NewClient(sshClient)
	if err != nil {
		stopCancel()
		_ = sshClient.Close()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("sftp: subsystem init failed: %w", err)
	}
	stopCancel()
	_ = rawConn.SetDeadline(time.Time{})
	if ctxErr := ctx.Err(); ctxErr != nil {
		_ = client.Close()
		_ = sshClient.Close()
		return ctxErr
	}

	f.mu.Lock()
	if f.client != nil {
		f.mu.Unlock()
		_ = client.Close()
		_ = sshClient.Close()
		return nil
	}
	f.client = client
	f.sshConn = sshClient
	f.mu.Unlock()

	f.logLogf("connected to SFTP client successfully")
	return nil
}

// Close closes the SFTP client and underlying SSH connection idempotently.
func (f *FileSystem) Close() error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	client := f.client
	sshConn := f.sshConn
	f.client = nil
	f.sshConn = nil
	f.mu.Unlock()

	var closeErr error
	if closer, ok := client.(io.Closer); ok && closer != nil {
		closeErr = errors.Join(closeErr, closer.Close())
	}
	if sshConn != nil {
		closeErr = errors.Join(closeErr, sshConn.Close())
	}
	return closeErr
}

// HealthCheck verifies SFTP connectivity by querying the working directory.
func (f *FileSystem) HealthCheck(ctx context.Context) (any, error) {
	if f == nil {
		return nil, errSFTPNotConnected
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	f.mu.RLock()
	client := f.client
	f.mu.RUnlock()
	if client == nil {
		return nil, errSFTPNotConnected
	}
	wd, err := client.Getwd()
	if err != nil {
		return nil, err
	}
	return map[string]any{"host": f.config.Host, "cwd": wd}, nil
}

func (f *FileSystem) getClient() sftpClient {
	if f == nil {
		return nil
	}
	f.mu.RLock()
	client := f.client
	f.mu.RUnlock()
	return client
}

func (f *FileSystem) Create(name string) (file.File, error) {
	status := statusError

	defer f.sendOperationStats(&FileLog{
		Operation: "CREATE",
		Location:  name,
		Status:    &status,
	}, time.Now())

	client := f.getClient()
	if client == nil {
		return nil, errSFTPNotConnected
	}

	newFile, err := client.Create(name)
	if err != nil {
		return nil, err
	}

	status = statusSuccess

	return sftpFile{
		File:   newFile,
		logger: f.logger,
	}, nil
}

func (f *FileSystem) Mkdir(name string, _ os.FileMode) error {
	status := statusSuccess

	defer f.sendOperationStats(&FileLog{Operation: "MKDIR", Location: name, Status: &status}, time.Now())

	client := f.getClient()
	if client == nil {
		status = statusError
		return errSFTPNotConnected
	}

	err := client.Mkdir(name)
	if err != nil {
		status = statusError
		return err
	}

	return nil
}

func (f *FileSystem) MkdirAll(path string, _ os.FileMode) error {
	status := statusSuccess

	defer f.sendOperationStats(&FileLog{Operation: "MKDIR", Location: path, Status: &status}, time.Now())

	client := f.getClient()
	if client == nil {
		status = statusError
		return errSFTPNotConnected
	}

	err := client.MkdirAll(path)
	if err != nil {
		status = statusError
		return err
	}

	return nil
}

func (f *FileSystem) Open(name string) (file.File, error) {
	status := statusSuccess

	defer f.sendOperationStats(&FileLog{Operation: "OPEN", Location: name, Status: &status}, time.Now())

	client := f.getClient()
	if client == nil {
		status = statusError
		return nil, errSFTPNotConnected
	}

	openedFile, err := client.Open(name)
	if err != nil {
		status = statusError

		return nil, err
	}

	return sftpFile{
		File:   openedFile,
		logger: f.logger,
	}, nil
}

func (f *FileSystem) OpenFile(name string, flag int, _ os.FileMode) (file.File, error) {
	status := statusSuccess

	defer f.sendOperationStats(&FileLog{Operation: "OPENFILE", Location: name, Status: &status}, time.Now())

	client := f.getClient()
	if client == nil {
		status = statusError
		return nil, errSFTPNotConnected
	}

	openedFile, err := client.OpenFile(name, flag)
	if err != nil {
		status = statusSuccess

		return nil, err
	}

	return sftpFile{
		File:   openedFile,
		logger: f.logger,
	}, nil
}

func (f *FileSystem) Remove(name string) error {
	status := statusSuccess

	defer f.sendOperationStats(&FileLog{Operation: "REMOVE", Location: name, Status: &status}, time.Now())

	client := f.getClient()
	if client == nil {
		status = statusError
		return errSFTPNotConnected
	}

	err := client.Remove(name)
	if err != nil {
		status = statusError
		return err
	}

	return nil
}

func (f *FileSystem) RemoveAll(path string) error {
	status := statusSuccess

	defer f.sendOperationStats(&FileLog{Operation: "REMOVEALL", Location: path, Status: &status}, time.Now())

	client := f.getClient()
	if client == nil {
		status = statusError
		return errSFTPNotConnected
	}

	err := client.RemoveAll(path)
	if err != nil {
		status = statusError
		return err
	}

	return nil
}

func (f *FileSystem) Rename(oldname, newname string) error {
	status := statusSuccess

	defer f.sendOperationStats(&FileLog{Operation: "RENAME", Location: fmt.Sprintf("%v to %v", oldname, newname),
		Status: &status}, time.Now())

	client := f.getClient()
	if client == nil {
		status = statusError
		return errSFTPNotConnected
	}

	err := client.Rename(oldname, newname)
	if err != nil {
		status = statusError
		return err
	}

	return nil
}

func (f *FileSystem) ReadDir(dir string) ([]file.FileInfo, error) {
	status := statusSuccess

	defer f.sendOperationStats(&FileLog{Operation: "READDIR", Location: dir, Status: &status}, time.Now())

	client := f.getClient()
	if client == nil {
		status = statusError
		return nil, errSFTPNotConnected
	}

	dirs, err := client.ReadDir(dir)
	if err != nil {
		status = statusError
		return nil, err
	}

	newDirs := make([]file.FileInfo, 0, len(dirs))

	for _, v := range dirs {
		newDirs = append(newDirs, v)
	}

	return newDirs, nil
}

func (f *FileSystem) Stat(name string) (file.FileInfo, error) {
	status := statusSuccess

	defer f.sendOperationStats(&FileLog{Operation: "STAT", Location: name, Status: &status}, time.Now())

	client := f.getClient()
	if client == nil {
		status = statusError
		return nil, errSFTPNotConnected
	}

	fileInfo, err := client.Stat(name)
	if err != nil {
		status = statusError
		return nil, err
	}

	return fileInfo, nil
}

func (f *FileSystem) ChDir(_ string) error {
	f.logErrorf("Chdir is not implemented for SFTP")
	return nil
}

func (f *FileSystem) Getwd() (string, error) {
	status := statusSuccess

	defer f.sendOperationStats(&FileLog{Operation: "STAT", Location: "", Status: &status}, time.Now())

	client := f.getClient()
	if client == nil {
		status = statusError
		return "", errSFTPNotConnected
	}

	name, err := client.Getwd()
	if err != nil {
		status = statusError
		return "", err
	}

	return name, err
}

// TODO : Implement metrics.
func (f *FileSystem) sendOperationStats(fl *FileLog, startTime time.Time) {
	duration := time.Since(startTime).Microseconds()

	fl.Duration = duration

	if f != nil && f.logger != nil {
		f.logger.Debug(fl)
	}
}
