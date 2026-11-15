package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"mime"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	file "github.com/sllt/pi/pkg/pi/datasource/file"
)

const (
	typeFile      = "file"
	typeDirectory = "directory"
)

var (
	errIncorrectFileType = errors.New("incorrect file type")
	errS3ConfigNil       = errors.New("s3: configuration is required")
	errS3BucketRequired  = errors.New("s3: bucket name is required")
	errS3RegionRequired  = errors.New("s3: region is required")
	errS3NotConnected    = errors.New("s3: client is not connected")
)

// client struct embeds the *s3.Client.
type client struct {
	*s3.Client
}

type FileSystem struct {
	mu      sync.RWMutex
	s3File  S3File
	conn    s3Client
	config  *Config
	logger  Logger
	metrics Metrics
}

// Config represents the s3 configuration.
type Config struct {
	EndPoint        string // AWS S3 base endpoint
	BucketName      string // AWS Bucket name
	Region          string // AWS Region
	AccessKeyID     string // Aws configs
	SecretAccessKey string // Aws configs
}

// New initializes a new instance of S3 FileSystem with provided configuration.
func New(config *Config) file.FileSystemProvider {
	return &FileSystem{config: config}
}

// FromEnv constructs an unstarted S3 FileSystem from an environment lookup function.
func FromEnv(get func(string) string) (*FileSystem, error) {
	if get == nil {
		return nil, errS3ConfigNil
	}

	firstNonEmpty := func(keys ...string) string {
		for _, k := range keys {
			if v := strings.TrimSpace(get(k)); v != "" {
				return v
			}
		}
		return ""
	}

	cfg := &Config{
		EndPoint:        firstNonEmpty("S3_ENDPOINT"),
		BucketName:      firstNonEmpty("S3_BUCKET", "S3_BUCKET_NAME"),
		Region:          firstNonEmpty("S3_REGION", "AWS_REGION"),
		AccessKeyID:     firstNonEmpty("S3_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID"),
		SecretAccessKey: firstNonEmpty("S3_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY"),
	}
	if cfg.BucketName == "" {
		return nil, errS3BucketRequired
	}
	if cfg.Region == "" {
		return nil, errS3RegionRequired
	}
	return &FileSystem{config: cfg}, nil
}

// UseLogger sets the Logger interface for the S3 file system.
func (f *FileSystem) UseLogger(logger any) {
	if l, ok := logger.(Logger); ok {
		f.logger = l
	}
}

// UseMetrics sets the Metrics interface.
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

func (f *FileSystem) getConn() s3Client {
	if f == nil {
		return nil
	}
	f.mu.RLock()
	conn := f.conn
	f.mu.RUnlock()
	return conn
}

// Connect initializes and validates the connection to the S3 service.
//
// This method sets up the S3 client using the provided configuration, including access key, secret key, region, and base endpoint.
// It loads the AWS configuration and creates an S3 client, which is then assigned to the `FileSystem` struct.
// This method also logs the outcome of the connection attempt.
func (f *FileSystem) Connect() {
	if f == nil || f.config == nil {
		return
	}

	var msg string

	st := statusErr

	defer f.sendOperationStats(&FileLog{
		Operation: "CONNECT",
		Location:  getLocation(f.config.BucketName),
		Status:    &st,
		Message:   &msg,
	}, time.Now())

	f.logDebugf("connecting to S3 bucket: %s", f.config.BucketName)

	opts := []func(*awsConfig.LoadOptions) error{
		awsConfig.WithRegion(f.config.Region),
	}
	if f.config.AccessKeyID != "" || f.config.SecretAccessKey != "" {
		opts = append(opts, awsConfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				f.config.AccessKeyID,
				f.config.SecretAccessKey,
				""),
		))
	}

	cfg, err := awsConfig.LoadDefaultConfig(context.TODO(), opts...)
	if err != nil {
		f.logErrorf("failed to load configuration: %v", err)
		return
	}

	s3Client := s3.NewFromConfig(cfg,
		func(o *s3.Options) {
			o.UsePathStyle = true
			if f.config.EndPoint != "" {
				o.BaseEndpoint = &f.config.EndPoint
			}
		},
	)

	f.mu.Lock()
	f.conn = client{s3Client}
	f.mu.Unlock()

	st = statusSuccess
	msg = "S3 Client connected."

	f.logLogf("connected to S3 bucket %s", f.config.BucketName)
}

// ConnectContext initializes the S3 client and verifies bucket reachability within ctx.
func (f *FileSystem) ConnectContext(ctx context.Context) error {
	if f == nil || f.config == nil {
		return errS3ConfigNil
	}
	if strings.TrimSpace(f.config.BucketName) == "" {
		return errS3BucketRequired
	}
	if strings.TrimSpace(f.config.Region) == "" {
		return errS3RegionRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	f.mu.RLock()
	existing := f.conn
	f.mu.RUnlock()
	if existing != nil {
		return nil
	}

	opts := []func(*awsConfig.LoadOptions) error{
		awsConfig.WithRegion(f.config.Region),
	}
	if f.config.AccessKeyID != "" || f.config.SecretAccessKey != "" {
		opts = append(opts, awsConfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				f.config.AccessKeyID,
				f.config.SecretAccessKey,
				""),
		))
	}

	cfg, err := awsConfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return fmt.Errorf("s3: failed to load configuration: %w", err)
	}

	endpoint := strings.TrimSpace(f.config.EndPoint)
	s3Client := s3.NewFromConfig(cfg,
		func(o *s3.Options) {
			o.UsePathStyle = true
			if endpoint != "" {
				o.BaseEndpoint = &endpoint
			}
		},
	)

	if _, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(f.config.BucketName),
		MaxKeys: aws.Int32(1),
	}); err != nil {
		return fmt.Errorf("s3: failed to verify bucket %q: %w", f.config.BucketName, err)
	}

	f.mu.Lock()
	f.conn = client{s3Client}
	f.mu.Unlock()

	f.logLogf("connected to S3 bucket %s", f.config.BucketName)
	return nil
}

// Close releases the S3 client reference idempotently.
func (f *FileSystem) Close() error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	f.conn = nil
	f.mu.Unlock()
	return nil
}

// HealthCheck probes S3 bucket reachability using ctx.
func (f *FileSystem) HealthCheck(ctx context.Context) (any, error) {
	if f == nil || f.config == nil {
		return map[string]any{"status": "DOWN"}, errS3ConfigNil
	}
	f.mu.RLock()
	conn := f.conn
	f.mu.RUnlock()
	if conn == nil {
		return map[string]any{"bucket": f.config.BucketName, "status": "DOWN"}, errS3NotConnected
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return map[string]any{"bucket": f.config.BucketName, "status": "DOWN"}, err
	}
	if _, err := conn.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(f.config.BucketName),
		MaxKeys: aws.Int32(1),
	}); err != nil {
		return map[string]any{"bucket": f.config.BucketName, "status": "DOWN"}, err
	}
	return map[string]any{"bucket": f.config.BucketName, "status": "UP"}, nil
}

// Create creates a new file in the S3 bucket.
//
// This method creates an empty file at the specified path in the S3 bucket. It first checks if the parent directory exists;
// if the parent directory does not exist, it returns an error. After creating the file, it retrieves the file metadata
// and returns a `file` object representing the newly created file.
func (f *FileSystem) Create(name string) (file.File, error) {
	if f == nil || f.config == nil {
		return nil, errS3ConfigNil
	}
	conn := f.getConn()
	if conn == nil {
		return nil, errS3NotConnected
	}

	var msg string

	st := statusErr

	defer f.sendOperationStats(&FileLog{
		Operation: "CREATE FILE",
		Location:  getLocation(f.config.BucketName),
		Status:    &st,
		Message:   &msg,
	}, time.Now())

	parentPath := path.Dir(name)

	// if parentPath is not empty, we check if it exists or not.
	if parentPath != "." {
		res2, err := conn.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
			Bucket: aws.String(f.config.BucketName),
			Prefix: aws.String(parentPath + "/"),
		})
		if err != nil {
			return nil, err
		}

		if len(res2.Contents) == 0 {
			f.logErrorf("Parentpath %q does not exist", parentPath)
			return nil, fmt.Errorf("%w: create parent path before creating a file", ErrOperationNotPermitted)
		}
	}

	_, err := conn.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:      aws.String(f.config.BucketName),
		Key:         aws.String(name),
		Body:        bytes.NewReader(make([]byte, 0)),
		ContentType: aws.String(mime.TypeByExtension(path.Ext(name))),
		// this specifies the file must be downloaded before being opened
		ContentDisposition: aws.String("attachment"),
	})
	if err != nil {
		f.logErrorf("Failed to create the file: %v", err)
		return nil, err
	}

	res, err := conn.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(f.config.BucketName),
		Key:    aws.String(name),
	})
	if err != nil {
		f.logErrorf("Failed to retrieve %q: %v", name, err)
		return nil, err
	}

	st = statusSuccess
	msg = "File creation on S3 successful."

	f.logLogf("File with name %s created.", name)

	return &S3File{
		conn:         conn,
		name:         path.Join(f.config.BucketName, name),
		logger:       f.logger,
		metrics:      f.metrics,
		body:         res.Body,
		contentType:  *res.ContentType,
		lastModified: *res.LastModified,
		size:         *res.ContentLength,
	}, nil
}

// Remove deletes a file from the S3 bucket.
//
// This method deletes the specified file from the S3 bucket. Currently, it supports the deletion of unversioned files
// from general-purpose buckets only. Directory buckets and versioned files are not supported for deletion by this method.
func (f *FileSystem) Remove(name string) error {
	if f == nil || f.config == nil {
		return errS3ConfigNil
	}
	conn := f.getConn()
	if conn == nil {
		return errS3NotConnected
	}

	var msg string

	st := statusErr

	defer f.sendOperationStats(&FileLog{
		Operation: "REMOVE FILE",
		Location:  getLocation(f.config.BucketName),
		Status:    &st,
		Message:   &msg,
	}, time.Now())

	_, err := conn.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(f.config.BucketName),
		Key:    aws.String(name),
	})
	if err != nil {
		f.logErrorf("Error while deleting file: %v", err)
		return err
	}

	st = statusSuccess
	msg = "File deletion on S3 successful"

	f.logLogf("File with path %q deleted", name)

	return nil
}

// Open retrieves a file from the S3 bucket and returns a `file` object representing it.
//
// This method fetches the specified file from the S3 bucket and returns a `file` object with its content and metadata.
// If the file cannot be retrieved, it returns an error.
func (f *FileSystem) Open(name string) (file.File, error) {
	if f == nil || f.config == nil {
		return nil, errS3ConfigNil
	}
	conn := f.getConn()
	if conn == nil {
		return nil, errS3NotConnected
	}

	var msg string

	st := statusErr

	defer f.sendOperationStats(&FileLog{
		Operation: "OPEN FILE",
		Location:  getLocation(f.config.BucketName),
		Status:    &st,
		Message:   &msg,
	}, time.Now())

	res, err := conn.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(f.config.BucketName),
		Key:    aws.String(name),
	})
	if err != nil {
		f.logErrorf("failed to retrieve %q: %v", name, err)
		return nil, err
	}

	st = statusSuccess
	msg = fmt.Sprintf("File with path %q retrieved successfully", name)

	return &S3File{
		conn:         conn,
		name:         path.Join(f.config.BucketName, name),
		logger:       f.logger,
		metrics:      f.metrics,
		body:         res.Body,
		contentType:  *res.ContentType,
		lastModified: *res.LastModified,
		size:         *res.ContentLength,
	}, nil
}

// OpenFile is a wrapper for the Open method to comply with the generic FileSystem interface.
//
// This method calls the `Open` method of the `fileSystem` struct to retrieve a file. It is provided to align with the
// FileSystem interface requirements in the Pi framework.
func (f *FileSystem) OpenFile(name string, _ int, _ os.FileMode) (file.File, error) {
	return f.Open(name)
}

// Rename changes the name of a file or directory within the S3 bucket.
//
// This method handles both files and directories. It ensures that:
// - The new name does not move the file to a different directory.
// - The file types of the old and new names match.
//
// If the old and new names are the same, no operation is performed.
func (f *FileSystem) Rename(oldname, newname string) error {
	if f == nil || f.config == nil {
		return errS3ConfigNil
	}
	conn := f.getConn()
	if conn == nil {
		return errS3NotConnected
	}

	var msg string

	st := statusErr

	defer f.sendOperationStats(&FileLog{
		Operation: "RENAME",
		Location:  getLocation(f.config.BucketName),
		Status:    &st,
		Message:   &msg,
	}, time.Now())

	// check if they have the same name or not
	if oldname == newname {
		f.logLogf("%q & %q are same", oldname, newname)
		return nil
	}

	// check if both exist at same location or not
	if path.Dir(oldname) != path.Dir(newname) {
		f.logErrorf("%q & %q are not in same location", oldname, newname)
		return fmt.Errorf("%w: renaming as well as moving file to different location is not allowed", ErrOperationNotPermitted)
	}

	// check if it is a directory
	if path.Ext(oldname) == "" {
		return f.renameDirectory(&st, &msg, oldname, newname)
	}

	// check if they are of the same type or not
	if path.Ext(oldname) != path.Ext(newname) {
		f.logErrorf("new file must be same as the old file type")
		return fmt.Errorf("%w: new filename must match the old file's type", errIncorrectFileType)
	}

	_, err := conn.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket: aws.String(f.config.BucketName),
		// The source object can be up to 5 GB.
		// If the source object is an object that was uploaded by using a multipart upload, the object copy
		// will be a single part object after the source object is copied to the destination bucket.
		CopySource:         aws.String(f.config.BucketName + "/" + oldname),
		Key:                aws.String(newname),
		ContentType:        aws.String(mime.TypeByExtension(path.Ext(newname))),
		ContentDisposition: aws.String("attachment"),
	})
	if err != nil {
		msg = fmt.Sprintf("Error while copying file: %v", err)
		return err
	}

	err = f.Remove(oldname)
	if err != nil {
		msg = fmt.Sprintf("failed to remove old file %s", oldname)
		return err
	}

	st = statusSuccess
	msg = "File renamed successfully"

	f.logLogf("File with path %q renamed to %q", oldname, newname)

	return nil
}
