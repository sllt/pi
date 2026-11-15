package redis

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/sllt/pi/pkg/pi/config"
	"github.com/sllt/pi/pkg/pi/datasource"
)

const (
	defaultRedisPort          = 6379
	defaultPubSubDB           = 15 // Highest default Redis database (0-15)
	modePubSub                = "pubsub"
	modeStreams               = "streams"
	defaultPubSubBufferSize   = 100
	defaultPubSubQueryLimit   = 10
	defaultPubSubQueryTimeout = 5 * time.Second
)

// getRedisConfig builds the Redis Config struct from the provided [Config].
// It supports TLS configuration using the following environment variables:
//
//	REDIS_TLS_ENABLED: set to "true" to enable TLS
//	REDIS_TLS_CA_CERT: PEM-encoded CA certificate (string)
//	REDIS_TLS_CERT:    PEM-encoded client certificate (string or file path)
//	REDIS_TLS_KEY:     PEM-encoded client private key (string or file path)
//
// If TLS is enabled, the function sets up the [tls.Config] for the [Redis] client.
func getRedisConfig(c config.Config, logger datasource.Logger) *Config {
	var redisConfig = &Config{}

	redisConfig.HostName = c.Get("REDIS_HOST")
	redisConfig.Username = c.Get("REDIS_USER")
	redisConfig.Password = c.Get("REDIS_PASSWORD")

	port, err := strconv.Atoi(c.Get("REDIS_PORT"))
	if err != nil {
		port = defaultRedisPort
	}

	redisConfig.Port = port

	db, err := strconv.Atoi(c.Get("REDIS_DB"))
	if err != nil {
		db = 0 // default to DB 0 if not specified
	}

	redisConfig.DB = db

	options := new(redis.Options)
	options.Addr = fmt.Sprintf("%s:%d", redisConfig.HostName, redisConfig.Port)
	options.Username = redisConfig.Username
	options.Password = redisConfig.Password
	options.DB = redisConfig.DB

	// Parse PubSub config if PUBSUB_BACKEND=REDIS
	if strings.EqualFold(c.Get("PUBSUB_BACKEND"), "REDIS") {
		parsePubSubConfig(c, redisConfig)
	}

	if c.Get("REDIS_TLS_ENABLED") != "true" {
		redisConfig.Options = options
		return redisConfig
	}

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if rawSkip := strings.TrimSpace(c.Get("REDIS_TLS_INSECURE_SKIP_VERIFY")); rawSkip != "" {
		if skipVerify, err := strconv.ParseBool(rawSkip); err != nil {
			if logger != nil {
				logger.Errorf("invalid REDIS_TLS_INSECURE_SKIP_VERIFY %q: %v", rawSkip, err)
			}
		} else {
			tlsConfig.InsecureSkipVerify = skipVerify //nolint:gosec // Explicitly configured via REDIS_TLS_INSECURE_SKIP_VERIFY
		}
	}

	if caCertPath := c.Get("REDIS_TLS_CA_CERT"); caCertPath != "" {
		caCert, err := os.ReadFile(caCertPath)
		if err != nil {
			logger.Errorf("failed to read CA cert file: %v", err)
		} else {
			initializeCerts(logger, caCert, tlsConfig)
		}
	}

	// Load client cert and key from file paths
	certPath := c.Get("REDIS_TLS_CERT")
	keyPath := c.Get("REDIS_TLS_KEY")

	if certPath != "" && keyPath != "" {
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			logger.Errorf("failed to load client cert/key pair: %v", err)
		} else {
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
	}

	options.TLSConfig = tlsConfig
	redisConfig.TLS = tlsConfig
	redisConfig.Options = options

	return redisConfig
}

// parsePubSubConfig parses PubSub configuration from environment variables.
func parsePubSubConfig(c config.Config, redisConfig *Config) {
	parsePubSubMode(c, redisConfig)
	parsePubSubCommonConfig(c, redisConfig)
}

// parsePubSubMode parses the PubSub mode configuration.
func parsePubSubMode(c config.Config, redisConfig *Config) {
	mode := strings.ToLower(c.GetOrDefault("REDIS_PUBSUB_MODE", modeStreams))

	if mode != modeStreams && mode != modePubSub {
		mode = modeStreams
	}

	redisConfig.PubSubMode = mode

	// Parse Streams config if mode is streams
	if mode == modeStreams {
		configStreams(c, redisConfig)
	}
}

// parsePubSubCommonConfig parses common PubSub configuration (buffer size, query timeout, query limit).
func parsePubSubCommonConfig(c config.Config, redisConfig *Config) {
	// Buffer Size
	bufferSizeStr := c.GetOrDefault("REDIS_PUBSUB_BUFFER_SIZE", strconv.Itoa(defaultPubSubBufferSize))

	bufferSize, err := strconv.Atoi(bufferSizeStr)
	if err != nil || bufferSize <= 0 {
		redisConfig.PubSubBufferSize = defaultPubSubBufferSize
	} else {
		redisConfig.PubSubBufferSize = bufferSize
	}

	// Query Timeout
	timeoutStr := c.GetOrDefault("REDIS_PUBSUB_QUERY_TIMEOUT", defaultPubSubQueryTimeout.String())

	timeout, err := time.ParseDuration(timeoutStr)
	if err != nil {
		redisConfig.PubSubQueryTimeout = defaultPubSubQueryTimeout
	} else {
		redisConfig.PubSubQueryTimeout = timeout
	}

	// Query Limit
	limitStr := c.GetOrDefault("REDIS_PUBSUB_QUERY_LIMIT", strconv.Itoa(defaultPubSubQueryLimit))

	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		redisConfig.PubSubQueryLimit = defaultPubSubQueryLimit
	} else {
		redisConfig.PubSubQueryLimit = limit
	}
}

func configStreams(c config.Config, redisConfig *Config) {
	streamsConfig := &StreamsConfig{
		ConsumerGroup: c.Get("REDIS_STREAMS_CONSUMER_GROUP"),
		ConsumerName:  c.Get("REDIS_STREAMS_CONSUMER_NAME"),
		PELRatio:      0.7, // Default: 70% PEL, 30% new messages
	}

	streamsConfig.Block = 1 * time.Second // default - reduced from 5s for better responsiveness
	if blockStr := c.Get("REDIS_STREAMS_BLOCK_TIMEOUT"); blockStr != "" {
		if block, err := time.ParseDuration(blockStr); err == nil {
			streamsConfig.Block = block
		}
	}

	if maxLenStr := c.Get("REDIS_STREAMS_MAXLEN"); maxLenStr != "" {
		if maxLen, err := strconv.ParseInt(maxLenStr, 10, 64); err == nil {
			streamsConfig.MaxLen = maxLen
		}
	}

	// Parse PEL ratio (0.0-1.0)
	if pelRatioStr := c.Get("REDIS_STREAMS_PEL_RATIO"); pelRatioStr != "" {
		if pelRatio, err := strconv.ParseFloat(pelRatioStr, 64); err == nil {
			// Validate range: 0.0 to 1.0
			if pelRatio >= 0.0 && pelRatio <= 1.0 {
				streamsConfig.PELRatio = pelRatio
			}
		}
	}

	redisConfig.PubSubStreamsConfig = streamsConfig
}

func initializeCerts(logger datasource.Logger, caCert []byte, tlsConfig *tls.Config) {
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		logger.Errorf("failed to append CA cert to pool")
	} else {
		tlsConfig.RootCAs = caCertPool
	}
}

// ValidateConfig validates Redis configuration purely in memory without disk I/O or network calls.
func ValidateConfig(c config.Config) error {
	if c == nil {
		return errors.New("redis: config is required")
	}
	if strings.TrimSpace(c.Get("REDIS_HOST")) == "" {
		return errors.New("redis: REDIS_HOST is required")
	}
	if rawPort := strings.TrimSpace(c.Get("REDIS_PORT")); rawPort != "" {
		port, err := strconv.Atoi(rawPort)
		if err != nil || port < 1 || port > 65535 {
			return fmt.Errorf("redis: invalid REDIS_PORT %q: must be 1..65535", rawPort)
		}
	}
	if rawDB := strings.TrimSpace(c.Get("REDIS_DB")); rawDB != "" {
		db, err := strconv.Atoi(rawDB)
		if err != nil || db < 0 {
			return fmt.Errorf("redis: invalid REDIS_DB %q: must be >= 0", rawDB)
		}
	}
	if rawTLS := strings.TrimSpace(c.Get("REDIS_TLS_ENABLED")); rawTLS != "" {
		if _, err := strconv.ParseBool(rawTLS); err != nil {
			return fmt.Errorf("redis: invalid REDIS_TLS_ENABLED %q: %w", rawTLS, err)
		}
	}
	if rawSkip := strings.TrimSpace(c.Get("REDIS_TLS_INSECURE_SKIP_VERIFY")); rawSkip != "" {
		if _, err := strconv.ParseBool(rawSkip); err != nil {
			return fmt.Errorf("redis: invalid REDIS_TLS_INSECURE_SKIP_VERIFY %q: %w", rawSkip, err)
		}
	}
	certPath := strings.TrimSpace(c.Get("REDIS_TLS_CERT"))
	keyPath := strings.TrimSpace(c.Get("REDIS_TLS_KEY"))
	if (certPath != "") != (keyPath != "") {
		return errors.New("redis: REDIS_TLS_CERT and REDIS_TLS_KEY must be configured together")
	}
	if strings.EqualFold(strings.TrimSpace(c.Get("PUBSUB_BACKEND")), "REDIS") {
		if err := validatePubSubOptions(c); err != nil {
			return err
		}
	}
	return nil
}

// ValidatePubSubConfig validates Redis PubSub configuration purely in memory without disk I/O or network calls.
func ValidatePubSubConfig(c config.Config) error {
	if err := ValidateConfig(c); err != nil {
		return err
	}
	return validatePubSubOptions(c)
}

func validatePubSubOptions(c config.Config) error {
	if rawDB := strings.TrimSpace(c.Get("REDIS_PUBSUB_DB")); rawDB != "" {
		db, err := strconv.Atoi(rawDB)
		if err != nil || db < 0 {
			return fmt.Errorf("redis: invalid REDIS_PUBSUB_DB %q: must be >= 0", rawDB)
		}
	}
	mode := modeStreams
	if rawMode := strings.ToLower(strings.TrimSpace(c.Get("REDIS_PUBSUB_MODE"))); rawMode != "" {
		if rawMode != modeStreams && rawMode != modePubSub {
			return fmt.Errorf("redis: invalid REDIS_PUBSUB_MODE %q: must be %q or %q", rawMode, modeStreams, modePubSub)
		}
		mode = rawMode
	}
	if rawBuf := strings.TrimSpace(c.Get("REDIS_PUBSUB_BUFFER_SIZE")); rawBuf != "" {
		buf, err := strconv.Atoi(rawBuf)
		if err != nil || buf <= 0 {
			return fmt.Errorf("redis: invalid REDIS_PUBSUB_BUFFER_SIZE %q: must be > 0", rawBuf)
		}
	}
	if rawTimeout := strings.TrimSpace(c.Get("REDIS_PUBSUB_QUERY_TIMEOUT")); rawTimeout != "" {
		d, err := time.ParseDuration(rawTimeout)
		if err != nil || d <= 0 {
			return fmt.Errorf("redis: invalid REDIS_PUBSUB_QUERY_TIMEOUT %q: must be a positive duration", rawTimeout)
		}
	}
	if rawLimit := strings.TrimSpace(c.Get("REDIS_PUBSUB_QUERY_LIMIT")); rawLimit != "" {
		limit, err := strconv.Atoi(rawLimit)
		if err != nil || limit <= 0 {
			return fmt.Errorf("redis: invalid REDIS_PUBSUB_QUERY_LIMIT %q: must be > 0", rawLimit)
		}
	}
	if mode == modeStreams {
		if rawBlock := strings.TrimSpace(c.Get("REDIS_STREAMS_BLOCK_TIMEOUT")); rawBlock != "" {
			d, err := time.ParseDuration(rawBlock)
			if err != nil || d < 0 {
				return fmt.Errorf("redis: invalid REDIS_STREAMS_BLOCK_TIMEOUT %q: must be >= 0", rawBlock)
			}
		}
		if rawMaxLen := strings.TrimSpace(c.Get("REDIS_STREAMS_MAXLEN")); rawMaxLen != "" {
			maxLen, err := strconv.ParseInt(rawMaxLen, 10, 64)
			if err != nil || maxLen < 0 {
				return fmt.Errorf("redis: invalid REDIS_STREAMS_MAXLEN %q: must be >= 0", rawMaxLen)
			}
		}
		if rawRatio := strings.TrimSpace(c.Get("REDIS_STREAMS_PEL_RATIO")); rawRatio != "" {
			ratio, err := strconv.ParseFloat(rawRatio, 64)
			if err != nil || math.IsNaN(ratio) || ratio < 0.0 || ratio > 1.0 {
				return fmt.Errorf("redis: invalid REDIS_STREAMS_PEL_RATIO %q: must be between 0.0 and 1.0", rawRatio)
			}
		}
	}
	return nil
}

func buildStrictRedisConfig(c config.Config, includePubSub bool) (*Config, error) {
	if includePubSub {
		if err := ValidatePubSubConfig(c); err != nil {
			return nil, err
		}
	} else {
		if err := ValidateConfig(c); err != nil {
			return nil, err
		}
	}

	redisConfig := &Config{
		HostName: strings.TrimSpace(c.Get("REDIS_HOST")),
		Username: c.Get("REDIS_USER"),
		Password: c.Get("REDIS_PASSWORD"),
		Port:     defaultRedisPort,
		DB:       0,
	}
	if rawPort := strings.TrimSpace(c.Get("REDIS_PORT")); rawPort != "" {
		redisConfig.Port, _ = strconv.Atoi(rawPort)
	}
	if rawDB := strings.TrimSpace(c.Get("REDIS_DB")); rawDB != "" {
		redisConfig.DB, _ = strconv.Atoi(rawDB)
	}

	options := &redis.Options{
		Addr:     fmt.Sprintf("%s:%d", redisConfig.HostName, redisConfig.Port),
		Username: redisConfig.Username,
		Password: redisConfig.Password,
		DB:       redisConfig.DB,
	}

	if includePubSub || strings.EqualFold(strings.TrimSpace(c.Get("PUBSUB_BACKEND")), "REDIS") {
		parsePubSubConfig(c, redisConfig)
	}

	tlsEnabled := false
	if rawTLS := strings.TrimSpace(c.Get("REDIS_TLS_ENABLED")); rawTLS != "" {
		tlsEnabled, _ = strconv.ParseBool(rawTLS)
	}
	if !tlsEnabled {
		redisConfig.Options = options
		return redisConfig, nil
	}

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if rawSkip := strings.TrimSpace(c.Get("REDIS_TLS_INSECURE_SKIP_VERIFY")); rawSkip != "" {
		skipVerify, _ := strconv.ParseBool(rawSkip)
		tlsConfig.InsecureSkipVerify = skipVerify //nolint:gosec // Explicitly configured via REDIS_TLS_INSECURE_SKIP_VERIFY
	}
	if caCertPath := strings.TrimSpace(c.Get("REDIS_TLS_CA_CERT")); caCertPath != "" {
		caCert, err := os.ReadFile(caCertPath)
		if err != nil {
			return nil, fmt.Errorf("redis: read REDIS_TLS_CA_CERT %q: %w", caCertPath, err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("redis: invalid PEM certificate in REDIS_TLS_CA_CERT %q", caCertPath)
		}
		tlsConfig.RootCAs = caCertPool
	}

	certPath := strings.TrimSpace(c.Get("REDIS_TLS_CERT"))
	keyPath := strings.TrimSpace(c.Get("REDIS_TLS_KEY"))
	if certPath != "" && keyPath != "" {
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, fmt.Errorf("redis: load client certificate/key pair: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	options.TLSConfig = tlsConfig
	redisConfig.TLS = tlsConfig
	redisConfig.Options = options
	return redisConfig, nil
}
