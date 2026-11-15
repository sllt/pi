package nats

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/sllt/pi/pkg/pi/datasource/pubsub"
)

const batchSize = 100

// Config defines the Client configuration.
type Config struct {
	Server      string
	CredsFile   string
	Stream      StreamConfig
	Consumer    string
	MaxWait     time.Duration
	MaxPullWait int
}

// StreamConfig holds stream settings for NATS jStream.
type StreamConfig struct {
	Stream     string
	Subjects   []string
	MaxDeliver int
	MaxWait    time.Duration
	MaxBytes   int64
	Storage    string
	Retention  string
	MaxAge     time.Duration
}

// New creates a new Client.
func New(cfg *Config, logger pubsub.Logger) *PubSubWrapper {
	if cfg == nil {
		cfg = &Config{}
	}

	client := &Client{
		Config:     cfg,
		subManager: newSubscriptionManager(batchSize),
		logger:     logger,
	}

	return &PubSubWrapper{Client: client}
}

// FromEnv constructs a zero-I/O NATS *PubSubWrapper from an environment/config lookup function.
//
// Supported keys:
//   - NATS_SERVER (fallback: NATS_URL, required)
//   - NATS_CREDS_FILE
//   - NATS_STREAM
//   - NATS_SUBJECTS (comma-separated; defaults to NATS_STREAM when non-empty)
//   - NATS_CONSUMER (default: "default")
//   - NATS_MAX_WAIT (Go duration, e.g. "5s")
//   - NATS_MAX_PULL_WAIT (int)
func FromEnv(get func(string) string) (*PubSubWrapper, error) {
	if get == nil {
		return nil, errServerNotProvided
	}

	server := strings.TrimSpace(get("NATS_SERVER"))
	if server == "" {
		server = strings.TrimSpace(get("NATS_URL"))
	}
	if server == "" {
		return nil, errServerNotProvided
	}

	stream := strings.TrimSpace(get("NATS_STREAM"))
	var subjects []string
	if rawSubjects := strings.TrimSpace(get("NATS_SUBJECTS")); rawSubjects != "" {
		for _, part := range strings.Split(rawSubjects, ",") {
			if s := strings.TrimSpace(part); s != "" {
				subjects = append(subjects, s)
			}
		}
	} else if stream != "" {
		subjects = []string{stream}
	}

	consumer := strings.TrimSpace(get("NATS_CONSUMER"))
	if consumer == "" {
		consumer = "default"
	}

	var maxWait time.Duration
	if s := strings.TrimSpace(get("NATS_MAX_WAIT")); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("nats: invalid NATS_MAX_WAIT %q", s)
		}
		maxWait = d
	}

	var maxPullWait int
	if s := strings.TrimSpace(get("NATS_MAX_PULL_WAIT")); s != "" {
		v, err := strconv.Atoi(s)
		if err != nil || v < 0 {
			return nil, fmt.Errorf("nats: invalid NATS_MAX_PULL_WAIT %q", s)
		}
		maxPullWait = v
	}

	cfg := &Config{
		Server:    server,
		CredsFile: strings.TrimSpace(get("NATS_CREDS_FILE")),
		Stream: StreamConfig{
			Stream:   stream,
			Subjects: subjects,
		},
		Consumer:    consumer,
		MaxWait:     maxWait,
		MaxPullWait: maxPullWait,
	}
	if err := validateConfigs(cfg); err != nil {
		return nil, err
	}

	return New(cfg, nil), nil
}

// validateConfigs validates the configuration for NATS jStream.
func validateConfigs(conf *Config) error {
	if conf.Server == "" {
		return errServerNotProvided
	}

	if len(conf.Stream.Subjects) == 0 {
		return errSubjectsNotProvided
	}

	if conf.Consumer == "" {
		return errConsumerNotProvided
	}

	return nil
}
