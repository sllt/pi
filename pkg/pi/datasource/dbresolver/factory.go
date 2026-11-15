package dbresolver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/sllt/pi/pkg/pi/config"
	piSQL "github.com/sllt/pi/pkg/pi/datasource/sql"
	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"go.opentelemetry.io/otel/trace"
)

const (
	minIdleReplicaDefault    = 2
	maxIdleReplicaCapDefault = 10
	minOpenReplicaDefault    = 5
	maxOpenReplicaCapDefault = 20
	expectedHostPortParts    = 2
)

var (
	errPrimaryNil               = errors.New("primary SQL connection is nil")
	errInvalidReplicaHostFormat = errors.New("invalid replica host format (expected host:port)")
	errDBNameRequired           = errors.New("DB_NAME is required")
	errEmptyCredentials         = errors.New("replica has empty credentials")
	errInvalidPort              = errors.New("invalid port for replica")
	errAllReplicasFailed        = errors.New("all replicas failed to connect")
)

// Config holds resolver configuration.
type Config struct {
	Strategy      StrategyType
	ReadFallback  bool
	MaxFailures   uint32
	TimeoutSec    uint32
	PrimaryRoutes []string
	Replicas      []ReplicaCredential
}

// ReplicaCredential stores credentials for a single replica.
type ReplicaCredential struct {
	Host     string `json:"host"`     // Format: "hostname:port" (e.g., "localhost:3307").
	User     string `json:"user"`     // Database username.
	Password string `json:"password"` // Database password (supports special characters).
}

type nopLogger struct{}

func (nopLogger) Debug(...any)          {}
func (nopLogger) Debugf(string, ...any) {}
func (nopLogger) Logf(string, ...any)   {}
func (nopLogger) Errorf(string, ...any) {}
func (nopLogger) Warnf(string, ...any)  {}
func (nopLogger) Info(...any)           {}
func (nopLogger) Infof(string, ...any)  {}
func (nopLogger) Error(...any)          {}
func (nopLogger) Warn(...any)           {}

// FromEnv constructs a Resolver Config from an environment/config lookup function.
//
// Supported keys:
//   - DB_RESOLVER_STRATEGY ("round_robin" | "random", default: "round_robin")
//   - DB_RESOLVER_READ_FALLBACK (bool, default: true)
//   - DB_RESOLVER_MAX_FAILURES (uint32, default: 5)
//   - DB_RESOLVER_TIMEOUT_SEC (uint32, default: 30)
//   - DB_RESOLVER_PRIMARY_ROUTES (comma-separated routes, e.g. "/admin/*,/health")
//   - DB_RESOLVER_REPLICAS (JSON array of ReplicaCredential)
func FromEnv(get func(string) string) (*Config, error) {
	cfg := &Config{
		Strategy:     StrategyRoundRobin,
		ReadFallback: true,
		MaxFailures:  defaultMaxFailures,
		TimeoutSec:   defaultTimeoutSec,
	}
	if get == nil {
		return cfg, nil
	}

	if s := strings.TrimSpace(get("DB_RESOLVER_STRATEGY")); s != "" {
		switch strings.ToLower(s) {
		case string(StrategyRoundRobin), "round_robin":
			cfg.Strategy = StrategyRoundRobin
		case string(StrategyRandom):
			cfg.Strategy = StrategyRandom
		default:
			return nil, fmt.Errorf("dbresolver: unsupported DB_RESOLVER_STRATEGY %q", s)
		}
	}
	if s := strings.TrimSpace(get("DB_RESOLVER_READ_FALLBACK")); s != "" {
		v, err := strconv.ParseBool(s)
		if err != nil {
			return nil, fmt.Errorf("dbresolver: invalid DB_RESOLVER_READ_FALLBACK %q: %w", s, err)
		}
		cfg.ReadFallback = v
	}
	if s := strings.TrimSpace(get("DB_RESOLVER_MAX_FAILURES")); s != "" {
		v, err := strconv.ParseUint(s, 10, 32)
		if err != nil || v == 0 {
			return nil, fmt.Errorf("dbresolver: invalid DB_RESOLVER_MAX_FAILURES %q", s)
		}
		cfg.MaxFailures = uint32(v)
	}
	if s := strings.TrimSpace(get("DB_RESOLVER_TIMEOUT_SEC")); s != "" {
		v, err := strconv.ParseUint(s, 10, 32)
		if err != nil || v == 0 {
			return nil, fmt.Errorf("dbresolver: invalid DB_RESOLVER_TIMEOUT_SEC %q", s)
		}
		cfg.TimeoutSec = uint32(v)
	}
	if s := strings.TrimSpace(get("DB_RESOLVER_PRIMARY_ROUTES")); s != "" {
		for _, part := range strings.Split(s, ",") {
			if route := strings.TrimSpace(part); route != "" {
				cfg.PrimaryRoutes = append(cfg.PrimaryRoutes, route)
			}
		}
	}
	if s := strings.TrimSpace(get("DB_RESOLVER_REPLICAS")); s != "" {
		if err := json.Unmarshal([]byte(s), &cfg.Replicas); err != nil {
			return nil, fmt.Errorf("dbresolver: invalid DB_RESOLVER_REPLICAS JSON: %w", err)
		}
	}

	return cfg, nil
}

// getStrategy returns the configured strategy.
func getStrategy(strategyType StrategyType) Strategy {
	switch strategyType {
	case StrategyRandom:
		return NewRandomStrategy()
	case StrategyRoundRobin:
		return NewRoundRobinStrategy()
	default:
		return NewRoundRobinStrategy()
	}
}

// HTTPMiddleware returns a standard HTTP middleware that injects the request method
// and path into the request context for read/write routing decisions.
func HTTPMiddleware() piHTTP.Middleware {
	return createHTTPMiddleware()
}

// createHTTPMiddleware injects HTTP method/path into context.
func createHTTPMiddleware() piHTTP.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			ctx = WithHTTPMethod(ctx, r.Method)
			ctx = WithRequestPath(ctx, r.URL.Path)

			r = r.WithContext(ctx)
			next.ServeHTTP(w, r)
		})
	}
}

type replicaOpenFunc func(ctx context.Context, cfg config.Config, logger Logger, metrics Metrics) (DB, error)

// OpenReplicasContext opens and pings replica connections using OpenContextWithTracerProvider
// without starting background reconnect goroutines or mutating global OTel state.
// If validation fails at replica i or context is cancelled, any already-opened replicas[0..i-1]
// are closed before returning the error.
func OpenReplicasContext(
	ctx context.Context,
	cfg *Config,
	appCfg config.Config,
	logger Logger,
	metrics Metrics,
	tp trace.TracerProvider,
) ([]DB, error) {
	return openReplicasWith(ctx, cfg, appCfg, logger, metrics, func(openCtx context.Context, rCfg config.Config, log Logger, met Metrics) (DB, error) {
		return piSQL.OpenContextWithTracerProvider(openCtx, rCfg, log, met, tp)
	})
}

// connectReplicas creates replica connections using legacy NewSQL (retained for backward-compatible unit tests).
func connectReplicas(cfg *Config, appCfg config.Config, logger Logger, metrics Metrics) ([]DB, error) {
	return openReplicasWith(context.Background(), cfg, appCfg, logger, metrics, func(_ context.Context, rCfg config.Config, log Logger, met Metrics) (DB, error) {
		return piSQL.NewSQL(rCfg, log, met), nil
	})
}

func openReplicasWith(
	ctx context.Context,
	cfg *Config,
	appCfg config.Config,
	logger Logger,
	metrics Metrics,
	openFn replicaOpenFunc,
) ([]DB, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if cfg == nil || len(cfg.Replicas) == 0 {
		return nil, nil
	}
	if appCfg == nil || appCfg.Get("DB_NAME") == "" {
		return nil, errDBNameRequired
	}
	if logger == nil {
		logger = nopLogger{}
	}

	replicas := make([]DB, 0, len(cfg.Replicas))
	var failedReplicas []string

	for i, cred := range cfg.Replicas {
		if err := ctx.Err(); err != nil {
			closeOpenedReplicas(replicas)
			return nil, err
		}

		if err := validateReplicaCredentials(cred, i); err != nil {
			closeOpenedReplicas(replicas)
			return nil, err
		}

		host, port, err := parseReplicaHost(cred.Host, i)
		if err != nil {
			closeOpenedReplicas(replicas)
			return nil, err
		}

		replicaCfg := &replicaConfig{
			base:     appCfg,
			host:     host,
			port:     port,
			user:     cred.User,
			password: cred.Password,
		}

		replica, err := openFn(ctx, replicaCfg, logger, metrics)
		if err != nil {
			logger.Warnf("Failed to connect to replica #%d (%s): %v", i+1, cred.Host, err)
			failedReplicas = append(failedReplicas, cred.Host)
			continue
		}

		replicas = append(replicas, replica)
		logger.Logf("Created DB replica connection to %s", cred.Host)
	}

	if len(replicas) == 0 {
		return nil, fmt.Errorf("%w (%d total)", errAllReplicasFailed, len(cfg.Replicas))
	}

	if len(failedReplicas) > 0 {
		logger.Warnf("%d/%d replicas failed: %v", len(failedReplicas), len(cfg.Replicas), failedReplicas)
	}

	return replicas, nil
}

func closeOpenedReplicas(replicas []DB) {
	for _, r := range replicas {
		if r != nil {
			_ = r.Close()
		}
	}
}

// validateReplicaCredentials checks if all required fields are present.
func validateReplicaCredentials(cred ReplicaCredential, index int) error {
	if cred.Host == "" || cred.User == "" || cred.Password == "" {
		return fmt.Errorf("%w at index %d", errEmptyCredentials, index)
	}

	return nil
}

// parseReplicaHost splits host:port and validates the format.
func parseReplicaHost(host string, index int) (validatedHost, validatedPort string, err error) {
	parts := strings.Split(host, ":")
	if len(parts) != expectedHostPortParts {
		return "", "", fmt.Errorf("%w at index %d: %s", errInvalidReplicaHostFormat, index, host)
	}

	hostname := strings.TrimSpace(parts[0])
	port := strings.TrimSpace(parts[1])

	p, err := strconv.Atoi(port)
	if err != nil || p <= 0 || p > 65535 {
		return "", "", fmt.Errorf("%w at index %d: %s", errInvalidPort, index, port)
	}

	return hostname, port, nil
}

// createReplicaConnection creates a single replica database connection.
func createReplicaConnection(cfg config.Config, host, port, user, password string, logger Logger, metrics Metrics) (DB, error) {
	if cfg == nil || cfg.Get("DB_NAME") == "" {
		return nil, errDBNameRequired
	}

	replicaCfg := &replicaConfig{
		base:     cfg,
		host:     host,
		port:     port,
		user:     user,
		password: password,
	}

	return piSQL.NewSQL(replicaCfg, logger, metrics), nil
}

// replicaConfig wraps the main config and overrides specific values.
type replicaConfig struct {
	base     config.Config
	host     string
	port     string
	user     string
	password string
}

func (r *replicaConfig) Get(key string) string {
	switch key {
	case "DB_HOST":
		return r.host
	case "DB_PORT":
		return r.port
	case "DB_USER":
		return r.user
	case "DB_PASSWORD":
		return r.password
	case "DB_MAX_IDLE_CONNECTION", "DB_MAX_IDLE_CONNECTIONS":
		return optimizedIdleConnections(r.base)
	case "DB_MAX_OPEN_CONNECTION", "DB_MAX_OPEN_CONNECTIONS":
		return optimizedOpenConnections(r.base)
	default:
		return r.base.Get(key)
	}
}

func (r *replicaConfig) GetOrDefault(key, defaultValue string) string {
	val := r.Get(key)
	if val == "" {
		return defaultValue
	}

	return val
}

func getReplicaConfigInt(cfg config.Config, key string, fallback int) int {
	valStr := cfg.Get(key)
	if valStr == "" {
		return fallback
	}

	val, err := strconv.Atoi(valStr)
	if err != nil {
		return fallback
	}

	return val
}

func optimizedConnections(cfg config.Config, key string, minDefault, maxDefault, defaultVal, multiplier int) string {
	val := getReplicaConfigInt(cfg, key, defaultVal)
	if val <= 0 {
		return strconv.Itoa(defaultVal)
	}

	optimized := val * multiplier

	if optimized < minDefault {
		optimized = minDefault
	}

	if optimized > maxDefault {
		optimized = maxDefault
	}

	return strconv.Itoa(optimized)
}

func optimizedIdleConnections(cfg config.Config) string {
	return optimizedConnections(
		cfg,
		"DB_REPLICA_MAX_IDLE_CONNECTIONS",
		minIdleReplicaDefault,
		maxIdleReplicaCapDefault,
		minIdleReplicaDefault,
		1,
	)
}

func optimizedOpenConnections(cfg config.Config) string {
	return optimizedConnections(
		cfg,
		"DB_REPLICA_MAX_OPEN_CONNECTIONS",
		minOpenReplicaDefault,
		maxOpenReplicaCapDefault,
		minOpenReplicaDefault,
		1,
	)
}
