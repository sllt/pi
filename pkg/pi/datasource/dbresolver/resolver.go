package dbresolver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sllt/pi/pkg/pi/config"
	"github.com/sllt/pi/pkg/pi/datasource"
	piSQL "github.com/sllt/pi/pkg/pi/datasource/sql"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Constants for strategies and intervals.
const (
	contextKeyHTTPMethod contextKey = "dbresolver.http_method"

	// contextKeyRequestPath stores the HTTP request path in context.
	// Used to determine if the route matches PrimaryRoutes configuration,
	// forcing queries to use the primary database instead of replicas.
	contextKeyRequestPath contextKey = "dbresolver.request_path"

	defaultMaxFailures = 5
	defaultTimeoutSec  = 30
)

var errReplicaFailedNoFallback = errors.New("replica query failed and fallback disabled")

type contextKey string

// DB is the 15-method SQL client interface routed by Resolver.
// It matches github.com/sllt/pi/pkg/pi/datasource/sql.Client without importing pkg/pi or pkg/pi/infra.
type DB interface {
	piSQL.Executor

	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
	Exec(query string, args ...any) (sql.Result, error)
	Prepare(query string) (*sql.Stmt, error)

	Begin() (*piSQL.Tx, error)
	BeginTxContext(ctx context.Context, opts *sql.TxOptions) (*piSQL.Tx, error)

	HealthCheck() *datasource.Health
	HealthCheckContext(ctx context.Context) *datasource.Health
	Owns(tx *piSQL.Tx) bool
	Dialect() string
	Close() error
}

var (
	_ DB = (*Resolver)(nil)
	_ DB = (*piSQL.DB)(nil)
)

// statistics holds atomic counters for various operations.
type statistics struct {
	primaryReads     atomic.Uint64
	primaryWrites    atomic.Uint64
	replicaReads     atomic.Uint64
	primaryFallbacks atomic.Uint64
	replicaFailures  atomic.Uint64
	totalQueries     atomic.Uint64
}

// Replica wrapper with circuit breaker.
type replicaWrapper struct {
	db      DB
	breaker *circuitBreaker
	index   int
}

// Resolver routes read queries to replicas with circuit-breaking fallback and writes/transactions to primary.
type Resolver struct {
	mu sync.RWMutex

	primary      DB
	replicas     []*replicaWrapper
	strategy     Strategy
	readFallback bool

	maxFailures    int32
	breakerTimeout time.Duration

	logger         Logger
	metrics        Metrics
	tracer         trace.Tracer
	tracerProvider trace.TracerProvider

	primaryRoutes   map[string]bool
	primaryPrefixes []string

	// Optional deferred replica initialization for zero-I/O construction (NewManaged).
	lazyCfg    *Config
	lazyAppCfg config.Config
	started    bool
	closed     bool

	metricsInit bool
	bgStarted   bool

	stats *statistics

	// Background task management.
	stopChan chan struct{}
	wg       sync.WaitGroup
	once     sync.Once
}

// NewResolver creates a new Resolver instance with pre-opened primary and replica connections.
func NewResolver(primary DB, replicas []DB, logger Logger, metrics Metrics, opts ...Option) DB {
	r := &Resolver{
		primary:        primary,
		readFallback:   true,
		maxFailures:    defaultMaxFailures,
		breakerTimeout: time.Duration(defaultTimeoutSec) * time.Second,
		logger:         logger,
		metrics:        metrics,
		stats:          &statistics{},
		primaryRoutes:  make(map[string]bool),
		stopChan:       make(chan struct{}),
		started:        true,
	}

	if len(replicas) > 0 {
		r.strategy = NewRoundRobinStrategy()
	}

	for _, opt := range opts {
		if opt != nil {
			opt(r)
		}
	}

	r.compilePrimaryPrefixes()
	r.replicas = r.wrapReplicas(replicas)

	r.initializeMetrics()
	r.startBackgroundTasks()

	if r.logger != nil {
		r.logger.Logf("DB Resolver initialized with %d replicas using circuit breakers", len(replicas))
	}

	return r
}

// NewManaged creates a zero-I/O Resolver that opens replicas during Start(ctx).
// It is designed for pi.Build(pi.WithSQL(resolver, pi.Owned)).
func NewManaged(primary DB, cfg *Config, appCfg config.Config, opts ...Option) *Resolver {
	var cfgCopy Config
	if cfg != nil {
		cfgCopy = *cfg
		if len(cfg.PrimaryRoutes) > 0 {
			cfgCopy.PrimaryRoutes = append([]string(nil), cfg.PrimaryRoutes...)
		}
		if len(cfg.Replicas) > 0 {
			cfgCopy.Replicas = append([]ReplicaCredential(nil), cfg.Replicas...)
		}
	}

	routes := make(map[string]bool, len(cfgCopy.PrimaryRoutes))
	for _, route := range cfgCopy.PrimaryRoutes {
		routes[route] = true
	}

	maxFailures := int32(defaultMaxFailures)
	if cfgCopy.MaxFailures > 0 {
		maxFailures = int32(cfgCopy.MaxFailures)
	}
	timeout := time.Duration(defaultTimeoutSec) * time.Second
	if cfgCopy.TimeoutSec > 0 {
		timeout = time.Duration(cfgCopy.TimeoutSec) * time.Second
	}

	r := &Resolver{
		primary:        primary,
		strategy:       getStrategy(cfgCopy.Strategy),
		readFallback:   cfg == nil || cfgCopy.ReadFallback,
		maxFailures:    maxFailures,
		breakerTimeout: timeout,
		primaryRoutes:  routes,
		lazyCfg:        &cfgCopy,
		lazyAppCfg:     appCfg,
		stats:          &statistics{},
		stopChan:       make(chan struct{}),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(r)
		}
	}

	r.compilePrimaryPrefixes()

	return r
}

func (r *Resolver) compilePrimaryPrefixes() {
	r.primaryPrefixes = nil
	for route := range r.primaryRoutes {
		if strings.HasSuffix(route, "*") {
			r.primaryPrefixes = append(r.primaryPrefixes, strings.TrimSuffix(route, "*"))
		}
	}
}

func (r *Resolver) wrapReplicas(replicas []DB) []*replicaWrapper {
	maxFailures := r.maxFailures
	if maxFailures <= 0 {
		maxFailures = defaultMaxFailures
	}
	timeout := r.breakerTimeout
	if timeout <= 0 {
		timeout = time.Duration(defaultTimeoutSec) * time.Second
	}

	wrappers := make([]*replicaWrapper, len(replicas))
	for i, replica := range replicas {
		wrappers[i] = &replicaWrapper{
			db:      replica,
			breaker: newCircuitBreaker(maxFailures, timeout),
			index:   i,
		}
	}
	return wrappers
}

// UseLogger injects a runtime logger before or during initialization.
func (r *Resolver) UseLogger(logger any) {
	if r == nil {
		return
	}
	if l, ok := logger.(Logger); ok && l != nil {
		r.mu.Lock()
		r.logger = l
		primary := r.primary
		r.mu.Unlock()
		if ul, ok := primary.(interface{ UseLogger(any) }); ok {
			ul.UseLogger(logger)
		}
	}
}

// UseMetrics injects runtime metrics and registers resolver collectors once.
func (r *Resolver) UseMetrics(metrics any) {
	if r == nil {
		return
	}
	if m, ok := metrics.(Metrics); ok && m != nil {
		r.mu.Lock()
		r.metrics = m
		r.initializeMetrics()
		primary := r.primary
		r.mu.Unlock()
		if um, ok := primary.(interface{ UseMetrics(any) }); ok {
			um.UseMetrics(metrics)
		}
	}
}

// UseTracerProvider injects an OpenTelemetry TracerProvider.
func (r *Resolver) UseTracerProvider(tp trace.TracerProvider) {
	if r == nil || tp == nil {
		return
	}
	r.mu.Lock()
	r.tracerProvider = tp
	r.tracer = tp.Tracer("pi-dbresolver")
	primary := r.primary
	r.mu.Unlock()
	if utp, ok := primary.(interface{ UseTracerProvider(trace.TracerProvider) }); ok {
		utp.UseTracerProvider(tp)
	}
}

// UseTracer injects an OpenTelemetry Tracer or TracerProvider.
func (r *Resolver) UseTracer(tracer any) {
	if r == nil {
		return
	}
	switch t := tracer.(type) {
	case trace.TracerProvider:
		r.UseTracerProvider(t)
	case trace.Tracer:
		r.mu.Lock()
		r.tracer = t
		primary := r.primary
		r.mu.Unlock()
		if ut, ok := primary.(interface{ UseTracer(any) }); ok {
			ut.UseTracer(t)
		}
	}
}

// IsStarted reports whether the resolver has started.
func (r *Resolver) IsStarted() bool {
	if r == nil {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return !r.closed && r.started
}

// HasActiveResource reports whether the resolver or its underlying primary holds open connections.
func (r *Resolver) HasActiveResource() bool {
	if r == nil {
		return false
	}
	r.mu.RLock()
	closed := r.closed
	started := r.started
	primary := r.primary
	hasReplicas := len(r.replicas) > 0
	r.mu.RUnlock()
	if closed {
		return false
	}
	if started || hasReplicas {
		return true
	}
	if primary == nil {
		return false
	}
	if sp, ok := primary.(interface{ IsStarted() bool }); ok {
		return sp.IsStarted()
	}
	if _, isLazy := primary.(interface{ Start(context.Context) error }); isLazy {
		return false
	}
	return true
}

// UnwrapSQL exposes the wrapped primary SQL client so App.SetSQL can transfer ownership cleanly.
func (r *Resolver) UnwrapSQL() any {
	if r == nil {
		return nil
	}
	return r.primary
}

// Start initializes primary (if lazy) and any deferred replica connections.
// Calling Start on an already-started Resolver is an idempotent no-op.
func (r *Resolver) Start(ctx context.Context) error {
	if r == nil || r.primary == nil {
		return errPrimaryNil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return sql.ErrConnDone
	}
	if r.started {
		r.mu.RUnlock()
		return nil
	}
	r.mu.RUnlock()

	primaryStartedHere := false
	if starter, ok := r.primary.(interface{ Start(context.Context) error }); ok {
		wasStarted := false
		if sp, ok := r.primary.(interface{ IsStarted() bool }); ok {
			wasStarted = sp.IsStarted()
		}
		if err := starter.Start(ctx); err != nil {
			return err
		}
		primaryStartedHere = !wasStarted
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		if primaryStartedHere {
			_ = r.primary.Close()
		}
		return sql.ErrConnDone
	}
	if r.started {
		r.mu.Unlock()
		return nil
	}
	lazyCfg := r.lazyCfg
	lazyAppCfg := r.lazyAppCfg
	logger := r.logger
	metrics := r.metrics
	tp := r.tracerProvider
	r.mu.Unlock()

	var replicas []DB
	if lazyCfg != nil && len(lazyCfg.Replicas) > 0 {
		var err error
		replicas, err = OpenReplicasContext(ctx, lazyCfg, lazyAppCfg, logger, metrics, tp)
		if err != nil {
			if primaryStartedHere {
				_ = r.primary.Close()
			}
			return err
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		closeOpenedReplicas(replicas)
		if primaryStartedHere {
			_ = r.primary.Close()
		}
		return sql.ErrConnDone
	}
	if r.started {
		closeOpenedReplicas(replicas)
		return nil
	}

	r.replicas = r.wrapReplicas(replicas)
	if len(replicas) == 0 {
		r.strategy = nil
	} else if r.strategy == nil {
		r.strategy = NewRoundRobinStrategy()
	}
	r.initializeMetrics()
	r.startBackgroundTasks()
	r.started = true

	if r.logger != nil {
		r.logger.Logf("DB Resolver initialized with %d replicas using circuit breakers", len(replicas))
	}

	return nil
}

// initializeMetrics sets up metrics following Pi patterns. Caller holds r.mu or is in constructor.
func (r *Resolver) initializeMetrics() {
	if r.metrics == nil || r.metricsInit {
		return
	}
	r.metricsInit = true

	buckets := []float64{0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0}
	r.metrics.NewHistogram("dbresolver_query_duration", "Response time of DB resolver operations in microseconds", buckets...)

	r.metrics.NewGauge("dbresolver_primary_reads", "Total reads routed to primary")
	r.metrics.NewGauge("dbresolver_primary_writes", "Total writes routed to primary")
	r.metrics.NewGauge("dbresolver_replica_reads", "Total reads routed to replicas")
	r.metrics.NewGauge("dbresolver_fallbacks", "Total fallbacks to primary")
	r.metrics.NewGauge("dbresolver_failures", "Total replica failures")
}

// startBackgroundTasks starts minimal background processing once. Caller holds r.mu or is in constructor.
func (r *Resolver) startBackgroundTasks() {
	if r.bgStarted {
		return
	}
	r.bgStarted = true
	r.wg.Add(1)

	go r.backgroundProcessor()
}

// backgroundProcessor handles metrics collection with reduced frequency.
func (r *Resolver) backgroundProcessor() {
	defer r.wg.Done()

	ticker := time.NewTicker(time.Duration(defaultTimeoutSec) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopChan:
			return
		case <-ticker.C:
			r.updateMetrics()
		}
	}
}

// updateMetrics updates gauge metrics.
func (r *Resolver) updateMetrics() {
	r.mu.RLock()
	metrics := r.metrics
	r.mu.RUnlock()

	if metrics == nil {
		return
	}

	metrics.SetGauge("dbresolver_primary_reads", float64(r.stats.primaryReads.Load()))
	metrics.SetGauge("dbresolver_primary_writes", float64(r.stats.primaryWrites.Load()))
	metrics.SetGauge("dbresolver_replica_reads", float64(r.stats.replicaReads.Load()))
	metrics.SetGauge("dbresolver_fallbacks", float64(r.stats.primaryFallbacks.Load()))
	metrics.SetGauge("dbresolver_failures", float64(r.stats.replicaFailures.Load()))
}

func (r *Resolver) snapshotState() ([]*replicaWrapper, Strategy, Logger, Metrics, trace.Tracer) {
	r.mu.RLock()
	replicas := r.replicas
	strategy := r.strategy
	logger := r.logger
	metrics := r.metrics
	tracer := r.tracer
	r.mu.RUnlock()
	return replicas, strategy, logger, metrics, tracer
}

// shouldUseReplica determines routing based on HTTP method and path.
func (r *Resolver) shouldUseReplica(ctx context.Context) bool {
	r.mu.RLock()
	hasReplicas := len(r.replicas) > 0
	r.mu.RUnlock()

	if !hasReplicas {
		return false
	}

	if path, ok := ctx.Value(contextKeyRequestPath).(string); ok {
		if r.isPrimaryRoute(path) {
			return false
		}
	}

	method, ok := ctx.Value(contextKeyHTTPMethod).(string)
	if !ok {
		return false
	}

	return method == "GET"
}

// isPrimaryRoute checks if path matches primary route patterns.
func (r *Resolver) isPrimaryRoute(path string) bool {
	if r.primaryRoutes[path] {
		return true
	}

	for _, prefix := range r.primaryPrefixes {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}

	return false
}

// selectHealthyReplica chooses an available replica using circuit breaker.
func (r *Resolver) selectHealthyReplica() *replicaWrapper {
	replicas, strategy, logger, _, _ := r.snapshotState()
	if len(replicas) == 0 || strategy == nil {
		return nil
	}

	var healthyIndices []int
	for i, wrapper := range replicas {
		if wrapper.breaker.allowRequest() {
			healthyIndices = append(healthyIndices, i)
		}
	}

	if len(healthyIndices) == 0 {
		if logger != nil {
			logger.Warn("All replicas are unavailable (circuit breakers open), falling back to primary")
		}
		return nil
	}

	idx := strategy.Next(len(healthyIndices))
	if idx < 0 || idx >= len(healthyIndices) {
		return nil
	}

	originalIdx := healthyIndices[idx]
	return replicas[originalIdx]
}

// addTrace adds tracing information to the context and returns a span.
func (r *Resolver) addTrace(ctx context.Context, method, query string) (context.Context, trace.Span) {
	r.mu.RLock()
	tracer := r.tracer
	r.mu.RUnlock()

	if tracer == nil {
		return ctx, nil
	}

	tracedCtx, span := tracer.Start(ctx, fmt.Sprintf("dbresolver-%s", method))
	if span != nil {
		span.SetAttributes(
			attribute.String("dbresolver.query", query),
			attribute.String("dbresolver.method", method),
		)
	}

	return tracedCtx, span
}

// recordStats records operation statistics and updates tracing spans.
func (r *Resolver) recordStats(start time.Time, method, target string, span trace.Span, isRead bool, replicaIndex *int) {
	duration := time.Since(start).Microseconds()

	if span != nil {
		defer span.End()

		attrs := []attribute.KeyValue{
			attribute.String("dbresolver.target", target),
			attribute.Int64("dbresolver.duration", duration),
			attribute.Bool("dbresolver.is_read", isRead),
		}

		if replicaIndex != nil {
			attrs = append(attrs, attribute.Int("dbresolver.replica_index", *replicaIndex))
		}

		span.SetAttributes(attrs...)
	}

	r.mu.RLock()
	metrics := r.metrics
	r.mu.RUnlock()

	if metrics != nil {
		metrics.RecordHistogram(context.Background(), "dbresolver_query_duration",
			float64(duration), "method", method, "target", target)
	}
}

type resolverErrConnector struct{ err error }

func (c resolverErrConnector) Connect(context.Context) (driver.Conn, error) { return nil, c.err }
func (c resolverErrConnector) Driver() driver.Driver                        { return c }
func (c resolverErrConnector) Open(string) (driver.Conn, error)             { return nil, c.err }

// Query routes to replica for reads, primary for writes.
func (r *Resolver) Query(query string, args ...any) (*sql.Rows, error) {
	return r.QueryContext(context.Background(), query, args...)
}

// QueryContext routes queries with optimized path.
func (r *Resolver) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if r == nil || r.primary == nil {
		return nil, errPrimaryNil
	}
	start := time.Now()

	r.stats.totalQueries.Add(1)

	tracedCtx, span := r.addTrace(ctx, "query", query)
	useReplica := r.shouldUseReplica(ctx)

	if useReplica {
		return r.executeReplicaQuery(tracedCtx, span, start, query, args...)
	}

	r.stats.primaryWrites.Add(1)
	rows, err := r.primary.QueryContext(tracedCtx, query, args...)

	r.recordStats(start, "query", "primary", span, false, nil)

	return rows, err
}

// executeReplicaQuery attempts to execute query on replica with fallback handling.
func (r *Resolver) executeReplicaQuery(ctx context.Context, span trace.Span, start time.Time,
	query string, args ...any) (*sql.Rows, error) {
	wrapper := r.selectHealthyReplica()

	if wrapper == nil {
		return r.fallbackToPrimary(ctx, span, start, query, "No healthy replica available, falling back to primary", args...)
	}

	rows, err := wrapper.db.QueryContext(ctx, query, args...)
	if err == nil {
		r.stats.replicaReads.Add(1)
		wrapper.breaker.recordSuccess()

		r.recordStats(start, "query", "replica", span, true, &wrapper.index)

		return rows, nil
	}

	wrapper.breaker.recordFailure()
	r.stats.replicaFailures.Add(1)

	r.mu.RLock()
	logger := r.logger
	r.mu.RUnlock()

	if logger != nil {
		logger.Errorf("Replica #%d failed, circuit breaker triggered: %v", wrapper.index+1, err)
	}

	return r.fallbackToPrimary(ctx, span, start, query, "Falling back to primary for read operation", args...)
}

// fallbackToPrimary handles primary fallback logic with custom warning message.
func (r *Resolver) fallbackToPrimary(ctx context.Context, span trace.Span, start time.Time,
	query, warningMsg string, args ...any) (*sql.Rows, error) {
	if !r.readFallback {
		r.recordStats(start, "query", "replica-failed", span, true, nil)

		return nil, errReplicaFailedNoFallback
	}

	r.stats.primaryFallbacks.Add(1)
	r.stats.primaryReads.Add(1)

	r.mu.RLock()
	logger := r.logger
	r.mu.RUnlock()

	if logger != nil && warningMsg != "" {
		logger.Warn(warningMsg)
	}

	rows, err := r.primary.QueryContext(ctx, query, args...)
	r.recordStats(start, "query", "primary-fallback", span, true, nil)

	return rows, err
}

// QueryRow routes to replica for reads, primary for writes.
func (r *Resolver) QueryRow(query string, args ...any) *sql.Row {
	return r.QueryRowContext(context.Background(), query, args...)
}

// QueryRowContext routes queries with circuit breaker.
func (r *Resolver) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if r == nil || r.primary == nil {
		if ctx == nil {
			ctx = context.Background()
		}
		db := sql.OpenDB(resolverErrConnector{errPrimaryNil})
		row := db.QueryRowContext(ctx, query, args...)
		_ = db.Close()
		return row
	}
	start := time.Now()

	r.stats.totalQueries.Add(1)

	useReplica := r.shouldUseReplica(ctx)

	tracedCtx, span := r.addTrace(ctx, "query-row", query)

	if useReplica {
		wrapper := r.selectHealthyReplica()
		if wrapper != nil {
			r.stats.replicaReads.Add(1)
			wrapper.breaker.recordSuccess()

			r.recordStats(start, "query-row", "replica", span, true, &wrapper.index)

			return wrapper.db.QueryRowContext(tracedCtx, query, args...)
		}

		r.stats.replicaFailures.Add(1)
	}

	r.stats.primaryWrites.Add(1)

	r.recordStats(start, "query-row", "primary", span, false, nil)

	return r.primary.QueryRowContext(tracedCtx, query, args...)
}

// Exec always routes to primary (write operation).
func (r *Resolver) Exec(query string, args ...any) (sql.Result, error) {
	return r.ExecContext(context.Background(), query, args...)
}

// ExecContext always routes to primary (write operation).
func (r *Resolver) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if r == nil || r.primary == nil {
		return nil, errPrimaryNil
	}
	start := time.Now()

	r.stats.primaryWrites.Add(1)
	r.stats.totalQueries.Add(1)

	tracedCtx, span := r.addTrace(ctx, "exec", query)
	defer r.recordStats(start, "exec", "primary", span, false, nil)

	return r.primary.ExecContext(tracedCtx, query, args...)
}

// Select routes to replica for reads, primary for writes.
func (r *Resolver) Select(ctx context.Context, data any, query string, args ...any) error {
	if r == nil || r.primary == nil {
		return errPrimaryNil
	}
	start := time.Now()

	r.stats.totalQueries.Add(1)

	tracedCtx, span := r.addTrace(ctx, "select", query)

	useReplica := r.shouldUseReplica(ctx)

	if useReplica {
		wrapper := r.selectHealthyReplica()

		if wrapper != nil {
			err := wrapper.db.Select(tracedCtx, data, query, args...)
			if err == nil {
				r.stats.replicaReads.Add(1)
				wrapper.breaker.recordSuccess()

				r.recordStats(start, "select", "replica", span, true, &wrapper.index)

				return nil
			}

			wrapper.breaker.recordFailure()
			r.stats.replicaFailures.Add(1)
			r.recordStats(start, "select", "replica-failed", span, true, &wrapper.index)

			return err
		}

		r.stats.replicaFailures.Add(1)
	}

	r.stats.primaryWrites.Add(1)
	err := r.primary.Select(tracedCtx, data, query, args...)
	r.recordStats(start, "select", "primary", span, false, nil)

	return err
}

// Prepare always routes to primary (consistency).
func (r *Resolver) Prepare(query string) (*sql.Stmt, error) {
	if r == nil || r.primary == nil {
		return nil, errPrimaryNil
	}
	r.stats.totalQueries.Add(1)

	return r.primary.Prepare(query)
}

// Begin always routes to primary (transactions).
func (r *Resolver) Begin() (*piSQL.Tx, error) {
	if r == nil || r.primary == nil {
		return nil, errPrimaryNil
	}
	r.stats.totalQueries.Add(1)

	return r.primary.Begin()
}

// BeginTxContext starts a context-aware transaction on primary with optional TxOptions.
func (r *Resolver) BeginTxContext(ctx context.Context, opts *sql.TxOptions) (*piSQL.Tx, error) {
	if r == nil || r.primary == nil {
		return nil, errPrimaryNil
	}
	r.stats.totalQueries.Add(1)

	return r.primary.BeginTxContext(ctx, opts)
}

// Owns reports whether tx was opened against this resolver's primary database.
func (r *Resolver) Owns(tx *piSQL.Tx) bool {
	if r == nil || r.primary == nil || tx == nil {
		return false
	}
	return r.primary.Owns(tx)
}

// Dialect returns the database dialect.
func (r *Resolver) Dialect() string {
	if r == nil || r.primary == nil {
		return ""
	}
	return r.primary.Dialect()
}

// HealthCheck returns comprehensive health information.
func (r *Resolver) HealthCheck() *datasource.Health {
	if r == nil || r.primary == nil {
		return &datasource.Health{
			Status:  datasource.StatusDown,
			Details: map[string]any{"error": errPrimaryNil.Error()},
		}
	}
	return r.buildHealth(r.primary.HealthCheck(), func(db DB) *datasource.Health {
		return db.HealthCheck()
	})
}

// HealthCheckContext returns context-bounded health information for primary and all replicas.
func (r *Resolver) HealthCheckContext(ctx context.Context) *datasource.Health {
	if r == nil || r.primary == nil {
		return &datasource.Health{
			Status:  datasource.StatusDown,
			Details: map[string]any{"error": errPrimaryNil.Error()},
		}
	}
	return r.buildHealth(r.primary.HealthCheckContext(ctx), func(db DB) *datasource.Health {
		return db.HealthCheckContext(ctx)
	})
}

func (r *Resolver) buildHealth(primaryHealth *datasource.Health, replicaHealthFn func(DB) *datasource.Health) *datasource.Health {
	status := datasource.StatusDown
	if primaryHealth != nil && primaryHealth.Status != "" {
		status = primaryHealth.Status
	}

	r.mu.RLock()
	replicas := r.replicas
	r.mu.RUnlock()

	health := &datasource.Health{
		Status: status,
		Details: map[string]any{
			"primary": primaryHealth,
			"stats": map[string]any{
				"primaryReads":     r.stats.primaryReads.Load(),
				"primaryWrites":    r.stats.primaryWrites.Load(),
				"replicaReads":     r.stats.replicaReads.Load(),
				"primaryFallbacks": r.stats.primaryFallbacks.Load(),
				"replicaFailures":  r.stats.replicaFailures.Load(),
				"totalQueries":     r.stats.totalQueries.Load(),
			},
		},
	}

	replicaDetails := make([]any, len(replicas))
	for i, wrapper := range replicas {
		replicaHealth := replicaHealthFn(wrapper.db)
		state := wrapper.breaker.state.Load()

		var stateStr string
		if state != nil {
			switch *state {
			case circuitStateClosed:
				stateStr = "CLOSED"
			case circuitStateOpen:
				stateStr = "OPEN"
			case circuitStateHalfOpen:
				stateStr = "HALF_OPEN"
			}
		}

		replicaDetails[i] = map[string]any{
			"index":         i,
			"health":        replicaHealth,
			"circuit_state": stateStr,
			"failures":      wrapper.breaker.failures.Load(),
		}
	}

	health.Details["replicas"] = replicaDetails

	return health
}

// Close cleans up resources properly and idempotently.
func (r *Resolver) Close() error {
	if r == nil {
		return nil
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	replicas := r.replicas
	r.replicas = nil
	r.started = false
	primary := r.primary
	r.mu.Unlock()

	r.once.Do(func() {
		close(r.stopChan)
		r.wg.Wait()
	})

	var errs []error
	if primary != nil {
		if closeErr := primary.Close(); closeErr != nil {
			errs = append(errs, closeErr)
		}
	}

	for _, wrapper := range replicas {
		if wrapper != nil && wrapper.db != nil {
			if closeErr := wrapper.db.Close(); closeErr != nil {
				errs = append(errs, closeErr)
			}
		}
	}

	if len(errs) == 1 {
		return errs[0]
	}

	return errors.Join(errs...)
}

// WithHTTPMethod adds HTTP method to context for routing decisions.
func WithHTTPMethod(ctx context.Context, method string) context.Context {
	return context.WithValue(ctx, contextKeyHTTPMethod, method)
}

// WithRequestPath adds request path to context.
func WithRequestPath(ctx context.Context, path string) context.Context {
	return context.WithValue(ctx, contextKeyRequestPath, path)
}
