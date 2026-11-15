package redis

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/sllt/pi/pkg/pi/config"
	"github.com/sllt/pi/pkg/pi/datasource"
)

// Handle has stable identity during dependency injection. Construction opens no
// network sockets, reads no TLS files, and starts no goroutines; Start activates
// the underlying connection before application hooks run.
type Handle struct {
	redis.Cmdable

	dummy          *redis.Client
	mu             sync.RWMutex
	real           *Redis
	closed         bool
	config         config.Config
	logger         datasource.Logger
	metrics        Metrics
	tracerProvider oteltrace.TracerProvider
}

// NewHandle creates a lazy Redis handle without opening network connections.
func NewHandle(c config.Config, l datasource.Logger, m Metrics) *Handle {
	return NewHandleWithTracerProvider(c, l, m, nil)
}

// NewHandleWithTracerProvider creates a lazy Redis handle bound to an instance TracerProvider.
func NewHandleWithTracerProvider(c config.Config, l datasource.Logger, m Metrics, tp oteltrace.TracerProvider) *Handle {
	h := &Handle{
		config:         c,
		logger:         l,
		metrics:        m,
		tracerProvider: tp,
	}
	dummy := newDummyClient(h)
	h.dummy = dummy
	h.Cmdable = dummy
	return h
}

func newDummyClient(h *Handle) *redis.Client {
	dummy := redis.NewClient(&redis.Options{
		Addr:                     "127.0.0.1:0",
		Protocol:                 2,
		MinIdleConns:             0,
		MaxRetries:               -1,
		DisableIdentity:          true,
		MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return nil, h.stateErr()
		},
	})
	dummy.AddHook(&handleHook{h: h})
	return dummy
}

var nilHandleDummy = sync.OnceValue(func() *redis.Client {
	return newDummyClient(nil)
})

func (h *Handle) dummyClient() *redis.Client {
	if h != nil && h.dummy != nil {
		return h.dummy
	}
	return nilHandleDummy()
}

// IsStarted reports whether the handle currently holds an active Redis connection.
func (h *Handle) IsStarted() bool {
	if h == nil {
		return false
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return !h.closed && h.real != nil
}

// UseLogger injects a runtime logger prior to Start.
func (h *Handle) UseLogger(v any) {
	if h == nil {
		return
	}
	l, ok := v.(datasource.Logger)
	if !ok || l == nil {
		return
	}
	h.mu.Lock()
	h.logger = l
	h.mu.Unlock()
}

// UseMetrics injects a runtime metrics collector prior to Start.
func (h *Handle) UseMetrics(v any) {
	if h == nil {
		return
	}
	m, ok := v.(Metrics)
	if !ok || m == nil {
		return
	}
	h.mu.Lock()
	h.metrics = m
	h.mu.Unlock()
}

// UseTracerProvider injects a runtime TracerProvider prior to Start.
func (h *Handle) UseTracerProvider(tp oteltrace.TracerProvider) {
	if h == nil || tp == nil {
		return
	}
	h.mu.Lock()
	h.tracerProvider = tp
	h.mu.Unlock()
}

// Start opens and pings the underlying Redis connection within ctx's deadline.
func (h *Handle) Start(ctx context.Context) error {
	if h == nil {
		return ErrClosed
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return ErrClosed
	}
	if h.real != nil {
		h.mu.Unlock()
		return nil
	}
	cfg, logger, m, tp := h.config, h.logger, h.metrics, h.tracerProvider
	h.mu.Unlock()

	client, err := OpenContextWithTracerProvider(ctx, cfg, logger, m, tp)
	if err != nil {
		return err
	}

	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		_ = client.Close()
		return ErrClosed
	}
	if h.real != nil {
		h.mu.Unlock()
		_ = client.Close()
		return nil
	}
	h.real = client
	h.mu.Unlock()
	return nil
}

// Close closes the underlying Redis connection if started and transitions the handle to closed.
func (h *Handle) Close() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	realClient := h.real
	h.real = nil
	h.mu.Unlock()

	if realClient != nil {
		return realClient.Close()
	}
	return nil
}

func (h *Handle) ready() (*Redis, error) {
	if h == nil {
		return nil, ErrClosed
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.closed {
		return nil, ErrClosed
	}
	if h.real == nil {
		return nil, ErrNotStarted
	}
	return h.real, nil
}

func (h *Handle) stateErr() error {
	_, err := h.ready()
	if err != nil {
		return err
	}
	return ErrNotStarted
}

func (h *Handle) hostAddr() string {
	if h == nil || h.config == nil {
		return ""
	}
	host := strings.TrimSpace(h.config.Get("REDIS_HOST"))
	if host == "" {
		return ""
	}
	port := defaultRedisPort
	if rawPort := strings.TrimSpace(h.config.Get("REDIS_PORT")); rawPort != "" {
		if parsed, err := strconv.Atoi(rawPort); err == nil {
			port = parsed
		}
	}
	return fmt.Sprintf("%s:%d", host, port)
}

// Do executes an arbitrary Redis command on the underlying client when started.
func (h *Handle) Do(ctx context.Context, args ...any) *redis.Cmd {
	realClient, err := h.ready()
	if err == nil {
		return realClient.Do(ctx, args...)
	}
	cmd := redis.NewCmd(ctx, args...)
	cmd.SetErr(err)
	return cmd
}

// Watch executes a Redis WATCH/MULTI/EXEC transaction on the underlying client when started.
func (h *Handle) Watch(ctx context.Context, fn func(*redis.Tx) error, keys ...string) error {
	realClient, err := h.ready()
	if err != nil {
		return err
	}
	return realClient.Watch(ctx, fn, keys...)
}

// Pipeline returns a Redis Pipeliner backed by the active connection or an error-propagating pipeline.
func (h *Handle) Pipeline() redis.Pipeliner {
	realClient, err := h.ready()
	if err == nil {
		return realClient.Pipeline()
	}
	return newErrorPipeliner(h.dummyClient().Pipeline(), h.stateErr)
}

// Pipelined executes fn against a pipeline, guaranteeing queued command variables are populated before failing if unstarted/closed.
func (h *Handle) Pipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
	realClient, err := h.ready()
	if err == nil {
		return realClient.Pipelined(ctx, fn)
	}
	pipe := newErrorPipeliner(h.dummyClient().Pipeline(), h.stateErr)
	return pipe.Pipelined(ctx, fn)
}

// TxPipeline returns a MULTI/EXEC Redis Pipeliner backed by the active connection or an error-propagating pipeline.
func (h *Handle) TxPipeline() redis.Pipeliner {
	realClient, err := h.ready()
	if err == nil {
		return realClient.TxPipeline()
	}
	return newErrorPipeliner(h.dummyClient().TxPipeline(), h.stateErr)
}

// TxPipelined executes fn against a MULTI/EXEC pipeline, guaranteeing queued command variables are populated before failing if unstarted/closed.
func (h *Handle) TxPipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
	realClient, err := h.ready()
	if err == nil {
		return realClient.TxPipelined(ctx, fn)
	}
	pipe := newErrorPipeliner(h.dummyClient().TxPipeline(), h.stateErr)
	return pipe.Pipelined(ctx, fn)
}

// HealthCheck checks connection health using a background context.
func (h *Handle) HealthCheck() datasource.Health {
	return h.HealthCheckContext(context.Background())
}

// HealthCheckContext checks connection health bounded by ctx without dialing if unstarted or closed.
func (h *Handle) HealthCheckContext(ctx context.Context) datasource.Health {
	realClient, err := h.ready()
	if err != nil {
		details := map[string]any{"error": err.Error()}
		if addr := h.hostAddr(); addr != "" {
			details["host"] = addr
		}
		return datasource.Health{
			Status:  datasource.StatusDown,
			Details: details,
		}
	}
	return realClient.HealthCheckContext(ctx)
}

type handleHook struct {
	h *Handle
}

func (hk *handleHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return nil, hk.h.stateErr()
	}
}

func (hk *handleHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		realClient, err := hk.h.ready()
		if err != nil {
			if cmd != nil && cmd.Err() == nil {
				cmd.SetErr(err)
			}
			return err
		}
		return realClient.Process(ctx, cmd)
	}
}

func (hk *handleHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		realClient, err := hk.h.ready()
		if err != nil {
			for _, cmd := range cmds {
				if cmd != nil && cmd.Err() == nil {
					cmd.SetErr(err)
				}
			}
			return err
		}
		pipe := realClient.Pipeline()
		if batchErr := pipe.BatchProcess(ctx, cmds...); batchErr != nil {
			return batchErr
		}
		_, execErr := pipe.Exec(ctx)
		return execErr
	}
}

type errorPipeliner struct {
	redis.Pipeliner
	errFn func() error
}

func newErrorPipeliner(base redis.Pipeliner, errFn func() error) *errorPipeliner {
	return &errorPipeliner{
		Pipeliner: base,
		errFn:     errFn,
	}
}

func (p *errorPipeliner) Pipeline() redis.Pipeliner {
	return p
}

func (p *errorPipeliner) TxPipeline() redis.Pipeliner {
	return p
}

func (p *errorPipeliner) Pipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
	if fn != nil {
		if err := fn(p); err != nil {
			return nil, err
		}
	}
	return p.Exec(ctx)
}

func (p *errorPipeliner) TxPipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
	return p.Pipelined(ctx, fn)
}

func (p *errorPipeliner) Exec(_ context.Context) ([]redis.Cmder, error) {
	err := p.errFn()
	cmds := p.Pipeliner.Cmds()
	if len(cmds) == 0 {
		return nil, err
	}
	out := make([]redis.Cmder, len(cmds))
	copy(out, cmds)
	p.Pipeliner.Discard()
	for _, cmd := range out {
		if cmd != nil && cmd.Err() == nil {
			cmd.SetErr(err)
		}
	}
	return out, err
}
