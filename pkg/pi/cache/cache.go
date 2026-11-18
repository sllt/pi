package cache

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sllt/pi/pkg/pi/apperror"
	piRedis "github.com/sllt/pi/pkg/pi/datasource/redis"
)

const (
	defaultJitterRatio = 0.1

	entryTagValue    byte = 0x01
	entryTagNotFound byte = 0x02
)

var (
	// ErrNotFound is returned when a negative-cache sentinel is hit for a key.
	ErrNotFound = errors.New("cache: cached not-found entry")

	errNilStore     = errors.New("cache: store is nil")
	errNilLoader    = errors.New("cache: loader must not be nil")
	errCorruptEntry = errors.New("cache: corrupt cached entry")
)

type cacheConfig struct {
	jitterRatio     float64
	notFoundTTL     time.Duration
	notFoundMatcher func(error) bool
	loadTimeout     time.Duration
	failOpen        bool
}

type allOptionsConfig struct {
	cache  cacheConfig
	memory memoryConfig
	redis  redisConfig
}

// Option configures a Cache (and store-specific constructors such as NewMemory and NewRedis).
type Option interface {
	applyOption(*allOptionsConfig)
}

type cacheOptionFunc func(*cacheConfig)

func (f cacheOptionFunc) applyOption(cfg *allOptionsConfig) {
	if f != nil {
		f(&cfg.cache)
	}
}

// WithJitter configures uniform random TTL jitter in [ttl*(1-ratio), ttl*(1+ratio)]
// when ratio is in (0, 1). Passing 0 (or outside (0, 1)) disables jitter.
// Default ratio is 0.1.
func WithJitter(ratio float64) Option {
	return cacheOptionFunc(func(c *cacheConfig) {
		c.jitterRatio = ratio
	})
}

// WithNotFoundTTL enables negative caching when loader returns an error matched by match.
// If no match functions are provided, errors matching apperror.NotFound or sql.ErrNoRows
// are negatively cached for ttl, and subsequent hits return an apperror wrapping ErrNotFound.
func WithNotFoundTTL(ttl time.Duration, match ...func(error) bool) Option {
	return cacheOptionFunc(func(c *cacheConfig) {
		c.notFoundTTL = ttl
		var filtered []func(error) bool
		for _, fn := range match {
			if fn != nil {
				filtered = append(filtered, fn)
			}
		}
		if len(filtered) > 0 {
			c.notFoundMatcher = func(err error) bool {
				for _, fn := range filtered {
					if fn(err) {
						return true
					}
				}
				return false
			}
		} else {
			c.notFoundMatcher = defaultNotFoundMatcher
		}
	})
}

// WithLoadTimeout caps the detached singleflight loader execution duration.
func WithLoadTimeout(d time.Duration) Option {
	return cacheOptionFunc(func(c *cacheConfig) {
		c.loadTimeout = d
	})
}

// WithFailOpen controls whether transient ByteStore errors during Remember fall through
// to the loader instead of failing the request. Enabled by default.
func WithFailOpen(enabled bool) Option {
	return cacheOptionFunc(func(c *cacheConfig) {
		c.failOpen = enabled
	})
}

func defaultNotFoundMatcher(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, sql.ErrNoRows) {
		return true
	}
	var appErr *apperror.Error
	return errors.As(err, &appErr) && appErr.Kind() == apperror.NotFound
}

type flightCall struct {
	done       chan struct{}
	raw        []byte
	err        error
	refcount   atomic.Int32
	cancelOnce sync.Once
	cancel     context.CancelFunc
}

// Cache provides stampede-safe generic caching over a ByteStore.
type Cache struct {
	store           ByteStore
	jitterRatio     float64
	notFoundTTL     time.Duration
	notFoundMatcher func(error) bool
	loadTimeout     time.Duration
	failOpen        bool

	flightMu sync.Mutex
	flights  map[string]*flightCall
}

func buildAllOptions(opts []Option) allOptionsConfig {
	cfg := allOptionsConfig{
		cache: cacheConfig{
			jitterRatio:     defaultJitterRatio,
			notFoundMatcher: defaultNotFoundMatcher,
			failOpen:        true,
		},
		memory: defaultMemoryConfig(),
		redis:  defaultRedisConfig(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt.applyOption(&cfg)
		}
	}
	return cfg
}

func newWithCacheConfig(store ByteStore, cfg cacheConfig) *Cache {
	if isNilByteStore(store) {
		store = nil
	}
	return &Cache{
		store:           store,
		jitterRatio:     cfg.jitterRatio,
		notFoundTTL:     cfg.notFoundTTL,
		notFoundMatcher: cfg.notFoundMatcher,
		loadTimeout:     cfg.loadTimeout,
		failOpen:        cfg.failOpen,
		flights:         make(map[string]*flightCall),
	}
}

// New constructs a Cache backed by store.
func New(store ByteStore, opts ...Option) *Cache {
	cfg := buildAllOptions(opts)
	return newWithCacheConfig(store, cfg.cache)
}

// NewMemory constructs an in-memory Cache without background goroutines.
func NewMemory(opts ...Option) *Cache {
	cfg := buildAllOptions(opts)
	return newWithCacheConfig(newMemoryStoreFromConfig(cfg.memory), cfg.cache)
}

// NewRedis constructs a Cache backed by a pi datasource Redis client.
func NewRedis(client piRedis.Client, opts ...Option) *Cache {
	cfg := buildAllOptions(opts)
	return newWithCacheConfig(newRedisStoreFromConfig(client, cfg.redis), cfg.cache)
}

// NewTiered constructs a two-tier Cache with L1 and L2 ByteStores.
func NewTiered(l1, l2 ByteStore, l1TTL time.Duration, opts ...Option) *Cache {
	return New(NewTieredStore(l1, l2, l1TTL), opts...)
}

// Delete removes one or more keys from the underlying store.
func (c *Cache) Delete(ctx context.Context, keys ...string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if c == nil {
		return nil
	}
	if c.store == nil {
		if c.failOpen {
			return nil
		}
		return errNilStore
	}
	return c.store.Delete(ctx, keys...)
}

// Delete removes one or more keys from c.
func Delete(ctx context.Context, c *Cache, keys ...string) error {
	return c.Delete(ctx, keys...)
}

// Close closes the underlying ByteStore.
func (c *Cache) Close() error {
	if c == nil || c.store == nil {
		return nil
	}
	return c.store.Close()
}

func (c *Cache) applyJitter(ttl time.Duration) time.Duration {
	if ttl <= 0 || c.jitterRatio <= 0 || c.jitterRatio >= 1 {
		return ttl
	}
	factor := (1 - c.jitterRatio) + 2*c.jitterRatio*rand.Float64()
	jittered := time.Duration(float64(ttl) * factor)
	if jittered <= 0 {
		return time.Nanosecond
	}
	return jittered
}

type notFoundMeta struct {
	Kind    apperror.Kind `json:"kind"`
	Code    int           `json:"code"`
	Message string        `json:"message"`
}

func defaultNotFoundMeta() notFoundMeta {
	return notFoundMeta{
		Kind:    apperror.NotFound,
		Code:    404,
		Message: "resource not found",
	}
}

func encodeNotFound(loadErr error) []byte {
	meta := defaultNotFoundMeta()
	var appErr *apperror.Error
	if errors.As(loadErr, &appErr) && appErr != nil {
		meta = notFoundMeta{
			Kind:    appErr.Kind(),
			Code:    appErr.Code(),
			Message: appErr.PublicMessage(),
		}
	}
	encoded, err := json.Marshal(meta)
	if err != nil {
		return []byte{entryTagNotFound}
	}
	payload := make([]byte, 1+len(encoded))
	payload[0] = entryTagNotFound
	copy(payload[1:], encoded)
	return payload
}

func encodeValue[T any](val T) ([]byte, error) {
	encoded, err := json.Marshal(val)
	if err != nil {
		return nil, err
	}
	payload := make([]byte, 1+len(encoded))
	payload[0] = entryTagValue
	copy(payload[1:], encoded)
	return payload, nil
}

func decodeValue[T any](raw []byte) (T, error) {
	var zero T
	if len(raw) == 0 {
		return zero, errCorruptEntry
	}
	switch raw[0] {
	case entryTagNotFound:
		meta := defaultNotFoundMeta()
		if len(raw) > 1 {
			var parsed notFoundMeta
			if err := json.Unmarshal(raw[1:], &parsed); err == nil && parsed.Kind != "" {
				meta = parsed
			}
		}
		return zero, apperror.New(meta.Kind, meta.Code, meta.Message).WithCause(ErrNotFound)
	case entryTagValue:
		var out T
		if err := json.Unmarshal(raw[1:], &out); err != nil {
			return zero, err
		}
		return out, nil
	default:
		return zero, errCorruptEntry
	}
}

// Get fetches and decodes a cached value of type T.
// Returns (zero, false, err) where errors.Is(err, ErrNotFound) is true if a
// negative-cache sentinel is present.
func Get[T any](ctx context.Context, c *Cache, key string) (T, bool, error) {
	var zero T
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return zero, false, err
	}
	if c == nil {
		return zero, false, nil
	}
	if c.store == nil {
		if c.failOpen {
			return zero, false, nil
		}
		return zero, false, errNilStore
	}

	raw, ok, err := c.store.Get(ctx, key)
	if err != nil || !ok {
		return zero, false, err
	}
	val, err := decodeValue[T](raw)
	if err != nil {
		return zero, false, err
	}
	return val, true, nil
}

// Set encodes and stores val under key with ttl (subject to configured jitter).
func Set[T any](ctx context.Context, c *Cache, key string, val T, ttl time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if c == nil {
		return nil
	}
	if c.store == nil {
		if c.failOpen {
			return nil
		}
		return errNilStore
	}
	if ttl < 0 {
		return c.store.Delete(ctx, key)
	}
	payload, err := encodeValue(val)
	if err != nil {
		return err
	}
	return c.store.Set(ctx, key, payload, c.applyJitter(ttl))
}

// Remember returns the cached value for key if present, or executes loader once across
// concurrent callers using refcounted context-detached singleflight.
func Remember[T any](
	ctx context.Context,
	c *Cache,
	key string,
	ttl time.Duration,
	loader func(context.Context) (T, error),
) (T, error) {
	var zero T
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if loader == nil {
		return zero, errNilLoader
	}
	if c == nil {
		return loader(ctx)
	}
	if c.store == nil {
		if c.failOpen {
			return loader(ctx)
		}
		return zero, errNilStore
	}

	raw, ok, err := c.store.Get(ctx, key)
	if err != nil {
		if !c.failOpen {
			return zero, err
		}
	} else if ok {
		val, decErr := decodeValue[T](raw)
		if decErr == nil {
			return val, nil
		}
		if errors.Is(decErr, ErrNotFound) {
			return zero, decErr
		}
		if !c.failOpen {
			return zero, decErr
		}
	}

	c.flightMu.Lock()
	call, exists := c.flights[key]
	if exists {
		call.refcount.Add(1)
		c.flightMu.Unlock()
	} else {
		base := context.WithoutCancel(ctx)
		var (
			flightCtx    context.Context
			flightCancel context.CancelFunc
		)
		if c.loadTimeout > 0 {
			flightCtx, flightCancel = context.WithTimeout(base, c.loadTimeout)
		} else {
			flightCtx, flightCancel = context.WithCancel(base)
		}
		call = &flightCall{
			done:   make(chan struct{}),
			cancel: flightCancel,
		}
		call.refcount.Store(1)
		c.flights[key] = call
		c.flightMu.Unlock()

		go c.runFlight(key, ttl, flightCtx, call, loader)
	}

	select {
	case <-call.done:
		if call.err != nil {
			return zero, call.err
		}
		return decodeValue[T](call.raw)

	case <-ctx.Done():
		c.flightMu.Lock()
		if call.refcount.Add(-1) == 0 {
			if c.flights[key] == call {
				delete(c.flights, key)
			}
			call.cancelOnce.Do(call.cancel)
		}
		c.flightMu.Unlock()
		return zero, ctx.Err()
	}
}

func (c *Cache) runFlight[T any](
	key string,
	ttl time.Duration,
	flightCtx context.Context,
	call *flightCall,
	loader func(context.Context) (T, error),
) {
	defer func() {
		if r := recover(); r != nil {
			call.err = fmt.Errorf("cache: loader panicked: %v", r)
		}
		call.cancelOnce.Do(call.cancel)
		c.flightMu.Lock()
		if c.flights[key] == call {
			delete(c.flights, key)
		}
		c.flightMu.Unlock()
		close(call.done)
	}()

	res, loadErr := loader(flightCtx)
	if loadErr != nil {
		if c.notFoundTTL > 0 && c.notFoundMatcher != nil && c.notFoundMatcher(loadErr) {
			writeCtx, cancel := context.WithTimeout(context.WithoutCancel(flightCtx), 2*time.Second)
			setErr := c.store.Set(writeCtx, key, encodeNotFound(loadErr), c.applyJitter(c.notFoundTTL))
			cancel()
			if setErr != nil && !c.failOpen {
				call.err = setErr
				return
			}
		}
		call.err = loadErr
		return
	}

	payload, encErr := encodeValue(res)
	if encErr != nil {
		call.err = encErr
		return
	}
	if ttl >= 0 {
		writeCtx, cancel := context.WithTimeout(context.WithoutCancel(flightCtx), 2*time.Second)
		setErr := c.store.Set(writeCtx, key, payload, c.applyJitter(ttl))
		cancel()
		if setErr != nil && !c.failOpen {
			call.err = setErr
			return
		}
	}

	call.raw = payload
}
