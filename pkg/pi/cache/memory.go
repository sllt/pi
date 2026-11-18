package cache

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultMemoryMaxEntries = 10000
	numMemoryShards         = 32
	memorySweepBatchSize    = 4
	memoryEvictSampleLimit  = 32
)

type memoryConfig struct {
	maxEntries int
	now        func() time.Time
}

// MemoryOption configures a MemoryStore (and implements cache.Option for NewMemory).
type MemoryOption func(*memoryConfig)

func (o MemoryOption) applyOption(cfg *allOptionsConfig) {
	if o != nil {
		o(&cfg.memory)
	}
}

// WithMaxEntries sets the maximum number of entries retained in MemoryStore.
// Non-positive values are ignored and keep the default limit (10000).
func WithMaxEntries(n int) MemoryOption {
	return func(c *memoryConfig) {
		if n > 0 {
			c.maxEntries = n
		}
	}
}

type memoryEntry struct {
	value      []byte
	expiresAt  time.Time
	lastAccess atomic.Int64
	seq        uint64
}

type memoryShard struct {
	mu      sync.RWMutex
	entries map[string]*memoryEntry
}

// MemoryStore is a goroutine-free, sharded in-memory ByteStore with lazy
// expiration and bounded opportunistic eviction on writes.
type MemoryStore struct {
	shards     [numMemoryShards]memoryShard
	maxEntries int
	count      atomic.Int64
	seq        atomic.Uint64
	now        func() time.Time
}

// NewMemoryStore constructs a sharded, goroutine-free in-memory ByteStore.
func NewMemoryStore(opts ...MemoryOption) *MemoryStore {
	cfg := defaultMemoryConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return newMemoryStoreFromConfig(cfg)
}

func defaultMemoryConfig() memoryConfig {
	return memoryConfig{
		maxEntries: defaultMemoryMaxEntries,
		now:        time.Now,
	}
}

func newMemoryStoreFromConfig(cfg memoryConfig) *MemoryStore {
	if cfg.maxEntries <= 0 {
		cfg.maxEntries = defaultMemoryMaxEntries
	}
	if cfg.now == nil {
		cfg.now = time.Now
	}
	ms := &MemoryStore{
		maxEntries: cfg.maxEntries,
		now:        cfg.now,
	}
	for i := range ms.shards {
		ms.shards[i].entries = make(map[string]*memoryEntry)
	}
	return ms
}

func shardIndex(key string) int {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	var h uint32 = offset32
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= prime32
	}
	return int(h & (numMemoryShards - 1))
}

// Get retrieves a cloned copy of the cached bytes for key.
func (m *MemoryStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	val, ok, _, err := m.getWithTTL(ctx, key)
	return val, ok, err
}

func (m *MemoryStore) getWithTTL(ctx context.Context, key string) ([]byte, bool, time.Duration, error) {
	if m == nil {
		return nil, false, 0, nil
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, false, 0, err
		}
	}

	s := &m.shards[shardIndex(key)]
	now := m.now()

	s.mu.RLock()
	entry, ok := s.entries[key]
	if !ok {
		s.mu.RUnlock()
		return nil, false, 0, nil
	}
	if !entry.expiresAt.IsZero() && !now.Before(entry.expiresAt) {
		s.mu.RUnlock()
		s.mu.Lock()
		if cur, exists := s.entries[key]; exists && !cur.expiresAt.IsZero() && !now.Before(cur.expiresAt) {
			delete(s.entries, key)
			m.count.Add(-1)
		}
		s.mu.Unlock()
		return nil, false, 0, nil
	}

	entry.lastAccess.Store(now.UnixNano())
	val := bytes.Clone(entry.value)
	var remaining time.Duration
	if !entry.expiresAt.IsZero() {
		remaining = entry.expiresAt.Sub(now)
	}
	s.mu.RUnlock()
	return val, true, remaining, nil
}

// Set stores a cloned copy of value under key with ttl (0 means no expiration; <0 deletes).
func (m *MemoryStore) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if m == nil {
		return nil
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}

	idx := shardIndex(key)
	s := &m.shards[idx]
	now := m.now()

	if ttl < 0 {
		s.mu.Lock()
		if _, existed := s.entries[key]; existed {
			delete(s.entries, key)
			m.count.Add(-1)
		}
		s.mu.Unlock()
		return nil
	}

	cloned := bytes.Clone(value)
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = now.Add(ttl)
	}
	seq := m.seq.Add(1)

	s.mu.Lock()
	if _, existed := s.entries[key]; existed {
		entry := &memoryEntry{
			value:     cloned,
			expiresAt: expiresAt,
			seq:       seq,
		}
		entry.lastAccess.Store(now.UnixNano())
		s.entries[key] = entry
		m.sweepExpiredLocked(s, now, memorySweepBatchSize)
		s.mu.Unlock()
		return nil
	}

	m.sweepExpiredLocked(s, now, memorySweepBatchSize)
	if m.maxEntries > 0 && int(m.count.Load()) >= m.maxEntries {
		for i := 0; i < memorySweepBatchSize && int(m.count.Load()) >= m.maxEntries; i++ {
			if !m.evictOneLocked(s, now) {
				break
			}
		}
		if int(m.count.Load()) >= m.maxEntries {
			s.mu.Unlock()
			m.evictAcrossShards(idx, now)
			s.mu.Lock()
			if _, existed := s.entries[key]; existed {
				entry := &memoryEntry{
					value:     cloned,
					expiresAt: expiresAt,
					seq:       seq,
				}
				entry.lastAccess.Store(now.UnixNano())
				s.entries[key] = entry
				s.mu.Unlock()
				return nil
			}
		}
	}

	entry := &memoryEntry{
		value:     cloned,
		expiresAt: expiresAt,
		seq:       seq,
	}
	entry.lastAccess.Store(now.UnixNano())
	s.entries[key] = entry
	m.count.Add(1)
	s.mu.Unlock()
	return nil
}

// Delete removes one or more keys from MemoryStore.
func (m *MemoryStore) Delete(ctx context.Context, keys ...string) error {
	if m == nil || len(keys) == 0 {
		return nil
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	for _, key := range keys {
		s := &m.shards[shardIndex(key)]
		s.mu.Lock()
		if _, existed := s.entries[key]; existed {
			delete(s.entries, key)
			m.count.Add(-1)
		}
		s.mu.Unlock()
	}
	return nil
}

// Close clears all entries from MemoryStore.
func (m *MemoryStore) Close() error {
	if m == nil {
		return nil
	}
	for i := range m.shards {
		s := &m.shards[i]
		s.mu.Lock()
		n := int64(len(s.entries))
		clear(s.entries)
		if n > 0 {
			m.count.Add(-n)
		}
		s.mu.Unlock()
	}
	return nil
}

func (m *MemoryStore) sweepExpiredLocked(s *memoryShard, now time.Time, limit int) {
	if len(s.entries) == 0 {
		return
	}
	checked := 0
	for k, e := range s.entries {
		if !e.expiresAt.IsZero() && !now.Before(e.expiresAt) {
			delete(s.entries, k)
			m.count.Add(-1)
		}
		checked++
		if checked >= limit {
			break
		}
	}
}

func (m *MemoryStore) evictOneLocked(s *memoryShard, now time.Time) bool {
	if len(s.entries) == 0 {
		return false
	}
	var (
		victimKey        string
		victimLastAccess int64
		victimSeq        uint64
		hasVictim        bool
		scanned          int
	)
	for k, e := range s.entries {
		if !e.expiresAt.IsZero() && !now.Before(e.expiresAt) {
			delete(s.entries, k)
			m.count.Add(-1)
			return true
		}
		la := e.lastAccess.Load()
		if !hasVictim || la < victimLastAccess || (la == victimLastAccess && e.seq < victimSeq) {
			victimKey = k
			victimLastAccess = la
			victimSeq = e.seq
			hasVictim = true
		}
		scanned++
		if scanned >= memoryEvictSampleLimit {
			break
		}
	}
	if hasVictim {
		delete(s.entries, victimKey)
		m.count.Add(-1)
		return true
	}
	return false
}

func (m *MemoryStore) evictAcrossShards(startIdx int, now time.Time) {
	for offset := 1; offset <= numMemoryShards && int(m.count.Load()) >= m.maxEntries; offset++ {
		idx := (startIdx + offset) & (numMemoryShards - 1)
		s := &m.shards[idx]
		s.mu.Lock()
		for i := 0; i < memorySweepBatchSize && int(m.count.Load()) >= m.maxEntries; i++ {
			if !m.evictOneLocked(s, now) {
				break
			}
		}
		s.mu.Unlock()
	}
}
