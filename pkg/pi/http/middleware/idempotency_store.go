package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"time"

	piRedis "github.com/sllt/pi/pkg/pi/datasource/redis"
)

const (
	defaultIdempotencyMaxKeys     = 10000
	defaultIdempotencyRedisPrefix = "pi:idem:"
	idempotencySweepBatchSize     = 8
	idempotencyEvictSampleLimit   = 32
)

var (
	errIdempotencyOwnershipLost = errors.New("idempotency: lock ownership lost or expired")
	errIdempotencyNilRedis      = errors.New("idempotency: redis client is nil")
	errIdempotencyRedisResult   = errors.New("idempotency: unexpected redis eval result")
)

// IsIdempotencyOwnershipLost reports whether err indicates the InProgress lock
// expired or was taken over before completion.
func IsIdempotencyOwnershipLost(err error) bool {
	return errors.Is(err, errIdempotencyOwnershipLost)
}

// IdempotencyRecord represents the persisted state of an idempotent request.
type IdempotencyRecord struct {
	InProgress  bool                `json:"in_progress"`
	OwnerToken  string              `json:"owner_token,omitempty"`
	Fingerprint string              `json:"fingerprint"`
	StatusCode  int                 `json:"status_code,omitempty"`
	Headers     map[string][]string `json:"headers,omitempty"`
	Body        []byte              `json:"body,omitempty"`
}

// IdempotencyStore abstracts atomic storage for idempotency locks and cached responses.
type IdempotencyStore interface {
	TryBegin(ctx context.Context, key, fingerprint, ownerToken string, lockTTL time.Duration) (existing *IdempotencyRecord, acquired bool, err error)
	Complete(ctx context.Context, key, ownerToken string, record IdempotencyRecord, ttl time.Duration) error
	Abort(ctx context.Context, key, ownerToken string) error
	Close() error
}

func cloneIdempotencyRecord(rec IdempotencyRecord) IdempotencyRecord {
	out := IdempotencyRecord{
		InProgress:  rec.InProgress,
		OwnerToken:  rec.OwnerToken,
		Fingerprint: rec.Fingerprint,
		StatusCode:  rec.StatusCode,
	}
	if len(rec.Headers) > 0 {
		out.Headers = make(map[string][]string, len(rec.Headers))
		for k, vals := range rec.Headers {
			copied := make([]string, len(vals))
			copy(copied, vals)
			out.Headers[k] = copied
		}
	}
	if rec.Body != nil {
		out.Body = make([]byte, len(rec.Body))
		copy(out.Body, rec.Body)
	}
	return out
}

type memoryIdempotencyEntry struct {
	record    IdempotencyRecord
	expiresAt time.Time
	seq       uint64
}

// MemoryIdempotencyStore is a goroutine-free in-memory IdempotencyStore with
// bounded capacity and opportunistic expiry cleanup on TryBegin.
type MemoryIdempotencyStore struct {
	mu      sync.Mutex
	entries map[string]memoryIdempotencyEntry
	maxKeys int
	nextSeq uint64
	now     func() time.Time
}

// NewMemoryIdempotencyStore creates a goroutine-free in-memory IdempotencyStore.
// If maxKeys is omitted or <= 0, it defaults to 10000.
func NewMemoryIdempotencyStore(maxKeys ...int) *MemoryIdempotencyStore {
	limit := defaultIdempotencyMaxKeys
	if len(maxKeys) > 0 && maxKeys[0] > 0 {
		limit = maxKeys[0]
	}
	return &MemoryIdempotencyStore{
		entries: make(map[string]memoryIdempotencyEntry),
		maxKeys: limit,
		now:     time.Now,
	}
}

// TryBegin atomically acquires an InProgress lock for key or returns the existing record.
func (m *MemoryIdempotencyStore) TryBegin(
	ctx context.Context,
	key, fingerprint, ownerToken string,
	lockTTL time.Duration,
) (*IdempotencyRecord, bool, error) {
	if m == nil {
		return nil, false, errors.New("idempotency: memory store is nil")
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
	}

	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()

	if entry, ok := m.entries[key]; ok {
		if !entry.expiresAt.IsZero() && !now.Before(entry.expiresAt) {
			delete(m.entries, key)
		} else {
			cloned := cloneIdempotencyRecord(entry.record)
			return &cloned, false, nil
		}
	}

	m.sweepExpiredLocked(now, idempotencySweepBatchSize)
	if m.maxKeys > 0 && len(m.entries) >= m.maxKeys {
		m.evictOneLocked(now)
	}

	var expiresAt time.Time
	if lockTTL > 0 {
		expiresAt = now.Add(lockTTL)
	}
	m.nextSeq++
	m.entries[key] = memoryIdempotencyEntry{
		record: IdempotencyRecord{
			InProgress:  true,
			OwnerToken:  ownerToken,
			Fingerprint: fingerprint,
		},
		expiresAt: expiresAt,
		seq:       m.nextSeq,
	}
	return nil, true, nil
}

// Complete transitions an InProgress entry owned by ownerToken into Completed state.
func (m *MemoryIdempotencyStore) Complete(
	ctx context.Context,
	key, ownerToken string,
	record IdempotencyRecord,
	ttl time.Duration,
) error {
	if m == nil {
		return errors.New("idempotency: memory store is nil")
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}

	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.entries[key]
	if !ok || !entry.record.InProgress || entry.record.OwnerToken != ownerToken {
		return errIdempotencyOwnershipLost
	}
	if !entry.expiresAt.IsZero() && !now.Before(entry.expiresAt) {
		delete(m.entries, key)
		return errIdempotencyOwnershipLost
	}

	stored := cloneIdempotencyRecord(record)
	stored.InProgress = false
	stored.OwnerToken = ""

	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = now.Add(ttl)
	}
	m.nextSeq++
	m.entries[key] = memoryIdempotencyEntry{
		record:    stored,
		expiresAt: expiresAt,
		seq:       m.nextSeq,
	}
	return nil
}

// Abort releases an InProgress entry only if it is still owned by ownerToken.
func (m *MemoryIdempotencyStore) Abort(ctx context.Context, key, ownerToken string) error {
	if m == nil {
		return nil
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.entries[key]
	if ok && entry.record.InProgress && entry.record.OwnerToken == ownerToken {
		delete(m.entries, key)
	}
	return nil
}

// Close clears all entries in the in-memory store.
func (m *MemoryIdempotencyStore) Close() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	clear(m.entries)
	m.mu.Unlock()
	return nil
}

func (m *MemoryIdempotencyStore) sweepExpiredLocked(now time.Time, limit int) {
	checked := 0
	for k, e := range m.entries {
		if !e.expiresAt.IsZero() && !now.Before(e.expiresAt) {
			delete(m.entries, k)
		}
		checked++
		if checked >= limit {
			break
		}
	}
}

func (m *MemoryIdempotencyStore) evictOneLocked(now time.Time) {
	var (
		completedKey string
		completedSeq uint64
		hasCompleted bool
		anyKey       string
		anySeq       uint64
		hasAny       bool
		scanned      int
	)
	for k, e := range m.entries {
		if !e.expiresAt.IsZero() && !now.Before(e.expiresAt) {
			delete(m.entries, k)
			return
		}
		if !e.record.InProgress && (!hasCompleted || e.seq < completedSeq) {
			completedKey = k
			completedSeq = e.seq
			hasCompleted = true
		}
		if !hasAny || e.seq < anySeq {
			anyKey = k
			anySeq = e.seq
			hasAny = true
		}
		scanned++
		if scanned >= idempotencyEvictSampleLimit {
			break
		}
	}
	if hasCompleted {
		delete(m.entries, completedKey)
		return
	}
	if hasAny {
		delete(m.entries, anyKey)
	}
}

const (
	redisIdemTryBeginScript = `
local cur = redis.call("GET", KEYS[1])
if cur then
	local ok, obj = pcall(cjson.decode, cur)
	if ok and type(obj) == "table" and (obj["fingerprint"] ~= nil or obj["in_progress"] ~= nil) then
		return {0, cur}
	end
end
local px = tonumber(ARGV[2])
if px and px > 0 then
	redis.call("SET", KEYS[1], ARGV[1], "PX", px)
else
	redis.call("SET", KEYS[1], ARGV[1])
end
return {1, ""}
`
	redisIdemCompleteScript = `
local cur = redis.call("GET", KEYS[1])
if not cur then
	return 0
end
local ok, obj = pcall(cjson.decode, cur)
if not ok or type(obj) ~= "table" or not obj["in_progress"] or obj["owner_token"] ~= ARGV[1] then
	return 0
end
local px = tonumber(ARGV[3])
if px and px > 0 then
	redis.call("SET", KEYS[1], ARGV[2], "PX", px)
else
	redis.call("SET", KEYS[1], ARGV[2])
end
return 1
`
	redisIdemAbortScript = `
local cur = redis.call("GET", KEYS[1])
if not cur then
	return 0
end
local ok, obj = pcall(cjson.decode, cur)
if ok and type(obj) == "table" and obj["in_progress"] and obj["owner_token"] == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`
)

// RedisIdempotencyStore implements IdempotencyStore using atomic Redis Lua scripts.
type RedisIdempotencyStore struct {
	client    piRedis.Client
	keyPrefix string
}

// NewRedisIdempotencyStore constructs a Redis-backed IdempotencyStore.
// If keyPrefix is omitted or empty, it defaults to "pi:idem:".
func NewRedisIdempotencyStore(client piRedis.Client, keyPrefix ...string) *RedisIdempotencyStore {
	prefix := defaultIdempotencyRedisPrefix
	if len(keyPrefix) > 0 && strings.TrimSpace(keyPrefix[0]) != "" {
		prefix = keyPrefix[0]
	}
	if isNilIdemRedisClient(client) {
		client = nil
	}
	return &RedisIdempotencyStore{
		client:    client,
		keyPrefix: prefix,
	}
}

func isNilIdemRedisClient(c piRedis.Client) bool {
	if c == nil {
		return true
	}
	rv := reflect.ValueOf(c)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
		return rv.IsNil()
	default:
		return false
	}
}

func (r *RedisIdempotencyStore) prefixedKey(key string) string {
	return r.keyPrefix + key
}

func durationToPX(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	ms := d.Milliseconds()
	if ms == 0 {
		return 1
	}
	return ms
}

// TryBegin atomically checks for an existing record or sets an InProgress lock with PX.
func (r *RedisIdempotencyStore) TryBegin(
	ctx context.Context,
	key, fingerprint, ownerToken string,
	lockTTL time.Duration,
) (*IdempotencyRecord, bool, error) {
	if r == nil || r.client == nil {
		return nil, false, errIdempotencyNilRedis
	}
	if ctx == nil {
		ctx = context.Background()
	}

	inProgressPayload, err := json.Marshal(IdempotencyRecord{
		InProgress:  true,
		OwnerToken:  ownerToken,
		Fingerprint: fingerprint,
	})
	if err != nil {
		return nil, false, err
	}

	res, err := r.client.Eval(
		ctx,
		redisIdemTryBeginScript,
		[]string{r.prefixedKey(key)},
		string(inProgressPayload),
		durationToPX(lockTTL),
	).Result()
	if err != nil {
		return nil, false, err
	}

	arr, ok := res.([]any)
	if !ok || len(arr) != 2 {
		return nil, false, errIdempotencyRedisResult
	}
	acquiredCode, ok := redisReplyInt64(arr[0])
	if !ok {
		return nil, false, errIdempotencyRedisResult
	}
	if acquiredCode == 1 {
		return nil, true, nil
	}

	raw, ok := redisReplyString(arr[1])
	if !ok {
		return nil, false, errIdempotencyRedisResult
	}
	var existing IdempotencyRecord
	if err := json.Unmarshal([]byte(raw), &existing); err != nil {
		return nil, false, err
	}
	return &existing, false, nil
}

// Complete verifies ownerToken matches the InProgress record and writes the Completed record with PX.
func (r *RedisIdempotencyStore) Complete(
	ctx context.Context,
	key, ownerToken string,
	record IdempotencyRecord,
	ttl time.Duration,
) error {
	if r == nil || r.client == nil {
		return errIdempotencyNilRedis
	}
	if ctx == nil {
		ctx = context.Background()
	}

	stored := cloneIdempotencyRecord(record)
	stored.InProgress = false
	stored.OwnerToken = ""

	payload, err := json.Marshal(stored)
	if err != nil {
		return err
	}

	updated, err := r.client.Eval(
		ctx,
		redisIdemCompleteScript,
		[]string{r.prefixedKey(key)},
		ownerToken,
		string(payload),
		durationToPX(ttl),
	).Int64()
	if err != nil {
		return err
	}
	if updated == 0 {
		return errIdempotencyOwnershipLost
	}
	return nil
}

// Abort deletes the key only if it is currently InProgress with matching ownerToken.
func (r *RedisIdempotencyStore) Abort(ctx context.Context, key, ownerToken string) error {
	if r == nil || r.client == nil {
		return errIdempotencyNilRedis
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return r.client.Eval(
		ctx,
		redisIdemAbortScript,
		[]string{r.prefixedKey(key)},
		ownerToken,
	).Err()
}

// Close is a no-op because the underlying piRedis.Client lifecycle is owned by pi.App.
func (r *RedisIdempotencyStore) Close() error {
	return nil
}

func redisReplyInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int:
		return int64(x), true
	case float64:
		return int64(x), true
	default:
		return 0, false
	}
}

func redisReplyString(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case []byte:
		return string(x), true
	default:
		return "", false
	}
}
