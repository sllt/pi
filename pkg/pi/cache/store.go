package cache

import (
	"context"
	"time"
)

// ByteStore defines the low-level byte storage backend for Cache.
type ByteStore interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Delete(ctx context.Context, keys ...string) error
	Close() error
}

// ttlByteStore is an optional extension implemented by stores that can report
// remaining TTL on Get so TieredStore can bound L1 entries by L2's remaining lifetime.
type ttlByteStore interface {
	getWithTTL(ctx context.Context, key string) (value []byte, ok bool, remaining time.Duration, err error)
}
