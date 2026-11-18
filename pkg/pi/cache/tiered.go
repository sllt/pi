package cache

import (
	"context"
	"errors"
	"reflect"
	"time"
)

const defaultTieredL1TTL = 30 * time.Second

// TieredStore combines a fast L1 ByteStore (typically MemoryStore) with a shared
// L2 ByteStore (typically RedisStore).
type TieredStore struct {
	l1    ByteStore
	l2    ByteStore
	l1TTL time.Duration
}

// NewTieredStore creates a two-tier ByteStore. If l1TTL <= 0, it defaults to 30s.
func NewTieredStore(l1, l2 ByteStore, l1TTL time.Duration) *TieredStore {
	if l1TTL <= 0 {
		l1TTL = defaultTieredL1TTL
	}
	if isNilByteStore(l1) {
		l1 = nil
	}
	if isNilByteStore(l2) {
		l2 = nil
	}
	return &TieredStore{
		l1:    l1,
		l2:    l2,
		l1TTL: l1TTL,
	}
}

func isNilByteStore(s ByteStore) bool {
	if s == nil {
		return true
	}
	rv := reflect.ValueOf(s)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
		return rv.IsNil()
	default:
		return false
	}
}

// Get checks L1 first; on miss (or L1 error), checks L2 and best-effort populates L1
// with min(l1TTL, l2RemainingTTL) when L2 exposes remaining TTL.
func (t *TieredStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if t == nil {
		return nil, false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	var l1Err error
	if t.l1 != nil {
		val, ok, err := t.l1.Get(ctx, key)
		if err == nil && ok {
			return val, true, nil
		}
		l1Err = err
	}

	if t.l2 == nil {
		return nil, false, l1Err
	}

	var (
		val       []byte
		hit       bool
		remaining time.Duration
		err       error
	)
	if ttlL2, ok := t.l2.(ttlByteStore); ok {
		val, hit, remaining, err = ttlL2.getWithTTL(ctx, key)
	} else {
		val, hit, err = t.l2.Get(ctx, key)
	}
	if err != nil || !hit {
		return nil, false, err
	}

	if t.l1 != nil {
		popTTL := t.l1TTL
		if remaining > 0 && remaining < popTTL {
			popTTL = remaining
		}
		_ = t.l1.Set(ctx, key, val, popTTL)
	}
	return val, true, nil
}

// Set writes to L2 with ttl and populates L1 with min(ttl, l1TTL) when ttl >= 0.
// When ttl < 0, deletes key from both L1 and L2.
func (t *TieredStore) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if t == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ttl < 0 {
		return t.Delete(ctx, key)
	}
	if t.l2 != nil {
		if err := t.l2.Set(ctx, key, value, ttl); err != nil {
			if t.l1 != nil {
				_ = t.l1.Delete(ctx, key)
			}
			return err
		}
	}
	if t.l1 != nil {
		l1TTL := t.l1TTL
		if ttl > 0 && ttl < l1TTL {
			l1TTL = ttl
		}
		return t.l1.Set(ctx, key, value, l1TTL)
	}
	return nil
}

// Delete removes keys from both L1 and L2, joining any errors.
func (t *TieredStore) Delete(ctx context.Context, keys ...string) error {
	if t == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var errs []error
	if t.l1 != nil {
		if err := t.l1.Delete(ctx, keys...); err != nil {
			errs = append(errs, err)
		}
	}
	if t.l2 != nil {
		if err := t.l2.Delete(ctx, keys...); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Close closes both L1 and L2 stores, joining any errors.
func (t *TieredStore) Close() error {
	if t == nil {
		return nil
	}
	var errs []error
	if t.l1 != nil {
		if err := t.l1.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if t.l2 != nil {
		if err := t.l2.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
