package redis

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"

	"github.com/sllt/pi/pkg/pi/datasource"
)

var (
	// ErrNotStarted indicates a managed Redis handle was used before Start(ctx) completed.
	ErrNotStarted = errors.New("redis: managed connection has not started")
	// ErrClosed indicates a managed Redis handle was used after Close() completed.
	ErrClosed = errors.New("redis: client is closed")
)

// Client is the canonical Redis capability interface exposed by App.Redis() and Context.Redis().
// It includes go-redis Cmdable plus generic Do, optimistic-locking Watch, bounded health checks, and Close.
type Client interface {
	redis.Cmdable
	Do(ctx context.Context, args ...any) *redis.Cmd
	Watch(ctx context.Context, fn func(*redis.Tx) error, keys ...string) error
	HealthCheck() datasource.Health
	HealthCheckContext(ctx context.Context) datasource.Health
	Close() error
}

var (
	_ Client = (*Redis)(nil)
	_ Client = (*Handle)(nil)
)
