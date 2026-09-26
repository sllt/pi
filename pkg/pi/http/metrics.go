package http

import "context"

// Metrics represents an interface for registering the default metrics in Pi framework.
type Metrics interface {
	IncrementCounter(ctx context.Context, name string, labels ...string)
}
