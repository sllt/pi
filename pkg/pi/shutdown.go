package pi

import (
	"context"
	"errors"
	"time"

	"github.com/sllt/pi/pkg/pi/config"
)

// ShutdownWithContext handles the shutdown process with context timeout.
// It takes a shutdown function and a force close function as parameters.
// If the context times out, the force close function is called.
func ShutdownWithContext(ctx context.Context, shutdownFunc func(ctx context.Context) error, forceCloseFunc func() error) error {
	errCh := make(chan error, 1) // Channel to receive shutdown error

	go func() {
		errCh <- shutdownFunc(ctx) // Run shutdownFunc in a goroutine and send any error to errCh
	}()

	// Both signals may be ready. Always check the budget after selecting so a
	// graceful result cannot bypass force-close when cancellation has occurred.
	var err error
	select {
	case err = <-errCh:
	case <-ctx.Done():
		// Keep an already available graceful error without waiting past the budget.
		select {
		case err = <-errCh:
		default:
		}
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		if err == nil {
			err = ctxErr
		} else if !errors.Is(err, ctxErr) {
			err = errors.Join(err, ctxErr)
		}
		if forceCloseFunc != nil {
			err = errors.Join(err, forceCloseFunc())
		}
	}
	return err
}

func getShutdownTimeoutFromConfig(cfg config.Config) (time.Duration, error) {
	value := cfg.GetOrDefault("SHUTDOWN_GRACE_PERIOD", "30s")
	if value == "" {
		return shutDownTimeout, nil
	}

	timeout, err := time.ParseDuration(value)
	if err != nil {
		return shutDownTimeout, err
	}

	return timeout, nil
}
