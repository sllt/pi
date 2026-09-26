# Lifecycle Hooks and Background Workers

Pi provides lifecycle APIs for work that should be managed by the application runtime itself.

This includes:

- synchronous startup hooks before traffic starts
- shutdown hooks during graceful shutdown
- long-running background workers tied to the app lifecycle

## Running the lifecycle

For standalone applications, keep using `app.Run()`:

```go
app := pi.New()
// register routes, hooks, workers...
app.Run()
```

`Run()` installs OS signal handling and blocks until the app shuts down.

For embedded hosts such as Fx, custom CLIs, or tests, use the composable lifecycle APIs:

```go
ctx := context.Background()

if err := app.Start(ctx); err != nil {
    return err
}

defer func() {
    shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    _ = app.Stop(shutdownCtx)
}()
```

You can also let the caller control the runtime context:

```go
if err := app.RunContext(ctx); err != nil {
    return err
}
```

### Behavior

- `Start(ctx)` runs `OnStart`, starts HTTP/gRPC/metrics servers, subscriptions, and background workers, then returns.
- `RunContext(ctx)` calls `Start(ctx)`, blocks until the context is canceled or a managed worker/server requests shutdown, then calls `Stop(ctx)`.
- `Stop(ctx)` gracefully shuts down servers, cron, runtime tasks, stop hooks, container resources, and metrics.
- Server startup failures are returned from `Start` / `RunContext`.
- Concurrent `Start` calls share one startup result; hooks, listeners and workers start once. The first caller's context remains the runtime parent. Later callers only bound their own wait.
- `Stop` runs cleanup once and preserves its terminal error. Later callers wait for the same cleanup using their own contexts; a waiting timeout does not mean cleanup completed.
- The first `Stop` context supplies the cleanup budget. A blocked hook or worker must still cooperate with cancellation; dependencies stay open until managed workers exit.
- Stopping or stopped instances cannot be restarted. Create a new App to retry a failed startup.
- Register `OnStart` and `Go` before startup. `OnStop` registration remains available until cleanup begins, including from a startup hook that is still running.
- Hooks must not synchronously wait on `Start` or `Stop` for their own App.

## OnStart

You can register a startup hook using the `a.OnStart()` method on your `app` instance.

## Usage

The method accepts a function with the signature `func(ctx *pi.Context) error`.

- The `*pi.Context` passed to the hook is fully initialized and provides access to all dependency-injection-managed services (e.g., `ctx.Container.SQL`, `ctx.Container.Redis`).
- If any `OnStart` hook returns an error, the application logs the error, rolls back startup with `Stop`, and returns the startup error.

### Example: Warming up a Cache

Here is an example of using `OnStart` to set an initial value in a Redis cache when the application starts.

```go
package main

import (
    "github.com/sllt/pi/pkg/pi"
)

func main() {
    a := pi.New()

    // Register an OnStart hook to warm up a cache.
    a.OnStart(func(ctx *pi.Context) error {
        ctx.Logger.Info("Warming up the cache...")

        // In a real app, this might come from a database or another service.
        cacheKey := "initial-data"
        cacheValue := "This is some data cached at startup."

        err := ctx.Redis.Set(ctx, cacheKey, cacheValue, 0).Err()
        if err != nil {
            ctx.Logger.Errorf("Failed to warm up cache: %v", err)
            return err // Return the error to halt startup if caching fails.
        }

        ctx.Logger.Info("Cache warmed up successfully!")

        return nil
    })

    // ... register your routes

    a.Run()
}
```

This ensures that critical startup tasks are completed successfully before the application begins accepting traffic.

## OnStop

You can register shutdown hooks using `a.OnStop()`.

- hooks run during graceful shutdown
- hooks run in **reverse registration order**
- hooks are attempted while the cleanup budget permits, and Pi joins their errors
- expiry prevents starting remaining hooks; completed or skipped hooks are not retried by another `Stop`

### Example

```go
app.OnStop(func(ctx *pi.Context) error {
    ctx.Logger.Info("flushing final state before shutdown")
    return nil
})
```

Use `OnStop` for:

- flushing buffers
- writing final checkpoints
- unregistering external leases
- closing app-level resources that should stop before the container is fully closed

## App.Go

`App.Go` registers a long-running background worker managed by Pi.

```go
app.Go("cache-warmer", func(ctx *pi.Context) error {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return nil
        case <-ticker.C:
            ctx.Logger.Info("refreshing cache")
        }
    }
})
```

### Behavior

- workers start with the application runtime
- the passed `*pi.Context` is canceled when shutdown begins
- returning `nil` or `context.Canceled` is treated as graceful exit
- returning any other error triggers application shutdown
- panics are recovered and logged

### Good use cases

- cache refresh loops
- outbox relays
- pollers / watchers
- heartbeat / lease renewals
- long-running sync workers
