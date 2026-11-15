# Datasources & Runtime Adapters

Pi splits datasource integrations into two tiers so core binaries remain lean while optional drivers stay isolated in standalone Go modules:

1. **Core Runtime Slots** (built into `github.com/sllt/pi`):
   - **SQL** (`pkg/pi/datasource/sql`): MySQL, PostgreSQL, SQLite — accessed via `app.SQL()` / `ctx.SQL()`.
   - **Redis** (`pkg/pi/datasource/redis`): Redis client & pipelines/transactions — accessed via `app.Redis()` / `ctx.Redis()`.
   - **PubSub** (`pkg/pi/datasource/pubsub/{kafka,google,mqtt}` + Redis Streams): accessed via `app.PubSub()` / `ctx.PubSub()`.
   - **File** (`pkg/pi/datasource/file`): local filesystem by default, or pluggable cloud/remote `FileSystemProvider` via `pi.WithFile(...)` — accessed via `app.File()` / `ctx.File()`.

2. **Standalone Adapter Submodules** (25 independent `go.mod` packages under `pkg/pi/datasource/*`):
   - Mounted into the runtime kernel via `pi.WithAdapter(name, adapter, pi.Owned)` (or `app.Mount(...)` on legacy `pi.New()`) and retrieved with compile-time type safety via `pi.Use[T](ctx)` / `pi.Resolve[T](app)` / `pi.UseNamed[T](ctx, name)`.

## Adapter Lifecycle & Observability Contract

When `pi.Owned` is specified, the runtime kernel manages the adapter's full lifecycle automatically without requiring adapter-specific methods on `pi.App` or `pi.Context`:

- **Configuration**: `FromEnv(cfg.Get)` constructs an unstarted adapter from an explicit configuration lookup function without global environment side effects.
- **Startup**: `ConnectContext(ctx context.Context) error` (preferred) or `Connect()` runs during `app.Start(ctx)` with bounded startup cancellation.
- **Observability Injection**: Adapters implementing optional `UseLogger(any)`, `UseMetrics(any)`, `UseTracer(any)`, or `UseTracerProvider(oteltrace.TracerProvider)` receive the `App` instance's logger, metrics, and OpenTelemetry tracer automatically before connection establishment.
- **Health Checks**: Adapters implementing `HealthCheckContext(ctx) error`, `HealthCheckContext(ctx) datasource.Health`, `HealthCheck(ctx) (any, error)`, `HealthCheck(ctx) error`, `Health() datasource.Health`, or `HealthCheck() datasource.Health` participate in `app.Health(ctx)` and `/.well-known/health`.
- **Graceful Shutdown**: `Shutdown(ctx context.Context) error` or `Close() error` runs in LIFO order during `app.Stop(ctx)` after HTTP/gRPC/Worker/Cron/Subscriber workloads drain.

### Mounting & Resolving an Adapter

```go
cfg, err := config.LoadSnapshot("configs", os.Environ())
if err != nil {
    return err
}

mongoClient, err := mongo.FromEnv(cfg.Get)
if err != nil {
    return err
}

app, err := pi.Build(
    pi.WithConfig(cfg.Values()),
    pi.WithManagedDatasources(),
    pi.WithAdapter("mongo", mongoClient, pi.Owned),
)
if err != nil {
    return err
}

app.GET("/orders", func(ctx *pi.Context) (any, error) {
    m, err := pi.Use[*mongo.Client](ctx)
    if err != nil {
        return nil, err
    }
    var orders []Order
    if err := m.Find(ctx, "orders", map[string]any{}, &orders); err != nil {
        return nil, err
    }
    return orders, nil
})
```

## Standalone Adapter Submodules (25 Modules)

| Submodule Path | Category | `FromEnv` | `ConnectContext` | `Close` / `Shutdown` | Health Check |
| --- | --- | :---: | :---: | :---: | :---: |
| `pkg/pi/datasource/arangodb` | Document / Graph | ✅ | ✅ | ✅ | ✅ |
| `pkg/pi/datasource/cassandra` | Wide-Column | ✅ | ✅ | ✅ | ✅ |
| `pkg/pi/datasource/clickhouse` | Columnar OLAP | ✅ | ✅ | ✅ | ✅ |
| `pkg/pi/datasource/couchbase` | Document / KV | ✅ | ✅ | ✅ | ✅ |
| `pkg/pi/datasource/dbresolver` | Multi-SQL Read/Write Routing | ✅ | ✅ | ✅ | ✅ |
| `pkg/pi/datasource/dgraph` | Graph | ✅ | ✅ | ✅ | ✅ |
| `pkg/pi/datasource/elasticsearch` | Search | ✅ | ✅ | ✅ | ✅ |
| `pkg/pi/datasource/file/azure` | File Storage | ✅ | ✅ | ✅ | ✅ |
| `pkg/pi/datasource/file/ftp` | File Storage | ✅ | ✅ | ✅ | ✅ |
| `pkg/pi/datasource/file/gcs` | File Storage | ✅ | ✅ | ✅ | ✅ |
| `pkg/pi/datasource/file/s3` | File Storage | ✅ | ✅ | ✅ | ✅ |
| `pkg/pi/datasource/file/sftp` | File Storage | ✅ | ✅ | ✅ | ✅ |
| `pkg/pi/datasource/influxdb` | Time-Series | ✅ | ✅ | ✅ | ✅ |
| `pkg/pi/datasource/kv-store/badger` | Embedded KV | ✅ | ✅ | ✅ | ✅ |
| `pkg/pi/datasource/kv-store/dynamodb` | Cloud KV | ✅ | ✅ | ✅ | ✅ |
| `pkg/pi/datasource/kv-store/nats` | JetStream KV | ✅ | ✅ | ✅ | ✅ |
| `pkg/pi/datasource/mongo` | Document | ✅ | ✅ | ✅ | ✅ |
| `pkg/pi/datasource/opentsdb` | Time-Series | ✅ | ✅ | ✅ | ✅ |
| `pkg/pi/datasource/oracle` | Relational SQL | ✅ | ✅ | ✅ | ✅ |
| `pkg/pi/datasource/pubsub/eventhub` | Streaming / PubSub | ✅ | ✅ | ✅ | ✅ |
| `pkg/pi/datasource/pubsub/nats` | JetStream PubSub | ✅ | ✅ | ✅ | ✅ |
| `pkg/pi/datasource/pubsub/sqs` | Queue / PubSub | ✅ | ✅ | ✅ | ✅ |
| `pkg/pi/datasource/scylladb` | Wide-Column | ✅ | ✅ | ✅ | ✅ |
| `pkg/pi/datasource/solr` | Search | ✅ | ✅ | ✅ | ✅ |
| `pkg/pi/datasource/surrealdb` | Multi-Model | ✅ | ✅ | ✅ | ✅ |
