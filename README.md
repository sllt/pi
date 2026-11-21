# Pi

[![Go Reference](https://pkg.go.dev/badge/github.com/sllt/pi.svg)](https://pkg.go.dev/github.com/sllt/pi)
[![Go Version](https://img.shields.io/badge/Go-1.27+-00ADD8?logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](./LICENSE)

English | [简体中文](./README.zh-CN.md)

**Pi** is a production-grade Go microservice and web application framework built around a lean runtime microkernel, explicit resource lifecycles, zero-lock generic adapter injection, actor-supervised background runtimes, zero-reflection-call generic HTTP contracts, and built-in OpenAPI 3.0.3 with an offline Scalar API Reference UI.

Pair Pi with the official [pi-layout](https://github.com/sllt/pi-layout) Fx scaffold and the `pi` CLI to generate reproducible, multi-transport Go services out of the box.

---

## Highlights

- **Runtime Microkernel & Generic Adapter Registry (`pi.Build` + `pi.Use[T]`)** — Side-effect-free construction with explicit `Owned` / `Borrowed` ownership (`WithManagedDatasources`, `WithManagedSQL`, `WithManagedRedis`, `WithManagedPubSub`, `WithAdapter`), symmetric read-only accessors (`ctx.SQL()`, `ctx.Redis()`, `ctx.PubSub()`, `ctx.File()`), lock-free O(1) generic resolution (`pi.Use[T]`, `pi.Resolve[T]`, `pi.MustUse[T]`), automatic capability probing (`ConnectContext`, `Shutdown`, `Close`, `HealthCheck`), and deterministic reverse-order rollback/shutdown.
- **Unified HTTP Routing, Streaming & SPA Hosting** — `GET`/`POST`/`PUT`/`PATCH`/`DELETE`/`HEAD`/`OPTIONS` on `*App` and `*RouteGroup` support both strongly-typed `func(*pi.Context, *Req) (*Res, error)` (or zero-reflection `pi.H(fn)`) and classic `func(*pi.Context) (any, error)`. Optional `pi.Meta` tags and five isolated input sources (`path`, `query`, `header`, `json`, and `form` multipart file upload) pair with native `*response.File`, `*response.XML`, `*response.Stream`, and first-class `*response.SSE` (`response.NewSSE`) responses, `app.StaticFS("/", embeddedFS, pi.WithStaticSubDir("dist"), pi.WithSPAFallback())` single-binary frontend hosting, and zero-config `Idempotency-Key` middleware (`UseIdempotency()` / `WithEndpointIdempotency()`), while route conflicts and invalid contracts are caught before serving via `app.RouteError()`.
- **Stampede-Safe Generic Cache (`pkg/pi/cache`)** — Goroutine-free `cache.NewMemory`, `cache.NewRedis`, and `cache.NewTiered` stores with unified options (`WithMaxEntries`, `WithKeyPrefix`, `WithJitter`, `WithNotFoundTTL`) paired with `cache.Remember[T]` / `cache.Delete`: refcounted context-detached singleflight, uniform TTL jitter, `*apperror.Error`-preserving negative caching, nil-safe pass-through, and fail-open resilience.
- **Zero-Tooling OpenAPI 3.0.3 & Offline Scalar UI** — Export or serve deterministic OpenAPI 3.0.3 JSON/YAML directly from registered routes—no `swag` comment annotations or external codegen required. Includes a 100% air-gapped, pre-compressed Scalar API Reference UI (`/.well-known/swagger`).
- **Actor-Supervised Runtime** — Powered by `github.com/sllt/tao/actor`, Pi supervises background workers (`AddWorker` with `SuperviseFatal`, `SuperviseRestart`, `SuperviseDegrade`), overlap-protected Cron jobs (`AddCron`), resilient Pub/Sub subscribers with static `AckSemantics` validation and Ack/Nack safety, and managed WebSocket sessions.
- **Unified Business Contracts & Security** — Protocol-agnostic `pkg/pi/apperror` maps cleanly across HTTP, gRPC, and CLI exit codes while sanitizing internal failures. `pkg/pi/auth` propagates verified `Principal` identities across transports; custom auth validators receive a full `*pi.Context` (`ctx.Header`, bounded request body, gRPC incoming metadata).
- **Observability & Singleflight Health Probes** — Instance-isolated OpenTelemetry tracing (with dynamic child-span `trace_id` propagation in `ctx.Trace`), Prometheus metrics, structured logging with secret redaction, three-state Kubernetes probes (`/.well-known/{startup,alive,ready}`), and caller-isolated singleflight deep health checks (`/.well-known/health`).
- **Reproducible CLI & Code Generation** — `cmd/pi` scaffolds verified projects (`pi init`), generates module skeletons (`pi create`), wraps protobuf gRPC servers/clients (`pi wrap grpc`), and scaffolds database migrations (`pi migrate`).

---

## Installation

### Framework Module

```bash
go get github.com/sllt/pi@v0.5.6
```

### Pi CLI

```bash
go install github.com/sllt/pi/cmd/pi@v0.5.6
```

Scaffold a new production-ready project from [pi-layout](https://github.com/sllt/pi-layout):

```bash
pi init --module example.com/acme/orders ./orders
# Pass --skip-verify for instant scaffolding without initial go mod download / go build
```

---

## Quick Start

Define strongly-typed request/response DTOs, register handlers directly via `app.GET` / `app.POST` / ..., and enable live OpenAPI 3.0.3 + Scalar UI (`pi.Meta` itself is optional when no extra OpenAPI metadata is needed, and `pi.Register` remains supported for compatibility):

```go
package main

import (
	"log"

	"github.com/sllt/pi/pkg/pi"
)

type GreetRequest struct {
	pi.Meta `tags:"Greetings" summary:"Greet a user"`
	Name    string `path:"name" validate:"required" dc:"Target user name"`
	Title   string `query:"title" default:"friend" dc:"Optional honorific"`
}

type GreetResponse struct {
	Message string `json:"message"`
}

func Greet(ctx *pi.Context, req *GreetRequest) (*GreetResponse, error) {
	ctx.Infof("greeting user=%s", req.Name)
	return &GreetResponse{
		Message: "Hello, " + req.Title + " " + req.Name + "!",
	}, nil
}

func main() {
	app, err := pi.Build(
		pi.WithConfig(map[string]string{
			"APP_NAME":  "greeting-service",
			"HTTP_ADDR": "127.0.0.1:8000",
		}),
		pi.WithExplicitHTTPStatus(),
		pi.WithOpenAPI(pi.OpenAPIConfig{
			Title:     "Greeting API",
			Version:   "1.0.0",
			ServeSpec: true,
			ServeUI:   true,
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	app.GET("/greetings/{name}", Greet)
	if err := app.RouteError(); err != nil {
		log.Fatal(err)
	}

	app.Run()
}
```

Run the server and explore the built-in endpoints:

- `GET http://127.0.0.1:8000/greetings/alice?title=captain` — Typed JSON endpoint
- `GET http://127.0.0.1:8000/.well-known/openapi.json` — Generated OpenAPI 3.0.3 JSON (`openapi.yaml` also available)
- `GET http://127.0.0.1:8000/.well-known/swagger` — Built-in offline Scalar API Reference UI
- `GET http://127.0.0.1:8000/.well-known/{startup,alive,ready,health}` — Startup, liveness, readiness, and deep health probes

---

## Core Capabilities

### 1. Managed Datasources, External Adapters & `pi.Use[T]`

`pi.Build` never reads implicit global state or opens network sockets during construction. Built-in SQL, Redis, and Pub/Sub handles—as well as any external datasource adapter—activate in registration order during `Start` and shut down in reverse order during `Stop`:

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
	pi.WithManagedSQL(),
	pi.WithManagedRedis(),
	pi.WithAdapter("mongo", mongoClient, pi.Owned),
)
if err != nil {
	return err
}
```

Inside any HTTP, gRPC, Pub/Sub, Cron, or Worker `*pi.Context` (or directly on `*pi.App`), access core slots via symmetric read-only methods and external adapters via zero-lock generic helpers:

```go
func GetOrder(ctx *pi.Context, req *GetOrderReq) (*OrderRes, error) {
	// Built-in core slots (guaranteed untyped nil when unconfigured)
	row := ctx.SQL().QueryRowContext(ctx, "SELECT id, total FROM orders WHERE id = ?", req.ID)
	_ = ctx.Redis().Get(ctx, "order:"+req.ID)

	// External or multi-instance adapters resolved with compile-time type safety
	docStore := pi.MustUse[*mongo.Client](ctx)
	_ = docStore
	// For multiple instances of the same type: pi.MustUseNamed[*mongo.Client](ctx, "mongo-archive")
	...
}
```

Inside database migrations (`migration.Datasource`), `migration.Resolve[T](d)` and `migration.ResolveNamed[T](d, name)` provide safe access to mounted adapters while strictly blocking accidental raw pool (`sql.Client`) bypasses of the active `d.SQL` transaction (`ErrTransactionalDatasource`).

### 2. Route Groups, Middleware & OpenAPI Security

Group routes cleanly and isolate same-prefix auth boundaries with `Scope()` while inheriting middleware, OpenAPI tags, and security schemes:

```go
v1 := app.Group("/api/v1").WithDoc(pi.DocTags("Users"))
v1.POST("/auth/login", userHandler.Login)

authed := v1.Scope().
	UseMiddleware(requireBearerMiddleware).
	WithDoc(pi.DocSecurity("Bearer"))
authed.GET("/users/profile", userHandler.GetProfile)
authed.PUT("/users/profile", userHandler.UpdateProfile)

if err := app.RouteError(); err != nil {
	return err
}
```

To export OpenAPI specifications in CI without starting listeners or hooks, call `app.OpenAPIJSON()` or `app.OpenAPIYAML()`.

### 3. Supervised Workers & Cron Scheduling

Register resilient background workers and overlap-safe scheduled jobs under Pi's actor supervisor:

```go
// Restarting background worker with bounded exponential backoff
err := app.AddWorker("outbox-relay", pi.WorkerPolicy{
	Mode:          pi.SuperviseRestart,
	Required:      true,
	MaxRestarts:   5,
	RestartWindow: time.Minute,
}, func(ctx *pi.Context) error {
	return relayOutbox(ctx)
})

// Overlap-protected Cron job drained gracefully before resources close
err = app.AddCron("0 */5 * * * *", "cleanup-expired-sessions", func(ctx *pi.Context) error {
	return cleanupSessions(ctx)
}, pi.CronJobOptions{
	Overlap:  pi.CronSkipIfRunning,
	Location: time.UTC,
})
```

### 4. Protocol-Agnostic Errors & Principal Authorization

Define domain errors once with `pkg/pi/apperror` and enforce authorization in your service layer with `pkg/pi/auth`:

```go
var ErrEmailTaken = apperror.New(
	apperror.Conflict,
	1001,
	"email address is already registered",
)

// Transport-neutral authorization check (self or explicit permission)
if _, err := auth.AuthorizeSubject(ctx, targetUserID, "users:write:any"); err != nil {
	return err
}
```

`apperror.Error` maps automatically to HTTP status envelopes, `grpc.MapError` status codes, and `cmd.MapError` process exit codes without leaking internal causes on 5xx errors.

### 5. Portless Testing with `testkit`

Exercise full routing, middleware, binding, validation, mock adapters, and lifecycle cleanup in unit/integration tests without opening TCP ports:

```go
func TestGreet(t *testing.T) {
	app := testkit.New(
		t,
		pi.WithExplicitHTTPStatus(),
		pi.WithAdapter("docs", &fakeDocStore{}, pi.Borrowed),
	)
	app.GET("/greetings/{name}", Greet)
	require.NoError(t, app.RouteError())

	req := httptest.NewRequest(http.MethodGet, "/greetings/world", nil)
	rec := testkit.Request(t, app, req)
	require.Equal(t, http.StatusOK, rec.Code)
}
```

### 6. SSE Streaming, Single-Binary SPA Hosting, Cache & Idempotency

Serve Server-Sent Events via first-class `*response.SSE` (`response.NewSSE`, zero `pi.Meta` needed for OpenAPI `text/event-stream`), embed a compiled SPA with `StaticFS`, protect mutations with zero-config `WithEndpointIdempotency()`, and coalesce cache misses via `cache.Remember[T]`:

```go
// Typed SSE endpoint with automatic text/event-stream OpenAPI schema, keepalive, and shutdown drain
app.GET("/events", func(ctx *pi.Context, _ *EventsReq) (*response.SSE, error) {
	return response.NewSSE(func(sctx context.Context, sse *response.SSEWriter) error {
		return sse.SendEvent("ready", map[string]string{"status": "ok"})
	}, response.WithSSEKeepAlive(15*time.Second)), nil
})

// Single-binary SPA hosting (sub-roots dist/, defaults to index.html, excludes /api and /.well-known)
app.StaticFS("/", embeddedFS,
	pi.WithStaticSubDir("dist"),
	pi.WithSPAFallback(),
	pi.WithStaticExclude("/api"),
)

// Endpoint-level Idempotency-Key deduplication (auto-uses app.Redis() when configured, else shared memory store)
app.POST("/orders", orderHandler.Create, pi.WithEndpointIdempotency())

// Stampede-safe generic cache with unified options, singleflight, jitter, and automatic 404 negative caching
memCache := cache.NewMemory(
	cache.WithMaxEntries(5000),
	cache.WithJitter(0.1),
	cache.WithNotFoundTTL(30*time.Second),
)
order, err := cache.Remember(ctx, memCache, "order:"+id, 5*time.Minute, func(c context.Context) (*Order, error) {
	return repo.FindByID(c, id)
})
```

---

## Datasources & Adapter Submodules

Pi keeps core SQL, Redis, local filesystem, and standard messaging contracts in the root module while isolating 25 third-party drivers in independent Go submodules (`FromEnv` + `ConnectContext` + `Shutdown`/`Close` + `HealthCheck`) so your `go.mod` stays lean.

| Category | Built-in (`github.com/sllt/pi`) | Independent Adapter Submodules (`@v0.5.6`) |
| --- | --- | --- |
| **Relational SQL** | MySQL, PostgreSQL, SQLite | `oracle`, `dbresolver` |
| **Key-Value / Cache** | Redis | `kv-store/badger`, `kv-store/dynamodb`, `kv-store/nats` |
| **Document / NoSQL** | — | `mongo`, `couchbase`, `arangodb`, `surrealdb` |
| **Columnar / Wide-Column** | — | `clickhouse`, `cassandra`, `scylladb` |
| **Time-Series** | — | `influxdb`, `opentsdb` |
| **Search & Graph** | — | `elasticsearch`, `solr`, `dgraph` |
| **Pub/Sub Messaging** | Kafka, MQTT, Google Pub/Sub, Redis Streams/PubSub | `pubsub/nats`, `pubsub/sqs`, `pubsub/eventhub` |
| **File Storage** | Local Filesystem | `file/s3`, `file/gcs`, `file/azure`, `file/sftp`, `file/ftp` |

Install any independent adapter submodule and mount it via `pi.WithAdapter`:

```bash
go get github.com/sllt/pi/pkg/pi/datasource/mongo@v0.5.6
go get github.com/sllt/pi/pkg/pi/datasource/pubsub/nats@v0.5.6
go get github.com/sllt/pi/pkg/pi/datasource/file/s3@v0.5.6
```

---

## Repository Structure

```text
pi/
├── cmd/pi/                  # Pi CLI (init, create, wrap, migrate)
├── pkg/pi/                  # Runtime microkernel, adapter registry, typed router & supervision
│   ├── apperror/            # Protocol-agnostic business error model
│   ├── auth/                # Verified Principal propagation & authorization helpers
│   ├── cache/               # Stampede-safe generic cache (Memory, Redis, Tiered)
│   ├── cli/                 # CLI subcommand & template generators
│   ├── config/              # Deterministic env/config snapshot loader & redaction
│   ├── datasource/          # Core SQL/Redis/PubSub/File contracts + 25 adapter submodules
│   ├── grpc/                # gRPC interceptors, health & error mapping
│   ├── http/                # Router, middleware, responder & contract compiler
│   ├── migration/           # Lock-protected, transaction-safe Migration v2 engine
│   ├── openapi/             # OpenAPI 3.0.3 AST, JSON/YAML serializer & Scalar UI helper
│   ├── service/             # Unsealed HTTP client interface with circuit-breaker, retry & rate limit
│   ├── testkit/             # Portless testing harness & lifecycle probes
│   └── websocket/           # Managed WebSocket connections & hubs
└── examples/                # Runnable HTTP, gRPC, Cron, PubSub & migration examples
```

---

## Development & Verification

```bash
make build          # Build root packages in readonly module mode
make unit           # Run unit tests
make race           # Run tests with the Go race detector enabled
make modules        # Build all 25 independent datasource submodules + OpenAPI verifier
make openapi-verify # Validate generated OpenAPI 3.0.3 schemas via kin-openapi
```

---

## License

Licensed under the [Apache License, Version 2.0](./LICENSE).
