# Pi

[![Go Reference](https://pkg.go.dev/badge/github.com/sllt/pi.svg)](https://pkg.go.dev/github.com/sllt/pi)
[![Go Version](https://img.shields.io/badge/Go-1.27+-00ADD8?logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](./LICENSE)

English | [简体中文](./README.zh-CN.md)

**Pi** is a production-grade Go microservice and web application framework built around explicit resource lifecycles, actor-supervised background runtimes, zero-reflection-call generic HTTP contracts, and built-in OpenAPI 3.0.3 with an offline Scalar API Reference UI.

Pair Pi with the official [pi-layout](https://github.com/sllt/pi-layout) Fx scaffold and the `pi` CLI to generate reproducible, multi-transport Go services out of the box.

---

## Highlights

- **Explicit Construction & Lifecycle (`pi.Build`)** — Pure, side-effect-free construction with explicit `Owned` / `Borrowed` resource ownership (`WithManagedSQL`, `WithResource`), deterministic reverse-order shutdown, and portless integration testing via `pkg/pi/testkit`.
- **Typed HTTP Contracts & Pre-flight Validation** — Generic `pi.Register[Req, Res]` and `pi.Meta` struct tags bind `path`, `query`, `header`, and `json` inputs without `reflect.Value.Call` on the hot path. Route conflicts, invalid validation rules, and incompatible group mounts are caught before serving via `app.RouteError()`.
- **Zero-Tooling OpenAPI 3.0.3 & Offline Scalar UI** — Export or serve deterministic OpenAPI 3.0.3 JSON/YAML directly from registered routes—no `swag` comment annotations or external codegen required. Includes a 100% air-gapped, pre-compressed Scalar API Reference UI (`/.well-known/swagger`).
- **Actor-Supervised Runtime** — Powered by `github.com/sllt/tao/actor`, Pi supervises background workers (`AddWorker` with `SuperviseFatal`, `SuperviseRestart`, `SuperviseDegrade`), overlap-protected Cron jobs (`AddCron`), resilient Pub/Sub subscribers with bounded backoff and Ack/Nack capabilities, and managed WebSocket sessions.
- **Unified Business Contracts & Security** — Protocol-agnostic `pkg/pi/apperror` maps cleanly across HTTP, gRPC, and CLI exit codes while sanitizing internal failures. `pkg/pi/auth` propagates verified `Principal` identities across transports; strict CORS, TLS validation, and bounded request body limits are enforced by default.
- **Observability & Health Probes** — Instance-isolated OpenTelemetry tracing, Prometheus metrics, structured logging with secret redaction, dynamic remote log levels, and built-in `/livez` & `/readyz` probes.
- **Reproducible CLI & Code Generation** — `cmd/pi` scaffolds verified projects (`pi init`), generates module skeletons (`pi create`), wraps protobuf gRPC servers (`pi wrap grpc`), and manages database migrations (`pi migrate`).

---

## Installation

### Framework Module

```bash
go get github.com/sllt/pi@v0.5.0
```

### Pi CLI

```bash
go install github.com/sllt/pi/cmd/pi@v0.5.0
```

Scaffold a new production-ready project from [pi-layout](https://github.com/sllt/pi-layout):

```bash
pi init --module example.com/acme/orders ./orders
```

---

## Quick Start

Define strongly-typed request/response DTOs with `pi.Meta`, register handlers with `pi.Register`, and enable live OpenAPI 3.0.3 + Scalar UI:

```go
package main

import (
	"log"

	"github.com/sllt/pi/pkg/pi"
)

type GreetRequest struct {
	pi.Meta `path:"/greetings/{name}" method:"GET" tags:"Greetings" summary:"Greet a user"`
	Name    string `path:"name" validate:"required" dc:"Target user name"`
	Title   string `query:"title" default:"friend" dc:"Optional honorific"`
}

type GreetResponse struct {
	Message string `json:"message"`
}

func Greet(ctx *pi.Context, req *GreetRequest) (*GreetResponse, error) {
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

	if err := pi.Register(app, Greet); err != nil {
		log.Fatal(err)
	}

	app.Run()
}
```

Run the server and explore the built-in endpoints:

- `GET http://127.0.0.1:8000/greetings/alice?title=captain` — Typed JSON endpoint
- `GET http://127.0.0.1:8000/.well-known/openapi.json` — Generated OpenAPI 3.0.3 JSON (`openapi.yaml` also available)
- `GET http://127.0.0.1:8000/.well-known/swagger` — Built-in offline Scalar API Reference UI
- `GET http://127.0.0.1:8000/livez` & `/readyz` — Liveness and readiness probes

---

## Core Capabilities

### 1. Explicit Configuration, Managed SQL & Transactions

`pi.Build` never reads implicit global state or opens network sockets during construction. Connections activate in `Start` and close in reverse order during `Stop`:

```go
cfg, err := config.LoadSnapshot("configs", os.Environ())
if err != nil {
	return err
}

app, err := pi.Build(
	pi.WithConfig(cfg.Values()),
	pi.WithManagedSQL(),
)
if err != nil {
	return err
}
```

Inside repositories or services, `BeginTxContext` returns a Pi `Tx` (`Executor`) with ownership tracking (`Owns`) and dialect-aware unique violation detection (`IsUniqueViolation`).

### 2. Route Groups, Middleware & OpenAPI Security

Group routes cleanly while inheriting middleware, OpenAPI tags, and security schemes:

```go
v1 := app.Group("/api/v1").WithDoc(pi.DocTags("Users"))
_ = pi.Register(v1, userHandler.Login)

authed := v1.Group("/users").
	UseMiddleware(requireBearerMiddleware).
	WithDoc(pi.DocSecurity("Bearer"))
_ = pi.Register(authed, userHandler.GetProfile)
_ = pi.Register(authed, userHandler.UpdateProfile)

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
	return relayOutbox(ctx.Context)
})

// Overlap-protected Cron job drained gracefully before resources close
err = app.AddCron("0 */5 * * * *", "cleanup-expired-sessions", func(ctx *pi.Context) error {
	return cleanupSessions(ctx.Context)
}, pi.CronJobOptions{
	Overlap:  pi.CronSkipIfRunning,
	Location: time.UTC,
})
```

### 4. Protocol-Agnostic Errors & Principal Authorization

Define domain errors once with `pkg/pi/apperror` and enforce authorization in your service layer with `pkg/pi/auth`:

```go
var ErrEmailTaken = apperror.New(
	apperror.KindConflict,
	"USER_EMAIL_TAKEN",
	"email address is already registered",
)

// Transport-neutral authorization check (self or explicit permission)
if err := auth.AuthorizeSubject(ctx, targetUserID, "users:write:any"); err != nil {
	return err
}
```

`apperror.Error` maps automatically to HTTP status envelopes, `grpc.MapError` status codes, and `cmd.MapError` process exit codes without leaking internal causes on 5xx errors.

### 5. Portless Testing with `testkit`

Exercise full routing, middleware, binding, validation, and lifecycle cleanup in unit/integration tests without opening TCP ports:

```go
func TestGreet(t *testing.T) {
	app := testkit.New(t, pi.WithExplicitHTTPStatus())
	if err := pi.Register(app, Greet); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/greetings/world", nil)
	rec := testkit.Request(t, app, req)
	require.Equal(t, http.StatusOK, rec.Code)
}
```

---

## Datasources & Adapter Submodules

Pi keeps core SQL, Redis, and standard messaging support in the root module while isolating heavy third-party drivers in independent Go submodules so your `go.mod` stays lean.

| Category | Built-in (`github.com/sllt/pi`) | Independent Adapter Submodules (`@v0.5.0`) |
| --- | --- | --- |
| **Relational SQL** | MySQL, PostgreSQL, SQLite | `oracle`, `dbresolver` |
| **Key-Value / Cache** | Redis | `kv-store/badger`, `kv-store/dynamodb`, `kv-store/nats` |
| **Document / NoSQL** | — | `mongo`, `couchbase`, `arangodb`, `surrealdb` |
| **Columnar / Wide-Column** | — | `clickhouse`, `cassandra`, `scylladb` |
| **Time-Series** | — | `influxdb`, `opentsdb` |
| **Search & Graph** | — | `elasticsearch`, `solr`, `dgraph` |
| **Pub/Sub Messaging** | Kafka, MQTT, Google Pub/Sub | `pubsub/nats`, `pubsub/sqs`, `pubsub/eventhub` |
| **File Storage** | Local Filesystem | `file/s3`, `file/gcs`, `file/azure`, `file/sftp`, `file/ftp` |

Install any independent adapter submodule with matching version tags:

```bash
go get github.com/sllt/pi/pkg/pi/datasource/mongo@v0.5.0
go get github.com/sllt/pi/pkg/pi/datasource/pubsub/nats@v0.5.0
go get github.com/sllt/pi/pkg/pi/datasource/file/s3@v0.5.0
```

---

## Repository Structure

```text
pi/
├── cmd/pi/                  # Pi CLI (init, create, wrap, migrate)
├── pkg/pi/                  # Core runtime, typed HTTP router, supervision & lifecycle
│   ├── apperror/            # Protocol-agnostic business error model
│   ├── auth/                # Verified Principal propagation & authorization helpers
│   ├── cli/                 # CLI subcommand & template generators
│   ├── config/              # Deterministic env/config snapshot loader & redaction
│   ├── datasource/          # Built-in SQL/Redis/PubSub + 25 independent adapter modules
│   ├── grpc/                # gRPC interceptors, health & error mapping
│   ├── http/                # Router, middleware, responder & contract compiler
│   ├── migration/           # Lock-protected Migration v2 engine
│   ├── openapi/             # OpenAPI 3.0.3 AST, JSON/YAML serializer & Scalar UI helper
│   ├── service/             # Circuit-breaker, retry & rate-limited HTTP client
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
