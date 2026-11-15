# Pi

[![Go Reference](https://pkg.go.dev/badge/github.com/sllt/pi.svg)](https://pkg.go.dev/github.com/sllt/pi)
[![Go Version](https://img.shields.io/badge/Go-1.27+-00ADD8?logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](./LICENSE)

[English](./README.md) | 简体中文

**Pi** 是一个面向生产环境的 Go 微服务与 Web 应用框架，围绕显式资源生命周期（`pi.Build`）、基于 Actor 的受监督后台运行时、热路径零反射调用的泛型类型化 HTTP 契约，以及内置 OpenAPI 3.0.3 与离线 Scalar API 文档界面构建。

配合官方 Fx 脚手架 [pi-layout](https://github.com/sllt/pi-layout) 与 `pi` CLI，可开箱生成结构清晰、可重复验证的多协议 Go 业务服务。

---

## 核心特性

- **显式构造与生命周期管理（`pi.Build`）** — 构造期纯内存无隐式副作用，不自动读取全局环境或抢占端口；通过 `Owned` / `Borrowed`（`WithManagedSQL`、`WithResource`）显式声明资源归属，启动按序激活、停机逆序排空释放，并提供免监听端口的集成测试工具包 `pkg/pi/testkit`。
- **泛型类型化路由与装配期预检** — 通过 `pi.Register[Req, Res]` 与 `pi.Meta` 结构体标签统一声明 `path`、`query`、`header`、`json` 四源输入与校验规则，请求热路径零 `reflect.Value.Call`。路由冲突、非法校验参数与不兼容的分组挂载在启动前即由 `app.RouteError()` 拦截。
- **零外部工具 OpenAPI 3.0.3 与内置离线 Scalar UI** — 无需手写 `swag` 注释或安装额外生成器，直接从真实注册路由导出或在线提供确定性的 OpenAPI 3.0.3 JSON/YAML，并内置 100% 离线预压缩的 Scalar API Reference 文档页（默认 `/.well-known/swagger`）。
- **Tao Actor 受监督运行时** — 基于 `github.com/sllt/tao/actor` 隔离监督树，统一托管后台 Worker（`AddWorker` 支持 `SuperviseFatal` / `SuperviseRestart` / `SuperviseDegrade`）、防重入 Cron 定时调度（`AddCron`）、带退避重连与 Ack/Nack 能力识别的 Pub/Sub 订阅器，以及服务端/客户端 WebSocket 会话。
- **统一业务契约与安全边界** — `pkg/pi/apperror` 提供协议无关的业务错误模型，统一映射 HTTP 状态包络、gRPC 状态码与 CLI 退出码，自动对未知 5xx 错误脱敏；`pkg/pi/auth` 在跨协议边界安全传递已验证 `Principal` 主体；默认启用严格 CORS、成对 TLS 校验与请求体大小上限。
- **可观测性与健康探针** — 实例级隔离的 OpenTelemetry 链路追踪、Prometheus 指标、敏感配置自动脱敏的结构化日志、远程动态日志级别，以及内置 `/livez` 与 `/readyz` 探针。
- **可重复构建的 CLI 与代码生成** — `cmd/pi` 支持脚手架初始化（`pi init`）、分层模块骨架生成（`pi create`）、Protobuf gRPC 服务端包装生成（`pi wrap grpc`）与数据库迁移管理（`pi migrate`）。

---

## 安装

### 安装框架主模块

```bash
go get github.com/sllt/pi@v0.5.0
```

### 安装 Pi CLI

```bash
go install github.com/sllt/pi/cmd/pi@v0.5.0
```

基于 [pi-layout](https://github.com/sllt/pi-layout) 初始化并自动编译验证一个新项目：

```bash
pi init --module example.com/acme/orders ./orders
```

---

## 快速开始

使用 `pi.Meta` 定义强类型请求/响应 DTO，通过 `pi.Register` 注册处理函数，并一键启用 OpenAPI 3.0.3 与离线 Scalar 文档页面：

```go
package main

import (
	"log"

	"github.com/sllt/pi/pkg/pi"
)

type GreetRequest struct {
	pi.Meta `path:"/greetings/{name}" method:"GET" tags:"问候接口" summary:"向指定用户发送问候"`
	Name    string `path:"name" validate:"required" dc:"目标用户名"`
	Title   string `query:"title" default:"friend" dc:"可选称谓"`
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

启动服务后即可访问内置端点：

- `GET http://127.0.0.1:8000/greetings/alice?title=captain` — 类型化业务接口
- `GET http://127.0.0.1:8000/.well-known/openapi.json` — 自动生成的 OpenAPI 3.0.3 JSON（同时提供 `openapi.yaml`）
- `GET http://127.0.0.1:8000/.well-known/swagger` — 内置离线 Scalar API 文档界面
- `GET http://127.0.0.1:8000/livez` 与 `/readyz` — 存活与就绪健康探针

---

## 核心能力速览

### 1. 显式配置快照、受管 SQL 与事务

`pi.Build` 在构造阶段不连接外部基础设施，仅分配稳定句柄；在 `Start` 中按声明顺序激活连接，并在 `Stop` 中等待在途任务排空后逆序关闭：

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

业务仓储层通过 `BeginTxContext` 获取带归属校验（`Owns`）的 `Tx` / `Executor`，并可使用 `IsUniqueViolation` 进行跨驱动唯一约束冲突分类。

### 2. 路由分组、中间件与 OpenAPI 安全声明

分组可统一挂载中间件，并向组内路由继承 OpenAPI 标签与鉴权要求：

```go
v1 := app.Group("/api/v1").WithDoc(pi.DocTags("用户模块"))
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

在离线构建或 CI 检查中，无需启动服务即可直接调用 `app.OpenAPIJSON()` / `app.OpenAPIYAML()` 导出规范文件。

### 3. 受监督后台 Worker 与防重入 Cron 调度

通过 Actor 监督树管理长期后台任务与定时作业，内置指数退避预算与优雅停机排空：

```go
// 支持指数退避重启与窗口熔断预算的后台 Worker
err := app.AddWorker("outbox-relay", pi.WorkerPolicy{
	Mode:          pi.SuperviseRestart,
	Required:      true,
	MaxRestarts:   5,
	RestartWindow: time.Minute,
}, func(ctx *pi.Context) error {
	return relayOutbox(ctx.Context)
})

// 防重入 Cron 定时任务，停机时等待在途执行结束后再关闭底层数据源
err = app.AddCron("0 */5 * * * *", "cleanup-expired-sessions", func(ctx *pi.Context) error {
	return cleanupSessions(ctx.Context)
}, pi.CronJobOptions{
	Overlap:  pi.CronSkipIfRunning,
	Location: time.UTC,
})
```

### 4. 协议无关业务错误与主体授权

使用 `pkg/pi/apperror` 定义领域错误，使用 `pkg/pi/auth` 在业务层统一校验主体身份与权限：

```go
var ErrEmailTaken = apperror.New(
	apperror.KindConflict,
	"USER_EMAIL_TAKEN",
	"该邮箱已被注册",
)

// 业务层执行本人校验或指定管理权限放行
if err := auth.AuthorizeSubject(ctx, targetUserID, "users:write:any"); err != nil {
	return err
}
```

同一错误可自动映射为 HTTP JSON 错误响应、`grpc.MapError` 状态码与 `cmd.MapError` 退出码，内部原因链保留在服务端日志而不对外泄露。

### 5. 使用 `testkit` 进行无端口集成测试

无需绑定本地端口或等待异步启动，即可完整测试路由编译、参数绑定、数据校验、中间件及资源清理：

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

## 数据源与独立适配器子模块

常用关系型数据库、Redis 与主流消息队列内置在主模块中；其余重量级第三方驱动拆分为 25 个独立 Go 子模块，按需引入，保持业务项目的 `go.mod` 精简干净。

| 类别 | 主模块内置（`github.com/sllt/pi`） | 独立适配器子模块（`@v0.5.0`） |
| --- | --- | --- |
| **关系型数据库** | MySQL, PostgreSQL, SQLite | `oracle`, `dbresolver` |
| **键值存储 / 缓存** | Redis | `kv-store/badger`, `kv-store/dynamodb`, `kv-store/nats` |
| **文档 / NoSQL** | — | `mongo`, `couchbase`, `arangodb`, `surrealdb` |
| **列式 / 宽表存储** | — | `clickhouse`, `cassandra`, `scylladb` |
| **时序数据库** | — | `influxdb`, `opentsdb` |
| **搜索与图数据库** | — | `elasticsearch`, `solr`, `dgraph` |
| **发布 / 订阅消息** | Kafka, MQTT, Google Pub/Sub | `pubsub/nats`, `pubsub/sqs`, `pubsub/eventhub` |
| **文件与对象存储** | 本地文件系统 | `file/s3`, `file/gcs`, `file/azure`, `file/sftp`, `file/ftp` |

按统一版本号按需安装独立子模块：

```bash
go get github.com/sllt/pi/pkg/pi/datasource/mongo@v0.5.0
go get github.com/sllt/pi/pkg/pi/datasource/pubsub/nats@v0.5.0
go get github.com/sllt/pi/pkg/pi/datasource/file/s3@v0.5.0
```

---

## 目录结构

```text
pi/
├── cmd/pi/                  # Pi CLI 工具（init / create / wrap / migrate）
├── pkg/pi/                  # 核心运行时、泛型类型化路由、监督树与生命周期管理
│   ├── apperror/            # 协议无关的业务错误模型
│   ├── auth/                # 已验证 Principal 传递与业务层授权辅助函数
│   ├── cli/                 # CLI 子命令与代码模板生成器
│   ├── config/              # 确定性配置快照加载与敏感字段脱敏
│   ├── datasource/          # 内置 SQL/Redis/PubSub 及 25 个独立数据源子模块
│   ├── grpc/                # gRPC 拦截器、隔离健康检查与错误映射
│   ├── http/                # 路由器、中间件、响应器与请求契约编译器
│   ├── migration/           # 带分布式锁与状态检查的 Migration v2 引擎
│   ├── openapi/             # OpenAPI 3.0.3 AST、JSON/YAML 序列化与 Scalar UI 支撑
│   ├── service/             # 带熔断、重试与限流的出站 HTTP 客户端
│   ├── testkit/             # 免端口测试底座与生命周期探针
│   └── websocket/           # 受监督的 WebSocket 连接与会话管理
└── examples/                # 可运行的 HTTP、gRPC、Cron、PubSub 与迁移示例
```

---

## 开发与验证命令

```bash
make build          # 以 -mod=readonly 编译根模块全部包
make unit           # 运行单元测试
make race           # 开启 Go 竞态检测器（-race）运行测试
make modules        # 逐个编译验证 25 个独立数据源子模块与 OpenAPI 校验模块
make openapi-verify # 使用 kin-openapi 对生成的 OpenAPI 3.0.3 规范执行合规校验
```

---

## 许可证

本项目基于 [Apache License 2.0](./LICENSE) 开源。
