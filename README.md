# Pi

### 业务契约（v0.4.1）

- `apperror.New(kind, code, publicMessage)` 定义业务错误，`WithCause` 保留内部错误链，`WithDetails` 只携带公开的字段/规则；未知错误统一脱敏。HTTP、`grpc.MapError`、`cmd.MapError` 分别映射协议状态和退出码，取消优先于其他错误，HTTP 兼容旧 Code/StatusCode 接口。
- `auth.WithPrincipal` 仅用于已验证的 transport 或可信任务，不能从请求 userId 直接建立身份。`auth.Require` / `AuthorizeSubject` 让业务层执行本人或显式权限判断，权限切片跨 context 边界复制。
- `response.OK/Created/Accepted/NoContent` 显式选择 200/201/202/204。`WithExplicitHTTPStatus()` 让其余成功结果默认 200；未选用时保留旧 method/data 推断。所有 204 都不写正文；error 优先于 data 和特殊响应。`response.Stream` 在 Handler 返回后同步写入，只继承客户端取消，长流自行设定时限；已提交后的错误只记录，不补 JSON。`response.Handled` 仅供明确接管响应的适配器。
- HTTP Handler 独占请求体、header 和路由参数快照；超时后业务仍可能继续，迟到结果不会重新写响应。multipart 临时文件等到 Handler 和响应均结束后清理。Build 默认正文上限 1 MiB（`HTTP_MAX_BODY_BYTES` 可调），旧 New 上限 32 MiB。请求超时也约束读取正文；自定义 writer 需支持 ResponseController 的 deadline 或由宿主设置读超时。
- SQL `BeginTxContext(ctx, opts)` 返回 Pi Tx/Executor 并保留所属 DB；嵌入的标准库 `BeginTx` 仍返回 `*database/sql.Tx`。`DB.Owns` / `Handle.Owns` 检查归属，`IsUniqueViolation` 分类唯一约束错误。SQL 日志不记录参数值。
- gRPC wrapper 使用完整服务名、按 App 隔离的 health、安全错误映射与 protobuf 克隆绑定。`Bind` 目标必须是同类型 protobuf 指针，业务 DTO 由手写 adapter 显式转换；重新生成时保留手写 server。`NewPrincipalUnaryInterceptor` / `NewPrincipalStreamInterceptor` 只从已验证 Bearer 建立身份，授权继续放在 service。

框架与 pi-layout 的业务事务、跨入口授权示例以 SQLite 实测为基线；HTTP/gRPC 仍由应用选择注册。
`make generator-integration` 使用固定 protoc/插件编译双 service 的 unary、server/client streaming、bidi 生成物。
本地候选生成测试使用临时 replace；发布消费另外以无 replace 的实际 tag 验证。

### 显式构造与宿主集成（v0.4.0）

```go
cfg, err := config.LoadSnapshot("configs", os.Environ()) // .env < 环境覆盖文件 < 显式环境
if err != nil { return err }
app, err := pi.Build(pi.WithConfig(cfg.Values()), pi.WithManagedSQL())
if err != nil { return err }
// 注册路由、OnStart/OnStop、Go workers，然后由宿主调用 Start/Stop。
```

`Build` 不读取隐式环境，不连接数据库、监听端口、安装全局 telemetry provider 或启动后台任务；
配置快照与默认校验器按 App 隔离，`Snapshot.Redacted()` 可用于脱敏输出。`WithLogger`、`WithMetrics`、
`WithValidator` 支持注入；指标监听默认关闭，启用时须提供 `WithMetricsHandler`，由应用显式管理 exporter。
`WithManagedSQL` 在构造时提供稳定的 `Container().SQL` 句柄，`Start` 在应用 Hook 前激活连接；
构造后的查询返回 `sql.ErrNotStarted`。不需要数据库时省略该选项。

外部 SQL 用 `WithSQL(db, pi.Borrowed)` 或 `pi.Owned` 声明关闭责任。其他资源用 `WithResource`：
Owned 按登记顺序启动、逆序关闭，失败的 Start 也执行 Stop；Borrowed 不执行启动/关闭 Hook。
仅已成功进入启动的资源与已经持有的 Owned 资源参与清理。构造参数不能在 Start 后修改。

新入口的 `Start(ctx)` 仅把 ctx 用作启动预算；运行期由 `Stop`、`RunContext` 的外部取消或 fatal Worker
错误结束。`Wait(ctx)` 等待完整清理并返回运行错误和清理错误，等待者超时不取消资源清理。
单一启动/关闭、失败回滚、停止后不重启语义保持不变。旧 `New()` 保留环境加载、自动连接和旧 Start
父 context 契约；迁移到 Build 时必须显式选用资源。完整观测 provider 管理与后台重连监督仍是后续范围。

`app.HTTPHandler()` 编译完整路由但不监听端口；`app.Handler(fn)` 包装单个 Handler；`app.NewContext(ctx)`
提供应用依赖但不授予身份。`pkg/pi/testkit` 提供自动清理、HTTP 请求与生命周期 Probe。
路由注册应在首次 HTTPHandler/Start 前完成。示例见 `examples/build`；Fx 与一次性迁移的参考实现分别
位于 pi-layout 的 `internal/bootstrap` 和 `internal/migrationcmd`。

### Reproducible projects (v0.3.2)

Install a specific CLI (`go install github.com/sllt/pi/cmd/pi@v0.4.1`), then run
`pi init --module example.com/company/orders ./orders`. The default template ref
matches the CLI version. `--ref` selects another tag/commit explicitly. Generated
`.pi-template.json` records the CLI, framework, template ref/commit and verification
result. Imports, proto options and serialized protobuf metadata use your module.
Initialization builds with `-mod=readonly` in a temporary directory before delivery.
`--offline --template /path/to/local/git-template` skips build verification and
records `verified: false`; it never reports a verified application.

Generators render/format before writing. Existing handwritten skeletons are
preserved; wrapper regeneration replaces only files marked as generated. An init
directory is delivered by rename; multi-file regeneration is not a filesystem
transaction, so an uncatchable SIGKILL may require rerunning the generator.
Use `make unit`, `make race`, `make generator` and `make modules` for separate
checks. `make integration` enables migration real-backend checks when their
`PI_MIGRATION_TEST_*` variables point to disposable databases. Independent adapter
module builds are separate from the root module and do not certify backend behavior.

一个为微服务开发而设计的 Go 语言框架。

项目已由 Kite 更名为 Pi，CLI 命令为 `pi`，脚手架为 [pi-layout](https://github.com/sllt/pi-layout)。新 module 从 v0.2.4 开始发布；v0.3.0 提供 Migration v2，见[发布说明](docs/releases/v0.3.0.zh-CN.md)和[迁移兼容性](docs/design/migration-v2.zh-CN.md)。历史 `v0.2.3` 仍使用旧路径。

## 核心特性

### v0.3.1 配置与错误响应

- `HTTP_ADDR`、`GRPC_ADDR`、`METRICS_ADDR` 接受 `host:port`，优先于对应 `*_HOST`/`*_PORT`；`[::1]:0` 支持 IPv6 和随机端口。启动后通过 `HTTPAddress()`、`GRPCAddress()`、`MetricsAddress()` 获取实际地址。
- `*_ENABLED=false` 显式禁用；旧 HTTP/GRPC `PORT=0` 仍回落默认端口，旧 `METRICS_PORT=0` 仍禁用。端口冲突由 Start 返回，不在注册/构造阶段 Fatal。
- HTTP 使用 `CERT_FILE`/`KEY_FILE`，gRPC 使用 `GRPC_CERT_FILE`/`GRPC_KEY_FILE`；必须成对、可解析且匹配，错误导致启动失败。
- CORS 由框架统一处理，默认不允许跨域。`CORS_ALLOWED_ORIGINS` 是逗号分隔的完整 origin；另有 `CORS_ALLOWED_METHODS`、`CORS_ALLOWED_HEADERS`、`CORS_ALLOW_CREDENTIALS`、`CORS_EXPOSE_HEADERS`、`CORS_MAX_AGE`。新键优先于旧 `ACCESS_CONTROL_*` 键；通配 origin 不能同时允许 credentials。
- HTTP 错误支持包装/Join。取消优先，其余按深度优先、从左到右选第一个状态错误，业务码与消息来自同一错误。5xx/未知错误默认隐藏内部消息；实现 `PublicMessage() string` 可显式提供安全消息。Handler 响应的 `X-Request-ID` 对应服务端错误日志中的 trace_id。

安全兼容变化：依赖默认 `*` 的跨域客户端需要配置来源；未知错误不再把原始 Error 文本发给客户端，错误响应也不会返回附带的成功数据/文件。

- **简洁的 API 语法** - 轻松定义路由和处理器
- **RESTful 规范** - 默认遵循 REST 最佳实践
- **配置管理** - 灵活的配置加载和管理
- **完整的可观测性** - 内置日志、追踪和指标支持
- **认证中间件** - 开箱即用的认证和自定义中间件
- **gRPC 支持** - 原生支持 gRPC 服务
- **HTTP 服务客户端** - 内置熔断器的 HTTP 客户端
- **发布/订阅** - 简化的消息队列集成
- **健康检查** - 所有数据源的自动健康检查
- **数据库迁移** - 内置迁移管理工具
- **定时任务** - Cron 任务调度支持
- **动态日志级别** - 无需重启即可更改日志级别
- **Swagger 文档** - 自动生成和渲染 API 文档
- **文件系统抽象** - 统一的文件操作接口
- **WebSocket** - 原生 WebSocket 支持

## 快速开始

### 安装

```bash
go get github.com/sllt/pi
```

### 简单示例

```go
package main

import "github.com/sllt/pi/pkg/pi"

func main() {
    app := pi.New()

    app.GET("/greet", func(ctx *pi.Context) (any, error) {
        return "Hello World!", nil
    })

    app.Run() // 监听 localhost:8000
}
```

运行应用：

```bash
go run main.go
```

访问 `http://localhost:8000/greet` 查看结果。

### 使用数据库

```go
package main

import (
    "fmt"
    "github.com/sllt/pi/pkg/pi"
)

func main() {
    app := pi.New()

    app.GET("/redis", func(c *pi.Context) (any, error) {
        val, err := c.Redis.Get(c, "key").Result()
        if err != nil {
            return nil, err
        }
        return val, nil
    })

    app.GET("/sql", func(c *pi.Context) (any, error) {
        var result int
        err := c.SQL.QueryRowContext(c, "SELECT 2+2").Scan(&result)
        if err != nil {
            return nil, err
        }
        return result, nil
    })

    app.Run()
}
```

## 支持的数据源

Pi 支持广泛的数据存储和服务：

| 类别 | 数据源 |
|------|--------|
| **关系型数据库** | MySQL, PostgreSQL, SQLite, Oracle |
| **NoSQL 数据库** | MongoDB, CouchBase, ArangoDB, SurrealDB |
| **键值存储** | Redis, KV-Store |
| **时序数据库** | InfluxDB, OpenTSDB |
| **搜索引擎** | Elasticsearch, Solr |
| **列式存储** | Cassandra, ScyllaDB, ClickHouse |
| **图数据库** | Dgraph |
| **消息队列** | PubSub (Kafka, Google PubSub 等) |
| **文件系统** | 本地文件系统、S3、GCS 等抽象文件系统 |
| **数据库路由** | DBResolver (多数据源管理) |

## 项目结构

```
pi/
├── pkg/pi/              # 核心框架代码
│   ├── datasource/        # 数据源连接器
│   ├── metrics/           # 指标收集
│   └── ...
├── examples/              # 示例应用
│   ├── http-server/       # HTTP 服务示例
│   ├── grpc/              # gRPC 示例
│   ├── using-migrations/  # 数据库迁移示例
│   └── ...
└── docs/                  # 文档
```

## 文档

- [GoDoc](https://pkg.go.dev/github.com/sllt/pi) - API 参考文档
- [示例目录](examples/) - 更多可运行示例

## 许可证

本项目采用 [Apache License 2.0](LICENSE) 许可证。

## 贡献

欢迎贡献代码、提出建议或报告问题。
