# 框架运行时（v0.4.1 源码基线）

## 定位

| 关注点 | 路径 |
| --- | --- |
| 构造、所有权、配置 | `pkg/pi/build.go`、`factory.go`、`config/snapshot.go`、`infra/isolated.go` |
| 生命周期 | `run.go`、`lifecycle.go`、`pi.go` 的 Stop/shutdown |
| HTTP | `handler.go`、`http/request.go`、`http/responder.go`、`http/response/result.go`、`route_registry.go` |
| 错误/身份/gRPC | `apperror/`、`auth/`、`grpc/`、`grpc.go`、`cli/wrap/` |
| 稳定 SQL 与测试入口 | `datasource/sql/{handle,open,db,errors}.go`、`testing.go`、`testkit/` |

路径均相对 `pkg/pi/`（表中已写完整前缀的除外）。

## 构造与资源

```go
cfg, err := config.LoadSnapshot("configs", os.Environ())
if err != nil { return err }
app, err := pi.Build(pi.WithConfig(cfg.Values()), pi.WithManagedSQL())
if err != nil { return err }
```

LoadSnapshot 顺序是 `.env` < `.APP_ENV.env`（默认 `.local.env`）< 传入的环境列表，不修改进程环境。
Build 不隐式读 env，不连接 DB、监听、安装全局 provider 或启动任务；Snapshot.Values/Redacted 返回副本。
无 SQL 的应用省略 WithManagedSQL。该选项提供构造期稳定 Handle，Start 在 Hook 前激活，未启动的查询返回 ErrNotStarted。
WithSQL(db, Borrowed) 不负责关闭，Owned 负责关闭；不同时使用 WithSQL 和 WithManagedSQL。

WithResource 的 Owned 资源按顺序启动、逆序关闭，失败的 Start 也清理，Stop 要容忍部分初始化。
Start=nil 表示已经持有的资源，即使 App 未启动也会关闭；Borrowed 不调用 Start/Stop。
logger/metrics/validator 可分别注入。Build 默认指标 manager 不导出，监听默认关闭；开启需要 WithMetricsHandler。
宿主独立运行的组件若也用某资源，应让宿主拥有它并借给 Pi，或把使用者纳入 Pi.Go；不要让 Pi 提前关掉外部组件仍在使用的 DB。

旧 `New()` 保留 env 修改、自动数据源连接、全局 tracing/metrics 等兼容行为；不是纯构造的别名。
新路径也不代表所有遗留适配器、cron、重连、tracing 已按实例统一监督，这部分仍需查具体实现。

## 启停

- Build.Start(ctx) 的 ctx 是启动预算；成功后取消它不会取消 Worker。New.Start 首个 ctx 仍是 runtime 父 context。
- 并发 Start 共享一次结果；启动失败回滚，停止后不能重启。RunContext 的外部取消负责请求停机。
- Build app 的 Wait(ctx) 等待完整清理并返回 fatal cause + cleanup error；等待超时只影响当前调用者。
- 首个 Stop ctx 提供清理预算，其他 Stop 只等待同一结果。超时不等于后台任务已经结束。
- `App.Go(name, func(*pi.Context) error)` 返回 void。返回 nil/context.Canceled 正常退出，panic/其他错误触发停机；Build 会自动清理并让 Wait 报告失败。这不是自动重启 supervisor。
- Go/OnStart/OnStop 的空、重复、迟到登记可能只记日志，不要把“无 error 返回”当成登记成功。
- Run 安装 signal handler；Fx/命令宿主已处理 signal 时用 Start/Stop/RunContext，避免重复接管。

## 监听与路由

`HTTP_ADDR` / `GRPC_ADDR` / `METRICS_ADDR` 为 host:port，优先于 HOST/PORT；`127.0.0.1:0`、`[::1]:0` 表示随机端口。
启动后用 HTTPAddress/GRPCAddress/MetricsAddress 获取实际地址。ENABLED=false 禁用；旧 HTTP/GRPC PORT=0 回落默认端口，旧 METRICS_PORT=0 禁用。
Build 默认显式禁用指标监听。HTTP/gRPC 仍需注册路由/服务才启动，端口占用由真实 Listen 返回错误。
HTTP CERT_FILE/KEY_FILE、gRPC GRPC_CERT_FILE/GRPC_KEY_FILE 必须配套且可解析，错误不回落明文。
CORS 由框架统一处理：默认不授权跨域，allowlist 用完整 origin，禁止 wildcard + credentials。

`Use` 接收 net/http middleware，`UseMiddleware` 接收 PiMiddleware；路由变量用 `/{id}`，读取用 ctx.PathParam。
首次 HTTPHandler/Start 前完成全部注册；registry 不是动态路由系统。

## HTTP 所有权与错误

Handler 使用独立的 body/header/路由参数快照；结果通过缓冲通道传递，迟到完成不再写响应。
multipart 临时文件在 Handler 与响应都结束后清理。Build 默认 1 MiB 正文（HTTP_MAX_BODY_BYTES 可调），New 默认 32 MiB。
超时不强杀忽略 context 的业务，也不撤销已提交的副作用；自定义 writer 的读期限能力需核对。

普通 JSON envelope 为 code/data/message，可有 meta/details。apperror 的 Kind/Code/PublicMessage 与 Cause 分开，Details 只放公开字段规则。
HTTP 支持包装/Join 与旧 Code/StatusCode，取消优先，其他错误按确定顺序选同一分支；未知错误通用 500，内部日志用 X-Request-ID 关联。
不要把 err.Error() 拼进 PublicMessage；gRPC/CLI 用各自 MapError。

`response.OK/Created/Accepted/NoContent` 为 200/201/202/204；WithExplicitHTTPStatus 让其他成功结果默认 200，未启用时保留旧 method/data 推断。
204 不编码正文；error 优先于任何 data/特殊响应；Result 的显式状态优先于内层特殊类型。
Stream 在 Handler 返回后同步执行，传入客户端 context；它不沿用普通 Handler 响应超时，长流自行设置时限并响应取消。
提交后失败只记日志，不再追加 JSON。Handled 只给已接管输出的可信适配器；已写响应的 net/http middleware 应直接返回。

## 测试与 gRPC

`testkit.New(t, opts...)` Build 并自动 Stop；`testkit.Request(t, app, req)` 走完整路由，`testkit.Handler(app, fn, req)` 走单 Handler；NewProbe 通过 channel 观察 Hook。
`app.HTTPHandler()` 返回 `(http.Handler,error)`，不监听；依赖 SQL 的 Handler 仍需 Start 或借用已激活 DB。
`app.NewContext(ctx)` 不授予身份，可信测试通过 auth.WithPrincipal 明确构造调用者。

gRPC Principal unary/stream interceptor 仅验证凭证，业务仍须授权。Register/Login 等公开方法按完整方法名显式放行，stream 默认要求身份。
wrapper 的 Bind 仅接受同类型、非 nil protobuf 指针，使用 protobuf clone 语义；不要绑定任意业务结构体。
health 状态按 App 分配；它不是完整 readiness/supervisor。wrapper 修改同时核对生成模板与产物。

订阅 error/panic/已观察取消不自动 Commit，但 broker 的自动 ack/offset/checkpoint 差异仍存在；不承诺 exactly-once 或所有后台循环均已统一等待。
