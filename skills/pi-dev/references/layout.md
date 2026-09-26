# pi-layout 与生成业务应用（v0.4.1）

## 装配

- `internal/bootstrap`: NewPiApp 读快照并 Build/ManagedSQL，NewDB 返回稳定句柄；Fx Hook 激活/停止 Pi，Wait 把运行失败传给宿主。
- `cmd/server`: 入口持有 signal context，Fx 构造/Start，Pi RunContext 等待，最后 Fx Stop。
- `cmd/task`: Fx 管理进程，gocron scheduler 登记在 Pi.Go 内；job 使用 runtime ctx，退出顺序保证先停 job 再关 SQL。调度表达式仍在示例中硬编码。
- `cmd/migration` / `internal/migrationcmd`: 不用 Fx；共享 Build/Owned SQL 契约并禁用监听，stdout 摘要见 [migrations.md](migrations.md)。
- `internal/server/{http,grpc}.go`: 协议装配，router 使用 fx.In 的 RouterDeps 注入 handler/service。

构造不激活 DB；不要在 repository provider 另开 NewSQL 或重复 Close。跨宿主资源声明清楚所有权。
若 Pi 之外的 Fx 组件也需要同一 SQL，替换默认 CoreModule 的 DB 装配：宿主 provider 创建
`sql.NewHandle(cfg, logger, metrics)` 并登记 `OnStart: handle.Start`、`OnStop: handle.Close` 的适配回调；
Pi provider 依赖该句柄并使用 `pi.WithSQL(db, pi.Borrowed)`，不再同时使用 `WithManagedSQL`。
其他 SQL 使用者也依赖这个 provider，使 DB Hook 先登记、先启动、最后关闭。
这样 Pi fatal 后不会提前关闭其他 Fx 组件仍在使用的借用连接；宿主统一 Stop 才释放它。
保留项目当前分层；不自动改成 monorepo 或强制重排目录。

## 增加模块

沿迁移/registry → model/types → repository → service → handler/adapter → Fx provider → router/server → 关键测试补齐闭环。
不是每次改动都要创建所有文件，`pi create all` 也不会自动完成 DTO、迁移、路由、Fx 注册和测试。
Go imports 使用实际应用 module；不要把官方 layout module 写进生成应用。
业务 service 接受标准 context 与 internal/types，不接受 HTTP DTO、protobuf 或 *pi.Context。

types.Validate 在 service 调用，transport 负责解码/大小限制。当前示例：邮箱校验、密码 8–72 字节、昵称最多 64 字符/256 字节。
不要把密码内容放进公开错误、Details 或日志；公开字段规则与内部 cause 分开。

## 身份、授权与协议

JWT 默认 Bearer-only，不读 cookie/query；密钥至少 32 字节且拒绝占位值，TTL 默认 1h，校验 HS256、issuer/audience、exp/iat、subject 与 userId。
`jwt.NewJwt(app)` / `jwt.New(config)` 都返回 `(*JWT,error)`；Issue 按配置 TTL 签发，旧缺 claims 的 token 要重新登录。
NoStrictAuth 让匿名访问公开路由，保护路由再强制身份；它不等于所有请求都已认证。

HTTP middleware 与 gRPC verifier 把已验证身份转为 auth.Principal；旧 ClaimsKey/AuthInfo 不是业务授权的替代品。
GetProfile/UpdateProfile 在 service 调用 auth.AuthorizeSubject；目标为空表示本人，访问他人分别需要 users:read:any / users:write:any。
用于订单等对象授权时，目标应是查询得到的属主 ID，而不是对象 ID；先拒绝空属主，避免空目标的“本人”语义放行。
HTTP 本人资料从 Principal 取主体，跨用户 query 拒绝；管理用例放在独立、明确授权的入口。
模板 JWT 不颁发管理权限；可信任务可显式 WithPrincipal 授权，绝不能直接复制请求 userId/permissions。

默认不注册用户 gRPC。需 GRPC_ENABLED=true 与 USER_GRPC_ENABLED=true；只设端口无效。
用户 RPC 当前为 unary，Register/Login 公开，其余使用 Bearer；框架的 stream Principal interceptor 可复用同一业务授权。
新增多个 gRPC 服务时，集中配置一次 Principal interceptor 并合并公开方法名单；分别叠加不同名单会互相拦截公开 RPC。
`userservice_server.go` 是手写 protobuf ↔ types adapter。ctx.Bind 的目标是对应 protobuf 指针，不能传业务 DTO。
wrapper 是生成层，先改模板再生成，编译/鉴权测试通过不等于自动启用部署中的 RPC。

`pkg/errcode.Error` 是 apperror.Error 别名，使用 Kind/Code/PublicMessage/WithCause/Details；不再依赖旧 BizCode/Message 字段。
邮箱冲突 HTTP 409 / gRPC AlreadyExists，业务码 1001；未知错误脱敏，gRPC 携带 ErrorInfo/字段违规详情。
成功状态：注册 201、登录/读取 200、修改 204 且无正文。Handler 用 response.Created/OK/NoContent，客户端不要解析 204 JSON。

## Repository 与事务

```go
err := s.tm.Transaction(ctx, func(txCtx context.Context) error {
    if err := s.orderRepo.Create(txCtx, order); err != nil { return err }
    return s.auditRepo.Create(txCtx, audit)
})
```

GetQuerier(ctx) 返回当前事务的 Pi Executor 或基础 DB；所有操作继续使用回调 ctx。
业务事务要求 DB 的可选 BeginTxContext 能力；infra.DB 基础接口未增加必选方法，不支持该能力的自定义 adapter 会明确失败。
Repository.TransactionWithOptions 传入 sql.TxOptions；不要误用返回标准 *sql.Tx 的嵌入 BeginTx。
嵌套加入现有事务，不是 savepoint；不能改变选项。嵌套 error/panic 即使被上层捕获，也使外层 rollback-only。
跨 DB 的 Transaction 和 GetQuerier 都拒绝复用并标记外层失败；不能借旧 ctx 在另一库意外提交。
取消/错误/panic 回滚，commit 失败不自动重试未知提交结果；不要只凭调用 rollback 就声称网络故障下已撤销所有副作用。

唯一性依赖表约束，预检查只是友好提示；IsUniqueViolation 识别驱动错误并转业务冲突。
资料调用 UpdateEmail 与 profile.Update，改密单独 UpdatePassword；不要用旧整行 Update 回写过时密码。
RowsAffected=0 在同一 context/事务查存在性，避免把 MySQL 未变化误判为 NotFound。

业务 DDL 与并发验收以 SQLite 为基线，默认单连接；切换 DB 同时核对 DDL、占位符、ID 返回和实库测试。
SQLite 默认未启用外键；单次 PRAGMA 不是连接池全局保证，需要按连接启用并验证。

## 日常验证

本地配置使用 configs/.env.example，实际 .env 不跟踪；不要覆盖已有配置或打印真实 secret。
迁移/启动命令会访问配置的资源，选临时库验证。`make unit`、`race`、`integration`、`generator`、`check-generated`、`smoke`、`container` 分别使用。
业务关键矩阵在 internal/server/contract_test.go，事务在 internal/repository/transaction_test.go，Fx/Handler 另有测试。
Handler 使用公开 testkit，不必监听真实端口；跨协议测试还要覆盖匿名、坏凭证、本人、越权、可信权限和相同输入错误。
已有 Fx 装配出的 app 用 `testkit.Request(t, app, request)` 测完整路由；`testkit.New` 会另建 App，不能替代验证原 Fx 图。
容器使用根 Dockerfile，三个入口 server/task/migration，UID 10001，storage 可写，配置从运行时注入。
