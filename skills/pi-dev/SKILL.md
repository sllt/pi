---
name: pi-dev
description: Develop, review, and debug the github.com/sllt/pi Go framework, the github.com/sllt/pi-layout Fx scaffold, generated Pi applications, and their CLI templates. Use for Pi runtime APIs, business modules, SQL transactions, migrations, workers, auth boundaries, or framework/layout evolution. Includes historical Kite naming; excludes unrelated Pi coding agents and Rust Loco projects.
---

# Pi 框架与脚手架开发

围绕真实调用链完成任务：区分框架能力、脚手架业务约定、生成模板和已发布依赖。
本 skill 的源码基线为 **2026-09-26 / Pi 与 pi-layout v0.4.1**；新任务以目标项目实际使用的源码版本为准。

## 先定位工作对象与有效版本

- 框架：module `github.com/sllt/pi`，核心 `pkg/pi/`，CLI `cmd/pi/`。
- 脚手架：module `github.com/sllt/pi-layout`，依赖装配在 `internal/bootstrap/`，采用 Fx。
- 生成应用：module 名由项目决定；检查 `go.mod` 的 Pi 依赖及本项目入口，不把官方 layout 路径写进它的 imports。
- 历史 Kite：根据对应 tag 的旧 module、`pkg/kite/` 和签名处理；不要在维护旧项目时自动全面改名。

先看目标仓库说明、工作区改动、`go.mod`、`go.work` 与实际入口。下面是定位命令，按任务需要选择：

```sh
git status --short
git log -1 --oneline
go env GOWORK GOMOD
rg -n 'github.com/sllt/(pi|kite)|^go |^toolchain|^replace' go.mod
```

框架 HEAD、工作树、Go workspace/replace、layout require 的 tag 可能不同。
已运行程序还要核对其实际构建来源，避免用新工作树解释旧二进制。
源码未在当前项目内时，先定位实际解析的 module（如 `go list -m -json github.com/sllt/pi`），再查实现。
本 skill 不要求固定的本地仓库位置，也不要求同时拥有两个仓库。

## 按任务读取

| 任务 | 参考 |
| --- | --- |
| App、HTTP/路由、中间件、生命周期、后台任务 | [framework.md](references/framework.md) |
| 业务模块、Fx、DTO/service/repository、身份与事务 | [layout.md](references/layout.md) |
| Migration v2、命令、状态、锁、失败与资源清理 | [migrations.md](references/migrations.md) |
| pi init/create/migrate/wrap、生成文件与模板 | [cli.md](references/cli.md) |
| 框架演进、版本兼容、验证和发布联动 | [development.md](references/development.md) |

只读与当前任务有关的参考。路径默认相对于正在说明的框架或 layout 根目录。

## 决定修改归属

- 生命周期、HTTP/error/route 公共语义、migration runtime、公共 datasource 能力：框架。
- 业务身份/权限、DTO、SQL、业务错误、Fx providers、项目配置和入口：layout 或生成应用。
- 可机械生成的骨架：CLI 模板，并核对生成物；生成器补丁不等于已经更新用户应用。
- 单一业务集成先放业务项目；不要仅因接入微信、K8s 等服务就增加框架核心依赖。

沿一条入口 → 装配 → transport/worker → service → repository/datasource 的链路定位和验证。
对普通业务扩展复用项目现有分层；大型项目目录/monorepo 属于另一个设计决策，不自动重排整个脚手架。

## 容易误用的当前契约

- 新项目使用 `pi.Build(...Option) (*App, error)`；配置通过 `WithConfig(snapshot.Values())` 显式传入，SQL 通过 `WithManagedSQL` 或 `WithSQL(db, Owned/Borrowed)` 选用。旧 `New()` 仍有环境/连接/全局 tracing 副作用。
- `Use` 接收 net/http middleware；`UseMiddleware` 接收 `pi.PiMiddleware`。路由参数为 `/{id}`，读取用 `ctx.PathParam("id")`。
- `Handler`、`App.Go`、OnStart/OnStop 的回调使用 `*pi.Context`；业务 service/repository 使用标准 `context.Context`。
- Build 的 Start ctx 只约束启动，Wait 等待完整清理并报告运行错误；旧 New 的首个 Start ctx 仍是 runtime 父 context。不要混淆两条路径。
- v0.2.4 已修复 HTTP 结果共享竞态、Start/Stop 一次执行、订阅 panic 后自动确认问题；不再把旧缺陷当成当前实现。
- v0.3.0 的 Migration v2 已发布。`Migrate/Run` 返回 `(Result, error)`；框架默认不开迁移锁，layout 的 `up` 默认显式开启。
- 业务迁移执行为 `go run ./cmd/migration up|plan|status`；`pi migrate` 只有 `create`，不是执行器。
- layout migration 不走 Fx，使用 Build/ManagedSQL 并禁用监听器；业务事务调用 BeginTxContext，嵌套失败标记 rollback-only，跨库复用被拒绝。
- Principal 必须来自验证凭证或可信任务。授权、types.Validate 和业务错误位于 service；HTTP、gRPC wrapper 的存在不能代替业务层授权。
- `response.OK/Created/Accepted/NoContent` 明确状态；所有 204 无正文。Stream 同步接管输出，error 优先于 data，超时不撤销业务副作用。
- layout 自带 DDL 为 SQLite。gRPC 默认关闭，`USER_GRPC_ENABLED=true` 加 `GRPC_ENABLED=true` 才注册安全用户示例；模板 JWT 不授予管理权限。
- 测试使用公开 `pkg/pi/testkit`；gRPC Bind 只克隆同类型 protobuf 指针，DTO/types 转换留在手写 adapter。

## 验证与交付

按改动做有意义的定向验证；并发、取消和资源所有权改动覆盖实际失败路径及 race。
迁移使用专用空库/临时实例；生成器先在临时目录验证产物，不覆盖业务文件。
根 module 的检查不覆盖独立 datasource modules；layout 的 unit/race/generator/check-generated/smoke/container 是不同验证层。
本地 workspace 可用于开发联调；发布消费检查另用 `GOWORK=off` 并确认无本地 replace。

说明改动归属、兼容性、已运行验证及实际边界。涉及版本开发时更新项目采用的开发计划和 Roadmap；
不因为 skill 包含发布流程就自动发布版本、推送代码或重写 tag，按用户当前任务及已有授权执行。
用户要求排除过程文档时，用明确路径暂存；计划、审查和验证记录留在本地或仓库外，持久的 API 文档与 skill 按交付范围维护。

本 skill 源文件放在框架仓库 `skills/pi-dev/`，个人技能目录可用链接指向它；更新时只维护这一份正文。
更改版本相关 API/命令后，同步对应参考并重做相关场景核对，保留“当前行为”与“拟改进”的区别。
