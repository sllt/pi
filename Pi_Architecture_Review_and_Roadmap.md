# Pi / pi-layout 架构评审与迭代规划

**初评日期：2026-09-24｜本地更新：2026-09-26｜文档版本：1.6｜范围：Go 框架、应用脚手架及交付链路**

**最新进展：** v0.2.4 与 v0.3.0 均已发布，Pi v0.3.0 为 `7a93b31`，pi-layout v0.3.0 为
`6db9888`。Migration v2、脚手架 up/plan/status、可靠错误退出及一次性连接清理已完成。
SQLite/MySQL/PostgreSQL/Redis、双进程锁、失败回滚、race 和公开 tag 消费验证通过；
见[验收记录](docs/verification/v0.3.0.zh-CN.md)。下文早期“未发布”描述保留为评审历史，当前状态以此段及开发计划发布记录为准。

**命名更新：** Kite / kite-layout 已更名为 Pi / pi-layout，CLI 为 `pi`。本文正文使用新名称；固定提交及历史 tag 内仍是旧 module/路径，证据链接保留历史文件位置。改名不代表功能修复或新版本已发布，详见[改名说明](docs/rename-to-pi.zh-CN.md)。

本文负责记录问题证据、架构方向和阶段边界；[Pi_Development_TODO.md](Pi_Development_TODO.md) 是依据本文拆出的版本执行清单，负责维护任务状态、依赖、验收证据和发布记录。开发排期以这两份文档为依据，不自动合并其他 Roadmap 的功能清单。

**1.1 修订：** 补充本地工作树与发布版本的区别；沿用已存在的 Migration v2；新增 F26 订阅 panic 后确认消息问题；拆小阶段 A；明确 HTTP 超时、Start context、纯构造与 Fx 的迁移边界；将最小 testkit 和交付验证前置。

**1.3 修订：** V024-02/03 已在本地工作树实现并通过定向 race 验证：默认关闭 gRPC
用户示例、修复 HTTP 结果传递及外层 Context 访问竞态。历史评审结论保留；当前进展
见第 2.5 节，未发布状态与请求输入生命周期限制不变。

**1.4 修订：** V024-01–07 开发与固定候选验收完成；补齐 Start/Stop 一次执行、终态
错误与订阅确认规则。隔离候选保留 Migration v1，主工作区 Migration v2 未改变；尚未发布。

**1.5 修订：** 单独进行候选 review，复现并修复已启动 App 的 RunContext 取消遗漏、
ShutdownWithContext 的预算竞争漏强关，并修正四个旧服务器测试的启动同步；
更新固定候选与消费 checksum。详见[评审记录](docs/verification/v0.2.4-review.zh-CN.md)。

## 1. 结论与决策摘要

**建议继续迭代，不建议重写。下一阶段的核心不是增加组件数量，而是把已有能力变成安全、可组合、可测试、可重复交付的契约。**

当前设计已有值得保留的骨架：基于标准 HTTP 生态的路由封装、标准 `context.Context` 的业务服务、Handler / Service / Repository 分层、Fx 构造函数装配、事务内统一 Executor、用户与资料跨表原子写入，以及新增的 `Start / Stop / RunContext` 生命周期。这不是一个需要从零设计的项目。[S02][S15][S16][S20][S25]

但按本次源码评审，**不宜直接把当前组合视为已经稳定的生产基础框架**。最优先的问题是：默认开启的 gRPC 用户接口缺少认证与资源授权；HTTP 超时处理有共享状态竞态；生命周期“重复调用不报错”不等于真正幂等；迁移失败不能可靠传播到命令退出状态；Docker 与生成器尚未形成可重复交付闭环。[S02][S03][S07][S11][S12][S17][S19][S22]

建议将产品定位收敛为：**面向业务 API、后台服务和模块化单体的 Go 应用底座；既能独立运行，也能作为宿主应用的一部分；基础设施按需接入，业务模块由应用拥有。** 这里的“轻内核”不是删除功能，而是不让没有使用的功能支配应用的初始化、依赖、配置与关闭行为。

### 1.1 三条优先决策

**先解决可导致越权、错误部署和运行时不确定性的问题。** 暂停扩充数据库和消息后端，优先完成 F01–F16 中与实际部署路径相关的修复，以及 F26 消息误确认问题。每项修复独立验收，不等待完整架构重构。

**保留现有开发体验，重构内部契约。** 不必更换 Go、chi、Fx，也不必另写 DI、ORM 或路由器。先增加兼容 API，再逐步缩减 `App / Container / Context` 的职责。

**把 pi-layout 当成受测试的参考产品，而不是复制代码的目录。** 框架、CLI、模板必须有明确兼容关系；“生成 → 配置 → 迁移 → 启动 → 登录 → HTTP/gRPC 调用 → 退出”应成为发布验收流水线。

## 2. 评审基线、证据等级与限制

### 2.1 初评版本基线（2026-09-24）

| 对象 | 初评读取的固定提交 | 用途 |
| --- | --- | --- |
| pi 默认分支 master | `a9e70484466bc40353d2aec2fe5747f41483f558` | 判断默认分支现状与发布差异 |
| pi-layout master | `16d306cec171b1e0167d6a98a6d2f44e8c99e009` | 脚手架评审基线 |
| pi v0.2.3 | `bdb884696a60422d7504b307776ff0fea8892c34` | 脚手架实际依赖的框架基线 |

初评固定提交中的脚手架依赖 `github.com/sllt/kite v0.2.3`。该标签解析到的提交含有 `RunContext`；默认分支读取到的 `run.go` 则仍是旧入口。**因此不能因为 master 缺少该 API，就认定该版本脚手架不能编译。** 本文框架实现问题以这一历史基线为主；默认分支差异单独归入发布治理问题。改名后的工作树使用本地 Pi module，不能把旧 tag 直接当作新的 `github.com/sllt/pi v0.2.3`。[S01][S02]

### 2.2 评审方法

初评通过 GitHub 工具读取了仓库元数据、固定版本源码、配置与构建文件、生成器、生命周期测试和冒烟脚本。2026-09-25 的复核读取本地两个仓库的工作树、Git 引用与直接调用链。重点追踪启动/退出、HTTP/gRPC 入口、鉴权、业务服务、事务、迁移及脚手架交付，不是仅根据 README 给出判断。

证据分三类：**A：源码可直接确认的实现缺陷或契约不一致；B：需特定部署、调用或浏览器条件才会造成损害的风险；C：架构演进建议，不冒充已经发生的故障。** 优先级与证据等级分开：P0 表示暴露相关功能前必须处理；P1 表示稳定生产使用前应处理；P2 表示后续工程化和可维护性工作。

### 2.3 验证边界

初评未完成两个仓库的整体构建、单元测试、数据库集成测试、Docker 构建或端口实测。初评环境无法解析 GitHub 域名来取得完整可执行副本，Go 为 1.23.2，低于脚手架声明的 1.24.10；GitHub 工具的源码读取正常。这些是初评环境记录，不代表本地复核环境的工具链或网络状态。本文不声称 CI 已通过，也不声称已全面核查所有驱动、所有协议分支或依赖漏洞。

初评另用仅依赖标准库的隔离程序复现了 F02 的共享变量模式：`go test -race -count=1 ./...` 报告两处数据竞争并以失败退出。**该实验不导入 Pi，验证的是已观察到的并发模式，不是仓库测试结果。** 附录保留重现方式。本地复核仅做源码与文档检查，没有重新执行该实验，也没有运行仓库构建、测试、迁移或服务。

### 2.4 改名前的本地执行基线（2026-09-25）

| 对象 | 本地事实 | 对开发计划的影响 |
| --- | --- | --- |
| Pi 本地 master / HEAD | `bdb8846`，与本地 `v0.2.3` 指向同一提交 | 初评中的远端默认分支快照不能当成本地 HEAD |
| Pi 本地 origin/master | 记录在 `a9e7048`，本轮没有 fetch 或重新核对远端 | 保留发布治理检查，不据此推断当前远端实时状态 |
| Pi 工作树 | Migration v2 尚未提交：Result/error/context、plan/status、target、dry-run、gap 与 SQL/Redis lock 已有实现 | 先验收、收口、发布；不是从零重做 F09，也不是已经完成发布 |
| pi-layout | HEAD 为 `16d306c`；历史依赖为 `github.com/sllt/kite v0.2.3` | 改名后通过本地工作区消费 Pi，仍未发布新 module；必须显式处理迁移结果 |
| layout 构建与配置 | Dockerfile 在工作树中已删除；`configs/.env` 仍被 Git 跟踪 | F14 应交付新的构建方案；F23 需要处理跟踪状态，不能仅追加 ignore |

后文未特别标注的问题描述沿用固定版本基线；F09、F14、F22 按本节区分发布状态与工作树状态。已有源码、已有测试文件、验证通过、已发布、layout 已接入分别记录，不能互相替代。现有未提交实现必须保留；若先发布热修复，从固定基线隔离实现，不清理或覆盖当前工作树。

改名阶段的构建与兼容验证另记于开发 TODO 的 R000 阶段，不改变初评“未运行仓库测试”的历史记录，也不表示已验证或修复本文所有问题。

### 2.5 第一批本地修复（2026-09-26）

- **F01：已缓解，未完整关闭。** layout 默认不注册 gRPC UserService，并明确恢复前的身份、资源授权、校验和权限测试条件。真实 HTTP 测试验证原公开/保护路由正常。
- **F02：首轮修复已验收，未发布。** 真实 Handler 在修复前报告 6 处 race；改用缓冲结果通道、固定的外层 context/responder 后，Handler 与路由定向 race 通过。结果传递安全不代表超时会终止业务，也不代表 multipart、WebSocket/SSE 已完成全面验收。
- **发布与开发分开记录。** 首个 Pi module 的发布（R000-05）仍是发版门禁；本地修复可先推进。隔离发布工作树、固定提交和无 replace 消费尚未完成，不将当前混合工作树直接作为 v0.2.4 发布。

第一批命令见[初轮记录](docs/verification/v0.2.4-first-batch.zh-CN.md)；HTTP 决策见 [ADR-003](docs/design/adr-003-http-response-ownership.zh-CN.md)。

### 2.6 v0.2.4 固定候选验收（2026-09-26）

V024-01–07 均已完成本地开发验收。framework/CLI 候选为 `6860233`，layout 为
`0b41420`，各自位于 `codex/v0.2.4-candidate`。候选保留 Migration v1，避免把
主工作区尚未发布的 Migration v2 带入补丁版本。框架运行时 race/构建、CLI 版本、
遥测兼容，以及 layout 无 go.work/replace 的本地 module proxy 消费均通过。

F03/F04 的一次执行、等待与错误缓存已经修复；F26 的框架自动确认路径已修复。
F01 仍仅缓解，消息后端实际重投递、完整 supervisor 和流式输入生命周期仍有后续边界。
具体命令与固定提交见[完整验收记录](docs/verification/v0.2.4-complete.zh-CN.md)。
R000-05 的远程发布/公开消费验证未完成；下一开发项为 V030-01。

## 3. 当前设计：保留什么，调整什么

### 3.1 值得保留的部分

**路由注册树有进一步发展的价值。** `RouteRegistry / GroupNode` 将声明与编译分开，区分标准 HTTP 中间件和 `PiMiddleware`，组合父子分组。这比在每个 Handler 中手工重复拼装中间件更适合作为稳定的路由元数据入口。后续应补注册校验、路由导出与安全属性，而不是替换底层路由器。[S25]

**业务服务没有全面绑死 HTTP 上下文。** `UserService` 接收标准 context 和内部输入类型，HTTP 与 gRPC 共用服务，这是正确方向。应把身份、校验、错误语义也统一到这层契约，而不是让不同入口承担不同的业务正确性条件。[S11][S15]

**事务抽象已有实用基础。** typed context key、统一 Executor、嵌套复用、错误回滚与 panic 回滚都已存在；用户和用户资料写入也已置于同一事务。问题不是“没有事务”，而是启动事务的 context、数据库归属、冲突翻译和嵌套语义还需完善。[S16]

**Fx 不是当前问题的根源。** 脚手架已把主服务的信号处理交给一处，并用 `RunContext` 管理 Pi。应保留这项改进。问题是 Pi 构造函数仍启动资源，且其他命令未采用同样的资源关闭契约。[S20][S21]

**已有测试与冒烟脚本值得继续加强。** 需要提高断言质量和执行范围，而不是把现状描述成“完全没有测试”。例如生命周期幂等测试已经存在，但没有断言启动副作用只发生一次。[S23]

### 3.2 主要架构矛盾

当前 `App` 同时承担配置入口、路由注册、协议服务器、生命周期、定时任务、订阅、迁移和观测初始化；`Container` 又集中持有许多数据源、服务客户端与 WebSocket 管理器。脚手架的 Fx 装配从这个 Container 取资源，相当于“框架内部一套装配，应用外部再包一套装配”。[S03][S05][S06][S20]

这并不要求移除 Container。建议把它逐步降级为**兼容访问门面**：资源由显式的构造与注册流程建立，所有权和关闭顺序由生命周期管理；业务服务只接收实际需要的依赖，不接收整个 Container。

当前推荐调用链应逐步变成：

```text
配置加载与校验 → 显式装配 → 生命周期启动 → 就绪
                        ├─ HTTP 适配器 → 身份/校验 → 应用服务
                        ├─ gRPC 适配器 → 身份/校验 → 应用服务
                        └─ Worker     → 可信任务上下文 → 应用服务
应用服务 → 业务策略 / Transaction / Repository 接口
实现层   → SQL / Redis / PubSub / 外部服务适配器
退出     → 停止接入 → 排空工作 → 逆序关闭自有资源 → 返回最终错误
```

## 4. 问题总表

| 编号 | 优先级 / 证据 | 问题 | 主要归属 |
| --- | --- | --- | --- |
| F01 | P0 / A+B | gRPC 资料接口绕开 HTTP 身份与权限检查 | layout |
| F02 | P1 / A | HTTP 超时分支与 Handler 共享结果变量发生竞态 | pi |
| F03 | P1 / A | 重复 Start 仍执行启动副作用 | pi |
| F04 | P1 / A | Stop 提前报告完成，丢失关闭结果 | pi |
| F05 | P1 / A | 构造函数启动资源，并存在 Fatal 退出路径 | pi |
| F06 | P1 / A+B | HTTP_HOST 未用于监听；半套 TLS 配置回落明文 | pi + layout |
| F07 | P1 / A | 包装、Join 后的错误丢失 HTTP/业务分类 | pi |
| F08 | P1 / A+B | 未分类内部错误直接返回客户端 | pi + layout |
| F09 | P1 / A | 迁移失败不能可靠传播到命令失败 | pi + layout |
| F10 | P1 / A+B | 两层 CORS 冲突，反射任意 Origin 并允许凭证 | pi + layout |
| F11 | P1 / A+B | JWT 策略宽松、90 天访问令牌、URL/Cookie 混用 | layout |
| F12 | P1 / A | 事务 Begin 未接入调用 context 与选项 | layout + SQL 接口 |
| F13 | P1 / A | 声明多数据库，但迁移使用 SQLite 方言 | layout |
| F14 | P1 / A | Docker 工具链及配置目录与项目不匹配 | layout |
| F15 | P1 / A | 初始化只改 Go 文件，proto 模块路径残留 | CLI + layout |
| F16 | P1 / A | gRPC 和业务服务缺少与 HTTP 一致的输入校验 | layout |
| F17 | P2 / A+C | Container 关闭逻辑与可持有资源范围不对称 | pi |
| F18 | P2 / A | task / migration 入口资源关闭不完整 | layout |
| F19 | P2 / A | 根据 HTTP 方法猜测成功状态码 | pi + layout |
| F20 | P2 / A+B | 唯一冲突、字段更新和并发写契约待完善 | layout |
| F21 | P2 / A | 测试入口范围不足，冒烟断言掩盖协议差异 | layout + pi |
| F22 | P2 / A+C | 默认分支、发布依赖、模板与工具版本未形成契约 | 两仓库 + CLI |
| F23 | P2 / A+B | 环境文件与构建上下文的隔离不完整 | layout |
| F24 | P2 / A+C | 内核职责和基础设施依赖过于集中 | pi |
| F25 | P2 / A | OpenAPI 注释、生成包装与实际路由存在漂移 | layout + CLI |
| F26 | P1 / A | 订阅 Handler panic 被恢复后仍可能确认消息 | pi |

说明：P1 的修复可分批交付，不等于每一项都已被利用；例如 TLS 问题需要错误配置，CORS 风险还取决于浏览器凭证策略。F01 的首要触发条件是 gRPC 端口对不可信调用方可达。

## 5. 高优先级问题与修复方案

### F01｜gRPC 用户接口缺少身份与资源授权

**2026-09-26 工作树状态：** 默认入口已关闭该服务，定向验证通过；以下保留原问题证据，完整授权修复仍待 v0.4.1。

**位置：** `internal/server/http.go`、`internal/grpc/user/userservice_server.go`、`userservice_pi.go`；框架 `pkg/pi/grpc.go`。[S10][S11][S12]

**现状与影响：** HTTP 的 `/api/v1/user` 分组检查登录身份，但启动代码同时注册 gRPC 用户服务。gRPC 默认拦截器只有恢复与观测；资料查询/修改直接采用请求中的 `UserId`，服务层也没有调用者与资源主体匹配检查。因此在端口可达时，未认证调用方可按指定用户 ID 查询或修改资料。这是跨协议安全契约缺失，不只是“忘加一个 JWT 中间件”。

**立即处理：** 安全实现完成前默认不注册该 gRPC 示例，或将部署入口明确限制为可信范围；网络限制只是临时缓解。正式修复应统一 Principal，HTTP 中间件与 gRPC unary/stream 拦截器共同负责认证，应用策略负责“能否操作目标资源”。“我的资料”从 Principal 取 ID；管理他人资料应是单独的管理用例并要求明确权限。

**验收：** HTTP/gRPC 同一测试矩阵覆盖无令牌、坏令牌、用户 A 访问 A、A 访问 B、管理员访问 B；不允许只校验令牌有效就放行任意 `UserId`。服务层单测不依赖 HTTP，也能拒绝越权。

### F02｜HTTP 超时处理的数据竞争

**2026-09-26 工作树状态：** 结果通道与外层 context/responder 隔离已实现，定向 race 通过，尚未发布；以下为原缺陷与验收依据，适用边界见 ADR-003。

**位置：** `pkg/pi/handler.go` 的 `handler.ServeHTTP`。[S07]

**现状与影响：** 后台 goroutine 写入外层 `result / err`；超时分支同时改写 `err`，随后读取 `result` 并响应。只有收到 `done` 的正常路径具有完成同步，超时路径没有。迟到的 Handler 会继续运行，可能与超时响应读写共享状态。隔离实验检测到结果读写、错误写写两处竞态；Go 的 race detector 只检测实际执行到的并发路径，所以单纯加 `-race` 而不构造超时路径还不够。[T01]

**首轮修复：** 保持普通 JSON Handler 现有响应超时契约，采用容量为 1 的结果通道传递 data/error/panic 结果，确保只有一个 goroutine 写响应。单独审查请求体、Context 可变字段和 ResponseWriter 的生命周期，避免外层返回后仍访问请求期对象。通道只解决结果传递问题，不会强制终止后台业务，也不能自动证明整条路径无竞态。

**后续设计：** 同步执行并依赖协作式取消，可以作为独立执行模式评估；它会改变“不配合取消时是否及时返回超时响应”的行为，不能在修竞态的补丁中静默切换默认值。先记录兼容性决策，再开放新模式；WebSocket/SSE/流式写使用独立的响应所有权契约。

**验收：** 超时后成功返回、超时后报错、panic、客户端取消与完成同时到达、长时间不响应取消；`-race` 无报告，响应只提交一次。另为 WebSocket/SSE/流式写建立独立路径，不套用普通 JSON 响应期限。

### F03｜重复 Start 不是真正幂等

**2026-09-26 状态：** 固定候选已修复并通过定向 race；首个 context 仍控制运行期，后续调用只控制等待。以下保留原问题证据。

**位置：** `pkg/pi/run.go` 的 `Start / startRuntime`。[S02]

**现状与影响：** `startRuntime` 在已启动时返回已有 context 和 nil，但外层 `Start` 接着运行启动钩子、服务启动及后台任务。重复调用可能重复执行业务初始化、启动第二份 Worker；已有 gRPC 监听还可能触发重复监听失败，进而关闭应用。当前幂等测试没有设置带计数的钩子和 Worker，不能发现这些副作用。[S23]

**建议：** 状态机明确区分 New、Starting、Running、Stopping、Stopped、Failed；并发 Start 等待同一个启动结果，Running 下 Start 不再执行任何启动步骤。完成之前不能把“已经进入启动过程”当成“启动已完成”。

**验收：** 串行调用两次以及并发多次调用，启动钩子、每个资源和 Worker 均只执行一次；启动失败的所有等待方得到同一失败结果；关闭后是否允许重启写入契约，建议首版不允许。

### F04｜Stop 完成语义和错误保存不正确

**2026-09-26 状态：** 固定候选已修复并通过定向 race；单一执行者、独立等待和缓存错误见 [ADR-002](docs/design/adr-002-lifecycle-v024.zh-CN.md)。以下保留原问题证据。

**位置：** `pkg/pi/pi.go` 的 `Stop / shutdown`。[S03]

**现状与影响：** `runtimeStopped=true` 在真正关闭前设置。第二个调用立即返回 nil，而第一轮可能仍在排空请求，甚至最终失败；后续调用也取不到先前关闭错误。这会误导宿主过早销毁依赖或退出。

**建议：** 使用 Stopping 状态、完成通道和缓存结果。仅一个关闭执行者；其他调用者等待完成，或在自己的等待 context 到期后返回等待超时。明确“关闭动作只执行一次”和“所有等待者得到同一终态结果”两个不同保证。若需要强制关闭或重试未关闭资源，提供显式策略，不静默重跑全部 Hook。

**验收：** 并发 Stop、慢 Hook、Hook 报错、截止时间到期、Start 与 Stop 交错；不得提前返回成功，不重复关闭，不丢失首轮错误。

### F05｜构造函数副作用和 Fatal 破坏可组合性

**位置：** `factory.go` 的 `New / initMetricsServer`、`infra/container.go` 的 `Create`、layout `bootstrap.NewPiApp`。[S05][S06][S20]

**现状与影响：** `New()` 已读配置、创建数据源和观测组件；指标端口探测失败存在 `Fatalf` 路径。Fx 调用构造函数时这些工作已发生，尚未进入 `Start` 的超时与回滚保护。“Start 会返回启动错误”的新契约因此没有覆盖全部初始化阶段。

**建议：** 新增兼容的 `Build(...) (*App, error)` 或 Builder，先解析配置、校验注册表，禁止网络访问和后台循环；资源启动放入生命周期。库层返回错误，不自行退出进程。保留旧 `New()` 作为明确标注的兼容入口，迁移脚手架后再考虑废弃。

**阶段边界：** 阶段 A 修正监听、配置错误和相关 Fatal 路径；纯构造与数据源延迟启动属于阶段 B。layout 的 `bootstrap.NewDB` 目前在 Fx 构造阶段读取 `app.Container().SQL`，因此不能只把 SQL 创建移动到 Start。需同时选定“构造稳定句柄、Start 激活资源”或“Fx 提供资源、Pi 借用”的装配方式，并验证 Repository 不会获得空或过期依赖。

**验收：** 构造不绑定端口、不连接数据库、不启动 goroutine；无效配置返回结构化错误；端口占用从 Start 返回，不结束测试进程。只运行迁移/任务不应被未使用的指标或 HTTP 端口影响。

### F06｜监听地址与 TLS 配置需要严格契约

**位置：** layout `configs/.env.example`；框架 `factory.go / http_server.go`。[S05][S09][S18]

**现状与影响：** 示例写着 `HTTP_HOST=127.0.0.1`，实际 HTTP 监听地址由 `fmt.Sprintf(":%d", port)` 生成，没有使用该 Host。用户以为仅限本机，实际绑定范围不同。此外，只有证书和私钥都非空才进入 TLS；只配置其中一个时会进入明文分支。是否能被外部访问还受防火墙、容器端口映射等影响，不能据此断言公网已暴露。

**建议：** 对 HTTP/gRPC/metrics 使用统一 ListenAddr 或 Host+Port 配置，实际通过 `net.JoinHostPort` 构造并打印地址；允许明确的关闭状态和测试随机端口。证书/私钥不成对直接报错；证书解析失败阻止就绪；不要通过“探测端口再监听”的先验检查替代真实 Listen 错误。

**验收：** 本机绑定、IPv6、随机端口、端口占用、仅证书、仅私钥、不匹配证书分别测试；配置中的地址与 listener 的实际地址一致。

### F07｜错误包装后丢失分类

**位置：** `http/responder.go` 的 `getHTTPStatusCode / getErrorCode`。[S08]

**现状与影响：** 直接用类型断言识别 `StatusCodeResponder / CodeResponder`。当调用者用 `%w` 添加上下文，或事务层 `errors.Join` 拼接回滚失败时，外层错误不再直接实现这些接口，原有 400/401/业务码可能变成 500/-1。[S16]

**建议：** 先通过 `errors.As / Is` 识别稳定错误类型，再转换为 HTTP/gRPC；定义多个错误同时存在时的优先级。业务拒绝、取消、超时、内部故障不应仅靠字符串或数值范围猜测。gRPC 适配器必须复用分类，而不是把业务错误原样返回。[S11][S27]

**验收：** 直接错误、单层/多层 `%w`、Join、取消/超时混合场景得到相同且可解释的分类；HTTP 与 gRPC 的语义对应。

### F08｜内部错误不应原样公开

**位置：** `http/responder.go` 的 `buildResponse`；Repository、Service 的原始错误返回路径。[S08][S15][S16]

**现状与影响：** 默认错误响应使用 `err.Error()`，数据库执行、连接与其他内部错误可能直接成为返回消息。泄漏的具体内容取决于底层错误文本；不能预设一定含密码，但数据库结构、内部地址等不该成为 API 契约。

**建议：** 区分公开消息与内部 Cause。已分类业务错误可返回安全描述；未知错误统一为内部错误并附 request ID，服务端日志保存完整错误链。HTTP/gRPC 共用这一规则。日志应有一次主要错误记录点，避免每层重复 Error 日志。

**验收：** 模拟含 SQL/内部地址的错误，响应不包含细节，日志能通过 request ID 找到原因；包装错误不改变脱敏行为。

### F09｜Migration v2 待收口，脚手架仍可能把迁移失败当作成功

**v0.3.0 已交付：** 保留已有 v2 设计并完成错误/状态/锁/取消清理收口；layout 移除无条件成功 hook，
通过 run(ctx) error 返回非零退出码，消费正式 Pi tag。补充 sql.OpenContext 支持单次命令资源所有权；
完整 App 纯构造仍属 v0.4.0。下列内容保留为原问题与演进依据。

**位置：** `App.Migrate`、`migration.Run`、layout `RegisterMigrateServer`。[S03][S17]

**已发布基线与影响：** layout 依赖的 v0.2.3 中，两层迁移 API 都不返回 error。迁移函数或提交失败的部分路径只记录日志后返回；App 还恢复 panic。脚手架随后无条件打印成功并请求正常停止。某些路径使用 Fatal，另一些路径却正常返回，部署流水线无法可靠判断结果。

**工作树进展：** 已有 `Migrate/MigrateContext(...opts) (migration.Result, error)`、`UpContext`、plan/status、target/dry-run/gap 与 SQL/Redis lock，不再另行设计仅返回 error 的重复 API。状态以 SQL 为权威源、Redis 为后备，其他 datasource 标明 legacy/imprecise。默认不加锁；显式 `WithLock()` 的租约可配置但目前不续期。plan/status 不执行业务迁移，但可能初始化状态表，不能描述为完全无写入。[S29]

**建议：** 对现有实现补齐发布验收，检查 panic/取消后的事务清理、提交和部分成功语义；保留 Result，命令顶层决定非零退出。框架发布后 layout 升级依赖、显式处理结果、只在成功后打印成功，并在执行命令中明确锁策略。单纯升级依赖不会修复丢弃返回值的旧调用。`UP` 和语句式调用可兼容，不代表函数类型、接口、`migration.Run` 等全部源码兼容，应在次版本发布中说明。Down/History、checksum、续租和失败恢复另立后续设计，多数据源不承诺原子提交。

**验收：** 故意失败的 migration、不可用数据库、提交失败、重复运行、两进程并发迁移；失败必须非零退出且无“成功”日志，重复成功运行不得重复执行迁移。

### F10｜CORS 配置重复且缺少可信来源边界

**位置：** 框架 `http/middleware/cors.go`、layout `internal/middleware/cors.go` 与全局注册顺序。[S09][S12][S13]

**现状与影响：** layout 回显任意 Origin 并允许凭证；认证还接受 Cookie。若浏览器允许携带相关 Cookie，某些跨源读取可能被放行。另一方面，框架外层 CORS 会提前处理 OPTIONS，内层脚手架的预检代码不一定执行，造成预检与实际响应策略不一致。不能笼统声称所有跨域写请求都会成功。[S14][T04]

**建议：** 只保留一个 CORS 决策层，使用明确来源白名单，动态来源添加 `Vary: Origin`，固定允许的方法/头。默认关闭 credentials；Cookie 认证需要专门设计 SameSite、Secure、CSRF 防护，不能用 CORS 替代 CSRF。处理 `Origin: null`、无 Origin 和拒绝来源时要有明确行为。

**验收：** 必须对真实框架+脚手架组合做集成测试；覆盖简单 GET、预检 PUT、允许/拒绝来源、Cookie 与 Bearer 两种模式，不能只测试单个中间件函数。

### F11｜JWT 与令牌承载策略需要收紧

**位置：** `pkg/jwt/jwt.go`、`internal/middleware/jwt.go`、`service/user.go`。[S14][S15]

**现状与影响：** 登录签发 90 天访问令牌；解析未显式约束预期算法、issuer/audience 等应用策略；密钥只检查非空。全局可选认证依次接受 Header、Cookie 和 URL 查询参数。不能据此认定存在 alg=none 伪造漏洞，但令牌适用范围与生命周期缺少明确边界。URL 携带令牌也扩大日志、历史记录等泄漏面。

**建议：** 默认只支持 Authorization Bearer；显式配置算法、issuer、audience、必须存在的到期声明，并校验非空主体。签名算法应使用库提供的约束选项，而不是自行解析算法字段。[T03] 访问令牌 TTL 改为可配置且明显短于当前 90 天；需要长期登录时增加可轮换的刷新凭证及吊销/会话策略。具体 TTL 由应用风险决定，不硬编码所谓通用安全值。

**验收：** 算法不匹配、issuer/audience 错误、缺少/过期 exp、空主体、密钥占位值、刷新重用、退出登录后权限状态变化。禁止生产模式静默接受示例密钥；过渡兼容旧令牌要有明确截止策略。

### F12｜事务创建未贯通 context

**位置：** `internal/repository/repository.go` 的 `Transaction`。[S16]

**现状与影响：** 查询用 context，但创建事务调用 `r.db.Begin()`，不能传入本次调用的 deadline 和事务选项。等待连接、事务生命期和查询取消不是同一个保证。标准库 `BeginTx` 的 context 持续到提交或回滚，取消时会触发回滚语义，应由适配层保留。[T02]

**建议：** 增加兼容的 BeginTx 能力并向下传递 context、隔离级别和只读选项。保留 typed key，但同时记录数据库归属；跨数据库不能误复用其他 DB 的 Tx。嵌套复用明确为 join-existing，不宣称 savepoint；内层错误被外层吞掉时是否标记 rollback-only，必须显式决定。

**验收：** 连接池耗尽后取消、事务内取消、Begin 失败、Commit 失败、panic 回滚、嵌套复用、跨库拒绝错误复用。Commit 失败后不得自动重试整个业务事务，除非业务声明可重试且满足幂等条件。

### F13｜多数据库支持停留在配置层

**位置：** `configs/.env.example`、`migrations/20260206104000_create_users_table.go`、Repository SQL。[S16][S18]

**现状与影响：** 配置示例列出 SQLite/MySQL/PostgreSQL，但迁移包含 `INTEGER PRIMARY KEY AUTOINCREMENT` 等 SQLite 方言。Repository 还使用 `?` 和 LastInsertId；这些也需要按驱动验证，但本次未完整追踪框架是否做占位符改写，因此不将所有查询都断言为 PostgreSQL 必然失败。

**建议：** 先把 SQLite 定为参考模板的已验证默认；对外声明 MySQL/PostgreSQL 完整可用前，提供对应迁移、查询执行与返回 ID 的契约测试。可以维护少量方言分支或明确拆模板，不必为此开发通用 ORM。

**验收：** 每个声明支持的数据库独立完成空库迁移、注册、并发重复注册、登录、资料修改、回滚；不使用 sqlmock 代替数据库兼容性验证。

### F14｜Docker 构建链路过期

**位置：** `deploy/build/Dockerfile`、`go.mod`、`.dockerignore`。[S01][S19][S24]

**现状与影响：** builder 使用 Go 1.19，而模块声明 Go 1.24.10；Dockerfile 移动 `config`，项目使用 `configs`。构建期间执行 `go mod tidy`，还会修改依赖描述而不是验证锁定状态。它不能被当作与当前项目一致的交付方案。本文未实际执行 Docker 构建。[T05]

**本地差异：** 上述是固定提交中的旧文件；当前工作树已删除该 Dockerfile，而 Makefile 仍引用它。实现时按现有删除状态提供新的可用构建入口，不直接恢复旧文件覆盖用户改动。

**建议：** 选择符合项目最低要求且仍受维护的 Go 工具链并固定镜像版本；先复制 go.mod/go.sum 下载依赖，再复制源码，构建不执行 tidy。明确构建上下文和命令目标；运行镜像使用非 root、正确 CA 证书、可写 storage 目录，运行时注入配置，不烘焙真实密钥。SQLite、CGO、目标架构的组合须测试后再选择镜像方案。

**验收：** 干净环境构建主服务/任务/迁移二进制，容器执行迁移与健康检查；SIGTERM 正常退出；镜像中没有本地 .env、开发数据库及临时日志；构建前后 go.mod/go.sum 不变化。

### F15｜CLI 初始化遗漏 proto 元数据

**位置：** `cli/bootstrap/init.go`、`api/proto/user/user.proto`。[S22]

**现状与影响：** 初始化只替换 `.go` 文件中的模块路径，proto 的 `go_package` 仍保留 `github.com/sllt/pi-layout/...`。已有生成文件可能暂时可用，但再次生成 protobuf 时会沿用旧路径。CLI 还把项目目录和 Go module path 当作同一个参数，且 tidy 失败后仍打印创建成功。

**建议：** 分开 `--dir` 和 `--module`，显式指定模板版本；渲染 Go import、proto go_package、文档/工具配置等需要替换的字段。先在临时目录完成生成、格式化和校验，再落盘；执行失败不得留下看似完成的项目。区分“文件生成成功但离线依赖尚未下载”与“已验证可运行”。

**验收：** 带域名模块路径、相对/绝对目录、已有目录、依赖下载失败、生成后再次执行 proto/包装生成；项目内不残留旧模块路径且构建成功。重复生成不得覆盖用户手写实现。

### F16｜入口不同导致业务输入规则不同

**位置：** HTTP DTO、gRPC 用户实现、`service/user.go`。[S11][S15]

**现状与影响：** HTTP 通过 Bind 使用字段标签；gRPC 直接把 proto 字段转成内部输入，随后调用没有统一输入校验的服务。因此邮箱格式、必填字段等规则不能只依赖 HTTP 入口。密码只检查必填也不足以形成完整账户策略。

**建议：** 传输层负责解码、大小限制与格式错误；应用输入验证负责跨入口一致的业务规则。为 RegisterInput、UpdateProfileInput 等建立显式验证，统一邮箱规范化、密码字节长度边界、昵称长度；真实业务状态约束仍由应用服务检查。避免在 HTTP 与 gRPC 各复制一套会漂移的校验逻辑。

**验收：** 同一批空字段、无效邮箱、过长输入和边界字符，同时调用服务、HTTP、gRPC，拒绝理由一致；错误响应不泄漏密码及哈希。

## 6. 工程化与架构债务

### F17｜资源关闭要覆盖所有“自有”资源

Container 可以持有许多类型，但 `Close` 只显式关闭 SQL、Redis、PubSub 和 WS 连接。不能据此断言每个字段都需要关闭，也不能默认外部注入对象都归框架销毁；问题是缺少可表达的所有权契约。[S06]

建议注册资源时同时登记名字、Owned/Borrowed、关闭函数和依赖顺序。自有资源逆序关闭；借用资源不关闭；HTTP 客户端连接池、日志后台刷新、OTel exporter、定时循环等逐项盘点。带 context 的关闭预算必须贯穿，不能用无截止时间 Close 默默突破宿主预算。验收以关闭计数、顺序、错误聚合和残留 goroutine 为准。

### F18｜task / migration 命令未形成统一清理流程

主服务有 Pi RunContext，但 task 入口只是 Fx Run 加 gocron Hook；migration 入口执行迁移后停止 Fx。CoreModule 的构造没有注册对 Pi 资源的关闭，两个命令也未调用 Pi Stop。进程结束会回收部分 OS 资源，不等于库级刷新、排空和优雅关闭已完成。[S20][S21]

建议所有命令复用资源生命周期，但迁移仅构造需要的数据库和日志，不创建无关服务器。任务的取消、禁止重入/允许并行、任务级超时与多副本执行语义应显式配置。单机 cron 不等于可靠队列；不要未经需求确认就在内核加入分布式调度系统。

### F19｜成功状态码不应由 HTTP 方法机械推断

Responder 对 POST 有数据返回 201、无数据返回 202；因此登录得到 201，同步注册可能得到 202，与 Handler 注释中的 200 不一致。DELETE 默认 204 后仍走 JSON 写入，也需要无正文契约。冒烟脚本同时接受多个状态码，掩盖了差异。[S08][S23][S26]

建议由操作显式选择 200/201/202/204；只有确实异步接收任务才返回 202。保留统一响应 envelope 作为默认，但文件/流/原始响应有独立契约，错误优先级明确。先通过兼容选项迁移，再修改默认值；不要在补丁版无说明改变已有客户端行为。

### F20｜数据库约束正确，但业务冲突与并发更新还需完善

users 已有 email/user_id 唯一约束，不能说并发注册必然插入重复账号。实际问题是“先查再写”不能替代唯一约束，竞争时仍可能冒出原始数据库错误；应把约束冲突翻译成稳定的 Conflict。邮箱是否大小写归一、Unicode/空格如何处理也需要产品规则。[S15][S16][S18]

Repository 的 User.Update 同时写 password/email，资料修改会带回读取时的 password；未来存在独立改密用例时有覆盖新值的风险。建议按用例更新字段，必要时使用版本号乐观锁。RowsAffected 要按数据库语义解释：MySQL 的零行可能是值未变化，不能统一等同 NotFound。验收覆盖并发重复注册、改密与改资料交错、空更新和资源删除竞争。

### F21｜测试需要从“函数返回”提升到“完整行为”

`make test` 只运行 `./test/server/...`，不会自动覆盖 `cmd/server`、middleware 等目录测试；生命周期幂等测试没有副作用计数；冒烟允许两种登录/注册状态码，readiness 只看 curl 能否完成请求。这里的问题是标准测试入口与契约断言不足，不是没有测试。[S23]

建议统一 unit、integration、race、smoke、container、generator 等目标。启动前先构建二进制，再管理真正的子进程，避免把 `go run` 包装进程 PID 当成稳定服务 PID。readiness 必须检查预期状态与服务标识；冒烟同时校验业务码和响应字段。CI 至少强制执行真实入口依赖图与失败路径。

### F22｜发布和模板必须可追溯、可复现

本次默认分支与 v0.2.3 的生命周期实现不同；CLI 又克隆模板默认分支，工具安装使用 `@latest`。所以“相同 CLI 命令”会随模板变化生成不同代码。这里确认的是版本快照差异，不推断具体操作历史，也不认定标签被重写。[S01][S22][S23]

建议确定权威开发分支，把有效发布提交纳入可追溯历史；发布记录绑定 framework、CLI、template 三元组与最低 Go 版本。模板用固定 tag/commit 或带校验的归档，工具也固定版本。增加当前发布版与下一候选版的模板验证矩阵，发布前用无本地 replace 的干净目录验证。

### F23｜环境文件与构建上下文隔离不完整

`.gitignore` 已列出 `configs/.env`，但本次 Git 树列表仍出现该路径；`.dockerignore` 只排除了 `.git/.idea`。忽略规则不是历史文件移除，也不是 Docker 构建忽略规则。本文未读取或披露该 .env 的值，不认定真实凭证已经泄漏。[S24]

建议停止跟踪本地配置，只提交明确的 example；审查历史内容，只有确认真实密钥暴露才执行相应轮换。构建上下文排除 .env、storage、日志、覆盖率和本地工具文件；生产配置校验拒绝示例密钥。文档提供“复制示例 → 设置密钥 → 迁移 → 启动”的完整步骤。

### F24｜轻内核需要依赖倒置，而不只是移动文件

Container 直接导入和创建多个基础设施实现，App 同时接管服务器、订阅、迁移和资源管理。即使把文件移到不同目录，只要核心继续直接引用全部适配器，依赖图和初始化耦合仍在。[S03][S06]

建议先在同仓库内部明确接口和组合根，再考虑将重型适配器拆成独立 Go module。不要一开始创建大量小仓库。HTTP-only 示例的依赖图中不应出现未使用的云 PubSub/数据库驱动；但模块下载图、包编译图和最终二进制链接图不是同一概念，要分别测量，不能只凭 go.mod 长度判断性能。

### F25｜文档与生成代码的协议元数据漂移

Handler Swagger 注释写 `/login`、`/register`、`/user`，真实路由有 `/api/v1`；主入口注释未声明对应 BasePath。生成的 gRPC 包装还使用固定 `Hello` 设置健康服务状态。这些属于可直接看见的元数据残留；实际生成产物还需做一致性测试。[S11][S20][S26]

建议路由、鉴权要求、请求/响应和状态码有单一可信来源，生成文档后用路由快照比对；gRPC 使用真实 full service name。不要为消除少量重复设计新的大型 IDL 平台；先固定 OpenAPI/proto 生成方式与工具版本。

### F26｜订阅 Handler panic 后仍可能确认消息

**2026-09-26 状态：** 框架失败/取消不自动 Commit 的首轮修复与计数 race 测试通过；各后端实际确认/重投递限制见[确认边界](docs/design/subscription-ack-v024.zh-CN.md)，未声称完成真实 broker 集群验收。

**位置：** `pkg/pi/subscriber.go` 的 `handleSubscription`。[S28]

**现状与影响：** 调用 Handler 的匿名函数在 defer 中恢复 panic，但没有把 panic 转为返回错误；恢复后返回零值 nil，外层随后进入 `msg.Commit()`。在消息带有 Committer 时，失败处理可能被当作成功确认，影响重投递。具体确认和重投递效果由后端决定；本轮为源码确认，未声称已完成消息后端复现。

**首轮修复：** 将恢复出的 panic 转为非 nil 处理错误；成功、业务 error、panic、取消分别定义确认行为。先保证 error/panic 不进入成功确认路径，再在统一任务监督阶段处理重试预算、退避、死信和后端差异，不把局部修复拖到完整 supervisor 重构。

**验收：** 使用可计数 Committer 验证成功仅确认一次、error/panic 不确认；验证处理结果能进入日志/监督策略；对声明支持且具有确认语义的消息后端补重投递集成测试。不承诺所有后端都有相同的 ack/nack 能力。

## 7. 目标架构与关键契约

本章是建议设计，不表示这些 API 已存在，也不要求一次性完成目录重构。

### 7.1 三个产品层

| 层 | 应当承担 | 不应承担 |
| --- | --- | --- |
| Pi Core | 生命周期、HTTP 基础适配、配置契约、错误分类、健康状态、观测接口 | 用户表、具体 RBAC 存储、业务迁移、默认启动所有驱动 |
| 可选集成 | gRPC、SQL、Redis、PubSub、OTel、Fx 等适配与生命周期绑定 | 把组件安装成所有应用的硬依赖 |
| pi-layout / 应用 | 模块装配、用户/权限等示例、具体迁移、部署与业务策略 | 复制框架已经提供的底层 CORS/错误转换逻辑 |

对于 SQL、JWT、日志这类常用能力，重点是“默认组合方便，底层可替换”，不是为了极端纯粹让每个业务项目写大量胶水代码。Fx 可保留为标准模板方案；框架自身不要求应用必须使用 Fx。

### 7.2 推荐包边界

```text
pi/
  pkg/pi/          现有 API 兼容门面
  runtime/           生命周期与组件编排
  httpx/             路由、绑定、响应和 HTTP 适配
  config/            配置加载、类型和验证
  apperr/            传输无关的错误分类
  identity/          最小 Principal / 认证结果类型
  health/            liveness / readiness / startup 状态
  integrations/      grpc、fx、sql、redis、pubsub、otel
  cmd/pi/          初始化、生成、检查工具
  examples/          minimal-http、worker、grpc、fx-host
```

该结构是方向，不是要求立即改 import path。先消除核心对具体适配器的反向依赖，用兼容门面桥接；稳定契约后再调整公开包。身份的表结构、刷新令牌存储和租户权限模型留在应用/可选账户模块，不进入最小 identity 包。

### 7.3 生命周期与宿主集成

建议语义为：构造只声明和校验；Start 完成必要初始化并能报告同步启动失败；Wait 等待运行期终止；Stop 等待资源关闭并返回可重复读取的结果；RunContext 是独立运行的便利组合。

```go
// 设计草案：可先新增，不直接破坏现有 New() 调用。
func Build(opts ...Option) (*App, error)
func (a *App) Start(startCtx context.Context) error
func (a *App) Wait() error
func (a *App) Stop(stopCtx context.Context) error
func (a *App) RunContext(runCtx context.Context) error
```

**必须区分启动预算 context 与长期运行 context。** Fx 的 OnStart context 不能直接成为 Worker 的永久父 context，否则宿主结束启动阶段后可能取消整个运行期。App 可以拥有内部运行 context，RunContext 负责把外部取消转换为 Stop；需要宿主父 context 时单独显式注入，不混用 Start 的 deadline。

这是目标契约，当前 `Start(ctx)` 会把 ctx 作为运行期父 context。阶段 A 的幂等修复保留这项取消行为；阶段 B 必须用 ADR 明确新构造入口/兼容入口的迁移方式，验证“启动预算到期不误停 Worker”和“RunContext 外部取消仍能完整停机”。不得只修改内部 context 创建方式而不说明调用方行为变化。`Wait` 还需明确返回运行故障还是清理完成结果，以及由谁触发 Stop，避免 Fx 宿主下 Worker 已失败但进程继续等待信号。

资源管理采用显式登记，不需要复杂插件平台。每个资源记录标识、所有权、启动/停止函数以及成功启动状态；失败时只回滚已完成或确有清理需要的资源。逆序释放依赖，错误聚合，日志与观测尽可能最后关闭。

纯构造与 Fx 的组合需同时交付：Repository 构造时拿到稳定的依赖句柄，资源就绪后才启动消费者；Pi 自有资源由 Pi 关闭，Fx/宿主提供的借用资源由宿主关闭。最小 testkit 随该阶段提供 config/logger 注入、Handler/路由调用与生命周期探针，先让 layout 无需复制私有 helper 即可验证这些边界。

运行期 Worker/协议服务故障要有统一策略：关键组件失败触发应用停止；可降级组件将 readiness 标记为降级或不可用并按策略重试。现有 Worker 会触发关闭，而订阅启动错误只记日志，后续应确认其业务角色并统一监督语义，不默认所有订阅失败都必须退出。[S02][S04]

**关闭预算不是强制终止 goroutine 的保证。** 不响应 context 的回调需通过隔离、监控、强制资源关闭或进程级边界处理；框架要准确报告超时，不能假装所有工作已停止。

### 7.4 身份、授权与多租户

Principal 最少包含 Subject、认证来源、可信租户标识和必要权限信息；不要把未经验证的 Header/Query 中 tenant_id 当作已认证租户。HTTP 与 gRPC 共享身份解析结果，授权仍围绕应用用例和目标资源。

多租户应在基本安全契约稳定后引入。先确定单库共享表、分 schema 或分库策略；Repository 的查询约束、唯一索引、后台任务和缓存 key 都需带租户语义。不要把“全局中间件读取 tenant_id”宣传成完整租户隔离。

账户模块可以在 layout 中提供可删除的示例，包括短期访问令牌、刷新/退出、账户禁用、密码更新和审计；不是每个服务都需要本地登录，有些服务只验证外部身份。框架应支持这两种情况。

### 7.5 错误、校验和响应

建议建立传输无关 Kind：InvalidArgument、Unauthenticated、PermissionDenied、NotFound、Conflict、RateLimited、Unavailable、Internal；业务码单独保存，Cause 可解包，PublicMessage 与日志信息分开。HTTP/gRPC 只负责映射。

保持错误链而不是在每层换成笼统 Internal；真正响应时决定哪些信息可公开。对 Cancelled/DeadlineExceeded 定义优先级，避免把正常客户端取消全部计入服务端故障告警。

HTTP 响应策略应显式支持：普通 JSON envelope、无正文、文件/重定向、流式/已接管连接。保留 `(any, error)` 的简洁 Handler 体验，但不要让任意返回值与错误组合产生不确定响应。先消除竞态和错误泄漏，再讨论泛型 Handler 或更复杂类型系统。

### 7.6 数据访问与迁移

保留标准 SQL + 少量扫描/事务辅助。接口优先围绕业务所需操作，不把所有 ORM 能力汇入通用 Repository。事务以单数据库连接的实际能力为界，不承诺跨 Redis/PubSub/SQL 原子性。

只有当业务确实要求“数据库提交与事件可靠投递一致”时，才在可选集成/应用层引入 outbox 等方案；简单 CRUD 不应被迫安装消息中间件。消息语义需要明确 ack、重试、幂等、重放与取消，但不在本轮顺手自研可靠队列。

迁移系统优先保证 error/context、锁、版本、校验和和失败恢复。涉及不支持事务性 DDL 的后端时，文档和状态机应承认部分执行的可能；部署采用向前兼容迁移，避免应用启动时各副本竞争改表。

### 7.7 观测、健康与配置

每个应用实例拥有自己的观测资源与注册范围；共享全局 Provider 必须是显式宿主策略。先保证 request ID、TraceID、低基数路由/方法标签和生命周期事件可定位，暂不堆叠多个 exporter。未完整审计当前观测实现，以上是目标契约，不是断言现有全部错误。

liveness 表示进程能否继续工作；readiness 表示是否接收新流量；startup 表示初始化是否完成。关闭开始先摘除 readiness，外部数据库短暂抖动不应不加区分地让存活探针杀进程。指标端口是否公开由部署决定，不能默认所有服务都监听固定指标端口。

配置应加载一次形成类型化快照：默认值、配置文件、环境变量与显式选项的优先级写清楚；开发 .env 是辅助，不是运行时全局状态。生产模式严格验证地址、TLS、密码/令牌、数据库方言及未知关键字段，输出有效配置时脱敏。

## 8. pi-layout 的演进方案

### 8.1 先保留分层，再逐步按功能组织

当前用户模块规模不需要立刻改成大量 DDD 目录。新增两三个真实模块后，可迁为 `internal/modules/user`、`internal/modules/order` 等，各模块内部保留 service/repository/transport 的必要边界。共同基础设施进入 platform/bootstrap，但不要让所有模块互相导入内部实现。

建议模块向组合根暴露构造与注册入口，而不是自动扫描目录、依赖 init() 副作用或运行期反射注入业务服务。跨模块调用采用小接口；不是所有服务都要机械拆一个只有单一实现的大接口，接口应由测试、替换或边界需要驱动。

### 8.2 提供三种模板组合，而非三套独立维护的代码

| 组合 | 默认包含 | 默认不包含 |
| --- | --- | --- |
| minimal | HTTP、配置、日志、健康、优雅退出 | 数据库、账户、Redis、gRPC、cron |
| api | minimal + SQLite 示例 + 迁移 + 用户模块 + 测试 | 不必要的消息后端、分布式任务 |
| service | 通过选项组合 gRPC/worker/SQL/Fx | 不启用未选择的公网接口 |

这些组合共享同一套模板片段和验收机制。先做好 minimal 与 api，再增加 service 组合；不必一次实现任意组件排列组合。未覆盖测试的组合明确标为实验性。

### 8.3 CLI 的重点应从“生成文件”转向“生成可维护项目”

在现有 init/create/wrap/migrate 命令基础上，增加显式 module/dir/template 参数、版本清单、dry-run/diff、冲突保护和失败清理；区分生成文件与用户拥有的实现文件。`doctor` 可检查 Go 版本、模板残留、配置缺项、端口配置和生成工具版本，但不应读取或上传真实密钥。

生成器验收至少包括：新建项目能构建；添加一个模块后能构建；重复运行不会覆盖手写逻辑；proto 和 mock 能重新生成；模板锁定的框架版本不依赖开发者本地环境。升级优先提供差异与迁移指南，不自动重写用户业务代码。

### 8.4 AI 辅助开发应使用结构化任务，而不是扩大隐式约定

为每个模块附最小 AGENTS.md/开发说明、架构边界、测试入口和一个可运行用例。给 Agent 的任务应包含允许修改的目录、兼容要求、验收命令与禁止引入的依赖。让 Agent 实现可测的小闭环，比为“方便生成”再增加一套宏大元编程机制更实用。

## 9. 分阶段迭代路线

以下版本号是开发目标，不代表仓库已有标签或已通过验收。框架版本是联合交付里程碑；CLI 与框架在同一主模块时使用同一 tag，layout 以固定 commit/tag 配套，不假设两个仓库必须同名发版。各任务的状态和验收记录统一维护在 [Pi_Development_TODO.md](Pi_Development_TODO.md)。

### 阶段 A｜安全与正确性基线：v0.2.4 / v0.3.0–v0.3.2

**目标：** 默认运行不出现明显越权，失败能可靠失败，基础生命周期无已知竞态。

**分批交付：**

| 版本 | 唯一主目标 | 范围边界 |
| --- | --- | --- |
| v0.2.4 | 运行时修复与 gRPC 临时关闭 | F01 缓解、F02、F03、F04、F26；保留 Start context 与普通响应超时语义 |
| v0.3.0 | 发布现有 Migration v2 并接通命令退出码 | F09、迁移命令清理；检查签名兼容性，不重做迁移 DSL |
| v0.3.1 | 配置与对外安全契约修复 | 地址/TLS、相关 Fatal、错误解包/脱敏、CORS/JWT 默认值、SQLite 支持声明；不设计完整 AppError/Principal |
| v0.3.2 | 固定版本脚手架的可重复交付 | Docker、CLI 模块路径与版本锁定、工具/测试入口、配置隔离、现有协议元数据 |

回归测试随每个修复交付；固定 framework/CLI/template 基线从第一版开始记录。标准测试入口逐步补齐，不能等 v0.3.2 才验证之前版本。纯构造、完整资源所有权和跨协议业务契约移至阶段 B。紧急修复可提前独立发布，表格不要求等待所有批次一起上线。

**交付物：** 固定版本安全模板、问题回归测试、错误/退出行为说明、可用容器构建、兼容性变更记录。

**出阶段条件：** 已启用功能的安全矩阵、重复启停、超时 race、订阅确认规则、失败迁移退出与容器 smoke 通过；尚未完成认证/授权/校验的 gRPC 用户示例保持关闭。关闭示例只是缓解，不能据此把 F01 完整修复标为完成。不用“已添加测试文件”代替“测试在对应版本运行成功”。

### 阶段 B｜稳定契约：v0.4.0 / v0.4.1

**目标：** 应用可以独立运行，也可以被 Fx/测试/宿主安全嵌入。

**工作包：** v0.4.0 交付纯构造、配置快照、生命周期 context/Wait 契约、资源所有权、Fx 装配、统一入口清理与最小 testkit；v0.4.1 交付传输无关 AppError/Principal、跨入口输入校验、BeginTx/context、冲突翻译、显式响应状态和安全的 gRPC 示例。新 API 优先通过兼容入口逐步接入，默认行为变化必须有迁移说明。

**交付物：** 6 份关键 ADR（构造/Fx/资源所有权；生命周期/context/Wait/Stop；Handler 执行/响应所有权；错误分类/公开消息；Principal/校验/授权边界；事务/方言/迁移恢复边界）、兼容 API、Fx 集成示例、最小 testkit、迁移说明和契约测试。ADR 先于对应行为变更，第三份在 F02 首轮修复时先记录兼容决定。

**出阶段条件：** 同进程多个 App 不互相破坏资源；借用资源不被误关闭；Start/Stop 并发语义可测；HTTP/gRPC 认证、校验与错误语义一致。对于不能保证的能力，写出限制而非静默兼容。

### 阶段 C｜内核与适配器收敛：v0.5.0 / v0.5.1

**目标：** 可选组件真正按需安装，核心不再依赖所有基础设施。

**工作包：** v0.5.0 统一协议服务、Worker、订阅、cron 与已承诺支持的后台循环的监督、取消和等待，接通 health/readiness/startup 与实例级观测；v0.5.1 实现核心/适配器依赖倒置、minimal/API 参考组合与真实数据库支持矩阵。迁移锁基础安排在 v0.3.0 验收，此处按实际部署需求完善租约、非事务性 DDL 的部分失败识别和人工恢复流程；Down/History/checksum 不默认纳入本阶段。

**交付物：** 清晰包边界、最小应用示例、实际依赖/构建/内存基线、数据库契约测试、适配器开发约定。

**出阶段条件：** 新 minimal 入口的 HTTP-only 编译依赖中不包含未使用的云消息/数据库实现；旧兼容门面的依赖负担单独披露。声明支持的每种数据库跑通完整业务流程；可选组件关闭后不产生后台任务或端口。基准只与自身固定场景比较，不作未经测量的高性能宣称。

### 阶段 D｜开发与交付体验：v0.6.0

**目标：** CLI 输出可重复、可二次生成、可升级的项目。

**工作包：** 在 v0.3.2 的可重复交付基础上，增加 minimal/api 模板组合、模块感知生成、dry-run/diff、冲突保护、doctor、升级差异指南与文档可执行验证；扩展 service 组合前先覆盖组合测试。用户模块的刷新、退出、密码更新等作为需求驱动的可选增量，另立验收，不阻塞 CLI 基础交付。

**交付物：** 模板版本清单、生成器 golden 与端到端测试、doctor、升级差异说明、完整示例文档。

**出阶段条件：** 干净机器或 CI 从固定 CLI+模板生成项目，能构建/迁移/启动/测试，再生成也不破坏手写代码；文档示例由 CI 执行验证。

### 阶段 E｜1.0 稳定化与真实业务验证：v1.0.0-rc.1 / v1.0.0

**目标：** 公共契约稳定，升级成本可预测。

**工作包：** rc 阶段冻结公开契约，至少在不同形态的真实应用中验证，例如普通业务 API 与独立 worker；执行性能/故障注入/长时间运行测试、安全审计、公开 API 兼容检查并确认废弃策略和支持矩阵。正式版只接收稳定化修复；rc 有契约变更时发布下一候选版并重验相关路径。

**出阶段条件：** 关键缺陷关闭；框架与模板升级路径经过验证；声明支持的功能都有验收证据。多租户、审计、可靠任务和 outbox 根据真实需求择优增加，不作为必须全部塞入 1.0 的清单。

## 10. 首批 PR 建议顺序

| PR | 范围 | 必须带的验收 | 目标 / 依赖 |
| --- | --- | --- | --- |
| 01 | 固定发布与本地基线、隔离热修复工作树、记录模板版本 | 不带本地 replace 的基线结果与已知失败 | v0.2.4；保护现有 Migration v2，不移动已有标签 |
| 02 | 默认关闭 gRPC 用户示例 | 默认未注册，HTTP 公开/保护路由仍符合预期 | v0.2.4；完整权限矩阵在 v0.4.1 恢复入口前验收 |
| 03 | HTTP 超时并发与响应所有权 | 迟到结果/panic/cancel 的真实 Handler race 测试 | v0.2.4；不切换默认执行模式 |
| 04 | Start/Stop 真正幂等 | 副作用计数、并发结果、启动失败回滚 | v0.2.4；保留现有 Start context 行为 |
| 05 | 订阅 panic/error 的确认规则 | 计数 Committer、失败不确认 | v0.2.4；重试/死信策略另行设计 |
| 06 | 收口工作树 Migration v2、发布框架 | Result/context/状态/锁、清理与兼容性测试 | v0.3.0；不重做已有实现 |
| 07 | layout migration up/plan/status 与退出码 | 失败非零、无成功日志、命令资源清理 | v0.3.0；依赖已发布框架 tag |
| 08 | errors.As/Join 分类、公开消息脱敏 | 包装/组合错误、取消/超时、未知错误 | v0.3.1；完整 AppError/gRPC mapper 在 v0.4.1 |
| 09 | 地址/TLS/CORS/JWT 配置修复 | 实际监听、半套 TLS、组合 CORS、令牌策略 | v0.3.1；按契约拆成多个小 PR |
| 10 | Docker、配置隔离、完整测试入口 | 镜像 smoke、子进程退出、模块测试清单 | v0.3.2；不在构建时 tidy |
| 11 | CLI 固定模板、module/dir、proto 路径与工具版本 | 新建→编译→再次生成、失败清理 | v0.3.2；保护手写文件 |

PR 编号表示建议顺序，可以继续按单一契约拆分，不表示必须恰好提交 11 个 PR。版本内具体任务与后续版本顺序见开发 TODO。

每个 PR 控制在一个主要契约内；不要让安全修复等待目录重构，也不要在同一个 PR 中同时更改公共 API、响应协议、模板结构与所有依赖版本。较大的阶段 B/C 重构应在这些回归测试保护下进行。

## 11. 验收与持续集成清单

| 测试域 | 必测场景 | 判定标准 |
| --- | --- | --- |
| 安全 | 未认证、错误主体、越权、管理权限；HTTP/gRPC 一致 | 无入口绕过，拒绝原因稳定 |
| HTTP 并发 | 超时、取消、迟到完成、panic、流式例外 | race 无报告，响应只提交一次 |
| 消息确认 | Handler 成功、error、panic、取消及后端重投递 | 成功才确认，后端能力边界明确 |
| 生命周期 | 重复/并发 Start/Stop、部分启动失败 | 一次副作用、确定结果、正确回滚 |
| 资源 | 自有/借用、关闭超时、关闭错误 | 不误关、不提前报成功、保留错误 |
| 配置 | 本机/IPv6、半套 TLS、示例密钥、未知方言 | 真实行为匹配配置，失败可诊断 |
| 错误契约 | 原始、包装、Join、内部错误 | 语义保留、公开内容安全 |
| 数据 | 真数据库迁移、重复注册、取消、嵌套事务 | 业务原子性与驱动差异已验证 |
| 命令 | migration 失败、task 退出、server 信号 | 退出码正确、释放资源 |
| 生成器 | 新 module、非默认目录、重生成、冲突 | 编译成功且不覆盖手写代码 |
| 发布 | 固定标签、候选版本、无本地补丁 | framework/CLI/template 三者一致 |
| 容器 | 非 root、存储权限、运行时配置、SIGTERM | 能启动/迁移/退出且无敏感文件 |
| 文档 | 实际路由/状态码/安全声明与生成内容 | 示例可执行，元数据无残留 |

建议命令入口分离，避免把网络/容器依赖混入快速单测：

```sh
# 以下是目标验收约定，需按仓库实际工具补齐，不表示目前已有这些目标。
make test-unit       # 所有包的快速测试
make test-race       # 并发/超时/生命周期关键路径
make test-integration
make test-generator
make test-smoke
make test-container
make check-generated # 重生成后不出现非预期 diff
```

每次框架发布验证固定模板；每次模板变更验证固定框架；候选框架用单独 CI 组合验证，不把临时 replace 提交到发布模板。发布前记录工具链、数据库版本、测试结果与已知限制。

测试清单必须按 Go module 列出范围：本地源码中的 datasource 目录已有 25 个独立 `go.mod`，根目录 `go test ./...` 不覆盖这些嵌套模块。为声明支持的适配器配置独立矩阵，未验证的适配器明确标为实验性。最小 testkit 在 v0.4.0 交付；之前的修复直接在真实框架执行路径补回归测试，不等待 testkit。

性能验收建议先建立自身基线：固定路由的延迟与分配、超时场景的 goroutine 增长、启动/关闭耗时、最小二进制体积、冷构建和热构建时间。无实测数据前不设拍脑袋的 QPS 承诺，不把依赖数量直接换算成运行时性能损失。

## 12. 暂不建议做的事情

**不建议换语言或重写技术栈。** 本次主要问题是安全、状态机、资源和交付契约，不是 Go/chi/Fx 选型导致。

**不建议继续扩张“支持某数据库/某云平台”的数量。** 先把少数常用组合完整测试，未验证适配器标为实验性即可。

**不建议在核心自研 ORM、DI、工作流、分布式任务队列或完整低代码平台。** 这些可以成为应用或集成，但不应成为普通 HTTP 服务的必要前置条件。

**不建议现在就把所有目录改成复杂 DDD/CQRS。** 真实业务复杂度出现后再拆功能模块；当前更有价值的是删除隐式初始化、统一身份与错误、缩短测试反馈路径。

**不建议通过大量兼容开关永久保留不安全默认。** 兼容模式应有明确范围和迁移期限；特别是未授权 gRPC、内部错误公开等问题，不应为了“零改动升级”无限保留。

## 13. 最终建议

Pi 最有价值的下一步，是把“很多组件可以接起来”推进为“业务开发者可以放心依赖这些组件的行为”。优先顺序应是：**安全与正确性 → 生命周期和公共契约 → 内核/适配器边界 → 模板与生成器闭环 → 真实业务验证。**

最值得保留的四项是：简洁 Handler、标准 context 的应用服务、基于构造函数的装配、统一事务执行器。最需要停止扩张的四项是：隐式全局配置、构造期副作用、巨型容器的直接实现依赖、未经端到端验证的“支持列表”。

## 附录 A. 源码证据索引

链接固定到本次读取提交；同一编号可包含构成调用链的多个文件。正文中的源码事实可沿对应编号复核，建议修复时在 PR 中补上最新行号和回归测试。

[S01] 版本与依赖：[layout go.mod](https://github.com/sllt/pi-layout/blob/16d306cec171b1e0167d6a98a6d2f44e8c99e009/go.mod)；[Pi master 运行入口](https://github.com/sllt/pi/blob/a9e70484466bc40353d2aec2fe5747f41483f558/pkg/kite/run.go)；[v0.2.3 标签对象](https://api.github.com/repos/sllt/pi/git/tags/01246796cd4565968d06b72ab1ebdb40bc2488aa)。

[S02] 生命周期启动与监督：[run.go](https://github.com/sllt/pi/blob/bdb884696a60422d7504b307776ff0fea8892c34/pkg/kite/run.go)。

[S03] App、Stop、Shutdown、Migrate：[pi.go](https://github.com/sllt/pi/blob/bdb884696a60422d7504b307776ff0fea8892c34/pkg/kite/kite.go)。

[S04] 后台 Worker：[lifecycle.go](https://github.com/sllt/pi/blob/bdb884696a60422d7504b307776ff0fea8892c34/pkg/kite/lifecycle.go)。

[S05] 构造与端口检查：[factory.go](https://github.com/sllt/pi/blob/bdb884696a60422d7504b307776ff0fea8892c34/pkg/kite/factory.go)。

[S06] 资源创建与关闭：[infra/container.go](https://github.com/sllt/pi/blob/bdb884696a60422d7504b307776ff0fea8892c34/pkg/kite/infra/container.go)。

[S07] HTTP Handler 并发处理：[handler.go](https://github.com/sllt/pi/blob/bdb884696a60422d7504b307776ff0fea8892c34/pkg/kite/handler.go)。

[S08] HTTP 错误与响应：[http/responder.go](https://github.com/sllt/pi/blob/bdb884696a60422d7504b307776ff0fea8892c34/pkg/kite/http/responder.go)。

[S09] HTTP 监听与中间件顺序：[http_server.go](https://github.com/sllt/pi/blob/bdb884696a60422d7504b307776ff0fea8892c34/pkg/kite/http_server.go)。

[S10] gRPC 服务与默认拦截器：[grpc.go](https://github.com/sllt/pi/blob/bdb884696a60422d7504b307776ff0fea8892c34/pkg/kite/grpc.go)。

[S11] gRPC 用户入口：[userservice_server.go](https://github.com/sllt/pi-layout/blob/16d306cec171b1e0167d6a98a6d2f44e8c99e009/internal/grpc/user/userservice_server.go)；[userservice_pi.go](https://github.com/sllt/pi-layout/blob/16d306cec171b1e0167d6a98a6d2f44e8c99e009/internal/grpc/user/userservice_kite.go)。

[S12] 协议注册与 HTTP 权限组：[server/http.go](https://github.com/sllt/pi-layout/blob/16d306cec171b1e0167d6a98a6d2f44e8c99e009/internal/server/http.go)；[router/user.go](https://github.com/sllt/pi-layout/blob/16d306cec171b1e0167d6a98a6d2f44e8c99e009/internal/router/user.go)。

[S13] 两层 CORS：[框架中间件](https://github.com/sllt/pi/blob/bdb884696a60422d7504b307776ff0fea8892c34/pkg/kite/http/middleware/cors.go)；[脚手架中间件](https://github.com/sllt/pi-layout/blob/16d306cec171b1e0167d6a98a6d2f44e8c99e009/internal/middleware/cors.go)。

[S14] JWT 与承载方式：[pkg/jwt/jwt.go](https://github.com/sllt/pi-layout/blob/16d306cec171b1e0167d6a98a6d2f44e8c99e009/pkg/jwt/jwt.go)；[middleware/jwt.go](https://github.com/sllt/pi-layout/blob/16d306cec171b1e0167d6a98a6d2f44e8c99e009/internal/middleware/jwt.go)。

[S15] 服务与输入 DTO：[service/user.go](https://github.com/sllt/pi-layout/blob/16d306cec171b1e0167d6a98a6d2f44e8c99e009/internal/service/user.go)；[api/v1/user.go](https://github.com/sllt/pi-layout/blob/16d306cec171b1e0167d6a98a6d2f44e8c99e009/api/v1/user.go)。

[S16] 事务与 Repository：[repository.go](https://github.com/sllt/pi-layout/blob/16d306cec171b1e0167d6a98a6d2f44e8c99e009/internal/repository/repository.go)；[repository/user.go](https://github.com/sllt/pi-layout/blob/16d306cec171b1e0167d6a98a6d2f44e8c99e009/internal/repository/user.go)。

[S17] 迁移执行链：[migration/migration.go](https://github.com/sllt/pi/blob/bdb884696a60422d7504b307776ff0fea8892c34/pkg/kite/migration/migration.go)；[server/migration.go](https://github.com/sllt/pi-layout/blob/16d306cec171b1e0167d6a98a6d2f44e8c99e009/internal/server/migration.go)。

[S18] 数据库与服务配置：[建表迁移](https://github.com/sllt/pi-layout/blob/16d306cec171b1e0167d6a98a6d2f44e8c99e009/migrations/20260206104000_create_users_table.go)；[配置示例](https://github.com/sllt/pi-layout/blob/16d306cec171b1e0167d6a98a6d2f44e8c99e009/configs/.env.example)。

[S19] 容器构建：[Dockerfile](https://github.com/sllt/pi-layout/blob/16d306cec171b1e0167d6a98a6d2f44e8c99e009/deploy/build/Dockerfile)。

[S20] 主服务与组合根：[cmd/server/main.go](https://github.com/sllt/pi-layout/blob/16d306cec171b1e0167d6a98a6d2f44e8c99e009/cmd/server/main.go)；[bootstrap.go](https://github.com/sllt/pi-layout/blob/16d306cec171b1e0167d6a98a6d2f44e8c99e009/internal/bootstrap/bootstrap.go)；[module.go](https://github.com/sllt/pi-layout/blob/16d306cec171b1e0167d6a98a6d2f44e8c99e009/internal/bootstrap/module.go)。

[S21] 其他命令：[cmd/task/main.go](https://github.com/sllt/pi-layout/blob/16d306cec171b1e0167d6a98a6d2f44e8c99e009/cmd/task/main.go)；[cmd/migration/main.go](https://github.com/sllt/pi-layout/blob/16d306cec171b1e0167d6a98a6d2f44e8c99e009/cmd/migration/main.go)；[server/task.go](https://github.com/sllt/pi-layout/blob/16d306cec171b1e0167d6a98a6d2f44e8c99e009/internal/server/task.go)。

[S22] CLI 与 proto：[bootstrap/init.go](https://github.com/sllt/pi/blob/bdb884696a60422d7504b307776ff0fea8892c34/pkg/kite/cli/bootstrap/init.go)；[user.proto](https://github.com/sllt/pi-layout/blob/16d306cec171b1e0167d6a98a6d2f44e8c99e009/api/proto/user/user.proto)；[CLI main.go](https://github.com/sllt/pi/blob/bdb884696a60422d7504b307776ff0fea8892c34/cmd/kite/main.go)。

[S23] 测试与工具：[lifecycle_test.go](https://github.com/sllt/pi/blob/bdb884696a60422d7504b307776ff0fea8892c34/pkg/kite/lifecycle_test.go)；[Makefile](https://github.com/sllt/pi-layout/blob/16d306cec171b1e0167d6a98a6d2f44e8c99e009/Makefile)；[smoke.sh](https://github.com/sllt/pi-layout/blob/16d306cec171b1e0167d6a98a6d2f44e8c99e009/scripts/smoke.sh)。

[S24] 文件隔离：[.gitignore](https://github.com/sllt/pi-layout/blob/16d306cec171b1e0167d6a98a6d2f44e8c99e009/.gitignore)；[.dockerignore](https://github.com/sllt/pi-layout/blob/16d306cec171b1e0167d6a98a6d2f44e8c99e009/.dockerignore)；[固定版本文件树](https://github.com/sllt/pi-layout/tree/16d306cec171b1e0167d6a98a6d2f44e8c99e009)。

[S25] 路由注册树：[route_registry.go](https://github.com/sllt/pi/blob/bdb884696a60422d7504b307776ff0fea8892c34/pkg/kite/route_registry.go)。

[S26] HTTP Handler 与注释：[handler/user.go](https://github.com/sllt/pi-layout/blob/16d306cec171b1e0167d6a98a6d2f44e8c99e009/internal/handler/user.go)。

[S27] 脚手架错误类型：[pkg/errcode/errcode.go](https://github.com/sllt/pi-layout/blob/16d306cec171b1e0167d6a98a6d2f44e8c99e009/pkg/errcode/errcode.go)。

[S28] 订阅处理与确认：[subscriber.go](pkg/pi/subscriber.go)。2026-09-25 本地复核 `handleSubscription` 的 panic recovery 与 Commit 调用链。

[S29] 未发布 Migration v2 工作树：[App 入口](pkg/pi/pi.go)、[执行器](pkg/pi/migration/migration.go)、[结果与选项](pkg/pi/migration/result.go)、[状态查询](pkg/pi/migration/plan.go)、[锁](pkg/pi/migration/lock.go)。本地路径会随开发变化，发布时在执行清单中补固定 commit/tag 与验收证据。

## 附录 B. 技术依据与隔离实验

[T01] Go 官方：[Data Race Detector](https://go.dev/doc/articles/race_detector)。用于共享变量竞态与运行期检测边界的依据。

[T02] Go 标准库：[database/sql](https://pkg.go.dev/database/sql)。用于 BeginTx/context 的语义依据。

[T03] golang-jwt 官方：[Parsing and Validating a JWT](https://golang-jwt.github.io/jwt/usage/parse/)。用于算法、issuer/audience 等验证策略。

[T04] WHATWG：[Fetch Standard](https://fetch.spec.whatwg.org/)。用于携带凭证的 CORS 与 Vary 策略。

[T05] Go 官方：[Go Toolchains](https://go.dev/doc/toolchain)。用于模块最低工具链要求和工具链选择。

### B.1 初评报告中记录的实验（本地复核未重跑）

环境：Go 1.23.2，linux/amd64，标准库，开启 race detector。实验仅复刻 F02 的共享变量结构，不导入或运行 Pi 仓库；本实验不代表原仓库测试完成。

```go
ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
defer cancel()
done := make(chan struct{})
var result any
var err error

go func() {
    time.Sleep(20 * time.Millisecond)
    result, err = "late result", nil
    close(done)
}()

select {
case <-ctx.Done():
    err = ctx.Err()
case <-done:
}
_ = fmt.Sprintf("%v %v", result, err)
<-done
```

执行：`GOTOOLCHAIN=local GOPROXY=off go test -race -count=1 ./...`。

结果：两次 `WARNING: DATA RACE`；分别对应 result 的读/写与 err 的写/写；`TestSharedResultAfterTimeout` 失败，测试退出状态为 1。末尾等待 done 只用于清理，不能为此前已经发生的未同步读写补上同步。

原仓库中的最终验收仍应把相同情景放进真实 `handler.ServeHTTP` 路径，并在修复后验证正常请求、超时与流式例外均无回归。
