# 开发、验证与版本联动

## 计划与事实分开

用户要求按框架规划推进时，读取当前工作区采用的 `Pi_Development_TODO.md` 和
`Pi_Architecture_Review_and_Roadmap.md`（若可用）。前者维护任务 ID/验收/发布，后者说明问题证据与架构边界。
这些可能是未提交的工作文档；发布包没有它们时，以用户给定计划和公开 API 文档为准，不把缺文件当成所有业务开发的阻塞。
不要把另一个 `ROADMAP*.md` 或旧 skill 的阶段重新并入当前计划。
普通业务需求不强制先完成框架整个 Roadmap，也不自动推进下一版本。

核对基线为 2026-09-26：

| 节点 | 已知状态或计划 |
| --- | --- |
| Pi v0.2.4 / layout v0.2.4 | 已发布：运行时与 gRPC 默认关闭修复；框架 `368447b` / layout `9d3a894` |
| Pi v0.3.0 / layout v0.3.0 | 已发布：Migration v2 与独立迁移命令；框架 `7a93b31` / layout `6db9888` |
| v0.3.1 | 监听/TLS、错误脱敏、CORS/JWT；框架 b8c80c5 / layout ec87477 |
| v0.3.2 | 固定模板、生成/容器交付；框架 0d2c067 / layout a5b43ac |
| v0.4.0 | Build/快照/所有权/Fx/testkit；框架 cc1c85a / layout 2018162 |
| v0.4.1 | 统一错误/Principal/校验、事务/并发写、显式响应、gRPC 安全选用；以实际 tag 核对发布状态 |
| v0.5.0 之后 | 统一监督、健康与观测、适配器边界、模板组合和稳定性；按最新 TODO 查具体归属 |

此表是基线，不是永久的“最新版”查询。工作树、tag、常量和运行中的二进制分别核实。
Build、testkit、response.Stream 已有实际 API；Down、History、checksum、锁续租、完整重启 supervisor 仍不能当作已交付。
Stream 的同步写入/取消范围见 framework.md，不把它扩大成任意异步 writer 都安全。

## 定向验证

先确认当前 module、Go 版本和测试的外部依赖。基线根 go.mod 是 Go 1.24.0、layout 是 1.24.10，
本地 workspace/发布验收用过 1.25.0；`GOWORK=off` 可能选中不同 toolchain，记录实际版本，不为了测试顺手改最低版本。

| 改动 | 优先核对 |
| --- | --- |
| Handler/路由/生命周期 | 对应 pkg/pi 测试；真实请求、并发启停、迟到结果、取消与关闭顺序，必要时 race |
| 迁移 runtime | `go test ./pkg/pi/migration`；用户 error/panic、begin/commit/rollback、取消、gap、锁与状态源 |
| 单次 SQL/日志 writer | 相关文件的定向测试，验证连接关闭和 stdout/stderr 分流 |
| layout migration | `go test ./internal/migrationcmd ./test`；实际二进制退出码/JSON/锁和取消路径 |
| layout 业务模块 | `./test/server/...`、`./internal/server` 跨协议矩阵、`./internal/repository` 实库事务、Fx 图与授权拒绝路径 |
| gRPC/CLI template | make generator-integration、layout generator/check-generated；实际产物编译与手写保护，不只比较模板文本 |
| 独立 datasource | 进入其 own go.mod 目录或显式 go -C；根 module 测试不替代子模块验证 |

Pi 迁移测试中的 `PI_MIGRATION_TEST_mysql_*`、`PI_MIGRATION_TEST_postgres_*` 和
`PI_MIGRATION_TEST_REDIS_ADDR` 可启用实库；只指向自建可丢弃实例，这些测试会清理专用表/键。
不因存在测试脚本就对实际业务库运行迁移。无需为纯文档变更运行所有外部服务测试。
定向验证通过后，只有新变更、失败或未解决的风险才扩大/重复验证。

## 兼容与改名

公开 API 变更要查直接调用、函数值、接口、结构体字面量、生成模板和旧文件，而不只查 happy path。
取消策略、默认锁、确认消息和响应状态的行为变化也属于兼容性，需对应说明与用例。
如果用户只需要局部修复，不顺带改所有 datasource、重排布局或搬成 monorepo。

Pi 是 Kite 的 module/品牌更名；持久化与协议兼容项不自动随包名改变。
除了 migration 表/锁键，改遥测相关逻辑还要核对 `PI_TELEMETRY` 对旧 `KITE_TELEMETRY` 的兼容，以及既有 trace metadata。
多个未提交主题混合时保留原工作区，必要时用独立 worktree 整理当前任务；不要 reset/clean 掉用户修改来准备发布。

## 用户授权发布时

1. 固定框架代码、版本常量、例子和模板，记录实际检查与能力边界。
2. 创建/推送所需 framework commit/tag；不要移动已经发布的 tag。CLI 跟随同一主 module 的版本来源。
3. 验证实际远程 module 内容与 checksum，再让 layout require 该版本；本地 workspace 联调不是这一步的替代。
4. layout 用 `GOWORK=off`、无 replace 构建/验证，提交所需模板变更并发布相应 ref。
5. 从新目录按用户路径执行固定版本 CLI init，核对 .pi-template.json 的版本三元组，运行代表性消费、检查依赖文件未变。
6. 记录 framework、CLI、layout 的 tag/commit、toolchain、后端矩阵、生成物与未验收范围；更新此 skill 涉及的契约。

根 tag 不自动发布所有嵌套 module：若交付独立 adapter，需要核对其 module 路径、前缀 tag 与自己的依赖。
本地 tag、候选 file proxy、远程 tag、公开 module 下载和业务消费成功是不同证据；失败时写出实际状态，不互相替代。
发布记录中不要暴露真实 env/凭据；只包含复现所需的版本、命令和结果。
用户要求排除过程 Markdown 时，明确暂存交付文件；不要把计划、审查、验证日志混进 commit，也不要用通配忽略掩盖需要维护的 README/API 文档或 skill。
