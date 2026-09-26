# ADR-002：v0.2.4 启停一次执行与完成语义

日期：2026-09-26。状态：实现并通过定向验证，待发布。关联 V024-04/05、F03/F04。

## Start

应用内部区分 new、starting、running、stopping、stopped。公开 API 签名不变。

- 首个 `Start(ctx)` 执行启动；该 ctx 仍是运行期父 context。取消它会取消 Worker
  和订阅处理，并由 `RunContext` 驱动关闭。直接使用 `Start` 的宿主仍需调用 `Stop`。
- starting 期间的其他 Start 等待同一结果，各自的 ctx 只约束等待；等待超时不会
  取消首个调用方的启动。running 下重复 Start 返回成功，不再执行 Hook、监听或 Worker。
- 启动失败先进入一次回滚，等待者读取同一启动错误及已取得的回滚错误。回滚使用
  `SHUTDOWN_GRACE_PERIOD`；预算耗尽时启动可返回错误，实际关闭仍由唯一执行者负责。
- stopping/stopped 后不允许重新启动，包括初始化失败后的实例；重试需创建新 App。
- Stop 与 startup 并发时，先取消运行期 context，再等待 startup 不再增加资源，
  才执行清理。已启动 HTTP 后发生 gRPC 监听失败时，HTTP 监听也会回滚。

`OnStart`、`Go` 的登记必须在 Start/Stop 开始前完成。`OnStop` 可在运行期或尚未
结束的 OnStart 中登记清理；一旦真正进入清理阶段就拒绝新登记。登记与状态检查由
同一把锁保护，执行 Hook 时不持有该锁。

Hook 不应同步调用并等待同一 App 的 Start/Stop：启动等待自身完成、关闭等待自身
Hook 返回都会形成自等待。由宿主驱动启停，Hook 负责具体资源并配合 context 取消。

## Stop

首个 Stop 创建唯一关闭执行者，其 ctx 同时提供清理预算与该调用方的等待预算。
后来的 Stop 只等待同一操作，自己的 ctx 不会替换或取消执行者的 ctx。

| 情形 | 返回与状态 |
| --- | --- |
| 清理尚未结束，等待方 ctx 到期 | 返回该等待方的 context 错误；应用仍为 stopping |
| 首个 Stop 的清理预算到期 | 调用方可返回；错误会保留，后续 Stop 不会重新执行清理 |
| 正在执行的 Hook 忽略取消 | 等待其返回后继续收尾；不会提前报告清理成功 |
| Worker 尚未退出 | 继续等待，不关闭其依赖的 Container；等待方仍可按自己的预算离开 |
| 清理完成 | 缓存组合错误，再进入 stopped；后续 Stop 返回同一结果 |
| 清理已完成，新调用方传入已取消 ctx | 优先返回已缓存的真实结果 |
| 从未 Start 就 Stop | 清理已构造资源一次；该实例随后不可 Start |

OnStop 逆序执行；错误与 panic 被收集。执行预算用尽时不再开始剩余 Hook，并返回
context 错误；已进入的 Hook 不会重跑。Container 关闭错误和执行预算错误也会保留。
没有通过重试 Stop 来补跑已跳过 Hook 的机制。

这里的 stopped 表示该次清理流程已经返回，其结果可能是错误；不承诺被跳过的 Hook
已经执行，也不承诺任意外部 SDK 的强制关闭已经完成全部内部工作。HTTP/gRPC
继续使用现有 graceful/force-close 机制；cron、telemetry 和适配器内部循环的统一
监督属于后续版本。

## 兼容与验证

普通 `Start`、`Stop`、`Shutdown`、`RunContext` 调用保持可编译。首个 Start context
的运行期父级语义不变；本轮没有切换为 Fx 启动预算与运行期 context 分离的契约。
`RunContext` 的命令行模式保留原先执行命令的路径。

有意改变：重复 Start 不重复副作用；重复 Stop 不掩盖旧错误；等待预算到期不代表
清理完成；失败后的实例不可重启。注册过晚的 Hook/Worker 被拒绝并记录日志。

`lifecycle_concurrency_test.go` 覆盖串行/并发计数、共享启动及回滚结果、等待方隔离、
关闭超时后的真实结果、Worker 资源顺序、启动与关闭竞争、关闭前未启动、panic 和
并发 Worker 登记；`lifecycle_test.go` 覆盖真实 HTTP 监听复用及部分启动失败回滚。
