# ADR-003：HTTP 超时与响应所有权（首轮修复）

日期：2026-09-26。状态：已采纳并通过定向验证，尚未发布。
关联：F02、V024-03；流式响应的完整契约留待后续设计。

## 决策

保留普通 HTTP Handler 的异步执行和响应超时行为。业务函数在工作 goroutine
中运行；成功、错误与恢复后的 panic 都通过一个容量为 1 的结果通道交付。
`ServeHTTP` 独占状态码、响应头和响应体的写入。

响应 goroutine 在启动业务函数前保存原始请求 context 和 responder，之后不再
读取业务侧的 `*Context`。`Context.Trace()` 或 Pi middleware 替换 `c.Context`
时，不会与超时分支竞争，也不会改变外层用于判定超时/客户端取消的 context。

容量为 1 的通道允许业务函数迟到返回；响应已完成时，结果发送仍能结束。
迟到的 data、headers 不再被编码或提交；迟到 error/panic 仍记录日志。

## 保留的兼容行为

| 场景 | 行为 |
| --- | --- |
| 普通成功或业务错误 | 保留现有 responder、envelope、状态码和自定义响应头规则 |
| 请求期限到达 | HTTP 408，`request timed out` |
| 父 context 被取消 | HTTP 499，`client closed request`；客户端断连时未必能收到该响应 |
| 选中 panic 结果 | HTTP 500，公开消息仍为 `Internal Server Error`，日志保留原因和堆栈 |
| 完成与取消同时就绪 | 任一分支均可胜出；只提交一次响应，不混合两份结果 |
| `REQUEST_TIMEOUT=0` | 不新增框架期限，仍遵循父 context |
| WebSocket upgrade | 保留不附加普通 HTTP 超时的行为；本次未重新设计升级/流式写入协议 |

公开 `Handler` 签名不变。不新增全局锁、后台任务监督器或强制终止机制。

## 请求期对象与业务责任

- `*Context` 由业务执行链使用；框架不保证应用自行启动的多个 goroutine
  可以并发修改同一个 `*Context`。需要并发工作时传递标准 `context.Context`
  和独立的业务数据。
- `Context.Bind` 会读取请求体，JSON/binary binder 还会替换底层请求的 Body；
  multipart binder 会更新表单缓存并可能创建临时文件。这些操作仍属于请求期，
  本次没有预读、复制或接管输入生命周期。
- 超时响应返回后，HTTP server 可关闭 Body、清理 multipart 临时文件；
  长任务应提前提取所需数据并配合取消，不能继续依赖请求体、临时上传文件或 writer。
  阻塞中的 Body 读取及 multipart 与取消竞争不在本轮无竞态保证内，后续需单独验收。
- 普通 Pi Handler 没有公开原始 writer；外部 `net/http` middleware 如果捕获它，
  仍须遵守 `ServeHTTP` 返回后不再写入、业务执行期间不并发写入的约定。
- HTTP 超时不会回滚已经发生的业务副作用，也不会终止忽略取消的 goroutine。
  DB/HTTP 等下游调用应使用传入的 `c.Context`。`c.Request.Context()` 仍保留原请求
  context 的既有语义，不把它当作框架附加期限的来源。

本次验收只关闭已覆盖的结果传递与外层 Context 访问竞态；不能从通道修复推导
任意用户 Handler、multipart、WebSocket 或 SSE 都已获得完整生命周期保证。

## 验证

真实 `handler.ServeHTTP` 测试覆盖原有成功/error/panic、自定义头、父期限、客户端
取消，以及新增的迟到成功/error/panic、Context 替换、100 次完成/取消竞争和
WebSocket 期限豁免。验证命令和原始失败结果见
[第一批修复记录](../verification/v0.2.4-first-batch.zh-CN.md)。
