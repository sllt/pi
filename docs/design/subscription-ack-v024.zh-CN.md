# v0.2.4 订阅处理与自动确认边界

日期：2026-09-26。关联 V024-06、F26。此处定义框架的自动 Commit 条件，不承诺
所有消息后端均支持相同的重投递、事务或 exactly-once 语义。

## 自动确认规则

- 成功收到消息且 Handler 返回 nil，提交前未观察到运行期或消息 context 取消，
  并且存在 Committer：框架调用一次 `Commit()`。
- Handler 返回 error 或发生 panic：返回处理错误，框架调用 Commit 的次数为零。
- 读取失败、没有消息、调用前已经取消：不执行成功确认。
- 消息接收后或处理过程中观察到取消：不开始处理或不确认，即使 Handler 吞掉取消并返回 nil。
- Handler 使用的 context 保留消息的值与追踪信息，同时接受运行期取消。
  对返回 detached message context 的后端也有效。传入下游的是 `c.Context`；
  `c.Request.Context()` 仍为后端提供的消息 context。

处理错误进入现有 2 秒重试间隔，成功后间隔恢复为零；关闭会中断重试等待。
本次没有增加主动 Nack、死信队列或通用重试预算。

取消检查与外部确认不是原子操作；取消恰好发生在最后一次检查之后时，已经开始的
Commit 不能撤销。`Commit()` 没有错误返回或 context 参数，框架也不能据此确认
broker 已持久接受结果。Handler 自己手动 Commit 不属于上述自动确认计数保证。

## 当前适配器差异（源码复核）

| 后端 | 当前 Commit 行为 | 限制 |
| --- | --- | --- |
| Kafka | `CommitMessages(context.Background(), msg)` | 错误只记录；同分区后续 offset 的提交可越过前面失败的消息，不能据零次 Commit 承诺该消息必然重投 |
| Google Pub/Sub | `Ack()` | 框架失败分支没有主动 Nack；重投与租约取决于客户端和服务端 |
| MQTT | 调用 Paho message 的 `Ack()` | 当前 options 未禁用 Paho 默认自动确认；框架零次显式 Commit 不等于 broker 未收到确认，QoS 0 也无此保证 |
| Redis Pub/Sub | 空操作 | 非持久消息通道，没有确认或重投保证 |
| Redis Streams | XACK，有限重试后记录错误 | 错误不向框架返回；pending 的重新处理依赖该适配器的消费配置 |
| NATS JetStream | Ack；部分实现失败后尝试 Nak | 通用 Committer 不暴露 Nak；错误只记录 |
| SQS | DeleteMessage | 失败只记录；未删除消息是否重投取决于可见性超时等配置 |
| Azure Event Hubs | processor 模式更新 checkpoint；直接读取模式仅日志 | checkpoint 是进度，不是逐条业务事务；后续推进可能越过失败事件 |

对应代码位于 `pkg/pi/datasource/pubsub/*/message.go`、NATS 的 `committer.go` 和
`pkg/pi/datasource/redis/messages.go`。MQTT 的自动确认默认值同时对照本地 Paho
v1.5.1 的 ClientOptions 与 Pi `getMQTTClientOptions`，本轮没有改变这些适配器策略。

## 验收

计数 Committer 测试覆盖成功、error、panic（含 nil panic）、无 Committer、无消息、
读取错误、读取前/后取消、处理内取消和消息 context 取消；另外验证 detached
消息保留值并响应运行期取消，以及取消时退出订阅循环。均运行 race detector。
未运行实际 broker 集群验收；本版关闭的是 panic 被误判为处理成功的框架缺陷。
