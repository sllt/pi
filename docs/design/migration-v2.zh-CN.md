# Migration v2：v0.3.0 契约

## API 与兼容性

| 调用方式 | v0.2.4 → v0.3.0 |
| --- | --- |
| `app.Migrate(defs)` 作为独立语句 | 仍能编译，但会丢弃结果和错误；部署代码必须处理返回值 |
| `migration.Run(defs, container)` | 不再编译；必须增加第一个 context 参数，改为 `migration.Run(ctx, defs, container)` 并处理返回值 |
| 返回值 | 改为 `(migration.Result, error)`；支持可变参数 `...migration.Option` |
| 旧函数值、接口 | 不再匹配无返回值的函数或接口方法，必须调整类型或显式写适配函数 |
| `Migrate{UP: func(Datasource) error}` | 保留；新代码使用 `UpContext`，同时指定时优先 `UpContext` |
| 未命名字段的 `Migrate{...}` 字面量 | 字段扩展后不兼容，改用命名字段 |
| 版本编号 | 必须为正数；0 为历史“尚未迁移”标记，不能作为可执行版本 |
| `Down` / `DownContext` | 预留，不执行；up 失败不调用 Down |
| 存储标识 | 保留 `kite_migrations`、`kite_migration_locks`、`kite:migration:lock`，避免改名导致重复执行 |

```go
defs := map[int64]migration.Migrate{
    1: {UP: func(d migration.Datasource) error { // 旧定义仍支持
        _, err := d.SQL.Exec("CREATE TABLE IF NOT EXISTS demo (id BIGINT PRIMARY KEY)")
        return err
    }},
    2: {Name: "seed_demo", UpContext: func(ctx context.Context, d migration.Datasource) error {
        _, err := d.SQL.ExecContext(ctx, "INSERT INTO demo (id) VALUES (1)")
        return err
    }},
}
result, err := app.MigrateContext(ctx, defs, migration.WithLock())
if err != nil {
    var versionErr *migration.VersionError
    if errors.As(err, &versionErr) {
        log.Printf("version=%d op=%s", versionErr.Version, versionErr.Op)
    }
    return err
}
log.Printf("applied=%v", result.AppliedVersions())
```

`Result` 保留已成功版本、跳过原因、失败版本和执行时间。begin/up/commit 失败带
`VersionError`，可用 `errors.Is` 继续检查底层原因；用户 panic 同时带 `ErrMigrationPanic`。
失败时停止后续版本。`MustMigrate` 仅适合明确接受 panic 的程序。

## 状态与执行

- SQL 存在时是唯一权威状态源；查询失败直接返回错误，不偷偷切换 Redis。
- 无 SQL 时优先 Redis。两者读取精确版本集合；其余 datasource 链根据最大版本推断，返回 `StatePrecise=false`。
- 只检查迁移 map 中定义的缺口，不要求编号连续。低于已执行最高版本但未执行的定义属于 gap。
- target 限制本次考虑的版本，不撤销已经执行的更高版本；重复 up 跳过已执行版本。
- Plan 的 gap 是 `PlanError` item；Status 通过 `Gaps` 暴露。layout 将这两种情况也转换成非零退出码。
- Plan/Status 不运行用户函数，但可能初始化权威状态表。dry-run 不开用户事务、不执行用户函数，仍可能初始化各配置后端的状态结构；显式请求锁时仍加锁。
- 迁移源码没有 checksum，修改已执行文件不会重跑或报 checksum 冲突。

## 锁、取消与资源所有权

框架默认不开锁；`WithLock()` 开启 SQL/Redis 租约。默认 TTL 为 15 分钟，
`WithLockTTL` 可修改；不支持自动续租、fencing 或无感接管。租约到期后另一进程可获得锁，
旧 owner 释放时不会删除新 owner 的锁。所有执行者都必须遵守锁协议。
后端故障返回 `ErrMigrationLockUnavailable`，真实竞争返回 `ErrMigrationLocked`。

调用方应设置短于租约的 deadline，并让用户迁移遵循 context。运行时不强杀忽略取消的函数。
用户 error/panic/观察到的取消会回滚，rollback 和 release 使用独立的 5 秒清理 context。
驱动或自定义后端若忽略 context，清理本身仍可能阻塞。

内置 SQL `BeginTxContext` 保持调用方 context；原 `Begin()` 与嵌入的标准 `BeginTx`
签名不变。自定义 `infra.DB` 可实现可选的
`BeginTxContext(context.Context, *sql.TxOptions) (*sql.Tx, error)`（这里返回 Pi SQL wrapper）。
仅实现旧 `Begin()` 时可以继续使用，但等待连接的 Begin 本身不能取消。

一次性命令可调用 `sql.OpenContext(ctx, cfg, logger, nil)` 获取可关闭的数据库，
传给 `migration.Run/Plan/Status` 的 `infra.Container`。该 API 校验连接，不创建 HTTP、
gRPC、metrics 服务，不启动重连/指标循环，也不安装 telemetry provider；调用方负责 `Close()`。
它不替换 `pi.New()`，完整纯构造和统一后台资源管理仍属于 v0.4.0。

## 原子性边界

SQL 事务不是跨库分布式事务。MySQL DDL 可隐式提交；Redis EXEC 中某条命令失败
不会回滚其他已经执行的命令，状态写入也可能已经执行。多 datasource 提交可能部分成功。
网络中断时 commit 结果可能不确定。遇到这些情况应先检查实际数据和迁移状态，再人工修复或重试，
不能把返回 error 或 rollback 调用等同于所有业务副作用都被撤销。

v0.3.0 实库验收覆盖 SQLite、MySQL、PostgreSQL、Redis；其他适配器保留既有实现和 mock 测试，
不据此声明所有外部后端已经生产验收。Supabase/CockroachDB 使用 PostgreSQL 占位符，未做服务实测。
