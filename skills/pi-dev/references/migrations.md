# Migration v2 与一次性命令

## 源码与版本

框架看 `pkg/pi/migration/{migration,result,plan,lock,sql,redis,oracle}.go`、`pkg/pi/pi.go` 的 wrappers，
SQL 单次连接看 `pkg/pi/datasource/sql/{open,db}.go`。
layout 看 `cmd/migration/main.go`、`internal/migrationcmd/{command,config}.go`、`migrations/`。
完整运维边界另见框架 `docs/design/migration-v2.zh-CN.md` 与 layout `docs/migration.md`。

| 版本 | 调用方式 |
| --- | --- |
| Kite v0.2.3 / Pi v0.2.4 | `app.Migrate(defs)`；`migration.Run(defs, container)`，无结构化返回值；主要使用 UP |
| Pi v0.3.0–v0.4.1 | `app.Migrate(defs, opts...)`、`app.MigrateContext(ctx, defs, opts...)`；`migration.Run(ctx, defs, container, opts...)`，均返回 Result/error |

旧 `app.Migrate(defs)` 独立语句仍编译但丢弃错误；旧 Run 调用必须增加 ctx。
旧函数值、interface 方法签名和未命名字段的 Migrate 字面量可能不兼容，不能只凭语句能编译判断兼容。
App 另有 MustMigrate（失败 panic）、MigrationPlan/Status 及 Context 版本。

## 定义与调用

```go
package migrations

import (
    "context"
    "github.com/sllt/pi/pkg/pi/migration"
)

func CreateNotes() migration.Migrate {
    return migration.Migrate{
        Name: "create_notes",
        UpContext: func(ctx context.Context, d migration.Datasource) error {
            _, err := d.SQL.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS notes (id BIGINT PRIMARY KEY)")
            return err
        },
    }
}
```

在 `migrations.All()` 注册正数版本，通常沿用已有时间戳格式；只有文件没有 registry 不会执行。
旧 `UP func(Datasource) error` 仍支持，同时给 UP/UpContext 时后者优先。
Down/DownContext 只是预留；没有 down/history/checksum/schema diff。

调用方必须处理 `(result, err)`，错误返回到 main/部署流程，不无条件打印成功。
Result 包含已成功版本、跳过原因、失败版本及时间；用 errors.As 找 `*migration.VersionError` 的 version/name/op，
用 errors.Is 找 cause、ErrMigrationFailed、ErrMigrationPanic 等。begin/up/commit 和 rollback/release 的错误不要丢弃。
原始 Result 中有 error 接口；要输出 JSON 使用显式 DTO 将 error 转成字符串，避免编码成 `{}`。

## 状态、target、dry-run

- SQL 存在时是权威状态源；查询失败直接报错，不切换 Redis。
- 无 SQL 时用 Redis。两者读取精确版本集合；其余 legacy 链根据最大版本推断，`StatePrecise=false`。
- gap 是迁移 map 中低于已应用版本的未应用定义，不要求版本数字连续。Run 拒绝 gap；Plan 返回 PlanError item，Status 返回 Gaps。
- target 限制本次考虑的版本，不撤销已应用的更高版本；重复 up 跳过已执行版本。
- Plan/Status 不跑用户函数、不取得执行锁，但可以创建权威状态结构。
- dry-run 不开用户事务/执行用户函数，仍可能初始化已配置后端的迁移状态；显式启用锁时仍加锁。
- 表/键保留 `kite_migrations`、`kite_migration_locks`、`kite:migration:lock`，不要因品牌更名自动迁移这些持久化标识。

## 锁与失败清理

框架默认不开锁；`WithLock()` 显式开启，`WithLockTTL` 默认租约 15 分钟，`WithoutLock()` 关闭。
SQL 优先，只有未配置 SQL 才选择 Redis；竞争为 ErrMigrationLocked，后端故障为 ErrMigrationLockUnavailable。
锁无续租/fencing；执行超过租约可能重叠，所有执行者必须协作，deadline 应短于 TTL，业务代码必须响应 ctx。
owner 匹配后才释放旧锁，release 使用独立的短超时 context。

用户 error/panic/已观察到取消进入 rollback；cleanup 不继承已经取消的 caller context。
内置 SQL 的 BeginTxContext 将 caller context 绑定到事务；旧自定义 infra.DB 的 Begin fallback 仍不能取消连接池等待。
这不能强杀忽略 context 的迁移，也不是每种 backend panic/驱动异常都能恢复所有资源的保证。
MySQL DDL 可隐式提交，Redis EXEC 可能部分成功，多 datasource 没有分布式原子事务；网络 commit 错误可能结果不确定。
出现这些情况先检查数据和状态，再决定重试/补偿，不以“调用过 rollback”证明副作用已全部撤销。

## layout 的执行入口

```sh
go run ./cmd/migration plan
go run ./cmd/migration up --target=20260206104000
go run ./cmd/migration up --dry-run
go run ./cmd/migration up --timeout=10m --lock-ttl=15m
go run ./cmd/migration status
```

这是 v0.3.0 示例语法；target 请替换成项目的实际版本。无子命令默认 up；参数放在子命令之后。
所有子命令支持 target/timeout/config-dir；dry-run/lock/lock-ttl 只用于 up。
up 默认加锁，`--lock=false` 才关闭；timeout 默认 10 分钟、TTL 默认 15 分钟且必须大于 timeout。
plan/status 的 gap 在 layout 转为失败退出。stdout 单条 JSON、stderr 诊断；参数错误/帮助不输出执行摘要。
用户函数、数据库、锁、超时或清理失败返回非零退出码；SIGINT/SIGTERM 转换为 caller context 取消。

v0.4.0 起命令通过 Build/WithManagedSQL 激活稳定 SQL 句柄并由 Stop 清理，显式关闭 HTTP/gRPC/metrics，不走 Fx。
底层 `sql.OpenContext(ctx, cfg, logger, nil)` 仍可用于独立单次 SQL，调用者拥有 Close，不启动旧 NewSQL 的后台重连/指标循环。
新路径不安装全局 telemetry provider。`logging.NewWriterLogger` 可把日志全部写 stderr，writer 由调用方管理。
命令共享 config.LoadSnapshot，OS > `.APP_ENV.env`（默认 `.local.env`）> `.env`，不修改进程环境；与旧 EnvLoader 的副作用不同。
随 layout 的业务 DDL 仅支持 SQLite，命令明确拒绝其他方言；更换后端必须先适配业务 DDL 和检查逻辑。

验证 Run 的成功/失败/取消、重复/gap/target/dry-run、锁竞争/过期/后端故障，
再验证命令进程退出码、摘要和连接关闭。实库测试只在专用临时数据库执行，不能对当前业务库直接跑测试清理。
