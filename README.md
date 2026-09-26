# Pi

一个为微服务开发而设计的 Go 语言框架。

项目已由 Kite 更名为 Pi，CLI 命令为 `pi`，脚手架为 [pi-layout](https://github.com/sllt/pi-layout)。新 module 从 v0.2.4 开始发布；v0.3.0 提供 Migration v2，见[发布说明](docs/releases/v0.3.0.zh-CN.md)和[迁移兼容性](docs/design/migration-v2.zh-CN.md)。历史 `v0.2.3` 仍使用旧路径。

## 核心特性

### v0.3.1 配置与错误响应

- `HTTP_ADDR`、`GRPC_ADDR`、`METRICS_ADDR` 接受 `host:port`，优先于对应 `*_HOST`/`*_PORT`；`[::1]:0` 支持 IPv6 和随机端口。启动后通过 `HTTPAddress()`、`GRPCAddress()`、`MetricsAddress()` 获取实际地址。
- `*_ENABLED=false` 显式禁用；旧 HTTP/GRPC `PORT=0` 仍回落默认端口，旧 `METRICS_PORT=0` 仍禁用。端口冲突由 Start 返回，不在注册/构造阶段 Fatal。
- HTTP 使用 `CERT_FILE`/`KEY_FILE`，gRPC 使用 `GRPC_CERT_FILE`/`GRPC_KEY_FILE`；必须成对、可解析且匹配，错误导致启动失败。
- CORS 由框架统一处理，默认不允许跨域。`CORS_ALLOWED_ORIGINS` 是逗号分隔的完整 origin；另有 `CORS_ALLOWED_METHODS`、`CORS_ALLOWED_HEADERS`、`CORS_ALLOW_CREDENTIALS`、`CORS_EXPOSE_HEADERS`、`CORS_MAX_AGE`。新键优先于旧 `ACCESS_CONTROL_*` 键；通配 origin 不能同时允许 credentials。
- HTTP 错误支持包装/Join。取消优先，其余按深度优先、从左到右选第一个状态错误，业务码与消息来自同一错误。5xx/未知错误默认隐藏内部消息；实现 `PublicMessage() string` 可显式提供安全消息。Handler 响应的 `X-Request-ID` 对应服务端错误日志中的 trace_id。

安全兼容变化：依赖默认 `*` 的跨域客户端需要配置来源；未知错误不再把原始 Error 文本发给客户端，错误响应也不会返回附带的成功数据/文件。

- **简洁的 API 语法** - 轻松定义路由和处理器
- **RESTful 规范** - 默认遵循 REST 最佳实践
- **配置管理** - 灵活的配置加载和管理
- **完整的可观测性** - 内置日志、追踪和指标支持
- **认证中间件** - 开箱即用的认证和自定义中间件
- **gRPC 支持** - 原生支持 gRPC 服务
- **HTTP 服务客户端** - 内置熔断器的 HTTP 客户端
- **发布/订阅** - 简化的消息队列集成
- **健康检查** - 所有数据源的自动健康检查
- **数据库迁移** - 内置迁移管理工具
- **定时任务** - Cron 任务调度支持
- **动态日志级别** - 无需重启即可更改日志级别
- **Swagger 文档** - 自动生成和渲染 API 文档
- **文件系统抽象** - 统一的文件操作接口
- **WebSocket** - 原生 WebSocket 支持

## 快速开始

### 安装

```bash
go get github.com/sllt/pi
```

### 简单示例

```go
package main

import "github.com/sllt/pi/pkg/pi"

func main() {
    app := pi.New()

    app.GET("/greet", func(ctx *pi.Context) (any, error) {
        return "Hello World!", nil
    })

    app.Run() // 监听 localhost:8000
}
```

运行应用：

```bash
go run main.go
```

访问 `http://localhost:8000/greet` 查看结果。

### 使用数据库

```go
package main

import (
    "fmt"
    "github.com/sllt/pi/pkg/pi"
)

func main() {
    app := pi.New()

    app.GET("/redis", func(c *pi.Context) (any, error) {
        val, err := c.Redis.Get(c, "key").Result()
        if err != nil {
            return nil, err
        }
        return val, nil
    })

    app.GET("/sql", func(c *pi.Context) (any, error) {
        var result int
        err := c.SQL.QueryRowContext(c, "SELECT 2+2").Scan(&result)
        if err != nil {
            return nil, err
        }
        return result, nil
    })

    app.Run()
}
```

## 支持的数据源

Pi 支持广泛的数据存储和服务：

| 类别 | 数据源 |
|------|--------|
| **关系型数据库** | MySQL, PostgreSQL, SQLite, Oracle |
| **NoSQL 数据库** | MongoDB, CouchBase, ArangoDB, SurrealDB |
| **键值存储** | Redis, KV-Store |
| **时序数据库** | InfluxDB, OpenTSDB |
| **搜索引擎** | Elasticsearch, Solr |
| **列式存储** | Cassandra, ScyllaDB, ClickHouse |
| **图数据库** | Dgraph |
| **消息队列** | PubSub (Kafka, Google PubSub 等) |
| **文件系统** | 本地文件系统、S3、GCS 等抽象文件系统 |
| **数据库路由** | DBResolver (多数据源管理) |

## 项目结构

```
pi/
├── pkg/pi/              # 核心框架代码
│   ├── datasource/        # 数据源连接器
│   ├── metrics/           # 指标收集
│   └── ...
├── examples/              # 示例应用
│   ├── http-server/       # HTTP 服务示例
│   ├── grpc/              # gRPC 示例
│   ├── using-migrations/  # 数据库迁移示例
│   └── ...
└── docs/                  # 文档
```

## 文档

- [GoDoc](https://pkg.go.dev/github.com/sllt/pi) - API 参考文档
- [示例目录](examples/) - 更多可运行示例

## 许可证

本项目采用 [Apache License 2.0](LICENSE) 许可证。

## 贡献

欢迎贡献代码、提出建议或报告问题。
