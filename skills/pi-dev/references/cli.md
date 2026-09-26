# CLI 与模板（v0.4.1）

命令以 cmd/pi/main.go 为准；CLIVersion 与 version.Framework 共用来源，查询用 `pi --version`。

```sh
pi init --module example.com/company/orders ./orders
pi create all order
pi migrate create AddOrders
pi wrap grpc server --proto api/order.proto --out internal/grpc/order
pi wrap grpc client --proto api/order.proto --out internal/client/order
```

## init

默认 template ref 与 CLI 版本相同，`--ref` 可显式指定 tag/commit，`--template` 可指定 Git 来源。
Directory 与 module 独立；旧 `pi init myapp` 仍可用，module 默认目录 basename。
`.pi-template.json` 记录 CLI/framework/template ref/commit 及 verified 状态。

在临时目录 checkout 固定 ref，替换 imports/proto go_package/序列化 protobuf 描述符，格式化后执行只读 build，再交付目录。
不 tidy、不复制本地 .env/go.work；冲突、校验失败、可处理的取消不留下成功目录。
`--offline --template /local/git/template` 需要本地 Git 模板，跳过 build 且明确 verified=false，不能当成已验证交付。
编译通过不代表完成业务配置/迁移/启动；实际消费还应检查无 replace、依赖文件不变与 smoke。
v0.4.1 init 的 AST 改写会把 protobuf rawDesc 字节数组排成单行；首次 `make generator` 会恢复 protoc 的多行格式。
初始化后先生成一次，再执行 `make check-generated` 验证稳定性；该已知差异应只有排版，描述符字节与其他代码仍需一致，不能据此忽略其他生成差异。

## create / migration

create 先整体渲染/格式化再写，已有手写文件保留；多文件交付非文件系统事务，SIGKILL 后可能需要重跑。
默认 internal/<type>，带路径参数使用其路径，不自动理解成业务模块 namespace；create all 自定义路径冲突会失败。
骨架不包含 DTO/types/router/Fx/迁移/测试，按 [layout.md](layout.md) 补齐。

migrate 只有 create，不执行迁移。函数名必须是 Go 标识符，生成 Name/UpContext，AST 保留现有 All map 项与注释。
不改进程 cwd，拒绝同秒冲突/重复函数；不能识别动态 All 实现时明确失败，不静默重写用户注册逻辑。
执行用应用 `go run ./cmd/migration up|plan|status`。

## gRPC

wrapper 不替代 protoc。server 输出 <service>_pi.go、health_pi.go、request_pi.go、<service>_server.go；client 输出 <service>_client.go、health_client.go。
手写 server skeleton 保留；其他文件只有带受支持的生成器标记才允许覆盖。共享 request 文件属于不同 proto 时明确拒绝，使用独立输出包；当前不是完整模块级生成器。
方法指标/health 使用完整 proto service name，版本头来自实际 CLI；health 按 App 分配，stream helper 避免多 service 名字冲突。
Bind 仅克隆相同 protobuf 类型，手写 adapter 显式转换业务 types；不按反射字段索引绑定。
错误通过 MapError，Principal interceptor 还需在应用装配，service 仍须授权；生成代码不是认证开关。

layout `make init` 将固定工具安装到 .tools/bin；工具版本看 Makefile/scripts/generate.sh，protoc 33.1、go plugin 1.28.0、grpc plugin 1.2.0、mockgen 1.6.0、swag 1.16.4。
make generator 在临时目录渲染/格式化/编译后交付，make check-generated 只比较；保留手写 adapter。
框架 make generator-integration 在临时模块测试两个 service 的 unary/三种 streaming 生成/再生成/编译；它使用候选本地 replace，不替代实际发布 tag 消费。
