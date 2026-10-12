# 仓库指南

`cloud-core` 是跨服务共享协议类型的 Rust 库 crate（edition 2024），统一维护
cloud-manager、agent-manager、tokengateway 三者之间 HTTP 交互的请求/响应结构
定义。仅包含数据结构与 serde 序列化契约，不含任何业务逻辑。

**本仓是 workspace 根，同时本身也是一个包。** `cloud-core-queue` 是同仓的第二个
crate，带运行时依赖（async-nats / tokio）；根包刻意**不**引入这些依赖，
因为 tokengateway 仍是 WASM 组件，拖入运行时依赖会影响其体积与可移植性。

## 项目结构与模块组织

- `Cargo.toml` — workspace 根 + 根包清单。`[workspace] members = ["cloud-core-queue"]`。
- `src/lib.rs` — 库入口，声明并 re-export 各协议模块。
- `src/agent.rs` — agent-manager ↔ cloud-manager 协议（任务下发/状态回推/产物回推/配置同步）。
- `src/tg.rs` — tokengateway ↔ cloud-manager 协议（配置同步/调用记录）。
- `cloud-core-queue/` — 面向原生服务的任务分发库（JetStream）。设计见
  `docs/cloud-core-queue-design.md`。
  - `src/types.rs` — **线格式的唯一事实源**（信封、元数据、对外状态）。
  - `src/events.rs` — 生命周期事件，`QueueEvent` 枚举（`#[serde(tag = "type")]`）。
  - `src/producer.rs` — 投递 / 取消 / 查状态 / 订阅事件（observer 已内置）。
  - `src/consumer.rs` — lease 续期、ack 语义、退避重试、自愈重建、并发闸门。
  - `src/nats.rs` — **只读** attach + 元数据 CAS。**绝不创建基础设施**。
  - `tests/wire_format.rs` — 线格式契约测试，用**字面量**锚定每个表面。
  - `tests/integration.rs` — 端到端（需 `CCQ_NATS_URL`，未设置则跳过）。
  - `tests/reliability.rs` — §9.2 可靠性回归（需 `CCQ_NATS_URL`，且必须
    `--test-threads=1`：它会 purge stream、删除 durable consumer）。
- `tests/` — 集成测试目录，按需创建。
- `/target` — 构建产物，已在 `.gitignore` 中忽略，切勿提交。

协议结构变更必须同时更新依赖方（cloud-manager、agent-manager、tokengateway），
并保持两端序列化契约一致（字段名、serde tag、默认值）。

## cloud-core-queue 的三条硬约束

改动这个 crate 前务必先读 `docs/cloud-core-queue-design.md`，尤其是：

1. **绝不创建基础设施** — `Queue::attach` 只走只读查询，缺失即返回
   `QueueError::InfraMissing`。**不要**引入 `get_or_create_*`（`ensure_worker`
   的 durable consumer 是唯一例外，理由见其文档注释）。
2. **业务层不接触裸 JSON** — 对外 API 的签名里不出现 `serde_json::Value`。
   序列化的唯一入口是 `types.rs` / `events.rs` 的类型。
3. **每个 `subscribe` 独立订阅** — 不做进程内扇出，事件完整投递、库内不过滤。

**消费者必须写元数据流转**：`submit` 只落 `queued`，派发 / 重试 / 终态分别
由 `Consumer` 的 `mark_dispatched` / `mark_retry` / `mark_terminal` 写入。漏掉
会让 `Producer::status` 对已跑完的任务永远返回非终态。终态**必须在 ack 之前**
写 —— workqueue 的消息一旦 Term 就消失，再没有机会补写。

**改线格式前先读 `tests/wire_format.rs`**：它是字面量锚定的护栏，
字段增删改名都会让它变红。线格式变更是破坏性变更，对应
「先消费者、后生产者」的迁移顺序。

## 构建、测试与开发命令

- `cargo build` — 编译 crate；需要优化构建时加 `--release`。
- `cargo test` — 运行全部单元与集成测试。
- `cargo test -p cloud-core-queue` — 只测队列 crate。
- `cargo clippy -- -D warnings` — 静态检查，提交前将警告视为错误。
- `cargo fmt --check` — 校验格式；执行 `cargo fmt` 应用格式化。

## 编码风格与命名约定

- 使用 `rustfmt` 格式化、`clippy` 检查，提交前两者都要执行。
- 使用 4 空格缩进（Rust 默认）。
- 遵循标准 Rust 命名：函数与变量用 `snake_case`，类型与 trait 用 `CamelCase`，常量用 `SCREAMING_SNAKE_CASE`。
- 优先使用显式错误处理（`Result`、`?`），避免 panic；库风格代码路径中不要使用 `unwrap`。
- 模块与函数保持小而聚焦；非平凡逻辑需附带单元测试。

## 测试指南

- 单元测试放在源文件底部的 `#[cfg(test)] mod tests` 块中。
- 集成测试放在 `tests/` 目录，按所验证的行为命名。
- 提交前运行 `cargo test` 与 `cargo clippy -- -D warnings`。

## 提交与 Pull Request 指南

- 提交信息使用祈使语气且清晰直接（如 `add agent sync protocol`、`fix task status serde`）。
- 每个提交只做单一逻辑改动；重构与修复分开提交。
- 开 PR 前确保分支构建通过、测试通过。

## 面向 Agent 的说明

- 验证优先使用 `cargo` 工具链（`cargo test -p cloud-core`、`cargo clippy`）。
- **新增依赖必须说明理由**。`cloud-core-queue` 引入了 async-nats / tokio /
  tokio-util / futures / bytes / tracing / uuid / async-trait / anyhow；
  根包不得引入这些（会污染 WASM 消费者）。
- **禁止 `unwrap` / `expect` / `panic!` 于库路径**（`QueueConfig::new` 除外 ——
  它接受的是编译期常量队列名，不是用户输入）。
- 禁止 `println!` / `eprintln!`，一律用 `tracing`。
- **JSX 裸注释的教训不适用于 Rust**：Rust 里 `//` 是真注释，不会泄漏到产物。
- 不要提交 `/target` 下的构建产物。
- 协议结构变更须同步更新依赖方仓库，文档未同步不算完成，并根据变更大小更新 Cargo.toml 中的版本号。
- 未经明确允许不得 `git push`。push 前（以及提交前）必须依次运行 `cargo fmt`、`cargo clippy -- -D warnings`、`cargo test`，并确认全部通过。
