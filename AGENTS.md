# 仓库指南

`cloud-core` 是跨服务共享协议类型的 Rust 库 crate（edition 2024），统一维护
cloud-manager、agent-manager、tokengateway 三者之间 HTTP 交互的请求/响应结构
定义。仅包含数据结构与 serde 序列化契约，不含任何业务逻辑。

## 项目结构与模块组织

- `Cargo.toml` — crate 清单与依赖声明。
- `src/lib.rs` — 库入口，声明并 re-export 各协议模块。
- `src/agent.rs` — agent-manager ↔ cloud-manager 协议（任务下发/状态回推/产物回推/配置同步）。
- `src/tg.rs` — tokengateway ↔ cloud-manager 协议（配置同步/调用记录）。
- `tests/` — 集成测试目录，按需创建。
- `/target` — 构建产物，已在 `.gitignore` 中忽略，切勿提交。

协议结构变更必须同时更新依赖方（cloud-manager、agent-manager、tokengateway），
并保持两端序列化契约一致（字段名、serde tag、默认值）。

## 构建、测试与开发命令

- `cargo build` — 编译 crate；需要优化构建时加 `--release`。
- `cargo test` — 运行全部单元与集成测试。
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
- 不要提交 `/target` 下的构建产物。
- 新增依赖时保持 `Cargo.toml` 整洁，并说明引入该依赖的原因。
- 协议结构变更须同步更新依赖方仓库，文档未同步不算完成，并根据变更大小更新 Cargo.toml 中的版本号。
- 未经明确允许不得 `git push`。push 前（以及提交前）必须依次运行 `cargo fmt`、`cargo clippy -- -D warnings`、`cargo test`，并确认全部通过。
