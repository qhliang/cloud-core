//! cloud-core — 跨服务共享协议类型。
//!
//! 统一维护 cloud-manager、agent-manager、tokengateway、workflow-manager
//! 之间的请求/响应结构定义（仅数据结构 + serde，不含任何业务逻辑）。

pub mod agent;
#[cfg(feature = "gateway")]
pub mod gateway;
pub mod tg;
pub mod workflow;

pub use agent::*;
pub use tg::*;
pub use workflow::*;

/// 跨服务**协议版本**（agent ↔ cloud-manager ↔ tokengateway ↔ workflow-manager）。
///
/// 任何不兼容的字段增删改（改名、改语义、改 serde tag、改必填性）都必须 +1，
/// 并同步递增 `Cargo.toml` 的版本号。
///
/// # 两处消费这个数字
///
/// 1. **队列信封**：`cloud_core_queue::TaskEnvelope::SCHEMA_VERSION` 必须与本值
///    相同。两个 crate 互不依赖（根包不得拖运行时依赖、队列库不得被协议类型
///    绑死），因此它是一处**必须手工同步**的字面量；下游仓同时依赖两者，
///    用一条测试断言它们相等（见 `cloud-manager-core` 的 `lib.rs` 尾部）。
/// 2. **运行时可见性**：各服务的版本 / 健康接口回显 `protocol`，部署后可直接
///    比对两端是否同版本，不必去翻镜像 tag。
///
/// # 为什么需要它
///
/// 四个仓库各自独立发版，`cloud-core` 的引用方式曾长期混用 `path` / `git`
/// （本地 `path` 看到的是工作区 HEAD，CI 看到的是 `origin/main`），因此
/// 「改了协议但另一个服务没跟上」不会有任何编译期信号。版本号把这种漂移
/// 从「运行时静默按旧结构解析」变成「一条明确的版本不兼容错误」。
pub const PROTOCOL_VERSION: u32 = 1;
