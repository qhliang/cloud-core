//! cloud-core — 跨服务共享协议类型。
//!
//! 统一维护 cloud-manager、agent-manager、tokengateway、workflow-manager
//! 之间的请求/响应结构定义（仅数据结构 + serde，不含任何业务逻辑）。

pub mod agent;
pub mod tg;
pub mod workflow;

pub use agent::*;
pub use tg::*;
pub use workflow::*;
