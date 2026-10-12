//! 面向原生服务的任务分发库（JetStream）。
//!
//! 本 crate 是 `wasmCloud/crates/task_queue_{core,worker}` 的重写版，服务于
//! **原生 Rust 进程**之间的任务分发。cloud-manager 从 wasmCloud WASM 组件
//! 迁移为普通 Docker 镜像后，不再需要依赖上游 wasmCloud fork。
//!
//! # 两个角色
//!
//! ```text
//! Queue::attach(&client, cfg).await?   // → Producer
//! Producer::submit / cancel / status / subscribe   // subscribe 即原 observer
//! Consumer::attach(..)/run(worker, shutdown)       // 消费者运行器
//! ```
//!
//! 没有第三个「observer」角色：订阅任务生命周期事件的能力**内置于 producer**。
//!
//! # 三条硬约束
//!
//! ## 1. 绝不创建基础设施
//!
//! [`Queue::attach`] 走只读查询；stream / KV bucket 不存在时返回
//! [`QueueError::InfraMissing`]，**不做静默补齐**。资源由外部 provision。
//!
//! ## 2. 业务层不接触裸 JSON
//!
//! 对外 API 的签名里不出现 `serde_json::Value`。业务拿到 [`Task`] 就是类型，
//! 既没有发送裸 JSON 的入口，也拿不到裸 JSON 的出口。
//!
//! ## 3. 每个 `subscribe` 独立订阅
//!
//! 将来会有别的服务作为 producer 往同一队列投递任务，调用方**不应**监听全部
//! 回调。`subscribe()` 各自建立一条 NATS 订阅；事件**完整投递、库内不��滤**，
//! 归属判断（`task_id` 没有服务前缀，只能反查）是调用方的责任。
//!
//! 详见 `docs/cloud-core-queue-design.md`。

pub mod config;
pub mod consumer;
pub mod error;
pub mod events;
pub mod nats;
pub mod producer;
pub mod types;
pub mod worker;

pub use config::QueueConfig;
pub use consumer::{ConcurrencyLimit, Consumer, fixed_limit};
pub use error::{InfraKind, QueueError};
pub use events::QueueEvent;
pub use nats::{
    Queue, metadata_bucket, metadata_key, result_filter, result_stream_name, result_subject,
    task_filter, task_subject, worker_consumer_name,
};
pub use producer::Producer;
pub use types::{
    AttemptErrorSource, AttemptFailure, AttemptFailureRecord, PayloadError, Task, TaskEnvelope,
    TaskError, TaskErrorSource, TaskInfo, TaskMeta, TaskOutput, TaskState, TaskStatus,
    base64_decode, base64_encode,
};
pub use worker::{CancellationSource, HeartbeatSink, TaskContext, Worker};
