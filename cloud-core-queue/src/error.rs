//! 错误语义。
//!
//! # 为什么区分 `InfraMissing` 与 `Task`
//!
//! 两者混在一起会造成实质危害：「基础设施没配好」会被当成「任务执行失败」
//! 写进业务表 —— 数据被污染，且事后极难排查（业务侧只会看到任务失败，
//! 而真正的原因是某个 stream 没建）。
//!
//! 调用方据此可以分别处理：
//! - [`QueueError::InfraMissing`] → **部署故障**，应退避重试，并反映到 `/healthz`
//! - [`QueueError::Task`] → 业务失败，重试没有意义
//! - [`QueueError::Nats`] → 传输层异常，结论未知

use std::fmt;

use std::error::Error;

/// 缺失的基础设施种类。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InfraKind {
    /// 任务投递用的 stream（`{queue}`）。
    TaskStream,
    /// 任务元数据 KV bucket（`{queue}-meta`）。
    MetadataBucket,
    /// 结果归档 stream（`{queue}-results`）。
    ResultStream,
    /// 消费者 durable consumer（`{queue}-worker`）。
    WorkerConsumer,
}

impl InfraKind {
    /// 稳定的人类可读名，用于日志与错误消息。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TaskStream => "stream",
            Self::MetadataBucket => "kv-bucket",
            Self::ResultStream => "result-stream",
            Self::WorkerConsumer => "consumer",
        }
    }
}

/// 队列操作的错误。
#[derive(Debug)]
pub enum QueueError {
    /// 基础设施缺失 —— stream / bucket / consumer 不存在。
    ///
    /// 语义是**部署故障**，不是业务失败。本 crate 绝不创建资源，因此这个
    /// 变体只会因为外部未 provision 而出现。
    InfraMissing {
        /// 缺失的资源种类。
        kind: InfraKind,
        /// 缺失的资源名。
        name: String,
    },

    /// 传输层异常（NATS 连接、订阅、ack 等）。失败结论未知。
    Nats(anyhow::Error),

    /// 任务级失败：投递本身成功，但任务执行出错。重试没有意义。
    Task(String),
}

impl fmt::Display for QueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InfraMissing { kind, name } => {
                write!(f, "infrastructure missing: {kind} `{name}` not found")
            }
            Self::Nats(err) => write!(f, "nats error: {err}"),
            Self::Task(message) => write!(f, "task failed: {message}"),
        }
    }
}

impl Error for QueueError {
    /// 保留底层 NATS 错误链，便于排障时看到原始错误。
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Nats(err) => Some(err.as_ref()),
            Self::InfraMissing { .. } | Self::Task(_) => None,
        }
    }
}

impl From<anyhow::Error> for QueueError {
    fn from(err: anyhow::Error) -> Self {
        Self::Nats(err)
    }
}

impl QueueError {
    /// 是否为「基础设施缺失」。
    ///
    /// 调用方可用它决定退避重试策略：基础设施缺失通常会自愈（人工 provision
    /// 或外部 Job 重建），而 `Task` 类错误不会。
    pub fn is_infra_missing(&self) -> bool {
        matches!(self, Self::InfraMissing { .. })
    }

    /// 构造一个基础设施缺失错误。
    pub fn infra_missing(kind: InfraKind, name: impl Into<String>) -> Self {
        Self::InfraMissing {
            kind,
            name: name.into(),
        }
    }
}

/// `InfraMissing` 的构造辅助，便于在 `attach` 里紧凑书写。
impl From<(InfraKind, String)> for QueueError {
    fn from((kind, name): (InfraKind, String)) -> Self {
        Self::infra_missing(kind, name)
    }
}

/// 让 `?` 能把 `anyhow::Error` 直接转成 `QueueError::Nats`（已由
/// `From<anyhow::Error>` 提供，此处仅为可读性保留说明）。
impl fmt::Display for InfraKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn infra_missing_is_distinguishable_from_task_failure() {
        let infra = QueueError::infra_missing(InfraKind::TaskStream, "agent-task");
        let task = QueueError::Task("boom".to_string());
        assert!(infra.is_infra_missing());
        assert!(!task.is_infra_missing());
    }

    #[test]
    fn infra_missing_message_names_the_resource() {
        let err = QueueError::infra_missing(InfraKind::MetadataBucket, "agent-task-meta");
        assert_eq!(
            err.to_string(),
            "infrastructure missing: kv-bucket `agent-task-meta` not found"
        );
    }

    #[test]
    fn infra_kind_names_are_stable() {
        assert_eq!(InfraKind::TaskStream.as_str(), "stream");
        assert_eq!(InfraKind::MetadataBucket.as_str(), "kv-bucket");
        assert_eq!(InfraKind::ResultStream.as_str(), "result-stream");
        assert_eq!(InfraKind::WorkerConsumer.as_str(), "consumer");
    }
}
