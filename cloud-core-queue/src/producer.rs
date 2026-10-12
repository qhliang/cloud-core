//! 生产者：投递、取消、查状态、订阅生命周期事件。
//!
//! # observer 已内置于此
//!
//! 旧形态里「订阅任务生命周期」是一个独立角色（`custom:task-queue` 的
//! `observer` 接口）。它在线格式上不过是订阅 `{queue}.events` 读 JSON，
//! 对原生进程而言没有任何特殊性 —— 一个既要投递任务、又要知晓其生命周期的
//! 服务，本质上就是生产者。因此 [`Producer::subscribe`] 就是它。
//!
//! # 每个调用方独立订阅
//!
//! `subscribe()` 每次调用都建立**一条独立的 NATS 订阅**并返回一个
//! [`mpsc::Receiver`]，不做进程内扇出。理由：
//!
//! > 将来会有别的服务作为 producer 往同一队列投递任务，调用方不应监听全部回调。
//!
//! 配合「库内不过滤」：`task_id` 由 UUID v7 生成、**没有服务前缀**，无法在
//! subject 层过滤，因此归属只能由调用方反查判断。库擅自丢弃事件会让调用方
//! 失去观测全量的能力，因此**完整投递**。

use std::sync::Arc;

use async_nats::jetstream::Context as JetStreamContext;
use tokio::sync::mpsc;

use crate::config::{PAYLOAD_MAX_BYTES, QueueConfig};
use crate::error::QueueError;
use crate::events::{QueueEvent, events_subject};
use crate::nats::{
    QueueHandles, get_metadata, now_ms, put_metadata, result_subject, task_subject,
    update_metadata_with,
};
use crate::types::{Task, TaskEnvelope, TaskInfo, TaskMeta, TaskState, TaskStatus};

/// `subscribe` 单个订阅者的缓冲深度。
///
/// 事件是通知性质的（走 core NATS 而非持久流），缓冲满时**丢弃最旧**而不是
/// 阻塞消费者 —— 消费者的 ack 路径绝不能被订阅者的慢消费拖住。
const SUBSCRIBE_BUFFER: usize = 256;

/// 生产者句柄。
#[derive(Clone)]
pub struct Producer {
    handles: Arc<QueueHandles>,
}

impl Producer {
    /// 附着到已存在的队列并返回生产者。**不创建任何资源。**
    pub async fn attach(client: JetStreamContext, config: QueueConfig) -> Result<Self, QueueError> {
        let handles = crate::nats::Queue::attach(client, config).await?;
        Ok(Self {
            handles: Arc::new(handles),
        })
    }

    /// 在已有句柄上构造生产者。
    pub fn from_handles(handles: Arc<QueueHandles>) -> Self {
        Self { handles }
    }

    /// 队列配置。
    pub fn config(&self) -> &QueueConfig {
        &self.handles.config
    }

    /// 投递一个任务，返回任务标识。
    ///
    /// # 顺序：先写元数据，再投投递流
    ///
    /// 若反之，消费者可能在元数据落库前就开始处理，随后 `get_metadata` 查不到
    /// 任何状态。代价是多写一次 KV —— 但那一次写换来的是状态永远可查。
    pub async fn submit(&self, task: Task) -> Result<String, QueueError> {
        if task.payload.len() > PAYLOAD_MAX_BYTES {
            return Err(QueueError::Task(format!(
                "payload exceeds {PAYLOAD_MAX_BYTES} bytes"
            )));
        }

        let task_id = uuid::Uuid::now_v7().to_string();
        let created_at_ms = now_ms();
        let deadline_ms =
            created_at_ms.saturating_add(duration_millis(self.handles.config.dispatch_timeout));

        let meta = TaskMeta {
            schema_version: TaskEnvelope::SCHEMA_VERSION,
            id: task_id.clone(),
            queue: self.handles.config.name.clone(),
            state: TaskState::Queued,
            attempt: 0,
            created_at_ms,
            dispatched_at_ms: None,
            completed_at_ms: None,
            deadline_ms,
            cancel_requested: false,
            attempts: Vec::new(),
        };
        put_metadata(&self.handles, &meta).await?;

        let envelope = TaskEnvelope::new(task_id.clone(), task.payload, deadline_ms);
        let payload = serde_json::to_vec(&envelope)
            .map_err(|err| QueueError::Nats(anyhow::anyhow!("encode task envelope: {err}")))?;
        self.handles
            .jetstream
            .publish(
                task_subject(&self.handles.config.name, &task_id),
                payload.into(),
            )
            .await
            .map_err(|err| QueueError::Nats(anyhow::anyhow!("publish task: {err}")))?;
        Ok(task_id)
    }

    /// 请求取消一个尚未终结的任务。
    ///
    /// 置位 `cancel_requested`；消费者在自己的检查点读取该标志并停止。
    /// 已在运行的任务不会被强杀。
    ///
    /// ⚠️ 用 CAS 写入而非无条件覆盖 —— 消费者同时在写 `state`/`attempt`，
    /// 无条件写会抹掉它的更新，反之亦然。
    pub async fn cancel(&self, task_id: &str) -> Result<(), QueueError> {
        let mut terminal = false;
        update_metadata_with(&self.handles, task_id, |meta| {
            // 终态任务取消没有意义，且会覆盖掉已完成的时间戳语义。
            if meta.state.is_terminal() {
                terminal = true;
                return Ok(());
            }
            meta.cancel_requested = true;
            Ok(())
        })
        .await?;
        if terminal {
            tracing::debug!(task_id = %task_id, "cancel ignored: task already terminal");
        }
        Ok(())
    }

    /// 查询任务对外状态。任务不存在时返回 `None`。
    pub async fn status(&self, task_id: &str) -> Result<Option<TaskInfo>, QueueError> {
        let Some(meta) = get_metadata(&self.handles, task_id).await? else {
            return Ok(None);
        };
        Ok(Some(task_info(&meta)))
    }

    /// 订阅任务生命周期事件。
    ///
    /// ⚠️ 每次调用都建立**一条独立订阅**（见模块级说明）。
    ///
    /// 事件**完整投递、不做过滤**：库不知道哪些任务是调用方的，反查是调用方的
    /// 责任。
    ///
    /// ⚠️ 缓冲区写满时 `try_send` 会**丢弃新到的那条**并 warn —— 这是
    /// **背压**语义，不是「保留最近 N 条」。调用方消费太慢时，最新终态
    /// 反而可能先丢；订阅方必须尽快 drain。
    pub async fn subscribe(&self) -> Result<mpsc::Receiver<QueueEvent>, QueueError> {
        let (tx, rx) = mpsc::channel(SUBSCRIBE_BUFFER);
        let queue = self.handles.config.name.clone();
        let subject = events_subject(&queue);
        // 取出 owned 的 client —— spawn 的任务不能借用 `self`。
        let client = self.handles.jetstream.client().clone();

        // 独立任务：负责把事件转成类型后投递，源流结束即退出。
        // spawn 是安全的 —— 没有需要跨 await 存活的请求级状态。
        tokio::spawn(async move {
            let mut stream = match client.subscribe(subject).await {
                Ok(stream) => stream,
                Err(err) => {
                    tracing::warn!(%queue, err = %err, "failed to subscribe task events");
                    return;
                }
            };
            use futures::StreamExt as _;
            while let Some(message) = stream.next().await {
                match serde_json::from_slice::<QueueEvent>(&message.payload) {
                    Ok(event) => {
                        if tx.try_send(event).is_err() {
                            // 订阅者跟不上。丢弃而非阻塞：消费者的 ack 路径
                            // 绝不能被这里拖住。
                            tracing::debug!(%queue, "task event dropped: subscriber too slow");
                        }
                    }
                    Err(err) => {
                        // 反序列化失败说明线上有未知格式，计数暴露出来便于发现。
                        tracing::warn!(%queue, err = %err, "undecodable task event");
                    }
                }
            }
        });
        Ok(rx)
    }

    /// 归档终态结果到 `{queue}-results`。
    ///
    /// 未启用 `results_archive` 时静默跳过。
    pub async fn archive_result(&self, event: &QueueEvent) -> Result<(), QueueError> {
        if !self.handles.results_archive {
            return Ok(());
        }
        let QueueEvent::Complete { id, .. } = event else {
            return Ok(());
        };
        let raw = serde_json::to_vec(event)
            .map_err(|err| QueueError::Nats(anyhow::anyhow!("encode result: {err}")))?;
        self.handles
            .jetstream
            .publish(result_subject(&self.handles.config.name, id), raw.into())
            .await
            .map_err(|err| QueueError::Nats(anyhow::anyhow!("publish result: {err}")))?;
        Ok(())
    }
}

/// 由内部状态机推导对外状态。
pub fn task_info(meta: &TaskMeta) -> TaskInfo {
    TaskInfo {
        id: meta.id.clone(),
        status: status_variant(&meta.state),
        attempt: meta.attempt.checked_sub(1),
        created_at_ms: meta.created_at_ms,
        dispatched_at_ms: meta.dispatched_at_ms,
        completed_at_ms: meta.completed_at_ms,
        cancel_requested: meta.cancel_requested,
    }
}

/// 内部状态 → 对外终态。
pub fn status_variant(state: &TaskState) -> TaskStatus {
    match state {
        TaskState::Succeeded => TaskStatus::Succeeded,
        TaskState::DispatchTimeoutPending | TaskState::DispatchTimeout => {
            TaskStatus::DispatchTimeout
        }
        TaskState::ExecutionTimeout => TaskStatus::ExecutionTimeout,
        TaskState::Cancelled => TaskStatus::Cancelled,
        TaskState::MaxRetriesExceeded => TaskStatus::MaxRetriesExceeded,
        TaskState::Queued | TaskState::Running | TaskState::Failed => TaskStatus::Failed,
    }
}

/// 构造终态结果（消费侧使用）。
pub fn terminal_result(
    task_id: &str,
    state: TaskState,
    attempt: u32,
    output: Option<Vec<u8>>,
    error: Option<String>,
) -> QueueEvent {
    QueueEvent::complete(
        task_id,
        attempt,
        status_variant(&state),
        output.as_deref(),
        error,
    )
}

/// 执行截止时刻（Unix 毫秒）。
pub fn execution_deadline(config: &QueueConfig, started_at_ms: u64) -> u64 {
    started_at_ms.saturating_add(duration_millis(config.execution_timeout))
}

/// 两个时刻之间的毫秒差。
pub fn duration_since_ms(started_at_ms: u64, now: u64) -> u64 {
    now.saturating_sub(started_at_ms)
}

fn duration_millis(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nats::required_metadata_buckets;

    #[test]
    fn status_variant_maps_internal_states() {
        assert_eq!(status_variant(&TaskState::Succeeded), TaskStatus::Succeeded);
        assert_eq!(
            status_variant(&TaskState::DispatchTimeoutPending),
            TaskStatus::DispatchTimeout
        );
        assert_eq!(
            status_variant(&TaskState::DispatchTimeout),
            TaskStatus::DispatchTimeout
        );
        assert_eq!(
            status_variant(&TaskState::ExecutionTimeout),
            TaskStatus::ExecutionTimeout
        );
        assert_eq!(status_variant(&TaskState::Cancelled), TaskStatus::Cancelled);
        assert_eq!(
            status_variant(&TaskState::MaxRetriesExceeded),
            TaskStatus::MaxRetriesExceeded
        );
        // 未成功即失败：这是对外可见的结论，不区分内部原因。
        assert_eq!(status_variant(&TaskState::Queued), TaskStatus::Failed);
        assert_eq!(status_variant(&TaskState::Running), TaskStatus::Failed);
        assert_eq!(status_variant(&TaskState::Failed), TaskStatus::Failed);
    }

    #[test]
    fn task_info_attempt_is_none_before_dispatch_and_zero_based_after() {
        let mut meta = TaskMeta {
            schema_version: 1,
            id: "t".to_string(),
            queue: "agent-task".to_string(),
            state: TaskState::Queued,
            attempt: 0,
            created_at_ms: 1,
            dispatched_at_ms: None,
            completed_at_ms: None,
            deadline_ms: 2,
            cancel_requested: false,
            attempts: Vec::new(),
        };
        // 未派发：内部 attempt=0 → 对外 None（而不是 Some(0)）。
        assert!(task_info(&meta).attempt.is_none());
        // 已派发：内部 1-based → 对外 0-based。
        meta.attempt = 1;
        assert_eq!(task_info(&meta).attempt, Some(0));
        meta.attempt = 2;
        assert_eq!(task_info(&meta).attempt, Some(1));
    }

    #[test]
    fn execution_deadline_adds_timeout_without_overflowing() {
        let config = QueueConfig::new("agent-task");
        assert_eq!(
            execution_deadline(&config, 1_000),
            1_000 + crate::config::DEFAULT_EXECUTION_TIMEOUT_MS
        );
        // 接近 u64::MAX 时必须饱和而非回绕。
        assert_eq!(
            execution_deadline(&config, u64::MAX),
            u64::MAX,
            "溢出必须饱和，不能回绕成过去的时间"
        );
    }

    #[test]
    fn terminal_result_carries_status_and_error() {
        let event = terminal_result(
            "task-1",
            TaskState::MaxRetriesExceeded,
            3,
            None,
            Some("boom".to_string()),
        );
        assert_eq!(event.id(), "task-1");
        assert_eq!(event.attempt(), 3);
        let QueueEvent::Complete { status, error, .. } = &event else {
            panic!("应为 complete 事件");
        };
        assert_eq!(*status, TaskStatus::MaxRetriesExceeded);
        assert_eq!(error.as_deref(), Some("boom"));
    }

    #[test]
    fn terminal_result_decodes_output() {
        let event = terminal_result(
            "task-1",
            TaskState::Succeeded,
            1,
            Some(b"done".to_vec()),
            None,
        );
        assert_eq!(event.decoded_output(), Some(b"done".to_vec()));
    }

    #[test]
    fn duration_since_saturates_at_zero() {
        assert_eq!(duration_since_ms(100, 50), 0, "时钟回拨时不得回绕");
        assert_eq!(duration_since_ms(100, 150), 50);
    }

    #[test]
    fn metadata_key_and_bucket_names_are_queue_scoped() {
        let config = QueueConfig::new("agent-task");
        assert_eq!(
            crate::nats::metadata_key(&config.name, "task-1"),
            "agent-task.task-1"
        );
        assert_eq!(
            required_metadata_buckets(&config),
            vec!["agent-task-meta".to_string()]
        );
    }

    #[test]
    fn empty_output_decodes_to_none_and_valid_base64_round_trips() {
        // QueueEvent::complete 的 output 恒为字符串，空表示「无输出」。
        let empty = QueueEvent::complete("task-1", 1, TaskStatus::Succeeded, None, None);
        assert!(empty.decoded_output().is_none());
        let present = QueueEvent::complete("task-1", 1, TaskStatus::Succeeded, Some(b"hi"), None);
        assert_eq!(present.decoded_output(), Some(b"hi".to_vec()));
    }
}
