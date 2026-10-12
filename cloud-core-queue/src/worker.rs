//! Worker trait 与任务执行上下文。
//!
//! 任务执行是协作式的：worker 在检查点调用 [`HeartbeatSink::send_heartbeat`]
//! 上报进度、调用 [`CancellationSource::is_cancelled`] 判断是否应停止。
//! **JetStream lease 的续期与此完全解耦**，由 [`crate::consumer::Consumer`]
//! 独立负责 —— 业务心跳失败绝不能影响 lease。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::config::HEARTBEAT_MAX_INFO_BYTES;
use crate::events::{QueueEvent, publish_event};
use crate::producer::status_variant;
use crate::types::{AttemptErrorSource, Task, TaskError, TaskOutput, TaskState, TaskStatus};

/// 上报业务进度。
#[async_trait]
pub trait HeartbeatSink: Send + Sync {
    /// 在 core NATS 上发布一条进度事件。
    ///
    /// 心跳**不**延长 JetStream lease，也不是存活信号 —— lease 由
    /// [`crate::consumer::Consumer`] 的续期任务负责。
    async fn send_heartbeat(&self, info: String) -> Result<(), TaskError>;
}

/// 查询取消请求。
#[async_trait]
pub trait CancellationSource: Send + Sync {
    /// 本地消费者是否已要求当前任务停止。
    ///
    /// 实现应在**开始新的副作用之前**的检查点调用；它不会抢占正在进行的
    /// 长时间同步工作。
    async fn is_cancelled(&self) -> Result<bool, TaskError>;
}

/// 一次任务执行的全部上下文。
#[derive(Clone)]
pub struct TaskContext {
    /// 来自 JetStream subject 的任务标识。
    pub task_id: String,
    /// 当前投递次数（从 1 开始）。
    pub attempt: u32,
    /// 执行截止时刻（Unix 毫秒，来自任务信封）。
    pub execution_deadline_ms: u64,
    /// 生产者投递的业务负载（已从信封解码）。
    pub payload: Vec<u8>,
    client: async_nats::Client,
    queue: String,
    cancellation: CancellationToken,
    /// 终态事件是否已发布。在各 clone 间共享，使 worker 派生到后台任务里的
    /// 上下文仍能与 runner 的 ack 兜底路径去重。
    terminate_published: Arc<AtomicBool>,
}

impl TaskContext {
    /// 构造上下文。由消费者内部调用。
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        task_id: impl Into<String>,
        attempt: u32,
        execution_deadline_ms: u64,
        payload: Vec<u8>,
        client: async_nats::Client,
        queue: impl Into<String>,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            task_id: task_id.into(),
            attempt,
            execution_deadline_ms,
            payload,
            client,
            queue: queue.into(),
            cancellation,
            terminate_published: Arc::new(AtomicBool::new(false)),
        }
    }

    /// 取出任务负载。
    pub fn task(&self) -> Task {
        Task {
            payload: self.payload.clone(),
        }
    }

    /// 是否已越过执行截止时刻。
    pub fn is_past_deadline(&self) -> bool {
        crate::nats::now_ms() > self.execution_deadline_ms
    }

    /// 发布终态事件（每次投递至多一次）。
    ///
    /// **幂等**：首次**成功**发布者胜出，之后的调用是空操作。发布失败会重新
    /// 清除标志，让调用方（通常是 runner 的 ack 兜底路径）可以重试，从而保住
    /// 「先发终态、后 ack」的保证。
    pub async fn publish_terminate(
        &self,
        state: TaskState,
        output: Option<Vec<u8>>,
        error: Option<String>,
    ) -> Result<(), TaskError> {
        if self.terminate_published() {
            return Ok(());
        }
        let event = QueueEvent::complete(
            &self.task_id,
            self.attempt,
            status_variant(&state),
            output.as_deref(),
            error,
        );
        self.publish(&event).await?;
        self.terminate_published.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// 以对外终态发布终态事件。
    pub async fn publish_terminate_status(
        &self,
        status: TaskStatus,
        output: Option<Vec<u8>>,
        error: Option<String>,
    ) -> Result<(), TaskError> {
        if self.terminate_published() {
            return Ok(());
        }
        let event = QueueEvent::complete(
            &self.task_id,
            self.attempt,
            status,
            output.as_deref(),
            error,
        );
        self.publish(&event).await?;
        self.terminate_published.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// 发布一次投递失败事件。
    pub async fn publish_attempt_failed(
        &self,
        source: AttemptErrorSource,
        error: &str,
    ) -> Result<(), TaskError> {
        let event = QueueEvent::attempt_failed(&self.task_id, self.attempt, source, error);
        self.publish(&event).await
    }

    /// 上报业务进度。
    pub async fn heartbeat(&self, info: impl Into<String>) -> Result<(), TaskError> {
        let info = info.into();
        // 心跳载荷必须足够小 —— 它走 core NATS，且所有订阅者都会收到。
        if info.len() > HEARTBEAT_MAX_INFO_BYTES {
            return Err(TaskError::guest("heartbeat exceeds maximum size"));
        }
        let event = QueueEvent::heartbeat(&self.task_id, self.attempt, info);
        self.publish(&event).await
    }

    async fn publish(&self, event: &QueueEvent) -> Result<(), TaskError> {
        publish_event(&self.client, &self.queue, event)
            .await
            .map_err(|err| TaskError::system(err.to_string()))
    }

    /// 终态事件是否已发布。
    ///
    /// 刻意保持私有：恰好一次的保证由 [`Self::publish_terminate`] 内部保证，
    /// 调用方无需轮询这个标志。公开它只会诱使业务代码在 runner 的终止状态机
    /// 之上再造一层自己的。
    fn terminate_published(&self) -> bool {
        self.terminate_published.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl HeartbeatSink for TaskContext {
    async fn send_heartbeat(&self, info: String) -> Result<(), TaskError> {
        self.heartbeat(info).await
    }
}

#[async_trait]
impl CancellationSource for TaskContext {
    async fn is_cancelled(&self) -> Result<bool, TaskError> {
        // 跨进程取消（经 KV）留作后续扩展；当前只有本地消费者发起的取消。
        Ok(self.cancellation.is_cancelled())
    }
}

/// 应用侧的执行器。
#[async_trait]
pub trait Worker: Send + Sync {
    /// 执行一次投递，返回终态输出或分类错误。
    ///
    /// `Ok` 视为成功（副作用与输出都已落地）。[`TaskErrorSource::Guest`]
    /// 错误会被重试直到 `max_deliver`；[`TaskErrorSource::System`] 错误
    /// 直接终结该次投递。
    async fn handle_task(&self, task: TaskContext) -> Result<TaskOutput, TaskError>;
}

/// 可共享的 worker 句柄。
pub type SharedWorker = Arc<dyn Worker>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TaskErrorSource;

    #[test]
    fn guest_and_system_errors_are_distinguishable() {
        // 这个区分决定了「重试」还是「终结」，错了会让毒任务无限重投。
        let guest = TaskError::guest("业务失败");
        let system = TaskError::system("基础设施失败");
        assert_eq!(guest.source, TaskErrorSource::Guest);
        assert_eq!(system.source, TaskErrorSource::System);
        assert_eq!(guest.to_string(), "guest: 业务失败");
        assert_eq!(system.to_string(), "system: 基础设施失败");
    }
}
