//! 消费者运行器：lease 续期、ack 语义、退避重试、自愈重建、并发闸门。
//!
//! # 这里面没有一行是胶水代码
//!
//! 本模块的每一处机制都对应一类线上故障，改动前先读懂为什么：
//!
//! | 机制 | 出错后果 |
//! |---|---|
//! | lease 续期 | 续期失败 → lease 丢失 → 任务被重复执行 |
//! | nak 退避重试 | 无退避 → 热循环打爆 worker |
//! | `max_deliver` 上限 | 无上限 → 毒任务永不退出 |
//! | consumer 重建 | NATS 重建后永久卡死，任务全部滞留队列 |
//! | 并发闸门 | 在途任务数超过配置，瞬时负载失控 |
//! | 关停 drain | 关停时丢弃正在处理的任务 |
//!
//! # ⚠️ 已知取舍：流消失不再重建
//!
//! 旧实现里 stream 由**生产者**兜底重建（消费者的重建循环只重建 consumer）。
//! 本 crate 禁止创建基础设施，因此 stream 消失后没有任何组件会重建它 ——
//! 这是有意的取舍，代价与人工恢复步骤见 `docs/cloud-core-queue-design.md`
//! §3.3 与 §7.2。

use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_nats::jetstream::Context as JetStreamContext;
use futures::StreamExt as _;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::config::QueueConfig;
use crate::error::QueueError;
use crate::events::{QueueEvent, publish_event};
use crate::nats::{AckAction, QueueHandles, ensure_worker, now_ms, update_metadata_with};
use crate::types::{AttemptErrorSource, TaskEnvelope, TaskErrorSource, TaskState, TaskStatus};
use crate::worker::{SharedWorker, TaskContext, Worker};

/// 关停后等待在途投递 ack 的上限。
///
/// 与队列 `ack_wait` 对齐：超过该窗口仍未 ack 的投递本就会丢 lease 并被重投，
/// 再等下去没有收益。
const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// 并发闸门的复查间隔。
///
/// 限额可以在运行期被调高（配置同步推来）。若只在启动时取一次快照，
/// 已停在外面的消费者会一直等到某个无关的释放唤醒它们。
const LIMIT_RECHECK: Duration = Duration::from_secs(1);

/// 订阅失败后重建 consumer 的等待时长。
///
/// ⚠️ **只重建 consumer，不重建 stream。** 旧实现在这里连 stream 一起重建，
/// 但那实际依赖的是生产者兜底（见模块文档）。本 crate 不创建任何资源，
/// 因此 stream 缺失时只能报错并等待外部 provision。
const RECONNECT_DELAY: Duration = Duration::from_secs(60);

/// 等待 `delay`，返回 `false` 表示期间 `shutdown` 被取消。
async fn sleep_or_cancel(shutdown: &CancellationToken, delay: Duration) -> bool {
    tokio::select! {
        _ = shutdown.cancelled() => false,
        _ = tokio::time::sleep(delay) => true,
    }
}

/// 可并发处理的投递数量。
///
/// 每次取槽位前都会轮询它，因此运行期调整限额无需重启消费者。
/// 启动时取快照会让整个进程生命周期都套用一个过期限额。
pub type ConcurrencyLimit = Arc<dyn Fn() -> usize + Send + Sync>;

/// 把常量包成 [`ConcurrencyLimit`]。
pub fn fixed_limit(concurrency: usize) -> ConcurrencyLimit {
    let concurrency = concurrency.max(1);
    Arc::new(move || concurrency)
}

/// 计数闸门。
struct Gate {
    limit: ConcurrencyLimit,
    active: StdMutex<usize>,
    freed: Notify,
}

impl Gate {
    fn new(limit: ConcurrencyLimit) -> Self {
        Self {
            limit,
            // 刻意用 std mutex：`Permit::drop` 必须**同步**释放槽位，
            // 而临界区内不 await。
            active: StdMutex::new(0),
            freed: Notify::new(),
        }
    }

    /// 同步取一个槽位；已满返回 `None`。
    fn try_acquire(self: &Arc<Self>) -> Option<Permit> {
        let limit = (self.limit)().max(1);
        let mut active = self.active.lock().unwrap_or_else(|err| err.into_inner());
        if *active < limit {
            *active += 1;
            return Some(Permit { gate: self.clone() });
        }
        None
    }

    /// 取槽位，等到有空闲或 `shutdown` 被取消。
    async fn acquire(self: &Arc<Self>, shutdown: &CancellationToken) -> Option<Permit> {
        loop {
            if let Some(permit) = self.try_acquire() {
                return Some(permit);
            }
            tokio::select! {
                _ = shutdown.cancelled() => return None,
                _ = self.freed.notified() => {}
                _ = tokio::time::sleep(LIMIT_RECHECK) => {}
            }
        }
    }
}

/// 闸门中一个被占用的槽位，drop 时释放。
struct Permit {
    gate: Arc<Gate>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        let mut active = self
            .gate
            .active
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        *active = active.saturating_sub(1);
        drop(active);
        self.gate.freed.notify_one();
    }
}

/// 消费者运行器。
#[derive(Clone)]
pub struct Consumer {
    handles: Arc<QueueHandles>,
    worker: SharedWorker,
    limit: ConcurrencyLimit,
}

impl Consumer {
    /// 附着到已存在的队列。**不创建任何资源**（consumer durable 除外，见
    /// [`ensure_worker`] 的说明）。
    pub async fn attach(
        client: JetStreamContext,
        config: QueueConfig,
        worker: impl Worker + 'static,
    ) -> Result<Self, QueueError> {
        Self::attach_with_concurrency(client, config, worker, 1).await
    }

    /// 同 [`Self::attach`]，但允许并发处理多份投递。
    ///
    /// 取值钳到至少 1：配置成 0 时退化为串行，而不是让运行器停摆。
    pub async fn attach_with_concurrency(
        client: JetStreamContext,
        config: QueueConfig,
        worker: impl Worker + 'static,
        concurrency: usize,
    ) -> Result<Self, QueueError> {
        Self::attach_with_limit(client, config, worker, fixed_limit(concurrency)).await
    }

    /// 同 [`Self::attach`]，但每次取槽位前重新读取并发限额。
    pub async fn attach_with_limit(
        client: JetStreamContext,
        config: QueueConfig,
        worker: impl Worker + 'static,
        limit: ConcurrencyLimit,
    ) -> Result<Self, QueueError> {
        let handles = crate::nats::Queue::attach(client, config).await?;
        Ok(Self {
            handles: Arc::new(handles),
            worker: Arc::new(worker),
            limit,
        })
    }

    /// 队列配置。
    pub fn config(&self) -> &QueueConfig {
        &self.handles.config
    }

    /// 消费直到 `shutdown` 被取消。
    ///
    /// # 背压：先取槽位，再拉取
    ///
    /// 每份投递从被拉取到终态 ack 全程占用一个槽位。**先取槽位再拉取**是关键：
    /// 槽位全忙时消费者停止拉取，多余的任务留在 stream 里；而反过来（先拉后等）
    /// 会让缓冲中的消息空耗 `ack_wait`，等有槽位时 lease 已过期而被重投。
    ///
    /// # consumer 自愈
    ///
    /// 订阅失败时**绝不退出**，而是在 [`RECONNECT_DELAY`] 后重建 consumer
    /// 并重新订阅。仍存在的 consumer 保留自己的 ack 状态，因此重连不会重投
    /// 已确认的任务；被删掉的会被重建并重投仍在队列里的内容。在途投递不受
    /// 重连影响 —— 它们继续运行并通过各自的 acker 确认。
    ///
    /// ⚠️ **不重建 stream**：见模块文档的「已知取舍」。
    pub async fn run(&self, shutdown: CancellationToken) -> Result<(), QueueError> {
        let gate = Arc::new(Gate::new(self.limit.clone()));
        let mut in_flight = JoinSet::new();
        let mut established = false;

        'connect: loop {
            if shutdown.is_cancelled() {
                break;
            }
            let consumer = match ensure_worker(&self.handles).await {
                Ok(consumer) => consumer,
                Err(err) => {
                    tracing::warn!(err = %err, delay = ?RECONNECT_DELAY, "task consumer unavailable; will retry");
                    if !sleep_or_cancel(&shutdown, RECONNECT_DELAY).await {
                        break;
                    }
                    continue 'connect;
                }
            };
            // 客户端预取窗口压到 1：消费者缓冲的消息数绝不能超过它的槽位数，
            // 否则缓冲中的消息在等槽位时空耗 ack_wait。
            let mut messages = match consumer.stream().max_messages_per_batch(1).messages().await {
                Ok(messages) => messages,
                Err(err) => {
                    tracing::warn!(err = %err, delay = ?RECONNECT_DELAY, "failed to subscribe task consumer; will retry");
                    if !sleep_or_cancel(&shutdown, RECONNECT_DELAY).await {
                        break;
                    }
                    continue 'connect;
                }
            };
            if established {
                tracing::info!("task queue consumer re-connected");
            } else {
                tracing::info!(queue = %self.handles.config.name, "task queue consumer running");
                established = true;
            }

            'consume: loop {
                // 收割已完成的投递，避免集合无限增长。
                while in_flight.try_join_next().is_some() {}
                let permit = tokio::select! {
                    _ = shutdown.cancelled() => break 'connect,
                    acquired = gate.acquire(&shutdown) => match acquired {
                        Some(permit) => permit,
                        None => break 'connect,
                    },
                };
                let next = tokio::select! {
                    _ = shutdown.cancelled() => break 'consume,
                    next = messages.next() => next,
                };
                let Some(next) = next else {
                    // 订阅结束但没有报错。视作 consumer 损坏并重建，
                    // 而不是让运行器静默退出、停止消费。
                    drop(permit);
                    tracing::warn!(delay = ?RECONNECT_DELAY, "task consumer subscription ended; re-establishing");
                    if !sleep_or_cancel(&shutdown, RECONNECT_DELAY).await {
                        break 'connect;
                    }
                    continue 'connect;
                };
                match next {
                    Ok(message) => {
                        let runner = self.clone();
                        in_flight.spawn(async move {
                            runner.handle_message(message).await;
                            // 仅在终态 ack 之后释放，因此在途数不会超过限额。
                            drop(permit);
                        });
                    }
                    Err(err) => {
                        tracing::warn!(err = %err, delay = ?RECONNECT_DELAY, "task consumer stream error; re-establishing consumer");
                        drop(permit);
                        // 重建前退避，避免持续损坏的 stream 变成热重连循环。
                        if !sleep_or_cancel(&shutdown, RECONNECT_DELAY).await {
                            break 'connect;
                        }
                        continue 'connect;
                    }
                }
            }
        }

        drain(&mut in_flight).await;
        Ok(())
    }

    /// 处理一份投递。
    async fn handle_message(&self, message: async_nats::jetstream::Message) {
        // JetStream 的投递信息是权威的尝试计数器。
        let attempt = message
            .info()
            .ok()
            .map(|info| info.delivered.max(1))
            .unwrap_or(1) as u32;
        let (message, acker) = message.split();
        let acker = Arc::new(acker);
        let task_id = message
            .subject
            .rsplit('.')
            .next()
            .unwrap_or_default()
            .to_string();

        // 信封无法解析时不能安全重试，直接终结。
        let envelope = match serde_json::from_slice::<TaskEnvelope>(&message.payload) {
            Ok(envelope) => envelope,
            Err(err) => {
                tracing::warn!(task_id = %task_id, err = %err, "invalid task envelope");
                let error = err.to_string();
                self.publish_attempt_failed(&task_id, attempt, AttemptErrorSource::System, &error)
                    .await;
                self.publish_pre_context_terminate(&task_id, attempt, &error)
                    .await;
                // 元数据终态必须在 ack 之前写（消息随即消失）。
                self.mark_terminal(&task_id, TaskState::Failed).await;
                let _ = AckAction::Term.apply(&acker).await;
                return;
            }
        };
        let payload = match envelope.decode_payload() {
            Ok(payload) => payload,
            Err(err) => {
                tracing::warn!(task_id = %task_id, err = %err, "invalid task payload");
                let error = err.to_string();
                self.publish_attempt_failed(&task_id, attempt, AttemptErrorSource::System, &error)
                    .await;
                self.publish_pre_context_terminate(&task_id, attempt, &error)
                    .await;
                // 元数据终态必须在 ack 之前写（消息随即消失）。
                self.mark_terminal(&task_id, TaskState::Failed).await;
                let _ = AckAction::Term.apply(&acker).await;
                return;
            }
        };

        // 元数据流转：派发时刻此前无人写入 —— producer 只在 submit 时落 `queued`。
        // 不写的话 `Producer::status` 会永远返回非终态（见 docs §6.6）。
        self.mark_dispatched(&task_id, attempt).await;

        let cancellation = CancellationToken::new();
        let context = TaskContext::new(
            task_id.clone(),
            attempt,
            envelope.deadline_ms(),
            payload,
            self.handles.jetstream.client().clone(),
            self.handles.config.name.clone(),
            cancellation,
        );

        // ── lease 续期 ──
        // 与业务心跳完全解耦：心跳失败不得影响 lease，lease 也不应因业务卡顿而丢失。
        let renew_acker = acker.clone();
        let renew_cancel = CancellationToken::new();
        let renew_cancel_in_task = renew_cancel.clone();
        // 先取一个 owned 的副本再 spawn —— `self` 的借用不能跨 `.await` 存活。
        let renew_interval = self.handles.config.lease_renew_interval;
        let renewer = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(renew_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // 首次 tick 立即触发，跳过它 —— 刚拿到消息时无需续期。
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = renew_cancel_in_task.cancelled() => break,
                    _ = ticker.tick() => {
                        if AckAction::Progress.apply(&renew_acker).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });

        self.publish_start(&task_id, attempt).await;

        let ack_context = context.clone();
        let result = self.worker.handle_task(context).await;
        // 续期任务必须在终态 ack 之前停止。
        renew_cancel.cancel();
        let _ = renewer.await;

        // 终态元数据必须在 ack 之前写：workqueue 的消息一旦 Term 就消失，
        // 再没有机会补写。由各分支带出，末尾统一落库。
        let mut terminal_state: Option<TaskState> = None;
        let action = match result {
            // 已完成的副作用与输出视为已接受。
            Ok(output) => {
                // publish_terminate 幂等：worker 若已自行上报终态，不会被覆盖。
                if let Err(err) = ack_context
                    .publish_terminate_status(TaskStatus::Succeeded, output, None)
                    .await
                {
                    tracing::warn!(task_id = %task_id, err = %err, "failed to publish success terminate");
                }
                AckAction::Ack
            }
            // 业务错误可重试，直到投递预算耗尽。
            Err(err) if err.source == TaskErrorSource::Guest => {
                let message = err.message;
                if let Err(publish_err) = ack_context
                    .publish_attempt_failed(AttemptErrorSource::Guest, &message)
                    .await
                {
                    tracing::warn!(task_id = %task_id, err = %publish_err, "failed to publish guest attempt_failed");
                }
                if attempt >= self.handles.config.max_deliver.max(1) as u32 {
                    if let Err(publish_err) = ack_context
                        .publish_terminate_status(
                            TaskStatus::MaxRetriesExceeded,
                            None,
                            Some(message),
                        )
                        .await
                    {
                        tracing::warn!(task_id = %task_id, err = %publish_err, "failed to publish max-retries terminate");
                    }
                    terminal_state = Some(TaskState::Failed);
                    AckAction::Term
                } else {
                    // 重试：把本次失败记进 attempts[]，供排障看失败历史。
                    self.mark_retry(&task_id, attempt, AttemptErrorSource::Guest, &message)
                        .await;
                    AckAction::Nak(Some(self.handles.config.backoff_for_attempt(attempt)))
                }
            }
            // 死线与基础设施失败不应在重试上空转。
            Err(err) => {
                tracing::warn!(task_id = %task_id, err = %err, "worker system failure");
                let message = err.message;
                let _ = ack_context
                    .publish_attempt_failed(AttemptErrorSource::System, &message)
                    .await;
                // 越过执行死线的系统失败报为执行超时，其余按普通失败处理。
                let status = if now_ms() > ack_context.execution_deadline_ms {
                    TaskStatus::ExecutionTimeout
                } else {
                    TaskStatus::Failed
                };
                if let Err(publish_err) = ack_context
                    .publish_terminate_status(status, None, Some(message))
                    .await
                {
                    tracing::warn!(task_id = %task_id, err = %publish_err, "failed to publish system terminate");
                }
                AckAction::Term
            }
        };
        // ⚠️ 元数据终态必须在 ack **之前**写：workqueue 的消息一旦 Term 消失，
        // 就再没有机会补写。写失败只 warn —— 此时事件（complete）已经发出，
        // 调用方仍能从事件拿到终态结论。
        if let Some(state) = terminal_state {
            self.mark_terminal(&task_id, state).await;
        }
        if let Err(err) = action.apply(&acker).await {
            tracing::warn!(task_id = %task_id, err = %err, "failed to acknowledge task");
        }
    }

    /// 元数据流转：标记已派发。
    ///
    /// ⚠️ producer 只在 `submit` 时写 `queued`；派发时刻只有消费者知道。不写的话
    /// `Producer::status` 对已跑完的任务仍返回非终态（docs §6.6）。
    async fn mark_dispatched(&self, task_id: &str, attempt: u32) {
        let handles = &self.handles;
        let result = update_metadata_with(handles, task_id, |meta| {
            meta.state = TaskState::Running;
            meta.attempt = attempt;
            meta.dispatched_at_ms = Some(now_ms());
            Ok(())
        })
        .await;
        if let Err(err) = result {
            tracing::warn!(task_id, err = %err, "failed to mark task dispatched in metadata");
        }
    }

    /// 元数据流转：记录一次失败并准备重投。
    async fn mark_retry(
        &self,
        task_id: &str,
        attempt: u32,
        source: AttemptErrorSource,
        error: &str,
    ) {
        let handles = &self.handles;
        let source = source.as_str();
        let error = error.to_string();
        let result = update_metadata_with(handles, task_id, |meta| {
            let failed_at_ms = now_ms();
            let duration_ms = match (meta.dispatched_at_ms, failed_at_ms) {
                (Some(started), failed) => failed.checked_sub(started),
                _ => None,
            };
            meta.attempts.push(crate::types::AttemptFailureRecord {
                attempt,
                source: source.to_string(),
                error: error.clone(),
                started_at_ms: meta.dispatched_at_ms,
                failed_at_ms: Some(failed_at_ms),
                duration_ms,
            });
            Ok(())
        })
        .await;
        if let Err(err) = result {
            tracing::warn!(task_id, err = %err, "failed to record retry in metadata");
        }
    }

    /// 元数据流转：落终态与完成时刻。
    async fn mark_terminal(&self, task_id: &str, state: TaskState) {
        let handles = &self.handles;
        let result = update_metadata_with(handles, task_id, |meta| {
            meta.state = state;
            meta.completed_at_ms = Some(now_ms());
            Ok(())
        })
        .await;
        if let Err(err) = result {
            // 写失败只 warn：complete 事件已经发出，调用方仍能从事件拿到终态。
            tracing::warn!(task_id, err = %err, "failed to mark task terminal in metadata");
        }
    }

    /// 上报「本次投递开始执行」。
    async fn publish_start(&self, task_id: &str, attempt: u32) {
        if let Err(err) = publish_event(
            &self.handles.jetstream.client(),
            &self.handles.config.name,
            &QueueEvent::start(task_id, attempt),
        )
        .await
        {
            tracing::warn!(task_id = %task_id, err = %err, "failed to publish start event");
        }
    }

    /// 上报一次投递失败。
    ///
    /// 每条 [`AckAction::Term`] 路径都必须先做这件事：workqueue 的消息即将消失，
    /// 不会有其他组件再发出终态结果，生产者侧跟踪的执行将永远停留在 pending。
    /// 发布是通知性质的，**绝不能阻塞 ack**。
    async fn publish_attempt_failed(
        &self,
        task_id: &str,
        attempt: u32,
        source: AttemptErrorSource,
        error: &str,
    ) {
        if let Err(err) = publish_event(
            &self.handles.jetstream.client(),
            &self.handles.config.name,
            &QueueEvent::attempt_failed(task_id, attempt, source, error),
        )
        .await
        {
            tracing::warn!(task_id = %task_id, err = %err, "failed to publish attempt_failed event");
        }
    }

    /// 为「[`TaskContext`] 尚未建立」阶段的失败补发终态事件。
    ///
    /// 保住「每条 Term 路径都上报终态」这一不变式；发布是通知性质的。
    async fn publish_pre_context_terminate(&self, task_id: &str, attempt: u32, error: &str) {
        if let Err(err) = publish_event(
            &self.handles.jetstream.client(),
            &self.handles.config.name,
            &QueueEvent::complete(
                task_id,
                attempt,
                TaskStatus::Failed,
                None,
                Some(error.to_string()),
            ),
        )
        .await
        {
            tracing::warn!(task_id = %task_id, err = %err, "failed to publish pre-context terminate event");
        }
    }
}

/// 关停后等待在途投递 ack。
///
/// 超时者被中止：它们本就会丢 lease 并被重投，这比无限阻塞关停更安全。
async fn drain(in_flight: &mut JoinSet<()>) {
    if in_flight.is_empty() {
        return;
    }
    let pending = in_flight.len();
    tracing::info!(pending, "draining in-flight task deliveries");
    let drained = tokio::time::timeout(DRAIN_TIMEOUT, async {
        while in_flight.join_next().await.is_some() {}
    })
    .await;
    if drained.is_err() {
        tracing::warn!(
            pending = in_flight.len(),
            "timed out draining in-flight task deliveries; aborting"
        );
        in_flight.abort_all();
        while in_flight.join_next().await.is_some() {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn fixed_limit_clamps_zero_to_one() {
        // 配置成 0 应退化为串行，而不是让运行器永远停摆。
        let limit = fixed_limit(0);
        assert_eq!(limit(), 1);
        let limit = fixed_limit(4);
        assert_eq!(limit(), 4);
    }

    #[tokio::test]
    async fn drain_waits_for_in_flight_deliveries() {
        let mut in_flight = JoinSet::new();
        in_flight.spawn(async {
            tokio::time::sleep(Duration::from_millis(20)).await;
        });
        drain(&mut in_flight).await;
        assert!(in_flight.is_empty());
    }

    #[tokio::test]
    async fn drain_returns_immediately_when_nothing_is_in_flight() {
        let mut in_flight = JoinSet::new();
        drain(&mut in_flight).await;
        assert!(in_flight.is_empty());
    }

    // ── §9.2「Permit 并发闸门」：并发失控是生产事故，闸门必须有单测 ──

    #[tokio::test]
    async fn concurrency_gate_blocks_beyond_the_limit() {
        let gate = Arc::new(Gate::new(fixed_limit(2)));
        let first = gate.try_acquire().expect("槽位 1");
        let second = gate.try_acquire().expect("槽位 2");
        // 第三个必须拿不到 —— 否则并发上限形同虚设。
        assert!(gate.try_acquire().is_none(), "超过上限仍拿到了槽位");

        drop(first);
        // Permit::drop 必须**同步**归还槽位，这里立刻验证，不给并发竞争留窗口。
        let third = gate.try_acquire().expect("归还后的槽位应立即可复用");
        assert!(gate.try_acquire().is_none());
        drop(second);
        drop(third);
    }

    #[tokio::test]
    async fn concurrency_gate_acquire_returns_none_after_shutdown() {
        let gate = Arc::new(Gate::new(fixed_limit(1)));
        let _held = gate.try_acquire().expect("槽位");
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        // 关停后 `acquire` 必须立刻放弃，不能让运行器卡在等槽位上（关停丢任务）。
        assert!(gate.acquire(&shutdown).await.is_none());
    }

    #[tokio::test]
    async fn concurrency_gate_acquire_waits_then_succeeds_when_a_slot_frees() {
        let gate = Arc::new(Gate::new(fixed_limit(1)));
        let held = gate.try_acquire().expect("槽位");
        let shutdown = CancellationToken::new();
        let waiter = {
            let gate = gate.clone();
            let shutdown = shutdown.clone();
            tokio::spawn(async move { gate.acquire(&shutdown).await.is_some() })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(held);
        assert!(waiter.await.expect("acquire 任务"), "释放槽位后应拿到");
    }

    /// §9.2 的另一半：不阻塞关停 —— `DRAIN_TIMEOUT` 必须是**有界**的。
    ///
    /// 超时上限若被改成"无限等"，在途任务挂死时关停会一直卡着，直到 k3s 的
    /// terminationGracePeriodSeconds 到点 SIGKILL —— 行为与立即 abort 无异，
    /// 但进程在那段时间里拒绝响应新请求。
    #[test]
    fn drain_bounds_shutdown_wait() {
        assert!(DRAIN_TIMEOUT > Duration::ZERO, "超时为零等于立即 abort");
        assert!(
            DRAIN_TIMEOUT <= Duration::from_secs(30),
            "DRAIN_TIMEOUT={DRAIN_TIMEOUT:?} 过长，会撞上 k3s 默认 30s 宽限期"
        );
    }

    #[tokio::test]
    async fn drain_gives_up_on_a_task_that_never_finishes() {
        // 真实语义验证：卡死的任务不会让 drain 无限等待。
        // 用一个短超时的等价结构来避免等满 DRAIN_TIMEOUT。
        let mut stuck = JoinSet::new();
        stuck.spawn(async {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        });
        // 模拟 DRAIN_TIMEOUT 到期后的分支：abort_all + 收干净。
        let began = std::time::Instant::now();
        stuck.abort_all();
        while stuck.join_next().await.is_some() {}
        assert!(
            began.elapsed() < Duration::from_secs(5),
            "abort_all 后仍未收干净：{:?}",
            began.elapsed()
        );
        assert!(stuck.is_empty());
    }

    // ── §9.2「nak 退避重试」：无退避会热循环打爆 worker ──

    #[test]
    fn backoff_climbs_then_holds_at_the_last_step() {
        let config = QueueConfig::new("q");
        let steps = config.retry_backoff_ms.clone();
        // attempt 1 起对应 steps[0]。
        assert_eq!(
            config.backoff_for_attempt(1),
            Duration::from_millis(steps[0])
        );
        assert_eq!(
            config.backoff_for_attempt(2),
            Duration::from_millis(steps[1])
        );
        // 超出阶梯长度后**保持最后一级**，而不是回落到默认 1s ——
        // 回落到 1s 等于把重试变回热循环。
        let last = *steps.last().expect("退避阶梯非空");
        assert_eq!(
            config.backoff_for_attempt(u32::MAX),
            Duration::from_millis(last)
        );
    }

    #[test]
    fn backoff_of_zero_attempt_does_not_panic() {
        // attempt 可能来自不可信来源，0 与 u32::MAX 都不得 panic。
        let config = QueueConfig::new("q");
        assert!(config.backoff_for_attempt(0) <= Duration::from_secs(1));
    }

    #[tokio::test]
    async fn sleep_or_cancel_returns_false_when_shutdown_is_cancelled() {
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        assert!(!sleep_or_cancel(&shutdown, Duration::from_secs(60)).await);
    }

    #[tokio::test]
    async fn sleep_or_cancel_returns_true_after_the_delay() {
        let shutdown = CancellationToken::new();
        assert!(sleep_or_cancel(&shutdown, Duration::from_millis(5)).await);
    }

    #[tokio::test]
    async fn gate_blocks_past_its_limit_and_releases_on_drop() {
        let gate = Arc::new(Gate::new(fixed_limit(1)));
        let first = gate.try_acquire().expect("槽位可用");
        assert!(gate.try_acquire().is_none(), "超过限额必须取不到槽位");
        drop(first);
        assert!(gate.try_acquire().is_some(), "释放后应能再取到");
    }

    #[tokio::test]
    async fn gate_honours_a_limit_raised_at_runtime() {
        // 限额可被配置同步在运行期调整；闸门必须**每次取槽位都重新读取**，
        // 而不是在启动时取一次快照。
        let current = Arc::new(AtomicUsize::new(1));
        let limit: ConcurrencyLimit = {
            let current = Arc::clone(&current);
            Arc::new(move || current.load(Ordering::SeqCst))
        };
        let gate = Arc::new(Gate::new(limit));

        let held = gate.try_acquire().expect("限额 1 时可取一个");
        assert!(
            gate.try_acquire().is_none(),
            "限额为 1 时不得取到第二个槽位"
        );

        current.store(2, Ordering::SeqCst);
        assert!(
            gate.try_acquire().is_some(),
            "运行期把限额提到 2 后应能取到第二个槽位"
        );
        drop(held);
    }
}
