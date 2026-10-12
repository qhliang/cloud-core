//! JetStream 资源句柄：只读 attach + 元数据 CAS。
//!
//! # ⚠️ 本模块绝不创建任何基础设施
//!
//! [`QueueHandles::attach`] 只做**只读查询**（`get_stream` / `get_key_value`），
//! 查不到就返回 [`QueueError::InfraMissing`]。全程不出现 `get_or_create_*`。
//!
//! 理由：
//! 1. 库不应在生产环境偷偷修改基础设施；
//! 2. 多副本并发创建本身就是竞态来源；
//! 3. 创建失败与「资源被外部删除」需要能被**明确区分**（见
//!    [`QueueError::InfraMissing`]），而 `get_or_create` 会把两者吞成同一个结果。
//!
//! # 移除的自愈能力
//!
//! 旧实现里 `QueueHandles::create` 会在每次进程启动时被调用，等于让**生产者**
//! 兜底重建 stream（消费者只重建 consumer，从不重建 stream）。移除创建能力后
//! 没有任何组件会重建 stream —— 这是有意的取舍，代价与恢复步骤见
//! `docs/cloud-core-queue-design.md` §3.2 / §7.2。

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_nats::jetstream::consumer::PullConsumer;
use async_nats::jetstream::consumer::pull::Config as PullConfig;
use async_nats::jetstream::kv::{Store, UpdateError, UpdateErrorKind};
use async_nats::jetstream::stream::Stream;
use async_nats::jetstream::{AckKind, Context as JetStreamContext};
use bytes::Bytes;

use crate::config::{MAX_MESSAGE_SIZE, QueueConfig};
use crate::error::{InfraKind, QueueError};
use crate::types::TaskMeta;

/// 元数据 CAS 的重试次数。
///
/// 三次足以扛过「我们读完之后有并发写入者落地」这一种情况；第四次意味着
/// 这个键被非派发逻辑的东西争抢。
const METADATA_CAS_ATTEMPTS: u32 = 3;

/// 一条队列在 JetStream 上的全部句柄。
#[derive(Clone)]
pub struct QueueHandles {
    /// 队列配置。
    pub config: QueueConfig,
    /// JetStream 上下文。
    pub jetstream: JetStreamContext,
    /// 任务投递 stream（`{queue}`）。
    pub task_stream: Stream,
    /// 元数据 KV bucket（`{queue}-meta`）。
    pub metadata: Store,
    /// 结果归档是否启用。
    pub results_archive: bool,
}

/// 队列入口。
pub struct Queue;

impl Queue {
    /// 附着到已存在的队列，**不创建任何资源**。
    ///
    /// 缺失的资源会以 [`QueueError::InfraMissing`] 报出，供调用方退避重试
    /// 并反映到 `/healthz`。
    pub async fn attach(
        client: JetStreamContext,
        config: QueueConfig,
    ) -> Result<QueueHandles, QueueError> {
        config
            .validate()
            .map_err(|err| QueueError::Nats(anyhow::Error::new(err)))?;

        let task_stream = client
            .get_stream(&config.name)
            .await
            .map_err(|err| classify_attach_error(InfraKind::TaskStream, &config.name, err))?;

        let metadata = client
            .get_key_value(&metadata_bucket(&config.name))
            .await
            .map_err(|err| {
                classify_attach_error(
                    InfraKind::MetadataBucket,
                    &metadata_bucket(&config.name),
                    err,
                )
            })?;

        if config.results_archive {
            client
                .get_stream(&result_stream_name(&config.name))
                .await
                .map_err(|err| {
                    classify_attach_error(
                        InfraKind::ResultStream,
                        &result_stream_name(&config.name),
                        err,
                    )
                })?;
        }

        Ok(QueueHandles {
            results_archive: config.results_archive,
            config,
            jetstream: client,
            task_stream,
            metadata,
        })
    }
}

/// 把 attach 阶段的错误分类为「资源缺失」或「传输故障」。
///
/// ⚠️ 这一步是「不创建」策略的关键：若把「资源不存在」笼统地报成传输错误，
/// 调用方无法区分「该退避重试（人工 provision 后会好）」与「该报警（配置错了）」。
///
/// async-nats 0.49 用**类型化错误**（`Error<Kind>`），不同操作的 Kind 各不相同；
/// 但它们都包着同一个服务端 `jetstream::Error`，而「不存在」最终都体现为
/// 服务端的 not-found 错误码或描述。故按 `Display` 文本 + 404 状态判定，
/// 两种形态都能覆盖。
fn classify_attach_error<Kind>(kind: InfraKind, name: &str, err: Kind) -> QueueError
where
    Kind: std::fmt::Display,
{
    let text = err.to_string();
    if is_not_found(&text) {
        QueueError::infra_missing(kind, name)
    } else {
        QueueError::Nats(anyhow::anyhow!(text))
    }
}

/// 服务端对「资源不存在」的报错形态较多，这里按文本特征判定。
fn is_not_found(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    lower.contains("not found")
        || lower.contains("does not exist")
        || lower.contains("no stream")
        || lower.contains("no responders")
}

/// 取得（或复用）消费者的 durable pull consumer。
///
/// ⚠️ 这里**允许** `get_or_create_consumer`：durable consumer 属于「消费位置」
/// 而非「基础设施拓扑」，且旧实现在重建路径上同样依赖它（JetStream volume 被清空时
/// consumer 会一起消失）。去掉它会让消费者在 NATS 重建后彻底卡死。
/// 即便如此，本函数**不会**创建 stream。
pub async fn ensure_worker(handles: &QueueHandles) -> Result<PullConsumer, QueueError> {
    // 同质 worker 共用同一个 durable consumer，JetStream 才能一次性跟踪
    // 投递次数、ack 状态与重试策略。
    let name = worker_consumer_name(&handles.config.name);
    handles
        .task_stream
        .get_or_create_consumer(
            name.as_str(),
            PullConfig {
                durable_name: Some(name.clone()),
                filter_subject: task_filter(&handles.config.name),
                ack_wait: handles.config.ack_wait,
                max_deliver: handles.config.max_deliver,
                ..Default::default()
            },
        )
        .await
        .map_err(|err| {
            if is_not_found(&err.to_string()) {
                // consumer 所属的 stream 没了 —— 这正是「不创建」策略的已知代价。
                QueueError::infra_missing(InfraKind::TaskStream, &handles.config.name)
            } else {
                QueueError::Nats(anyhow::anyhow!(err.to_string()))
            }
        })
}

/// 读取任务元数据。任务不存在时返回 `Ok(None)`。
pub async fn get_metadata(
    handles: &QueueHandles,
    task_id: &str,
) -> Result<Option<TaskMeta>, QueueError> {
    let key = metadata_key(&handles.config.name, task_id);
    let entry = handles
        .metadata
        .entry(key.as_str())
        .await
        .map_err(|err| QueueError::Nats(anyhow::Error::new(err)))?;
    let Some(entry) = entry else {
        return Ok(None);
    };
    let meta = serde_json::from_slice(entry.value.as_ref())
        .map_err(|err| QueueError::Nats(anyhow::anyhow!("decode task metadata: {err}")))?;
    Ok(Some(meta))
}

/// 读取任务元数据并带回 KV revision（CAS 需要）。
pub async fn get_metadata_with_revision(
    handles: &QueueHandles,
    task_id: &str,
) -> Result<Option<(u64, TaskMeta)>, QueueError> {
    let key = metadata_key(&handles.config.name, task_id);
    let entry = handles
        .metadata
        .entry(key.as_str())
        .await
        .map_err(|err| QueueError::Nats(anyhow::Error::new(err)))?;
    let Some(entry) = entry else {
        return Ok(None);
    };
    let meta = serde_json::from_slice(entry.value.as_ref())
        .map_err(|err| QueueError::Nats(anyhow::anyhow!("decode task metadata: {err}")))?;
    Ok(Some((entry.revision, meta)))
}

/// 写入任务元数据（无条件覆盖）。仅用于**新建**任务。
pub async fn put_metadata(handles: &QueueHandles, meta: &TaskMeta) -> Result<u64, QueueError> {
    let raw = serde_json::to_vec(meta)
        .map_err(|err| QueueError::Nats(anyhow::anyhow!("encode task metadata: {err}")))?;
    let key = metadata_key(&handles.config.name, &meta.id);
    handles
        .metadata
        .put(key.as_str(), Bytes::from(raw))
        .await
        .map_err(|err| QueueError::Nats(anyhow::Error::new(err)))
}

/// 在 KV revision 的 CAS 保护下修改任务元数据。
///
/// # 为什么必须是 CAS 而不是读-改-写
///
/// 同一个键有**两个独立写入者**：消费者把任务标记为 running，生产者请求取消。
/// 无条件 `put` 会静默丢掉对方的字段 —— 尤其 `cancel_requested`：一次取消会被
/// 下一次派发写入抹掉，任务于是无视取消请求跑完。
///
/// # 为什么只重试 revision 冲突
///
/// `WrongLastRevision` 说明服务端**拒绝了**这次写入，变更可证明未生效，因此可以
/// 拿新快照安全重算。其余失败（`TimedOut`、传输、store 被删）意味着写入结果
/// **未知** —— 超时的写可能已在服务端生效，重放会让非幂等的 `mutate`
/// （比如往 `attempts` 里追加）产生重复效果。
///
/// `mutate` 是 `FnMut` 而非 `Fn`，因为它可能每轮重试跑一次，让调用方能报告
/// 重放走了哪条分支。
///
/// 返回获胜写入产生的 revision。
pub async fn update_metadata_with<F>(
    handles: &QueueHandles,
    task_id: &str,
    mut mutate: F,
) -> Result<u64, QueueError>
where
    F: FnMut(&mut TaskMeta) -> Result<(), QueueError>,
{
    let key = metadata_key(&handles.config.name, task_id);
    let mut attempts = 0;
    loop {
        attempts += 1;
        let Some((revision, mut meta)) = get_metadata_with_revision(handles, task_id).await? else {
            return Err(QueueError::Task(format!("task not found: {task_id}")));
        };
        mutate(&mut meta)?;
        let raw = serde_json::to_vec(&meta)
            .map_err(|err| QueueError::Nats(anyhow::anyhow!("encode task metadata: {err}")))?;
        match handles
            .metadata
            .update(key.as_str(), Bytes::from(raw), revision)
            .await
        {
            Ok(next) => return Ok(next),
            Err(err) => {
                if !is_retryable_update_error(&err, attempts) {
                    return Err(QueueError::Nats(anyhow::Error::new(err)));
                }
                tracing::debug!(
                    task_id = %task_id,
                    attempts,
                    "task metadata cas conflict; replaying mutation"
                );
            }
        }
    }
}

/// CAS 失败是否值得重放。
///
/// 只有 revision 冲突算。其余种类的结果未知（超时可能已生效），重放会让
/// 非幂等的变更产生重复效果，因此直接上报。
fn is_retryable_update_error(err: &UpdateError, attempts: u32) -> bool {
    err.kind() == UpdateErrorKind::WrongLastRevision && attempts < METADATA_CAS_ATTEMPTS
}

/// JetStream 的四种确认语义。
#[derive(Debug, Clone)]
pub enum AckAction {
    /// 成功终结，不再投递。
    Ack,
    /// 仍在执行，延长 lease。
    Progress,
    /// 失败，按给定延迟后重投；`None` 用服务端默认值。
    Nak(Option<std::time::Duration>),
    /// 终结，不再投递且不重试。
    Term,
}

impl AckAction {
    /// 应用到 JetStream。
    pub async fn apply(
        &self,
        acker: &async_nats::jetstream::message::Acker,
    ) -> Result<(), QueueError> {
        let result = match self {
            Self::Ack => acker.ack_with(AckKind::Ack).await,
            Self::Progress => acker.ack_with(AckKind::Progress).await,
            Self::Nak(delay) => acker.ack_with(AckKind::Nak(*delay)).await,
            Self::Term => acker.ack_with(AckKind::Term).await,
        };
        result.map_err(|err| QueueError::Nats(anyhow::anyhow!("acknowledge task: {err}")))
    }
}

/// 任务 subject 的订阅过滤器。
pub fn task_filter(queue: &str) -> String {
    format!("{queue}.tasks.>")
}

/// 单个任务的投递 subject。
pub fn task_subject(queue: &str, task_id: &str) -> String {
    format!("{queue}.tasks.{task_id}")
}

/// 结果 subject 的订阅过滤器。
pub fn result_filter(queue: &str) -> String {
    format!("{queue}.results.>")
}

/// 单个任务的结果 subject。
pub fn result_subject(queue: &str, task_id: &str) -> String {
    format!("{queue}.results.{task_id}")
}

/// 元数据 KV bucket 名。
pub fn metadata_bucket(queue: &str) -> String {
    format!("{queue}-meta")
}

/// 结果归档 stream 名。
pub fn result_stream_name(queue: &str) -> String {
    format!("{queue}-results")
}

/// 消费者 durable consumer 名。
pub fn worker_consumer_name(queue: &str) -> String {
    format!("{queue}-worker")
}

/// 元数据 KV 的键。
pub fn metadata_key(queue: &str, task_id: &str) -> String {
    format!("{queue}.{task_id}")
}

/// 当前 Unix 毫秒。
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or_default()
}

/// 共享句柄，便于在多个任务间传递。
pub type SharedQueueHandles = Arc<QueueHandles>;

/// 供外部 provision 工具复用的资源规格。
///
/// ⚠️ 本 crate **不用**它创建任何东西 —— 它存在的意义是让运维脚本与
/// `docs` §7.2 的手工命令保持同一份事实来源。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InfraSpec {
    /// stream 名。
    pub stream: String,
    /// 订阅的 subject 列表。
    pub subjects: Vec<String>,
    /// 保留策略（`workqueue` 或 `limits`）。
    pub retention: &'static str,
    /// 最大消息字节数。
    pub max_message_size: i32,
}

/// 按配置列出该队列**必须预先存在**的基础设施。
///
/// 供部署前检查与文档生成使用；本 crate 不据此创建资源。
pub fn infra_specs(config: &QueueConfig) -> Vec<InfraSpec> {
    let mut specs = vec![InfraSpec {
        stream: config.name.clone(),
        subjects: vec![task_filter(&config.name)],
        retention: "workqueue",
        max_message_size: MAX_MESSAGE_SIZE,
    }];
    if config.results_archive {
        specs.push(InfraSpec {
            stream: result_stream_name(&config.name),
            subjects: vec![result_filter(&config.name)],
            retention: "limits",
            max_message_size: MAX_MESSAGE_SIZE,
        });
    }
    specs
}

/// 该队列必须预先存在的 KV bucket 名。
pub fn required_metadata_buckets(config: &QueueConfig) -> Vec<String> {
    vec![metadata_bucket(&config.name)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_names_are_stable() {
        // 这些名字同时出现在线上与恢复手册里，改动即破坏兼容性。
        assert_eq!(task_filter("agent-task"), "agent-task.tasks.>");
        assert_eq!(
            task_subject("agent-task", "task-1"),
            "agent-task.tasks.task-1"
        );
        assert_eq!(result_filter("agent-task"), "agent-task.results.>");
        assert_eq!(
            result_subject("agent-task", "task-1"),
            "agent-task.results.task-1"
        );
        assert_eq!(metadata_bucket("agent-task"), "agent-task-meta");
        assert_eq!(result_stream_name("agent-task"), "agent-task-results");
        assert_eq!(worker_consumer_name("agent-task"), "agent-task-worker");
        assert_eq!(metadata_key("agent-task", "task-1"), "agent-task.task-1");
    }

    #[test]
    fn metadata_keys_are_namespaced_by_queue() {
        assert_eq!(
            metadata_key("agent-task", "0198e57c-0000-7000-8000-000000000000"),
            "agent-task.0198e57c-0000-7000-8000-000000000000"
        );
        assert_ne!(
            metadata_key("agent-task", "t1"),
            metadata_key("workflow-task", "t1")
        );
    }

    #[test]
    fn revision_conflict_is_retryable_until_budget_is_spent() {
        let conflict = UpdateError::new(UpdateErrorKind::WrongLastRevision);
        assert!(is_retryable_update_error(&conflict, 1));
        assert!(is_retryable_update_error(
            &conflict,
            METADATA_CAS_ATTEMPTS - 1
        ));
        // 最后一次允许的尝试已经用掉，再重试就等于没有新快照地改状态。
        assert!(!is_retryable_update_error(&conflict, METADATA_CAS_ATTEMPTS));
    }

    #[test]
    fn unknown_outcome_write_errors_are_never_replayed() {
        // 超时/传输失败可能已在服务端生效，结果未知，重放会让非幂等变更重复。
        for kind in [
            UpdateErrorKind::TimedOut,
            UpdateErrorKind::Other,
            UpdateErrorKind::InvalidKey,
        ] {
            assert!(
                !is_retryable_update_error(&UpdateError::new(kind), 1),
                "{kind:?} 不得重放"
            );
        }
    }

    #[test]
    fn infra_specs_match_the_documented_provisioning_commands() {
        let config = QueueConfig::new("agent-task");
        let specs = infra_specs(&config);
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].stream, "agent-task");
        assert_eq!(specs[0].retention, "workqueue");
        assert_eq!(specs[1].stream, "agent-task-results");
        assert_eq!(specs[1].retention, "limits");
        assert_eq!(required_metadata_buckets(&config), vec!["agent-task-meta"]);
    }

    #[test]
    fn results_archive_disabled_omits_result_stream() {
        let mut config = QueueConfig::new("agent-task");
        config.results_archive = false;
        let specs = infra_specs(&config);
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].stream, "agent-task");
    }
}
