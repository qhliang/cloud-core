//! 生命周期事件（控制面 `{queue}.events`）。
//!
//! # 为什么是枚举而不是四个自由函数
//!
//! v1 用 `serde_json::json!` 手工拼四种事件、接收侧再用
//! `value.get("id")?.as_str()?` 逐字段拆（10 处取值语句）。那种写法：
//! - `type` 是裸字符串，拼错不报错；
//! - 四份字段集各写一遍，`heartbeat` 漏了 `attempt` 这类不一致无从发现；
//! - 收发两端各有一份字段知识，改一处忘另一处不会编译失败。
//!
//! v2 改为**一个带 `#[serde(tag = "type")]` 的枚举**：判别字段由 serde 生成、
//! 由编译器校对拼写；四个变体集中在一处，公共字段的一致性肉眼可查；
//! 收发共用同一份定义。
//!
//! # 库内不过滤
//!
//! 事件**完整投递**给每个订阅者。`task_id` 由 UUID v7 生成、没有服务前缀，
//! 无法在 subject 层过滤；而将来会有别的服务往同一队列投递任务。因此
//! 「这个任务是不是我的」必须由调用方反查判断，库擅自丢弃会让调用方失去
//! 观测全量的能力。

use serde::{Deserialize, Serialize};

use crate::types::{AttemptErrorSource, TaskStatus, base64_decode, base64_encode};

/// `{queue}.events` 的 subject。
pub fn events_subject(queue: &str) -> String {
    format!("{queue}.events")
}

/// 任务的一次生命周期事件。
///
/// 四个变体的公共字段统一为 `id` + `attempt`（v1 的 `heartbeat` 缺 `attempt`）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum QueueEvent {
    /// 任务某次投递开始执行。
    Start {
        /// 任务标识。
        id: String,
        /// 第几次投递（从 1 开始）。
        attempt: u32,
    },
    /// 业务进度上报。
    ///
    /// ⚠️ 心跳**不**延长 JetStream lease，也不是存活信号 —— lease 由
    /// [`crate::consumer::Consumer`] 独立续期。
    Heartbeat {
        /// 任务标识。
        id: String,
        /// 第几次投递。
        attempt: u32,
        /// 进度描述，受 `HEARTBEAT_MAX_INFO_BYTES` 限制。
        info: String,
    },
    /// 某次投递失败。
    AttemptFailed {
        /// 任务标识。
        id: String,
        /// 第几次投递。
        attempt: u32,
        /// 失败来源。
        source: AttemptErrorSource,
        /// 失败原因。
        error: String,
    },
    /// 任务到达终态。
    Complete {
        /// 任务标识。
        id: String,
        /// 第几次投递。
        attempt: u32,
        /// 终态结论。
        status: TaskStatus,
        /// 失败原因；成功时为 `None`。
        error: Option<String>,
        /// base64 编码的终态输出；无输出时为空串（**不是** `null`）。
        output: String,
    },
}

impl QueueEvent {
    /// 任务标识。四个变体共有，故在此统一提取。
    pub fn id(&self) -> &str {
        match self {
            Self::Start { id, .. }
            | Self::Heartbeat { id, .. }
            | Self::AttemptFailed { id, .. }
            | Self::Complete { id, .. } => id,
        }
    }

    /// 第几次投递。四个变体共有。
    pub fn attempt(&self) -> u32 {
        match self {
            Self::Start { attempt, .. }
            | Self::Heartbeat { attempt, .. }
            | Self::AttemptFailed { attempt, .. }
            | Self::Complete { attempt, .. } => *attempt,
        }
    }

    /// 构造 `start`。
    pub fn start(id: impl Into<String>, attempt: u32) -> Self {
        Self::Start {
            id: id.into(),
            attempt,
        }
    }

    /// 构造 `heartbeat`。
    pub fn heartbeat(id: impl Into<String>, attempt: u32, info: impl Into<String>) -> Self {
        Self::Heartbeat {
            id: id.into(),
            attempt,
            info: info.into(),
        }
    }

    /// 构造 `attempt_failed`。
    pub fn attempt_failed(
        id: impl Into<String>,
        attempt: u32,
        source: AttemptErrorSource,
        error: impl Into<String>,
    ) -> Self {
        Self::AttemptFailed {
            id: id.into(),
            attempt,
            source,
            error: error.into(),
        }
    }

    /// 构造 `complete`。`output` 为 `None` 时序列化为空串。
    pub fn complete(
        id: impl Into<String>,
        attempt: u32,
        status: TaskStatus,
        output: Option<&[u8]>,
        error: Option<String>,
    ) -> Self {
        Self::Complete {
            id: id.into(),
            attempt,
            status,
            error,
            output: output.map(base64_encode).unwrap_or_default(),
        }
    }

    /// 取出终态输出。解码失败返回 `None`。
    pub fn decoded_output(&self) -> Option<Vec<u8>> {
        let Self::Complete { output, .. } = self else {
            return None;
        };
        if output.is_empty() {
            return None;
        }
        base64_decode(output).ok()
    }
}

/// 把事件发布到 core NATS 的 `{queue}.events`。
///
/// ⚠️ 发布是**通知性质**的：生命周期事件走 core NATS 而非 JetStream 持久流，
/// 订阅方漏收不会导致任务卡住。因此这里的失败只返回给调用方记录日志，
/// 绝不能阻塞消费者的 ack 路径。
pub async fn publish_event(
    client: &async_nats::Client,
    queue: &str,
    event: &QueueEvent,
) -> Result<(), crate::QueueError> {
    let payload = serde_json::to_vec(event)
        .map_err(|err| crate::QueueError::Nats(anyhow::Error::new(err)))?;
    client
        .publish(events_subject(queue), payload.into())
        .await
        .map_err(|err| crate::QueueError::Nats(anyhow::anyhow!("publish task event: {err}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn start_event_shape_is_stable() {
        // 对照线上现有 JSON：{"type":"start","id":...,"attempt":...}
        let event = QueueEvent::start("task-1", 1);
        assert_eq!(
            serde_json::to_value(&event).expect("serializes"),
            json!({"type": "start", "id": "task-1", "attempt": 1})
        );
    }

    #[test]
    fn heartbeat_event_now_carries_attempt() {
        // v1 的 heartbeat 漏了 attempt —— 这正是本次要修的不一致之一。
        let event = QueueEvent::heartbeat("task-1", 3, "扫描中");
        assert_eq!(
            serde_json::to_value(&event).expect("serializes"),
            json!({"type": "heartbeat", "id": "task-1", "attempt": 3, "info": "扫描中"})
        );
    }

    #[test]
    fn attempt_failed_event_shape_is_stable() {
        let event = QueueEvent::attempt_failed("task-1", 2, AttemptErrorSource::Guest, "boom");
        assert_eq!(
            serde_json::to_value(&event).expect("serializes"),
            json!({
                "type": "attempt_failed", "id": "task-1", "attempt": 2,
                "source": "guest", "error": "boom",
            })
        );
    }

    #[test]
    fn complete_event_uses_kebab_case_status_and_base64_output() {
        let event = QueueEvent::complete(
            "task-1",
            1,
            TaskStatus::MaxRetriesExceeded,
            Some(b"out"),
            Some("failed".to_string()),
        );
        assert_eq!(
            serde_json::to_value(&event).expect("serializes"),
            json!({
                "type": "complete", "id": "task-1", "attempt": 1,
                "status": "max-retries-exceeded", "error": "failed", "output": "b3V0",
            })
        );
    }

    #[test]
    fn complete_event_without_output_uses_empty_string_not_null() {
        let event = QueueEvent::complete("task-1", 1, TaskStatus::Failed, None, None);
        let json = serde_json::to_value(&event).expect("serializes");
        assert_eq!(json["output"], "");
        assert!(json["output"].is_string());
        assert!(event.decoded_output().is_none());
    }

    #[test]
    fn every_variant_round_trips_through_json() {
        let events = [
            QueueEvent::start("t", 1),
            QueueEvent::heartbeat("t", 1, "x"),
            QueueEvent::attempt_failed("t", 1, AttemptErrorSource::System, "e"),
            QueueEvent::complete("t", 1, TaskStatus::Succeeded, Some(b"o"), None),
        ];
        for event in events {
            let bytes = serde_json::to_vec(&event).expect("serializes");
            let back: QueueEvent = serde_json::from_slice(&bytes).expect("deserializes");
            assert_eq!(back, event);
        }
    }

    #[test]
    fn unknown_event_type_is_rejected_rather_than_ignored() {
        // v1 的手写解析会在遇到未知 type 时静默丢弃；枚举让它明确失败。
        let err = serde_json::from_str::<QueueEvent>(r#"{"type":"bogus","id":"t"}"#);
        assert!(err.is_err(), "未知 type 必须报错，不能静默丢弃");
    }

    #[test]
    fn id_and_attempt_are_extractable_from_every_variant() {
        let events = [
            QueueEvent::start("task-x", 7),
            QueueEvent::heartbeat("task-x", 7, "i"),
            QueueEvent::attempt_failed("task-x", 7, AttemptErrorSource::Guest, "e"),
            QueueEvent::complete("task-x", 7, TaskStatus::Succeeded, None, None),
        ];
        for event in events {
            assert_eq!(event.id(), "task-x");
            assert_eq!(event.attempt(), 7);
        }
    }

    #[test]
    fn events_subject_is_queue_scoped() {
        assert_eq!(events_subject("agent-task"), "agent-task.events");
    }
}
