//! 线格式契约测试。
//!
//! # 为什么用字面量锚定而不是与旧实现对比
//!
//! v2 **明确不兼容** v1（见 `docs/cloud-core-queue-design.md` §6.2），因此
//! 「与 `task_queue_core` 的运行时输出对比」是错的基准 —— 那是拿自己和��己比，
//! 测不出回归。
//!
//! 正确的基准是**线上现有 JSON 的字面量**：把每个表面的期望 JSON 直接写死在这里，
//! 任何字段增删改名都会让测试变红。这组测试是线格式的护栏，不是回归工具。
//!
//! 改动线格式时**必须**同步更新本文件，并视作一次破坏性变更（对应
//! 「先消费者、后生产者」的迁移顺序）。

use cloud_core_queue::events::QueueEvent;
use cloud_core_queue::types::{AttemptErrorSource, TaskEnvelope, TaskMeta, TaskState, TaskStatus};
use cloud_core_queue::{
    metadata_bucket, metadata_key, result_filter, result_stream_name, result_subject, task_filter,
    task_subject, worker_consumer_name,
};
use serde_json::{Value, json};

// ── 数据面：任务信封 ──────────────────────────────────────────────

#[test]
fn envelope_wire_shape_is_frozen() {
    let envelope = TaskEnvelope::new("task-1", b"hello".to_vec(), 1_757_000_000_000);
    let value = serde_json::to_value(&envelope).expect("serializes");
    assert_eq!(
        value,
        json!({
            "schema_version": 1,
            "id": "task-1",
            "encoding": "base64",
            // "hello" 的 base64
            "payload": "aGVsbG8=",
            "deadline_ms": 1_757_000_000_000u64,
        })
    );
}

#[test]
fn envelope_carries_arbitrary_bytes_intact() {
    // 线格式能承载任意字节，是 v2 收敛成单一 base64 字段的前提。
    let payload: Vec<u8> = (0u8..=255).collect();
    let envelope = TaskEnvelope::new("task-1", payload.clone(), 0);
    let json = serde_json::to_string(&envelope).expect("serializes");
    let back: TaskEnvelope = serde_json::from_str(&json).expect("deserializes");
    assert_eq!(back.decode_payload().expect("decodes"), payload);
}

#[test]
fn envelope_round_trips_through_json() {
    let envelope = TaskEnvelope::new("task-1", vec![0u8, 0xff, 0xfe], 42);
    let bytes = serde_json::to_vec(&envelope).expect("serializes");
    let back: TaskEnvelope = serde_json::from_slice(&bytes).expect("deserializes");
    assert_eq!(back.id(), "task-1");
    assert_eq!(back.deadline_ms(), 42);
    assert_eq!(
        back.decode_payload().expect("decodes"),
        vec![0u8, 0xff, 0xfe]
    );
}

// ── 控制面：生命周期事件 ──────────────────────────────────────────

#[test]
fn start_event_wire_shape_is_frozen() {
    let value: Value = serde_json::to_value(QueueEvent::start("task-1", 1)).expect("serializes");
    assert_eq!(
        value,
        json!({"type": "start", "id": "task-1", "attempt": 1})
    );
}

#[test]
fn heartbeat_event_wire_shape_is_frozen() {
    // v2 相对 v1 的唯一新增：heartbeat 现在也带 attempt。
    let value: Value =
        serde_json::to_value(QueueEvent::heartbeat("task-1", 2, "扫描中")).expect("serializes");
    assert_eq!(
        value,
        json!({
            "type": "heartbeat", "id": "task-1", "attempt": 2, "info": "扫描中",
        })
    );
}

#[test]
fn attempt_failed_event_wire_shape_is_frozen() {
    for (source, text) in [
        (AttemptErrorSource::Guest, "guest"),
        (AttemptErrorSource::System, "system"),
    ] {
        let value: Value =
            serde_json::to_value(QueueEvent::attempt_failed("task-1", 3, source, "boom"))
                .expect("serializes");
        assert_eq!(
            value,
            json!({
                "type": "attempt_failed", "id": "task-1", "attempt": 3,
                "source": text, "error": "boom",
            }),
            "source={text} 的线格式不应变化"
        );
    }
}

#[test]
fn complete_event_wire_shape_is_frozen() {
    let cases: [(TaskStatus, &str); 6] = [
        (TaskStatus::Succeeded, "succeeded"),
        (TaskStatus::Failed, "failed"),
        (TaskStatus::DispatchTimeout, "dispatch-timeout"),
        (TaskStatus::ExecutionTimeout, "execution-timeout"),
        (TaskStatus::Cancelled, "cancelled"),
        (TaskStatus::MaxRetriesExceeded, "max-retries-exceeded"),
    ];
    for (status, text) in cases {
        let value: Value = serde_json::to_value(QueueEvent::complete(
            "task-1",
            1,
            status,
            Some(b"out"),
            Some("why".to_string()),
        ))
        .expect("serializes");
        assert_eq!(
            value,
            json!({
                "type": "complete", "id": "task-1", "attempt": 1,
                "status": text, "error": "why",
                // "out" 的 base64
                "output": "b3V0",
            }),
            "status={text} 的拼写不应变化"
        );
    }
}

#[test]
fn complete_event_without_output_serializes_empty_string() {
    // v1 是 `null`，v2 收紧为空串：output 恒为字符串，反序列化侧无需判空。
    let value: Value = serde_json::to_value(QueueEvent::complete(
        "task-1",
        1,
        TaskStatus::Failed,
        None,
        None,
    ))
    .expect("serializes");
    assert_eq!(value["output"], json!(""));
    assert!(value["output"].is_string());
    assert_eq!(value["error"], Value::Null);
}

#[test]
fn every_event_variant_round_trips() {
    let events = [
        QueueEvent::start("task-1", 1),
        QueueEvent::heartbeat("task-1", 1, "info"),
        QueueEvent::attempt_failed("task-1", 1, AttemptErrorSource::Guest, "err"),
        QueueEvent::complete("task-1", 1, TaskStatus::Succeeded, Some(b"o"), None),
    ];
    for event in events {
        let bytes = serde_json::to_vec(&event).expect("serializes");
        let back: QueueEvent = serde_json::from_slice(&bytes).expect("deserializes");
        assert_eq!(back, event);
    }
}

#[test]
fn unknown_event_type_fails_loudly() {
    // 线上出现未知 type 时必须报错，不能静默丢弃 —— 否则事件静默消失，
    // 生产者侧的任务永远停在 pending。
    for raw in [
        r#"{"type":"bogus","id":"t","attempt":1}"#,
        r#"{"id":"t","attempt":1}"#,
    ] {
        assert!(
            serde_json::from_str::<QueueEvent>(raw).is_err(),
            "{raw} 应当被拒绝"
        );
    }
}

// ── 元数据面：TaskMeta ───────────────────────────────────────────

#[test]
fn task_meta_wire_shape_is_frozen() {
    let meta = TaskMeta {
        schema_version: 1,
        id: "task-1".to_string(),
        queue: "agent-task".to_string(),
        state: TaskState::Queued,
        attempt: 0,
        created_at_ms: 1_000,
        dispatched_at_ms: None,
        completed_at_ms: None,
        deadline_ms: 2_000,
        cancel_requested: false,
        attempts: Vec::new(),
    };
    assert_eq!(
        serde_json::to_value(&meta).expect("serializes"),
        json!({
            "schema_version": 1,
            "id": "task-1",
            "queue": "agent-task",
            "state": "queued",
            "attempt": 0,
            "created_at_ms": 1_000u64,
            "dispatched_at_ms": null,
            "completed_at_ms": null,
            "deadline_ms": 2_000u64,
            "cancel_requested": false,
            "attempts": [],
        })
    );
}

#[test]
fn task_meta_state_uses_snake_case() {
    for (state, text) in [
        (TaskState::Queued, "queued"),
        (
            TaskState::DispatchTimeoutPending,
            "dispatch_timeout_pending",
        ),
        (TaskState::Running, "running"),
        (TaskState::DispatchTimeout, "dispatch_timeout"),
        (TaskState::ExecutionTimeout, "execution_timeout"),
        (TaskState::Cancelled, "cancelled"),
        (TaskState::MaxRetriesExceeded, "max_retries_exceeded"),
        (TaskState::Succeeded, "succeeded"),
        (TaskState::Failed, "failed"),
    ] {
        let value = serde_json::to_value(state).expect("serializes");
        assert_eq!(value, json!(text), "TaskState::{state:?} 的拼写不应变化");
    }
}

// ── subject 与资源命名 ───────────────────────────────────────────
//
// 这些名字同时出现在线上与恢复手册（docs §7.2）里，改动即破坏兼容性。

#[test]
fn subject_and_resource_names_are_frozen() {
    let queue = "agent-task";
    assert_eq!(task_filter(queue), "agent-task.tasks.>");
    assert_eq!(task_subject(queue, "task-1"), "agent-task.tasks.task-1");
    assert_eq!(result_filter(queue), "agent-task.results.>");
    assert_eq!(result_subject(queue, "task-1"), "agent-task.results.task-1");
    assert_eq!(metadata_bucket(queue), "agent-task-meta");
    assert_eq!(result_stream_name(queue), "agent-task-results");
    assert_eq!(worker_consumer_name(queue), "agent-task-worker");
    assert_eq!(
        cloud_core_queue::events::events_subject(queue),
        "agent-task.events"
    );
    assert_eq!(metadata_key(queue, "task-1"), "agent-task.task-1");
}

// ── 跨表面一致性 ─────────────────────────────────────────────────

#[test]
fn every_surface_uses_id_not_task_id() {
    // 「id 统一」是 v2 的首要原则。任何表面退回 task_id 都会破坏统一性。
    let envelope = TaskEnvelope::new("task-1", b"x".to_vec(), 0);
    let envelope_json = serde_json::to_string(&envelope).expect("serializes");
    assert!(!envelope_json.contains("task_id"), "{envelope_json}");

    let events = [
        QueueEvent::start("task-1", 1),
        QueueEvent::heartbeat("task-1", 1, "i"),
        QueueEvent::attempt_failed("task-1", 1, AttemptErrorSource::Guest, "e"),
        QueueEvent::complete("task-1", 1, TaskStatus::Succeeded, None, None),
    ];
    for event in events {
        let json = serde_json::to_string(&event).expect("serializes");
        assert!(!json.contains("task_id"), "{json}");
        assert_eq!(event.id(), "task-1");
    }
}

#[test]
fn every_event_variant_carries_an_attempt() {
    // v1 的 heartbeat 缺 attempt；v2 四个变体统一携带。
    let events = [
        QueueEvent::start("task-1", 4),
        QueueEvent::heartbeat("task-1", 4, "i"),
        QueueEvent::attempt_failed("task-1", 4, AttemptErrorSource::System, "e"),
        QueueEvent::complete("task-1", 4, TaskStatus::Failed, None, None),
    ];
    for event in events {
        assert_eq!(event.attempt(), 4, "{event:?} 必须携带 attempt");
        let json = serde_json::to_value(&event).expect("serializes");
        assert_eq!(json["attempt"], json!(4));
    }
}
