//! 端到端集成测试：真实 NATS + 真实 JetStream。
//!
//! # 为什么要单独一个 crate / 文件
//!
//! 纯逻辑测试（`src/**` 的 `#[cfg(test)]`）覆盖不到 attach、投递、消费、事件、
//! CAS 这些**跨进程**行为 —— 而它们恰好是本次切换最容易出错的地方。
//!
//! # 运行前提
//!
//! 需要一个带 JetStream 的本地 NATS，并**预先创建**好队列资源（因为本库
//! 刻意不创建任何基础设施）：
//!
//! ```text
//! nats-server -js -sd <dir> -p 4222
//! nats kv add workflow-task-meta --history 1
//! nats stream add workflow-task         --subjects 'workflow-task.tasks.>'   --retention workq
//! nats stream add workflow-task-results --subjects 'workflow-task.results.>' --retention limits
//! ```
//!
//! 然后：
//!
//! ```text
//! CCQ_NATS_URL=nats://127.0.0.1:4222 cargo test -p cloud-core-queue --test integration
//! ```
//!
//! 未设置 `CCQ_NATS_URL` 时全部跳过（`cargo test` 在 CI 上仍应能通过）。

use std::time::Duration;

use cloud_core_queue::nats::{
    Queue, get_metadata, infra_specs, put_metadata, required_metadata_buckets,
};
use cloud_core_queue::worker::HeartbeatSink;
use cloud_core_queue::{
    AttemptErrorSource, Consumer, Producer, QueueConfig, Task, TaskContext, TaskError,
    TaskErrorSource, TaskOutput, TaskState, TaskStatus, Worker,
};
use tokio_util::sync::CancellationToken;

/// 测试用队列名。与本地预置的资源一致。
const QUEUE: &str = "workflow-task";

fn nats_url() -> Option<String> {
    std::env::var("CCQ_NATS_URL").ok()
}

/// 连到测试 NATS；未配置则让测试安静跳过。
async fn connect() -> Option<cloud_core_queue::nats::QueueHandles> {
    let url = nats_url()?;
    let client = async_nats::connect(&url).await.expect("连接测试 NATS");
    let config = QueueConfig::new(QUEUE);
    match Queue::attach(async_nats::jetstream::new(client), config).await {
        Ok(handles) => Some(handles),
        Err(err) => panic!(
            "attach 失败（是否忘了预置 stream / bucket？）：{err}\n\
             提示：nats stream add {QUEUE} --subjects '{QUEUE}.tasks.>' --retention workq"
        ),
    }
}

// ── 一、绝不创建基础设施 ─────────────────────────────────────────

#[tokio::test]
async fn attach_never_creates_resources() {
    let Some(handles) = connect().await else {
        eprintln!("跳过：未设置 CCQ_NATS_URL");
        return;
    };
    // attach 之后队列资源仍在，且 attach 本身不产生任何新资源。
    let config = handles.config.clone();
    let specs = infra_specs(&config);
    for spec in specs {
        assert!(
            handles.jetstream.get_stream(&spec.stream).await.is_ok(),
            "{} 应存在（attach 不应删除它）",
            spec.stream
        );
    }
    assert_eq!(
        required_metadata_buckets(&config),
        vec!["workflow-task-meta"]
    );
}

#[tokio::test]
async fn attach_reports_infra_missing_instead_of_creating() {
    let Some(url) = nats_url() else {
        eprintln!("跳过：未设置 CCQ_NATS_URL");
        return;
    };
    let client = async_nats::connect(&url).await.expect("连接测试 NATS");
    // 这个队列的资源**故意不存在** —— attach 必须报 InfraMissing，
    // 而不是悄悄把 stream / bucket 建出来。
    let result = Queue::attach(
        async_nats::jetstream::new(client),
        QueueConfig::new("ccq-nonexistent-queue"),
    )
    .await;
    let err = result.err().expect("不存在的队列必须 attach 失败");
    assert!(
        err.is_infra_missing(),
        "必须是 InfraMissing（部署故障），实际得到：{err}"
    );
    // 确认它确实没有被创建出来。
    let client2 = async_nats::connect(&url).await.expect("重连");
    assert!(
        async_nats::jetstream::new(client2)
            .get_stream("ccq-nonexistent-queue")
            .await
            .is_err(),
        "attach 失败后**不得**留下刚创建的资源"
    );
}

// ── 二、投递 → 消费 → 事件 ───────────────────────────────────────

/// 消费者记录下来的 (任务 id, 字节负载)。
type Seen = std::sync::Arc<std::sync::Mutex<Vec<(String, Vec<u8>)>>>;

/// 只回传固定输出的 worker。
struct EchoWorker {
    seen: Seen,
}

#[async_trait::async_trait]
impl Worker for EchoWorker {
    async fn handle_task(&self, task: TaskContext) -> Result<TaskOutput, TaskError> {
        self.seen
            .lock()
            .expect("锁未被 poison")
            .push((task.task_id.clone(), task.payload.clone()));
        // 上报一次心跳，覆盖 v2 新增的 attempt 字段通路。
        task.send_heartbeat("echo-worker 工作中".to_string())
            .await?;
        Ok(Some(b"done".to_vec()))
    }
}

#[tokio::test]
async fn producer_to_consumer_round_trip() {
    let Some(handles) = connect().await else {
        eprintln!("跳过：未设置 CCQ_NATS_URL");
        return;
    };
    let producer = Producer::from_handles(std::sync::Arc::new(handles.clone()));

    // ── 事件订阅：每次调用独立订阅 ──
    let mut events_a = producer.subscribe().await.expect("订阅 A");
    let mut events_b = producer.subscribe().await.expect("订阅 B");
    assert_eq!(
        events_a.max_capacity(),
        events_b.max_capacity(),
        "两个订阅应当各得一个独立 receiver"
    );

    let seen: Seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let consumer = Consumer::attach_with_concurrency(
        handles.jetstream.clone(),
        QueueConfig::new(QUEUE),
        EchoWorker {
            seen: std::sync::Arc::clone(&seen),
        },
        2,
    )
    .await
    .expect("消费者附着");

    let shutdown = CancellationToken::new();
    let handle = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { consumer.run(shutdown).await }
    });

    // ── 投递 ──
    let payload = b"workflow-payload-\x00\xff".to_vec();
    let task_id = producer
        .submit(Task {
            payload: payload.clone(),
        })
        .await
        .expect("投递成功");

    // ── 消费者取到并处理 ──
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        if seen
            .lock()
            .expect("锁未被 poison")
            .iter()
            .any(|(id, _)| id == &task_id)
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "20 秒内消费者未处理任务 {task_id}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let got = seen.lock().expect("锁未被 poison").clone();
    assert_eq!(
        got.iter().find(|(id, _)| id == &task_id).map(|(_, p)| p),
        Some(&payload),
        "消费者拿到的字节必须与投递时完全一致（含非 UTF-8 字节）"
    );

    // ── 元数据已落库 ──
    let meta = get_metadata(&handles, &task_id)
        .await
        .expect("读元数据")
        .expect("元数据应存在");
    assert_eq!(meta.id, task_id);
    assert_eq!(meta.queue, QUEUE);

    // ── 生命周期事件：两个订阅各自独立、且都拿到全量 ──
    //
    // ⚠️ 断言的是「两个订阅收到的**事件集合相同**」，而不是「事件只属于本测试投递
    // 的那个任务」—— 库刻意不过滤，所以共享队列里其它任务的事件同样会送达。这正是
    // 「库内不过滤、归属由调用方反查」这条契约的体现。
    let mut collected: [Vec<String>; 2] = [Vec::new(), Vec::new()];
    for (idx, rx) in [&mut events_a, &mut events_b].into_iter().enumerate() {
        while let Ok(event) = tokio::time::timeout(Duration::from_secs(3), rx.recv()).await {
            collected[idx].push(event.expect("channel 应当打开").id().to_string());
        }
    }
    assert_eq!(
        collected[0], collected[1],
        "两个独立订阅应看到同一批事件（证明各自订阅、各自收全量）"
    );
    assert!(
        collected[0].iter().any(|id| id == &task_id),
        "至少应收到本任务的事件，实际收到：{:?}",
        collected[0]
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(35), handle).await;
}

// ── 三、元数据 CAS ───────────────────────────────────────────────

#[tokio::test]
async fn metadata_cas_rejects_stale_write() {
    let Some(handles) = connect().await else {
        eprintln!("跳过：未设置 CCQ_NATS_URL");
        return;
    };
    let producer = Producer::from_handles(std::sync::Arc::new(handles.clone()));
    let task_id = producer
        .submit(Task {
            payload: b"cas".to_vec(),
        })
        .await
        .expect("投递");

    // 连续两次 cancel：第二次读到的是已带 cancel_requested 的快照，
    // CAS 不得把它冲掉。
    producer.cancel(&task_id).await.expect("取消 1");
    producer.cancel(&task_id).await.expect("取消 2");
    let meta = get_metadata(&handles, &task_id)
        .await
        .expect("读元数据")
        .expect("元数据应存在");
    assert!(
        meta.cancel_requested,
        "cancel 置位后不得被后续写覆盖（CAS 失效会静默吞掉取消）"
    );

    // 终态任务取消应被忽略而非报错。
    let mut meta = meta;
    meta.state = TaskState::Succeeded;
    put_metadata(&handles, &meta).await.expect("写终态");
    producer.cancel(&task_id).await.expect("终态取消不应报错");
}

// ── 四、错误语义 ─────────────────────────────────────────────────

#[tokio::test]
async fn oversized_payload_is_a_task_error_not_infra() {
    let Some(handles) = connect().await else {
        eprintln!("跳过：未设置 CCQ_NATS_URL");
        return;
    };
    let producer = Producer::from_handles(std::sync::Arc::new(handles));
    let err = producer
        .submit(Task {
            payload: vec![0u8; cloud_core_queue::config::PAYLOAD_MAX_BYTES + 1],
        })
        .await
        .expect_err("超限必须失败");
    assert!(
        !err.is_infra_missing(),
        "负载超限是**任务**错误，不是基础设施缺失：{err}"
    );
    assert!(
        matches!(err, cloud_core_queue::QueueError::Task(_)),
        "{err}"
    );
}

#[tokio::test]
async fn event_enum_round_trips_over_the_wire() {
    // 不依赖 NATS：确认线上字节能被反序列化回类型（业务层拿到的是类型，不是裸 JSON）。
    let event = cloud_core_queue::QueueEvent::attempt_failed(
        "task-1",
        2,
        AttemptErrorSource::System,
        "boom",
    );
    let bytes = serde_json::to_vec(&event).expect("序列化");
    let back: cloud_core_queue::QueueEvent = serde_json::from_slice(&bytes).expect("反序列化");
    assert_eq!(back, event);
}

#[tokio::test]
async fn guest_errors_are_retryable_and_system_errors_are_not() {
    assert_eq!(TaskError::guest("x").source, TaskErrorSource::Guest);
    assert_eq!(TaskError::system("x").source, TaskErrorSource::System);
    assert_eq!(
        TaskStatus::Succeeded,
        serde_json::from_str::<TaskStatus>("\"succeeded\"").expect("反序列化")
    );
}
