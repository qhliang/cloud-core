//! §9.2 可靠性机制的回归测试。
//!
//! 设计文档把这 7 项列为「**可靠性关键代码，不是胶水**」——每项配一个明确的
//! 出错后果。缺了纯逻辑覆盖的那几项（并发闸门、退避阶梯）已放在
//! `src/consumer.rs` 的单测里；本文件覆盖**必须真跑 JetStream** 才能证伪的
//! 那几项：
//!
//! | 机制 | 本文件中的测试 | 出错后果 |
//! |---|---|---|
//! | `max_deliver` 上限 | `poison_task_stops_at_max_deliver` | 毒任务永不退出 |
//! | consumer 消失后重建并重订阅 | `consumer_is_recreated_after_deletion` | 队列卡死 |
//! | 不阻塞关停的 lease 处理 | `shutdown_grants_in_flight_tasks_a_grace_period` | 关停丢任务 |
//! | `InfraMissing` 与 `Task` 区分 | `missing_infra_is_not_retryable_by_worker` | 部署故障被误记为任务失败 |
//!
//! # 运行前提
//!
//! 与 `integration.rs` 相同：需预置好的本地 NATS，然后
//!
//! ```text
//! CCQ_NATS_URL=nats://127.0.0.1:4222 cargo test -p cloud-core-queue --test reliability -- --test-threads=1
//! ```
//!
//! 未设置 `CCQ_NATS_URL` 时全部跳过。
//!
//! ⚠️ **必须 `--test-threads=1`**：本文件会删除 durable consumer、反复投递
//! 同一个队列，与 `integration.rs` 并发跑会互相干扰。

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use cloud_core_queue::nats::{Queue, get_metadata};
use cloud_core_queue::{
    Consumer, Producer, QueueConfig, Task, TaskError, TaskErrorSource, TaskOutput, Worker,
};
use tokio_util::sync::CancellationToken;

/// 与 `integration.rs` 共用同一个预置队列（须 `--test-threads=1` 串行）。
const QUEUE: &str = "workflow-task";

/// 超时辅助：断言型等待，避免测试在故障时挂死。
async fn wait_for(mut predicate: impl FnMut() -> bool, what: &str) {
    // 预算须覆盖 RECONNECT_DELAY(60s)。
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    while std::time::Instant::now() < deadline {
        if predicate() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("等待超时：{what}");
}

/// 同上，但谓词需要 await（如读元数据）。
async fn wait_for_async<F, Fut>(mut predicate: F, what: &str)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    // 预算须覆盖 RECONNECT_DELAY(60s)。
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    while std::time::Instant::now() < deadline {
        if predicate().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("等待超时：{what}");
}

/// 连上测试 NATS；未配置则安静跳过。
///
/// 顺带**清空**两个 stream：上一轮失败运行留下的消息会让 `max_deliver`
/// 之类的断言读到脏数据（消息堆积后投递次数对不上）。本文件只在本地
/// 测试 NATS 上跑，purge 是安全的。
async fn skip_unless_configured() -> Option<String> {
    let url = std::env::var("CCQ_NATS_URL").ok()?;
    let client = async_nats::connect(&url).await.expect("连接测试 NATS");
    let jetstream = async_nats::jetstream::new(client);
    for name in [QUEUE, &format!("{QUEUE}-results")] {
        if let Ok(stream) = jetstream.get_stream(name).await {
            let _ = stream.purge().await;
        }
    }
    Some(url)
}

/// 反复失败的 worker：每次投递都返回可重试的 guest error。
struct PoisonWorker {
    attempts: Arc<AtomicUsize>,
}

#[async_trait]
impl Worker for PoisonWorker {
    async fn handle_task(
        &self,
        _task: cloud_core_queue::TaskContext,
    ) -> Result<TaskOutput, TaskError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        // Guest 错误 = 可重试。返回 System 会直接终结，无法验证 max_deliver。
        Err(TaskError::guest("poison"))
    }
}

/// §9.2「`max_deliver` 上限」——毒任务永不退出。
///
/// 验证两件事：尝试次数**恰好**停在 `max_deliver`，且终态是
/// `MaxRetriesExceeded`。只验证「最终停了」不够 —— 少一次说明上限算错，
/// 多一次说明没有上限。
#[tokio::test]
async fn poison_task_stops_at_max_deliver() {
    let Some(url) = skip_unless_configured().await else {
        return;
    };

    let client = async_nats::connect(&url).await.expect("连接");
    let mut config = QueueConfig::new(QUEUE);
    // 退避压到最小，否则阶梯会让测试跑到分钟级。
    config.retry_backoff_ms = vec![10];
    let max_deliver = config.max_deliver.max(1) as usize;

    let attempts = Arc::new(AtomicUsize::new(0));
    let consumer = Consumer::attach_with_concurrency(
        async_nats::jetstream::new(client.clone()),
        config.clone(),
        PoisonWorker {
            attempts: attempts.clone(),
        },
        1,
    )
    .await
    .expect("attach");

    let shutdown = CancellationToken::new();
    let handle = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { consumer.run(shutdown).await }
    });

    let producer = Producer::attach(async_nats::jetstream::new(client.clone()), config.clone())
        .await
        .expect("producer attach");
    let task_id = producer
        .submit(Task {
            payload: b"{}".to_vec(),
        })
        .await
        .expect("submit");

    wait_for(
        || attempts.load(Ordering::SeqCst) >= max_deliver,
        "毒任务投递次数达到 max_deliver",
    )
    .await;

    // 再等一拍，确认**不会**继续重投（上限真的封住了）。
    tokio::time::sleep(Duration::from_millis(500)).await;
    let observed = attempts.load(Ordering::SeqCst);
    assert_eq!(
        observed, max_deliver,
        "尝试次数应恰好等于 max_deliver={max_deliver}"
    );

    let handles = Queue::attach(async_nats::jetstream::new(client.clone()), config.clone())
        .await
        .expect("attach handles");
    wait_for_async(
        || async {
            matches!(get_metadata(&handles, &task_id).await, Ok(Some(m)) if m.state.is_terminal())
        },
        "毒任务落到终态",
    )
    .await;

    shutdown.cancel();
    let _ = handle.await;
}

/// §9.2「consumer 消失后重建并重订阅」——队列卡死。
///
/// 运行器 attach 后把 durable consumer 删掉，验证它能自愈并继续消费。
/// 没有这条，运行中的 consumer 一旦被误删，队列会**永久卡死**直到重启 Pod。
#[tokio::test]
async fn consumer_is_recreated_after_deletion() {
    let Some(url) = skip_unless_configured().await else {
        return;
    };

    let client = async_nats::connect(&url).await.expect("连接");
    let config = QueueConfig::new(QUEUE);

    let handled = Arc::new(AtomicUsize::new(0));
    let consumer = Consumer::attach_with_concurrency(
        async_nats::jetstream::new(client.clone()),
        config.clone(),
        CountingWorker {
            handled: handled.clone(),
        },
        1,
    )
    .await
    .expect("attach");

    let shutdown = CancellationToken::new();
    let handle = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { consumer.run(shutdown).await }
    });

    // 先跑通一次，确认消费链路本来就正常，避免把"链路本来就不通"误判成重建生效。
    let producer = Producer::attach(async_nats::jetstream::new(client.clone()), config.clone())
        .await
        .expect("producer attach");
    producer
        .submit(Task {
            payload: b"{}".to_vec(),
        })
        .await
        .expect("submit");
    wait_for(|| handled.load(Ordering::SeqCst) >= 1, "首次消费成功").await;

    // 删掉 durable consumer —— 模拟误操作 / NATS 重启后 consumer 丢失。
    let jetstream = async_nats::jetstream::new(client.clone());
    jetstream
        .delete_consumer_from_stream(format!("{QUEUE}-worker"), QUEUE)
        .await
        .expect("删除 durable consumer");

    // 自愈后应能继续消费。
    // ⚠️ 预算必须 ≥ RECONNECT_DELAY(60s)：consumer 消失后运行器不会立刻察觉，
    // 它要等挂起的 pull 请求报错或流结束才回到 'connect 循环。给太短的预算
    // 会把"自愈很慢"误报成"没有自愈"。
    producer
        .submit(Task {
            payload: b"{}".to_vec(),
        })
        .await
        .expect("重建后 submit");
    wait_for(
        || handled.load(Ordering::SeqCst) >= 2,
        "consumer 被删后自愈并继续消费",
    )
    .await;

    shutdown.cancel();
    let _ = handle.await;
}

struct CountingWorker {
    handled: Arc<AtomicUsize>,
}

#[async_trait]
impl Worker for CountingWorker {
    async fn handle_task(
        &self,
        _task: cloud_core_queue::TaskContext,
    ) -> Result<TaskOutput, TaskError> {
        self.handled.fetch_add(1, Ordering::SeqCst);
        Ok(None)
    }
}

/// §9.2「不阻塞关停的 lease 处理」——**关停丢任务**。
///
/// 断言的不是"关停要快"，而是**在途任务能拿到宽限期**：收到 shutdown 后，
/// `drain` 先等在途投递结束（DRAIN_TIMEOUT 上限内），超时才 abort。
/// 若实现改成"收到 shutdown 立刻 abort"，本测试会看到 finished=false。
///
/// 反过来也不能无限等 —— `drain` 的超时是硬上限，超过即 abort_all，
/// 否则 k3s 的 terminationGracePeriodSeconds 到点 SIGKILL，行为与立即 abort 无异。
/// 该上限由 `drain_bounds_shutdown_wait` 这条单测守住。
#[tokio::test]
async fn shutdown_grants_in_flight_tasks_a_grace_period() {
    let Some(url) = skip_unless_configured().await else {
        return;
    };

    let client = async_nats::connect(&url).await.expect("连接");
    let mut config = QueueConfig::new(QUEUE);
    // lease 拉长，确保慢任务不会被误判成丢 lease 而重投（否则测的是重投不是关停）。
    config.ack_wait = Duration::from_secs(300);
    config.lease_renew_interval = Duration::from_secs(30);

    let tracker = Arc::new(SlowWorkerTracker::default());
    let consumer = Consumer::attach_with_concurrency(
        async_nats::jetstream::new(client.clone()),
        config.clone(),
        SlowWorker {
            tracker: tracker.clone(),
        },
        1,
    )
    .await
    .expect("attach");

    let shutdown = CancellationToken::new();
    let handle = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { consumer.run(shutdown).await }
    });

    let producer = Producer::attach(async_nats::jetstream::new(client.clone()), config)
        .await
        .expect("producer attach");
    producer
        .submit(Task {
            payload: b"{}".to_vec(),
        })
        .await
        .expect("submit");
    wait_for(
        || tracker.started.load(Ordering::SeqCst) >= 1,
        "慢任务开始执行",
    )
    .await;

    // 任务正在 worker 里跑，此刻取消关停。它应在宽限期内**跑完**。
    shutdown.cancel();
    // run() 正常返回（drain 完成），不是 Err。
    let run_result = tokio::time::timeout(Duration::from_secs(20), handle)
        .await
        .expect("关停被无限阻塞 —— drain 的 DRAIN_TIMEOUT 失效了");
    run_result
        .expect("run 任务 join 失败")
        .expect("run 返回了错误");

    assert!(
        tracker.finished.load(Ordering::SeqCst) == 1,
        "在途任务应在关停宽限期内跑完（finished={}），而不是被 abort 丢掉",
        tracker.finished.load(Ordering::SeqCst)
    );
}

#[derive(Default)]
struct SlowWorkerTracker {
    started: AtomicUsize,
    finished: AtomicUsize,
}

struct SlowWorker {
    tracker: Arc<SlowWorkerTracker>,
}

#[async_trait]
impl Worker for SlowWorker {
    async fn handle_task(
        &self,
        _task: cloud_core_queue::TaskContext,
    ) -> Result<TaskOutput, TaskError> {
        self.tracker.started.fetch_add(1, Ordering::SeqCst);
        // 必须**短于** drain 的 DRAIN_TIMEOUT(30s)：跑不完的任务会被 abort，
        // 那测的是"超时上限"而非"关停丢任务"。此处测的是前者不该误伤后者。
        tokio::time::sleep(Duration::from_secs(3)).await;
        self.tracker.finished.fetch_add(1, Ordering::SeqCst);
        Ok(None)
    }
}

/// §9.2「`InfraMissing` 与 `Task` 的区分」——部署故障被误记为任务失败。
///
/// 关键断言：`InfraMissing` **不会**被 worker 看到、更不会进业务侧。
/// 若 attach 失败被降级成 `TaskError`，cloud-manager 会把「基础设施没配好」
/// 写进执行记录的业务表，污染数据且极难排查。
#[tokio::test]
async fn missing_infra_is_not_retryable_by_worker() {
    let Some(url) = skip_unless_configured().await else {
        return;
    };

    let client = async_nats::connect(&url).await.expect("连接");
    // 队列名合法但资源不存在 ⇒ 必须 InfraMissing，且不得创建任何资源。
    let config = QueueConfig::new("no-such-queue-ccq-test");

    let attached = Consumer::attach_with_concurrency(
        async_nats::jetstream::new(client.clone()),
        config.clone(),
        CountingWorker {
            handled: Arc::new(AtomicUsize::new(0)),
        },
        1,
    )
    .await;

    // 不存在的队列必须 attach 失败。
    let Err(err) = attached else {
        panic!("不存在的队列竟然 attach 成功了，违反「绝不创建基础设施」");
    };
    assert!(
        matches!(
            err,
            cloud_core_queue::error::QueueError::InfraMissing { .. }
        ),
        "期望 InfraMissing，实际：{err:?}"
    );

    // 失败后**不能**留下任何资源（库绝不创建基础设施）。
    let jetstream = async_nats::jetstream::new(client);
    jetstream
        .get_stream(&config.name)
        .await
        .expect_err("attach 失败后却创建了 stream，违反「绝不创建基础设施」");
}

/// `TaskErrorSource` 必须能让业务区分「可重试」与「确定性失败」——
/// 上面的 max_deliver 测试依赖 Guest 可重试、System 直接终结这一语义。
#[test]
fn error_source_separates_retryable_from_terminal() {
    let guest = TaskError::guest("bad input");
    let system = TaskError::system("kv down");
    assert_eq!(guest.source, TaskErrorSource::Guest);
    assert_eq!(system.source, TaskErrorSource::System);
    assert_ne!(guest.source, system.source);
}
