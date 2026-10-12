# cloud-core-queue 设计文档

> **状态**：库已落地（`cloud-core-queue`），workflow-manager 与 agent-manager
> 已切换；cloud-manager 尚未迁移（它是唯一的生产者，必须最后切）。
> **定位**：把 `wasmCloud/crates/task_queue_{core,worker}` 的任务分发能力重写为
> 面向**原生服务**的队列库，收编进 `cloud-core` 仓，作为跨服务协议层的一部分。
> **动因**：cloud-manager 从 wasmCloud WASM 组件迁移为普通 Docker 镜像（Rust 二进制）。

---

## 1. 为什么重写而不是复用

`task_queue_core`（1,568 行）+ `task_queue_worker`（625 行）位于 `wasmCloud`
上游 fork 内。cloud-manager 迁移为原生二进制后，若继续依赖它，就得在 Dockerfile
与 CI 里长期保留对上游 fork 的路径依赖（双 checkout + `WASMCLOUD_REPO`/
`WASMCLOUD_REF` 钉版本 + `ln -s / /host-root`）。

把队列协议收进 `cloud-core`（本仓自有）后，cloud-manager 只剩**一个** sibling
依赖，上游 wasmCloud 的升级不再牵连它。

### 1.1 复用可行性已核实（结论：不复用）

调查结论记录在此，避免后来者重复评估：

- `task_queue_core` / `task_queue_worker` **确实零 wasmtime/wit 依赖**，技术上
  可被原生二进制直接使用（`workflow-manager` 已在生产中用 `WorkerRunner` 消费同一队列）。
- 但重写的收益（摆脱对上游 fork 的依赖 + 重新设计机会）大于复用收益，且
  现有实现在**事件构造与解析上是裸 JSON**（见 §5.1），本身就需要返工。

---

## 2. 两个角色，没有 observer

```
cloud_core_queue::Queue::attach(&client, cfg) -> Result<Producer>

Producer
  submit / cancel / status
  subscribe() -> mpsc::Receiver<QueueEvent>      ← 原 observer 内置于此

Consumer
  attach(...) / run(worker, shutdown)            ← 消费者运行器
```

### 2.1 为什么去掉 observer

现有 observer 只是 `custom_plugin_task_queue` 插件内部的派发层
（`CallbackEvent` 四个变体 → WIT guest export `on_start` / `on_heartbeat` /
`on_attempt_failed` / `on_terminate`）。这套 guest-export 回调机制是 WASM 专属的，
没有任何 crate 提供；而线格式上它就是订阅 `{queue}.events` 读 JSON。

原生形态下，一个既要投递任务、又要知晓任务生命周期的服务，本质上就是
**producer**。把订阅能力内置进 producer，角色从三个收敛为两个。

### 2.2 `subscribe` 的契约：每个调用方独立订阅

**每个调用方各自建立一条 NATS 订阅、各自持有一个 `mpsc::Receiver`。**

理由（设计约束，务必保留）：

> **将来会有其他服务作为 producer 往同一队列投递任务，cloud-manager 不应监听全部回调。**

配套事实：`task_id` 由 `uuid::Uuid::now_v7()` 生成（`task_queue_core/src/queue.rs:39`），
**没有服务前缀**，无法在 subject 层过滤。归属只能靠反查 —— 这已是 cloud-manager
的现有做法（`handle_start` 依次反查 agent / workflow / chat 三张映射表，查不到即
静默忽略）。

因此 `QueueEvent` **完整投递、库内不做任何过滤**。库若擅自过滤，调用方反而无法
观测全量、也无法自行决定归属规则。过滤是调用方的责任，库只保证 `id` 字段可用。

---

## 3. 绝不创建基础设施

### 3.1 规则

`cloud-core-queue` **只使用** stream / bucket / consumer，**永不创建**。

- `attach()` 走只读查询（`get_stream` / `stream_info` / `get_key_value` 等）
- 全程不出现 `get_or_create_*`
- 基础设施缺失 → 立即返回 `QueueError::InfraMissing`，不做静默补齐

理由：库不应在生产环境偷偷修改基础设施；多副本并发创建也是竞态来源。

### 3.2 移除的自愈能力（重要）

`task_queue_worker` 的文档注释明确写着：

> the runner rebuilds the consumer — **and the stream, when the producer has not recreated it yet**

核对代码后确认：`QueueHandles::create` 在 `run()` 全程**只调用一次**（连接阶段），
重建循环只调 `ensure_worker()`（`get_or_create_consumer`），
**stream 从不由消费者重建** —— 现状下这个责任隐式落在 producer 身上。

移除 producer 的创建能力后，**没有任何组件会重建 stream**。

### 3.3 故障模式

```
NATS pod 重建 → JetStream 在 emptyDir 上被清空 → stream / bucket 全部消失
    改前：消费者重试 60s；producer 被再次调用时顺手重建 stream → 自愈
    改后：消费者重试 60s；无人重建 → 需人工按 §7.2 恢复
```

`Consumer` **保留**「consumer 消失后重建 consumer 并重订阅」（`RECONNECT_DELAY = 60s`），
但流消失时不再重建 —— 这与「不创建」的决策一致。

### 3.4 本期运维现状

**NATS 仍是内置的、JetStream 在 emptyDir 上；stream / bucket 被清空后由人工处理。**

这是已知取舍。未来方向：迁到外部稳定的 NATS（不再使用 emptyDir 的内置 NATS）后，
stream / bucket 变成「建一次就一直在」的持久资源，本节的人工路径自然消失。

---

## 4. 错误语义

```rust
pub enum QueueError {
    /// 基础设施缺失 —— stream / bucket / consumer 不存在。
    /// 语义是**部署故障**，不是业务失败：调用方应重试，而非标记任务失败。
    InfraMissing { what: &'static str, name: String },
    Nats(anyhow::Error),
    Task(String),
}
```

**`InfraMissing` 必须与 `Task` 严格区分** —— 混在一起会导致「基础设施没配好」被当成
「任务执行失败」写进业务表，污染数据且难以排查。

### 4.1 `attach()` 失败时的进程行为

```
启动 → attach 失败 → error 日志 + 进入退避重试
                    → 状态反映到 /healthz（供探针发现）
                    → 进程**不退出**
```

不立即退出的理由：基础设施「晚就绪」不应引发 CrashLoop 噪音。

### 4.2 `/healthz` 契约

沿用 agent-manager 的既有教训（`agent-manager/src/api.rs:14-28`）：

> 顶层 `status` **恒为 `ok`**，绝不因队列降级改 503。readinessProbe 打在这里，
> 队列降级不该触发 Pod 重启 —— 重启救不了它，反而打断正在跑的任务。

```json
{
  "status": "ok",
  "queues": {
    "agent-task":    { "attached": false, "last_error": "InfraMissing(stream, agent-task)" },
    "workflow-task": { "attached": true,  "last_error": null }
  }
}
```

判据是 `attached` 字段，由监控自行判断。

---

## 5. 类型系统边界：禁止操作裸 JSON

### 5.1 现状违规点

| 位置 | 现状 |
|---|---|
| `task_queue_core/src/events.rs` | 4 个构造函数用 `serde_json::json!` 拼装，返回 `serde_json::Value` |
| `custom_plugin_task_queue/src/lib.rs:1005` `parse_control_event` | `value.get("id")?.as_str()?` 等 **10 处手写取值** |

而 `types.rs` 里 `Task` / `TaskEnvelope` / `TaskMeta` / `TaskInfo` /
`AttemptFailure` **已经是带 `Serialize/Deserialize` 的类型**。
即：**传输层已类型化，事件的构造与解析却是裸 JSON** —— 一半好一半坏。

### 5.2 规则

> 下发任务、解析任务、回调任务状态时，**禁止在业务层直接解析 NATS 中的 JSON，
> 或直接发送 JSON**。发送时先序列化为对象再传输，接收时先反序列化为对象。
> 禁止直接操纵最底层传输的原始 JSON。

### 5.3 约束的落点（只靠类型系统，不引入 lint）

对外导出的一切签名中，`serde_json::Value` 不出现：

```rust
impl Producer {
    pub async fn submit(&self, task: Task) -> Result<String>;
    pub async fn cancel(&self, task_id: &str) -> Result<()>;
    pub async fn status(&self, task_id: &str) -> Result<Option<TaskInfo>>;
    pub async fn subscribe(&self) -> Result<mpsc::Receiver<QueueEvent>>;
}
// ❌ 不存在：fn submit_raw(&self, json: &str)
// ❌ 不存在：fn subscribe_raw(&self) -> Receiver<serde_json::Value>
```

业务层拿到 `Task` 就是类型，既没有发送裸 JSON 的入口，也拿不到裸 JSON 的出口。
规则由编译器保证，而非靠纪律。

### 5.4 类型不分层

**不引入独立的 wire 类型模块** —— 一套类型同时充当领域模型与线上格式。
对传输库而言，serde 属性出现在领域类型上的代价可以接受，换来的是简单。

---

## 6. 线格式 v2

### 6.1 设计原则

1. **`id` 统一** —— 所有表面都用 `id`，无 `task_id`
2. **一对字段表达一件事** —— 消除 `payload` / `payload_base64` / `payload_encoding` 三选一
3. **只保留实际在用的** —— 删掉历史包袱字段
4. **类型即契约** —— serde tag 让判别字段由编译器校对

### 6.2 `schema_version` 保持 1

**不升级版本号。** 新库上线时旧库已不再被使用，版本号只服务于运行中的格式漂移。

⚠️ **代价与前置条件**：版本号不升级的前提下，新旧信封在 JetStream 里无法区分。
若迁移时队列里还压着旧格式任务，新消费者会当 v1 解析而**静默出错**。

> **迁移前置条件：切换前确认两个队列均已排空**（无 pending / dispatched / running
> 任务）。见 §8。

### 6.3 现状命名盘点

除 `TaskEnvelope` 外，各表面**已经统一用 `id`**：

| 表面 | 字段 |
|---|---|
| `start` / `heartbeat` / `attempt_failed` / `complete` 事件 | `id` ✅ |
| `TaskMeta`（KV） | `id` ✅ |
| `TaskInfo` | `id` ✅ |
| `AttemptFailure` | `id` ✅ |
| **`TaskEnvelope`（数据面）** | **`task_id`** ❌ 唯一的不一致 |

---

### 6.4 任务信封（数据面 `{queue}.tasks.{id}`）

**现状**（三选一，冗余）：
```json
{ "schema_version":1, "task_id":"...", "payload_encoding":"raw",
  "payload_base64":"...", "payload":null, "execution_deadline_ms":0 }
```

`payload_encoding` + `payload_base64` + `payload` 三个字段表达「一段字节」，
消费侧要写 `match` 试三种组合；且 `raw` 与 `binary` 两个分支**序列化后完全相同**
（都是 base64），`decode_payload()` 的两个分支实为重复。

**v2**（单一字段）：
```json
{ "schema_version":1, "id":"...", "payload":"<base64>",
  "encoding":"base64", "deadline_ms":1757000000000 }
```

| 字段 | 现状 | v2 | 理由 |
|---|---|---|---|
| `task_id` | `task_id` | **`id`** | 统一 |
| `payload_encoding` | `payload_encoding` | **`encoding`** | 去掉冗余前缀 |
| `payload_base64` / `payload` | 二选一 | **`payload` 恒为 base64 字符串** | 消除三态 |
| `execution_deadline_ms` | 同名 | **`deadline_ms`** | 与 `TaskMeta.deadline_ms` 对齐 |

---

### 6.5 生命周期事件（控制面 `{queue}.events`）

**现状** —— 四个字段集各写一遍，`type` 是裸字符串：
```json
{"type":"start","id":"...","attempt":1}
{"type":"heartbeat","id":"...","info":"..."}
{"type":"attempt_failed","id":"...","attempt":1,"source":"...","error":"..."}
{"type":"complete","id":"...","status":"succeeded","attempt":1,"output":null,"error":null}
```

问题：
- `heartbeat` **缺 `attempt`**，其余三个有 —— 不一致
- `type` 是裸字符串，拼错不报错
- `output` 可为 null，语义松散

**v2**：
```json
{"type":"start","id":"...","attempt":1}
{"type":"heartbeat","id":"...","attempt":1,"info":"..."}
{"type":"attempt_failed","id":"...","attempt":1,"source":"...","error":"..."}
{"type":"complete","id":"...","attempt":1,"status":"succeeded","error":null,"output":""}
```

对应类型：
```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum QueueEvent {
    Start     { id: String, attempt: u32 },
    Heartbeat { id: String, attempt: u32, info: String },
    AttemptFailed { id: String, attempt: u32, source: AttemptErrorSource, error: String },
    Complete  { id: String, attempt: u32, status: TaskStatus, error: Option<String>, output: String },
}
```

| 项 | 现状 | v2 |
|---|---|---|
| `type` | 裸字符串 | `serde(tag)`，编译期校对 |
| `heartbeat` | 缺 `attempt` | **补齐**，四变体公共字段一致 |
| `complete` 状态字段 | `status` | 保留（事件语义是终态结果） |
| `output` | 可为 null | **恒为 base64 字符串**，空则 `""` |

---

### 6.6 元数据（KV `{queue}-meta` / `{queue}.{id}`）

**现状已较规范**（`id`、时间戳统一 `_ms`），**v2 不做改动**：
```json
{"schema_version":1,"id":"...","queue":"agent-task","state":"queued","attempt":0,
 "created_at_ms":...,"dispatched_at_ms":null,"completed_at_ms":null,
 "deadline_ms":...,"cancel_requested":false,"attempts":[]}
```

`state`（内部状态机）与 `status`（对外结果）的**有意区分必须保留**，避免后人
「统一」掉：前者是库内部的状态流转，后者是任务的终态结论。

### 6.7 结果归档（`{queue}-results`，可选）

`results_archive` 开启时的落档格式，**直接复用 `QueueEvent::Complete` 的字段集**，
不另立类型。

### 6.8 subject 与资源命名

```
{queue}.tasks.{task_id}      投递
{queue}.results.{task_id}    结果
{queue}.events               生命周期事件
{queue}-meta                 KV bucket，key = {queue}.{task_id}
{queue}-results              结果 stream
{queue}-worker               消费者 durable consumer
```

### 6.9 `QueueConfig` 参数

| 参数 | 说明 |
|---|---|
| `name` | 队列名 |
| `ack_wait` | 等待 ack 的窗口 |
| `lease_renew_interval` | lease 续期间隔 |
| `dispatch_timeout` | 派发超时 |
| `execution_timeout` | 执行超时 |
| `max_deliver` | 最大投递次数（毒任务上限） |
| `retry_backoff_ms` | 重试退避阶梯 |
| `results_archive` | 是否归档结果 |
| ~~`external_worker`~~ | **已移除** —— v2 里「只投递不消费」由选择 `Producer` 而非 `Consumer` 天然决定，保留一个从不读的字段只会误导后人以为有运行时分支 |

---

## 7. 基础设施契约

### 7.1 必须预置的资源

`cloud-core-queue` 启动前，下列资源必须已存在，否则 `attach()` 返回 `InfraMissing`：

| 资源 | 类型 | 配置 |
|---|---|---|
| `{queue}` | stream | `subjects: ['{queue}.tasks.>']`，`retention: workqueue` |
| `{queue}-results` | stream（可选） | `subjects: ['{queue}.results.>']`，`retention: limits` |
| `{queue}-meta` | KV bucket | `history: 1` |

需预置的队列：`agent-task`、`workflow-task`。

### 7.2 人工恢复步骤（emptyDir 被清空后）

```bash
# agent-task（workflow-task 同理，替换队列名）
nats stream add agent-task          --subjects 'agent-task.tasks.>'   --retention workqueue
nats stream add agent-task-results  --subjects 'agent-task.results.>' --retention limits
nats kv    add agent-task-meta     --history 1
```

⚠️ 参数必须与 `QueueConfig` 一致（`retention`、`history`、`max_value_size`），
否则线格式虽正确、行为仍会不一致。

集群内可用既有工具排查：`kubectl exec -n wasmcloud deploy/nats-box -- nats ...`

---

## 8. 迁移顺序

线格式不兼容（§6.2 直接改名），因此**必须按「先消费者、后生产者」顺序**，
且每次只停一个消费者：

```
① cloud-core-queue 落地（无消费者，仅跑线格式验证）
② 停 workflow-manager → 切到 cloud-core-queue → 验证一轮任务正常
③ 停 agent-manager    → 切到 cloud-core-queue → 验证
④ cloud-manager 改造（WASM → Docker），最后切
⑤ 确认三服务均已切换后，再评估清理 wasmCloud 中的 task_queue_*
```

⚠️ **顺序不可颠倒**：cloud-manager 是唯一的生产者。若它先切，投递的新格式信封
旧消费者无法解析。生产者必须最后迁。

### 8.1 与 cloud-manager 迁移的关系

本设计与 cloud-manager 的 WASM → Docker 改造**可并行推进**：

- cloud-manager 的阶段 0/1（骨架 + D1 换 reqwest）不依赖队列改造
- 阶段 3（接入队列）才需要本库就绪
- 阶段 2/4/5（KV/blobstore/k8s/http/event-monitor）与队列无关

已确认 cloud-manager 的工作分层：约 60% 与 wasmCloud 零耦合、15% 换 crate 即可、
25% 需重新设计。

---

## 9. 验证方法

### 9.1 线格式对照测试

cloud-core 与 wasmCloud 同处一个 workspace 时，跑同一组场景
（提交 / 心跳 / 成功 / 失败 / 重试 / 取消 / metadata CAS），断言两侧产生的
**subject、KV key、metadata JSON、event JSON 完全一致**，再做交叉消费
（A 投递 / B 消费）。

> 该测试用于证明「移植未改变行为」。由于 v2 明确不兼容旧格式（§6.2），
> 对照对象应是**线上现有 JSON 的字面量**，而非旧 crate 的运行时输出。

本地 `nats-server -js` 即可运行；集群有 `nats` 与 `nats-box` 可查现网格式。

### 9.2 可靠性机制的回归测试

以下机制是**可靠性关键代码，不是胶水**，必须有专门测试覆盖：

| 机制 | 出错后果 |
|---|---|
| JetStream lease 续期 | lease 丢失 → 任务重复执行 |
| nak 退避重试 | 无退避 → 热循环打爆 worker |
| `max_deliver` 上限 | 无限重试 → 毒任务永不退出 |
| consumer 消失后重建并重订阅 | 队列卡死 |
| Permit 并发闸门 | 并发失控 |
| 不阻塞关停的 lease 处理 | 关停丢任务 |
| `InfraMissing` 与 `Task` 的区分 | 部署故障被误记为任务失败 |

---

## 10. 实施注意

以下细节一丢即线上事故，编码时逐条对照：

| 项 | 要求 |
|---|---|
| **SIGTERM** | 必须处理 `tokio::signal::unix::signal(SignalKind::terminate())`。agent-manager 只监听 `ctrl_c()`，k3s 发 SIGTERM 走不到优雅关闭路径 —— **那是缺陷，不是样板** |
| **`/healthz` 顶层 status** | 恒为 `ok`，不因队列降级改 503（§4.2） |
| **构建期 smoke test** | `RUN <bin> --version`。agent-manager 0.10.0 的教训：构建全绿、上线后动态加载器拒绝启动 |
| **payload 解码** | JSON 无法安全承载任意二进制，`payload` 恒 base64；解码失败要报错而非静默返回空 |
| **metadata CAS** | KV 用 revision 做 CAS（`update_metadata_with`），不可退化为读-改-写 |

---

## 11. 附：与 cloud-manager 迁移的关系

cloud-manager 从 wasmCloud WASM 组件迁移为普通 Docker 镜像，其六个阶段中，
**仅阶段 3 依赖本库**：

| 阶段 | 内容 | 是否依赖本库 |
|---|---|---|
| 0 | 骨架：axum + tokio + `/healthz`，删除 init-service | 否 |
| 1 | D1 换 reqwest（只改 `d1_query`，170 处调用点零改动） | 否 |
| 2 | KV / blobstore / k8s / cron 换 crate | 否 |
| 3 | 接入队列 producer + 内置 observer | **是** |
| 4 | HTTP 层收敛到 axum | 否 |
| 5 | event-monitor → kube-rs watcher + jsonlogic | 否 |

### 11.1 cloud-manager 的两个实现约束

1. **D1 继续远程调用** —— 部署拓扑不变，`d1_query` 签名不变
2. **保留 rust-embed** —— 前端 29,871 行 TS/TSX 一行不改

### 11.2 cloud-manager 侧必须守住的既有约束

（与本库无关，但同期改造时容易踩）

- **CORS**：`allow-origin: *` + `allow-credentials: true` 违反规范但浏览器容忍，
  **别「顺手修正」**
- **gzip 阈值 1024**（不是常规 256）：`/stream` 高频轮询压了反而慢，有守门测试
- **SPA fallback**：`/internal/*` 漏配会「返回 HTML」，有回归测试
- **blobstore 键校验**：内部免鉴权端点的唯一护栏，换 S3 SDK 时原样保留
- **`chat_lock_acquire`**：现为「读-改-复读」，换真 KV 后升级为 CAS
- **`panic = "unwind"`**：WASM 下依赖 unwind 传 panic，改 `abort` 需重新评估

---

## 12. 决策记录

| 议题 | 决策 | 理由 |
|---|---|---|
| 复用还是重写 task-queue | **重写为 cloud-core-queue** | 摆脱对上游 fork 的依赖；现有事件构造/解析本就是裸 JSON，需返工 |
| 线格式兼容性 | **不兼容，重新设计** | 一并统一命名、收紧类型；迁移按「先消费者后生产者」排序 |
| `schema_version` | **保持 1** | 旧库届时已不再使用；代价是要求迁移前排空队列 |
| crate 位置 | **cloud-core 下新增独立 crate** | `cloud-core` 保持纯类型（WASM 的 tokengateway 不被拖入运行时依赖） |
| 类型分层 | **不新增 wire 类型模块** | 一套类型同时是领域模型与线上格式，对传输库代价可接受 |
| 裸 JSON 约束的守卫 | **只靠类型系统边界** | 对外签名不含 `serde_json::Value`，编译器保证 |
| 创建基础设施 | **禁止，只使用** | 库不应在生产环境改基础设施；多副本并发创建是竞态来源 |
| attach 失败行为 | **退避重试 + 暴露到 `/healthz`** | 避免基础设施晚就绪引发 CrashLoop 噪音 |
| subscribe 扇出 | **每个调用方独立订阅** | 未来其他服务也会投递任务，cloud-manager 不应监听全部回调 |
| NATS 持久化 | **本期不动，人工恢复** | 先做代码约束；未来迁到外部稳定 NATS |