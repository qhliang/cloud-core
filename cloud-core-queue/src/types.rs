//! 任务记录与线上格式类型。
//!
//! ⚠️ **本文件是线格式的唯一事实源。** 所有进出 JetStream 的结构都在这里定义；
//! 禁止在其他模块用 `serde_json::json!` 之类的手段另行拼装。
//!
//! # v2 相对 v1 的变更
//!
//! | 字段 | v1 | v2 | 理由 |
//! |---|---|---|---|
//! | 任务标识 | `task_id` | **`id`** | 统一：其余所有表面都用 `id` |
//! | payload 编码 | `payload_encoding` + `payload_base64` / `payload` 三选一 | **`payload` 恒为 base64 字符串** | JSON 无法安全承载任意二进制；v1 的 `raw` 与 `binary` 两个分支序列化后完全相同，`decode_payload` 的重复分支是纯负担 |
//! | 编码标记 | `payload_encoding` | **`encoding`** | 去掉冗余的 `payload_` 前缀 |
//! | 执行截止 | `execution_deadline_ms` | **`deadline_ms`** | 与 `TaskMeta.deadline_ms` 对齐 |
//!
//! ⚠️ **不与 v1 兼容**：`schema_version` 仍为 1，因此新旧信封无法区分。
//! 迁移时**必须先排空队列**再切换，否则旧任务会被新消费者按 v2 解析而静默出错。

use serde::{Deserialize, Serialize};

use crate::error::QueueError;

/// 任务标识。
pub type TaskId = String;
/// 任务的终态输出（`None` 表示无输出）。
pub type TaskOutput = Option<Vec<u8>>;

/// 生产者要投递的业务负载。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Task {
    /// 任意字节。序列化进信封时统一走 base64。
    pub payload: Vec<u8>,
}

/// 任务的内部状态机。
///
/// ⚠️ 与 [`TaskStatus`] 的**有意区分**：`state` 是库内部的状态流转，
/// `status` 是对外的任务终态结论。**不要**把它们「统一」掉。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    /// 已入队，尚未派发。
    Queued,
    /// 派发超时，等待重试或取消。
    DispatchTimeoutPending,
    /// 执行中。
    Running,
    /// 派发超时（终态）。
    DispatchTimeout,
    /// 执行超时（终态）。
    ExecutionTimeout,
    /// 已取消（终态）。
    Cancelled,
    /// 超过最大投递次数（终态）。
    MaxRetriesExceeded,
    /// 成功（终态）。
    Succeeded,
    /// 失败（终态）。
    Failed,
}

impl TaskState {
    /// 终态不得再次执行。
    pub fn is_terminal(self) -> bool {
        !matches!(
            self,
            Self::Queued | Self::DispatchTimeoutPending | Self::Running
        )
    }
}

/// 对外的任务终态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TaskStatus {
    /// 成功。
    Succeeded,
    /// 失败。
    Failed,
    /// 派发超时。
    DispatchTimeout,
    /// 执行超时。
    ExecutionTimeout,
    /// 已取消。
    Cancelled,
    /// 超过最大投递次数。
    MaxRetriesExceeded,
}

impl TaskStatus {
    /// 字符串拼写，与 serde 的 kebab-case 序列���一致。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::DispatchTimeout => "dispatch-timeout",
            Self::ExecutionTimeout => "execution-timeout",
            Self::Cancelled => "cancelled",
            Self::MaxRetriesExceeded => "max-retries-exceeded",
        }
    }
}

/// 一次投递失败的明细。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptFailureRecord {
    /// 第几次投递（从 1 开始）。
    pub attempt: u32,
    /// 失败来源：`guest` 或 `system`。
    pub source: String,
    /// 失败原因。
    pub error: String,
    /// 本次投递开始时刻（Unix 毫秒）。
    pub started_at_ms: Option<u64>,
    /// 本次投递失败时刻（Unix 毫秒）。
    pub failed_at_ms: Option<u64>,
    /// 本次投递耗时（毫秒）。
    pub duration_ms: Option<u64>,
}

/// 任务元数据，存于 KV bucket（`{queue}-meta`，键 `{queue}.{id}`）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskMeta {
    /// 元数据语义版本。
    pub schema_version: u32,
    /// 任务标识。
    pub id: String,
    /// 所属队列名。
    pub queue: String,
    /// 内部状态机。
    pub state: TaskState,
    /// 当前投递次数（0 表示尚未派发）。
    pub attempt: u32,
    /// 创建时刻（Unix 毫秒）。
    pub created_at_ms: u64,
    /// 派发时刻（Unix 毫秒）。
    pub dispatched_at_ms: Option<u64>,
    /// 终态时刻（Unix 毫秒）。
    pub completed_at_ms: Option<u64>,
    /// 派发截止时刻（Unix 毫秒）。
    pub deadline_ms: u64,
    /// 是否已请求取消。
    pub cancel_requested: bool,
    /// 历史投递失败明细。
    #[serde(default)]
    pub attempts: Vec<AttemptFailureRecord>,
}

impl TaskMeta {
    /// 追加一条投递失败明细；同一次投递（`attempt` 相同）只记一次。
    ///
    /// 投递语义是 at-least-once，重复投递会多次走到这里；`attempts` 会被序列化进
    /// 1 MiB 上限的 KV 值，因此必须去重。按 `attempt` 去重是因为它标识一次投递，
    /// 而一次投递至多产生一条失败。
    ///
    /// 返回是否真的追加了。
    pub fn record_attempt(&mut self, record: AttemptFailureRecord) -> bool {
        if self
            .attempts
            .iter()
            .any(|recorded| recorded.attempt == record.attempt)
        {
            return false;
        }
        self.attempts.push(record);
        true
    }
}

/// 任务信封，发布于 `{queue}.tasks.{id}`。
///
/// ⚠️ **payload 恒为 base64 字符串**：`payload` 字段是唯一的负载载体，
/// `encoding` 恒为 `"base64"`。保留 `encoding` 是为了让未来若要换编码
/// （如超大负载走分片引用）不必再改线格式。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskEnvelope {
    /// 信封语义版本。
    pub schema_version: u32,
    /// 任务标识（⚠️ v1 曾叫 `task_id`）。
    pub id: String,
    /// 负载编码方式，恒为 [`TaskEnvelope::ENCODING_BASE64`]。
    pub encoding: String,
    /// base64 编码的负载。
    pub payload: String,
    /// 执行截止时刻（Unix 毫秒，⚠️ v1 曾叫 `execution_deadline_ms`）。
    pub deadline_ms: u64,
}

impl TaskEnvelope {
    /// 信封版本。
    pub const SCHEMA_VERSION: u32 = 1;
    /// 唯一支持的负载编码。
    pub const ENCODING_BASE64: &str = "base64";

    /// 由任意字节构造信封。
    pub fn new(id: impl Into<String>, payload: Vec<u8>, deadline_ms: u64) -> Self {
        Self {
            schema_version: Self::SCHEMA_VERSION,
            id: id.into(),
            encoding: Self::ENCODING_BASE64.to_string(),
            payload: base64_encode(&payload),
            deadline_ms,
        }
    }

    /// 任务标识。
    pub fn id(&self) -> &str {
        &self.id
    }

    /// 执行截止时刻（Unix 毫秒）。
    pub fn deadline_ms(&self) -> u64 {
        self.deadline_ms
    }

    /// 解码负载。
    ///
    /// ⚠️ 解码失败**必须报错**而非静默返回空 —— 静默会让消费者以为任务无负载，
    /// 进而以错误的输入执行。
    pub fn decode_payload(&self) -> Result<Vec<u8>, PayloadError> {
        if self.encoding != Self::ENCODING_BASE64 {
            return Err(PayloadError::UnsupportedEncoding(self.encoding.clone()));
        }
        base64_decode(&self.payload).map_err(|_| PayloadError::InvalidBase64)
    }
}

/// 负载解码错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PayloadError {
    /// 声明的编码方式本库不支持。
    UnsupportedEncoding(String),
    /// base64 串非法。
    InvalidBase64,
}

impl std::fmt::Display for PayloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedEncoding(encoding) => {
                write!(f, "unsupported payload encoding: {encoding}")
            }
            Self::InvalidBase64 => f.write_str("invalid base64 payload"),
        }
    }
}

impl std::error::Error for PayloadError {}

/// 任务对外状态视图。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskInfo {
    /// 任务标识。
    pub id: String,
    /// 终态结论。
    pub status: TaskStatus,
    /// 当前投递次数（`None` 表示尚未派发）。
    pub attempt: Option<u32>,
    /// 创建时刻（Unix 毫秒）。
    pub created_at_ms: u64,
    /// 派发时刻（Unix 毫秒）。
    pub dispatched_at_ms: Option<u64>,
    /// 终态时刻（Unix 毫秒）。
    pub completed_at_ms: Option<u64>,
    /// 是否已请求取消。
    pub cancel_requested: bool,
}

/// 失败来源。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptErrorSource {
    /// 业务错误：可重试，直到投递次数耗尽。
    Guest,
    /// 系统错误：不应重试。
    System,
}

impl AttemptErrorSource {
    /// 字符串拼写，与 serde 的 snake_case 序列化一致。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Guest => "guest",
            Self::System => "system",
        }
    }
}

impl std::str::FromStr for AttemptErrorSource {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "guest" => Ok(Self::Guest),
            "system" => Ok(Self::System),
            other => anyhow::bail!("unknown attempt error source: {other}"),
        }
    }
}

/// 一次失败的投递。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptFailure {
    /// 任务标识。
    pub id: String,
    /// 第几次投递。
    pub attempt: u32,
    /// 失败来源。
    pub source: AttemptErrorSource,
    /// 失败原因。
    pub error: String,
    /// 开始时刻（Unix 毫秒）。
    pub started_at_ms: Option<u64>,
    /// 失败时刻（Unix 毫秒）。
    pub failed_at_ms: Option<u64>,
    /// 耗时（毫秒）。
    pub duration_ms: Option<u64>,
}

/// 失败来源，决定重试策略。
///
/// ⚠️ 这个区分是可靠性关键：判错会让毒任务无限重投，或让可恢复的业务失败
/// 直接终结。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskErrorSource {
    /// 业务错误：可重试，直到投递次数耗尽。
    Guest,
    /// 系统错误（基础设施 / 死线）：不应重试。
    System,
}

impl TaskErrorSource {
    /// 字符串拼写，与 [`AttemptErrorSource::as_str`] 一致。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Guest => "guest",
            Self::System => "system",
        }
    }
}

/// Worker 返回的分类错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskError {
    /// 失败来源，决定重试与否。
    pub source: TaskErrorSource,
    /// 失败原因。
    pub message: String,
}

impl TaskError {
    /// 业务错误（可重试）。
    pub fn guest(message: impl Into<String>) -> Self {
        Self {
            source: TaskErrorSource::Guest,
            message: message.into(),
        }
    }

    /// 系统错误（不重试）。
    pub fn system(message: impl Into<String>) -> Self {
        Self {
            source: TaskErrorSource::System,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.source.as_str(), self.message)
    }
}

impl std::error::Error for TaskError {}

impl From<TaskError> for QueueError {
    fn from(err: TaskError) -> Self {
        QueueError::Task(err.to_string())
    }
}

/// base64 编码。
///
/// ⚠️ 手写而非引依赖：本库只需要标准字母表，且 `TaskEnvelope::new` 在热路径上
/// 会被每个任务调用一次。
pub fn base64_encode(value: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let alphabet_char = |index: u32| -> char {
        let offset = usize::try_from(index & 63).unwrap_or_default();
        ALPHABET.get(offset).map_or('\0', |byte| *byte as char)
    };
    let mut encoded = String::with_capacity(value.len().div_ceil(3) * 4);
    for chunk in value.chunks(3) {
        let a = chunk.first().map_or(0u32, |v| u32::from(*v));
        let b = chunk.get(1).map_or(0u32, |v| u32::from(*v));
        let c = chunk.get(2).map_or(0u32, |v| u32::from(*v));
        let bits = (a << 16) | (b << 8) | c;
        encoded.push(alphabet_char(bits >> 18));
        encoded.push(alphabet_char(bits >> 12));
        encoded.push(if chunk.len() > 1 {
            alphabet_char(bits >> 6)
        } else {
            '='
        });
        encoded.push(if chunk.len() > 2 {
            alphabet_char(bits)
        } else {
            '='
        });
    }
    encoded
}

/// base64 解码。失败返回描述性错误，**不返回部分结果**。
pub fn base64_decode(value: &str) -> Result<Vec<u8>, anyhow::Error> {
    let mut output = Vec::with_capacity(value.len() / 4 * 3);
    let mut bits = 0u64;
    let mut count = 0u32;
    for byte in value.bytes() {
        let digit = match byte {
            b'A'..=b'Z' => u32::from(byte - b'A'),
            b'a'..=b'z' => u32::from(byte - b'a') + 26,
            b'0'..=b'9' => u32::from(byte - b'0') + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' if count == 2 => {
                output.push(u8::try_from((bits >> 4) & 255).unwrap_or_default());
                break;
            }
            b'=' if count == 3 => {
                output.push(u8::try_from((bits >> 10) & 255).unwrap_or_default());
                output.push(u8::try_from((bits >> 2) & 255).unwrap_or_default());
                break;
            }
            b'=' => anyhow::bail!("unexpected base64 padding"),
            _ => anyhow::bail!("invalid base64 character"),
        };
        bits = (bits << 6) | u64::from(digit);
        count += 1;
        if count == 4 {
            output.extend_from_slice(&bits.to_be_bytes()[5..]);
            bits = 0;
            count = 0;
        }
    }
    if count == 1 || (value.ends_with('=') && !value.len().is_multiple_of(4)) {
        anyhow::bail!("invalid base64 length");
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_round_trips_arbitrary_bytes() {
        // 含 0x00 与非 UTF-8 字节：这两类正是 v1 「raw / binary 双分支」
        // 想覆盖的场景，v2 单一 base64 字段即可完整表达。
        let payload = vec![0u8, 1, 2, 0xff, 0xfe, b'h', b'i'];
        let envelope = TaskEnvelope::new("task-1", payload.clone(), 123);
        assert_eq!(envelope.decode_payload().expect("valid payload"), payload);
    }

    #[test]
    fn envelope_uses_id_not_task_id() {
        let envelope = TaskEnvelope::new("task-1", b"hi".to_vec(), 0);
        let json = serde_json::to_value(&envelope).expect("serializes");
        assert!(json.get("id").is_some(), "v2 必须用 id：{json}");
        assert!(
            json.get("task_id").is_none(),
            "v1 的 task_id 必须消失：{json}"
        );
    }

    #[test]
    fn envelope_payload_is_always_base64_string() {
        let envelope = TaskEnvelope::new("task-1", vec![0u8, 0xff], 0);
        let json = serde_json::to_value(&envelope).expect("serializes");
        assert_eq!(json["encoding"], "base64");
        assert!(json["payload"].is_string(), "payload 必须是字符串：{json}");
        // v1 的两个可选字段都不应再出现。
        assert!(json.get("payload_base64").is_none(), "{json}");
        assert!(json.get("payload_encoding").is_none(), "{json}");
        assert!(json.get("execution_deadline_ms").is_none(), "{json}");
        assert_eq!(json["deadline_ms"], 0);
    }

    #[test]
    fn unsupported_encoding_is_rejected() {
        let mut envelope = TaskEnvelope::new("task-1", b"hi".to_vec(), 0);
        envelope.encoding = "xml".to_string();
        assert!(matches!(
            envelope.decode_payload(),
            Err(PayloadError::UnsupportedEncoding(_))
        ));
    }

    #[test]
    fn invalid_base64_is_rejected_rather_than_returned_empty() {
        let mut envelope = TaskEnvelope::new("task-1", b"hi".to_vec(), 0);
        envelope.payload = "!!!!".to_string();
        assert_eq!(envelope.decode_payload(), Err(PayloadError::InvalidBase64));
    }

    #[test]
    fn base64_round_trips_every_byte_value() {
        for len in 0..=8usize {
            let bytes: Vec<u8> = (0..len).map(|i| (i as u8).wrapping_mul(37)).collect();
            let encoded = base64_encode(&bytes);
            assert_eq!(
                base64_decode(&encoded).expect("round trip"),
                bytes,
                "len={len} encoded={encoded}"
            );
        }
    }

    #[test]
    fn status_names_are_stable() {
        assert_eq!(TaskStatus::Succeeded.as_str(), "succeeded");
        assert_eq!(TaskStatus::DispatchTimeout.as_str(), "dispatch-timeout");
        assert_eq!(
            TaskStatus::MaxRetriesExceeded.as_str(),
            "max-retries-exceeded"
        );
    }

    #[test]
    fn status_serializes_as_kebab_case() {
        let json = serde_json::to_value(TaskStatus::MaxRetriesExceeded).expect("serializes");
        assert_eq!(json, "max-retries-exceeded");
    }

    #[test]
    fn pending_states_are_not_terminal() {
        assert!(!TaskState::Queued.is_terminal());
        assert!(!TaskState::Running.is_terminal());
        assert!(!TaskState::DispatchTimeoutPending.is_terminal());
    }

    #[test]
    fn terminal_states_match_the_expected_set() {
        for state in [
            TaskState::Succeeded,
            TaskState::Failed,
            TaskState::DispatchTimeout,
            TaskState::ExecutionTimeout,
            TaskState::Cancelled,
            TaskState::MaxRetriesExceeded,
        ] {
            assert!(state.is_terminal(), "{state:?} 应当是终态");
        }
        for state in [
            TaskState::Queued,
            TaskState::Running,
            TaskState::DispatchTimeoutPending,
        ] {
            assert!(!state.is_terminal(), "{state:?} 不应是终态");
        }
    }

    fn failure(attempt: u32) -> AttemptFailureRecord {
        AttemptFailureRecord {
            attempt,
            source: "guest".to_string(),
            error: "boom".to_string(),
            started_at_ms: Some(1),
            failed_at_ms: Some(2),
            duration_ms: Some(1),
        }
    }

    fn meta() -> TaskMeta {
        TaskMeta {
            schema_version: TaskEnvelope::SCHEMA_VERSION,
            id: "task-1".to_string(),
            queue: "agent-task".to_string(),
            state: TaskState::Running,
            attempt: 1,
            created_at_ms: 0,
            dispatched_at_ms: None,
            completed_at_ms: None,
            deadline_ms: 0,
            cancel_requested: false,
            attempts: Vec::new(),
        }
    }

    #[test]
    fn record_attempt_appends_once_per_attempt() {
        let mut meta = meta();
        assert!(meta.record_attempt(failure(1)));
        // at-least-once 下同一投递会重复到达，不得重复记账。
        assert!(!meta.record_attempt(failure(1)));
        assert_eq!(meta.attempts.len(), 1);
        assert!(meta.record_attempt(failure(2)));
        assert_eq!(meta.attempts.len(), 2);
    }

    #[test]
    fn attempt_error_source_round_trips() {
        for source in [AttemptErrorSource::Guest, AttemptErrorSource::System] {
            let parsed: AttemptErrorSource = source.as_str().parse().expect("parses");
            assert_eq!(parsed, source);
        }
        assert!("nope".parse::<AttemptErrorSource>().is_err());
    }
}
