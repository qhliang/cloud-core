//! 队列配置。
//!
//! 默认值是协议级安全上限。**同一队列的所有绑定方必须对这些值达成一致** ——
//! `ack_wait` 与 `max_deliver` 变了，消费者与生产者的行为就会分叉。

use std::time::Duration;

/// 默认 ack 等待窗口（毫秒）。
pub const ACK_WAIT_MS: u64 = 30_000;
/// 默认 lease 续期间隔（毫秒）。
pub const LEASE_RENEW_INTERVAL_MS: u64 = 10_000;
/// 默认派发超时（毫秒）。
pub const DEFAULT_DISPATCH_TIMEOUT_MS: u64 = 600_000;
/// 默认执行超时（毫秒）。
pub const DEFAULT_EXECUTION_TIMEOUT_MS: u64 = 3_600_000;
/// 默认重试退避阶梯（毫秒）。
pub const DEFAULT_RETRY_BACKOFF_MS: &[u64] = &[1_000, 5_000, 15_000, 60_000];
/// 业务负载上限（字节）。
pub const PAYLOAD_MAX_BYTES: usize = 1_048_576;
/// 心跳 info 上限（字节）。
pub const HEARTBEAT_MAX_INFO_BYTES: usize = 8_192;
/// JetStream 消息上限：负载上限 + 信封与 base64 膨胀的余量。
pub const MAX_MESSAGE_SIZE: i32 = (PAYLOAD_MAX_BYTES as i32) + 4_096;
/// 默认最大投递次数。
pub const MAX_DELIVERIES: i64 = 3;

/// 队列配置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueConfig {
    /// 队列名，同时决定所有 NATS 资源名（见 `docs` §6.8）。
    pub name: String,
    /// 消费者等待 ack 的窗口。
    pub ack_wait: Duration,
    /// lease 续期间隔。
    pub lease_renew_interval: Duration,
    /// 派发超时：超过则任务被判定为派发超时。
    pub dispatch_timeout: Duration,
    /// 执行超时：超过则任务被判定为执行超时。
    pub execution_timeout: Duration,
    /// 最大投递次数；超过后任务以 `max-retries-exceeded` 终结。
    pub max_deliver: i64,
    /// 重试退避阶梯（毫秒）。
    pub retry_backoff_ms: Vec<u64>,
    /// 是否归档终态结果到 `{queue}-results`。
    pub results_archive: bool,
}

impl QueueConfig {
    /// 以协议默认值构造。
    ///
    /// ⚠️ 队列名不合法时直接 panic —— 它来自代码里的常量而非用户输入，
    /// 写错属于编程错误，不该在运行期才发现。
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        validate_queue_name(&name).expect("queue name is a compile-time constant");
        Self {
            name,
            ack_wait: Duration::from_millis(ACK_WAIT_MS),
            lease_renew_interval: Duration::from_millis(LEASE_RENEW_INTERVAL_MS),
            dispatch_timeout: Duration::from_millis(DEFAULT_DISPATCH_TIMEOUT_MS),
            execution_timeout: Duration::from_millis(DEFAULT_EXECUTION_TIMEOUT_MS),
            max_deliver: MAX_DELIVERIES,
            retry_backoff_ms: DEFAULT_RETRY_BACKOFF_MS.to_vec(),
            results_archive: true,
        }
    }

    /// 校验全部字段，返回配置问题（若有）。
    ///
    /// [`Self::new`] 之外的构造方式（从配置文件读取）应先过这里。
    pub fn validate(&self) -> Result<(), QueueConfigError> {
        validate_queue_name(&self.name)?;
        if self.max_deliver < 1 {
            return Err(QueueConfigError::InvalidValue {
                field: "max_deliver",
                value: self.max_deliver.to_string(),
            });
        }
        if self.retry_backoff_ms.is_empty() {
            return Err(QueueConfigError::InvalidValue {
                field: "retry_backoff_ms",
                value: "(empty)".to_string(),
            });
        }
        Ok(())
    }

    /// 第 `attempt` 次投递失败后的重试延迟。
    ///
    /// 第 1 次已经失败，因此它对应退避表的**第一项**；之后的投递钳到最后一��。
    pub fn backoff_for_attempt(&self, attempt: u32) -> Duration {
        let index = attempt.saturating_sub(1) as usize;
        let millis = self
            .retry_backoff_ms
            .get(index)
            .copied()
            .or_else(|| self.retry_backoff_ms.last().copied())
            .unwrap_or(1_000)
            .min(u64::from(u32::MAX));
        Duration::from_millis(millis)
    }
}

/// 队列配置问题。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueueConfigError {
    /// 队列名不合法。
    InvalidName(String),
    /// 某个字段的取值不合法。
    InvalidValue {
        /// 字段名。
        field: &'static str,
        /// 实际取值。
        value: String,
    },
}

impl std::fmt::Display for QueueConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidName(name) => write!(f, "invalid queue name: {name}"),
            Self::InvalidValue { field, value } => {
                write!(f, "invalid {field}: {value}")
            }
        }
    }
}

impl std::error::Error for QueueConfigError {}

/// 校验队列名：`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`。
///
/// ⚠️ 队列名会直接拼进 NATS 的 subject、stream 名与 KV bucket 名，
/// 含 `.` `*` `>` 等通配符会导致 subject 注入，因此必须严格校验。
pub fn validate_queue_name(queue: &str) -> Result<(), QueueConfigError> {
    let mut chars = queue.chars();
    let first = chars.next();
    let valid_first = first
        .map(|ch| ch.is_ascii_alphanumeric())
        .unwrap_or_default();
    let valid_tail = chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-');
    if valid_first && valid_tail && queue.len() <= 64 {
        Ok(())
    } else {
        Err(QueueConfigError::InvalidName(queue.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_name_accepts_valid_characters() {
        assert!(validate_queue_name("agent-task").is_ok());
        assert!(validate_queue_name("workflow-task").is_ok());
        assert!(validate_queue_name("Agent_2").is_ok());
    }

    #[test]
    fn queue_name_rejects_nats_wildcards() {
        // 队列名直接进 subject，'*' '>' '.' 会造成 subject 注入。
        for name in ["", "-agent", "agent.*", "agent task", "agent>tasks"] {
            assert!(validate_queue_name(name).is_err(), "{name:?} 必须被拒绝");
        }
        assert!(validate_queue_name(&"a".repeat(65)).is_err());
    }

    #[test]
    fn backoff_uses_attempt_index_and_clamps_to_last() {
        let config = QueueConfig::new("agent-task");
        assert_eq!(config.backoff_for_attempt(1).as_millis(), 1_000);
        assert_eq!(config.backoff_for_attempt(2).as_millis(), 5_000);
        assert_eq!(config.backoff_for_attempt(99).as_millis(), 60_000);
    }

    #[test]
    fn defaults_are_self_consistent() {
        let config = QueueConfig::new("agent-task");
        assert!(config.validate().is_ok());
        // lease 续期必须显著快于 ack 窗口，否则会在两次续期之间丢 lease。
        assert!(config.lease_renew_interval < config.ack_wait);
    }

    #[test]
    fn validate_rejects_bad_values() {
        let mut config = QueueConfig::new("agent-task");
        config.max_deliver = 0;
        assert!(config.validate().is_err());

        let mut config = QueueConfig::new("agent-task");
        config.retry_backoff_ms.clear();
        assert!(config.validate().is_err());
    }
}
