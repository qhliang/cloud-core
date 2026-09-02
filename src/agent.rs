//! agent-manager ↔ cloud-manager 协议类型。
//!
//! 覆盖任务下发、状态回推、产物回推与配置同步四类交互。

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// 任务生命周期状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Killed,
    Timeout,
}

impl TaskStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            TaskStatus::Succeeded | TaskStatus::Failed | TaskStatus::Killed | TaskStatus::Timeout
        )
    }
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            TaskStatus::Pending => "pending",
            TaskStatus::Running => "running",
            TaskStatus::Succeeded => "succeeded",
            TaskStatus::Failed => "failed",
            TaskStatus::Killed => "killed",
            TaskStatus::Timeout => "timeout",
        };
        f.write_str(value)
    }
}

/// LLM token 用量统计。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TokenUsage {
    #[serde(default)]
    pub input: Option<u64>,
    #[serde(default)]
    pub output: Option<u64>,
}

impl TokenUsage {
    pub fn total(&self) -> (u64, u64) {
        (self.input.unwrap_or(0), self.output.unwrap_or(0))
    }
}

/// 任务创建请求（cloud-manager → agent-manager `POST /api/v1/tasks`）。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct CreateTaskRequest {
    pub task_id: String,
    pub prompt: String,
    pub model: Option<String>,
    pub base_url: Option<String>,
    pub api_key: Option<String>,
    pub context_window: Option<u64>,
    #[serde(alias = "subagents")]
    pub agents: Vec<String>,
    pub mcps: Vec<String>,
    /// 缓存的远程 MCP 工具名（按 MCP 名称分组）；agent-manager 执行时与
    /// 内置 tools 合并写入 pi `--tools`。旧版本 agent-manager 忽略此字段。
    pub mcp_tools: HashMap<String, Vec<String>>,
    pub skills: Vec<String>,
    pub files: HashMap<String, String>,
    pub timeout_secs: u64,
    pub memory_limit_mb: u64,
}

/// 任务对象（agent-manager 查询/创建响应）。
#[derive(Debug, Clone, Serialize)]
pub struct Task {
    pub id: String,
    pub status: TaskStatus,
    pub pid: Option<u32>,
    pub workspace: String,
    pub provider: String,
    pub model: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub finished_at: Option<chrono::DateTime<chrono::Utc>>,
    pub timeout_secs: u64,
    pub memory_limit_mb: u64,
    pub output: String,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
}

/// 智能体运行时配置（agent-manager 通过 sync 动态拉取）。
///
/// 序列化契约：`llm_sk` 为空时输出 `null`，agent-manager 侧反序列化为
/// `None`（表示使用默认/保持当前值）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AgentRuntimeConfig {
    pub max_concurrent_tasks: u64,
    pub sync_interval_secs: u64,
    pub llm_url: String,
    pub llm_model: String,
    pub llm_sk: Option<String>,
    /// 模型上下文窗口（token 数），写入 models.json 的 contextWindow。
    pub context_window: u64,
    /// 出站网络白名单（`host:port`）；运行时会自动附加集群 DNS 的 UDP 53 端口。
    pub net_allow: Vec<String>,
    /// sandlock 只读路径。
    pub fs_read: Vec<String>,
    /// sandlock 可读写路径；运行时会自动附加任务临时工作目录。
    pub fs_read_write: Vec<String>,
    pub compression_min_bytes: u64,
    pub artifact_retention_secs: u64,
    /// Pi 扩展执行策略：safe / balanced（默认）/ permissive。
    pub extension_policy: String,
    /// 运行模式：true = SandLock 沙箱包裹 Pi，false = 直接执行 Pi。
    pub use_sandbox: bool,
    /// smol 角色模型规格（低成本/快速工作，如子代理分发）；可选 `:thinking` 后缀。
    #[serde(default)]
    pub smol: Option<String>,
    /// slow 角色模型规格（深度推理）。
    #[serde(default)]
    pub slow: Option<String>,
    /// plan 角色模型规格（计划模式）。
    #[serde(default)]
    pub plan: Option<String>,
    /// advisor 角色模型规格（回合审阅第二模型）。
    #[serde(default)]
    pub advisor: Option<String>,
    /// 启动即进入只读计划模式，直到计划被批准（对应 `--plan-mode`）。
    #[serde(default)]
    pub plan_mode: bool,
    /// 自动批准已提交的计划，用于无人值守运行（对应 `--plan-yolo`）。
    #[serde(default)]
    pub plan_yolo: bool,
    /// 工具审批模式：always-ask（默认）/ write / yolo。
    #[serde(default)]
    pub approval_mode: String,
    /// provider 请求超时秒数；0 表示不设限。默认 60s（云厂商）/ 600s（本地）。
    #[serde(default)]
    pub request_timeout_secs: u64,
    /// 扩展思考级别：off / minimal / low / medium / high / xhigh / max。
    #[serde(default)]
    pub thinking: String,
    /// 覆盖系统提示词。
    #[serde(default)]
    pub system_prompt: String,
    /// 追加到系统提示词（文本内容）。
    #[serde(default)]
    pub append_system_prompt: String,
}

impl Default for AgentRuntimeConfig {
    fn default() -> Self {
        Self {
            max_concurrent_tasks: 10,
            sync_interval_secs: 60,
            llm_url: "http://tokengateway.wasmcloud.svc:80/v1".to_string(),
            llm_model: "agent-manager".to_string(),
            llm_sk: None,
            context_window: 200_000,
            net_allow: vec!["tokengateway.wasmcloud.svc:80".to_string()],
            fs_read: vec![
                "/usr".to_string(),
                "/lib".to_string(),
                "/lib64".to_string(),
                "/bin".to_string(),
                "/etc".to_string(),
            ],
            fs_read_write: Vec::new(),
            compression_min_bytes: 1024,
            artifact_retention_secs: 3600,
            extension_policy: "balanced".to_string(),
            use_sandbox: false,
            smol: None,
            slow: None,
            plan: None,
            advisor: None,
            plan_mode: false,
            plan_yolo: false,
            approval_mode: "always-ask".to_string(),
            request_timeout_secs: 60,
            thinking: "medium".to_string(),
            system_prompt: String::new(),
            append_system_prompt: String::new(),
        }
    }
}

/// Sync manifest 中的 skill 条目。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillEntry {
    pub name: String,
    #[serde(default)]
    pub updated_at: Option<i64>,
    /// SKILL.md 全文，随 sync manifest 内联下发，不再走 HTTP 下载。
    #[serde(default)]
    pub content: String,
    #[serde(default)]
    pub attachments: Vec<AttachmentEntry>,
}

/// skill 附件条目。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttachmentEntry {
    pub name: String,
    #[serde(default)]
    pub content: String,
}

/// Sync manifest 中的 mcp 条目。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpEntry {
    pub name: String,
    #[serde(default)]
    pub updated_at: Option<i64>,
    /// pi `--mcp-config` 格式的 JSON 内容，内联下发。
    #[serde(default)]
    pub content: String,
}

/// Sync manifest 中的 subagent 条目。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubagentEntry {
    pub name: String,
    #[serde(default)]
    pub updated_at: Option<i64>,
    /// subagent markdown 全文（frontmatter + system prompt），内联下发。
    #[serde(default)]
    pub content: String,
}

/// 配置同步响应（cloud-manager `GET /api/sync` → agent-manager）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncManifest {
    #[serde(default)]
    pub updated_at: i64,
    #[serde(default)]
    pub config: Option<AgentRuntimeConfig>,
    #[serde(default)]
    pub skills: Vec<SkillEntry>,
    #[serde(default)]
    pub mcp: Vec<McpEntry>,
    #[serde(default)]
    pub subagents: Vec<SubagentEntry>,
}

/// 状态回推请求体（agent-manager → cloud-manager `POST /api/agent/status`）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusPush {
    pub task_id: String,
    #[serde(default)]
    pub timestamp: String,
    pub payload: StatusPushPayload,
}

/// 状态回推的 payload（按 `type` 区分事件类型）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StatusPushPayload {
    /// 智能体开始执行，将任务标记为 running。
    AgentStart,
    /// 智能体结束，回写 usage 与平均资源指标（不修改终态）。
    AgentEnd {
        #[serde(default)]
        usage: TokenUsage,
        #[serde(default)]
        memory_kb: u64,
        #[serde(default)]
        cpu_percent: f64,
    },
    /// 任务终结，一次性回写累计输出与原始终态。
    Finalizer {
        #[serde(default)]
        status: String,
        #[serde(default)]
        exit_code: Option<i32>,
        #[serde(default)]
        accumulated: String,
    },
}

/// 产物回推请求体（agent-manager → cloud-manager `POST /api/agent/artifacts`）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactSubmit {
    pub task_id: String,
    #[serde(default)]
    pub submitted_at: String,
    #[serde(default)]
    pub artifacts: Vec<ArtifactEntry>,
}

/// 单个产物条目。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactEntry {
    pub path: String,
    #[serde(default)]
    pub content_base64: String,
    #[serde(default)]
    pub size: u64,
}
