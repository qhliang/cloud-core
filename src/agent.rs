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
    /// 任务初始文件的**引用**（内容存放在 cloud-manager 的 blobstore）。
    ///
    /// 与 `files`（内联内容）并存：`files` 兼容历史定义不动，新路径走这里 ——
    /// 内联内容整包跟随 task-queue payload，受 1 MiB 上限约束；引用体积极小，无此限制。
    /// 老版本 agent-manager 会忽略该字段（serde 默认放过未知字段）⇒ 不生效但不报错。
    #[serde(default)]
    pub file_refs: Vec<TaskFileRef>,
    /// 任务级环境变量，注入 pi 子进程（进而继承给其拉起的 MCP 子进程）。
    /// 结构整体带 `#[serde(default)]` ⇒ 老版本 agent-manager 发来的载荷缺这个字段也能解析；
    /// 反过来老版本 agent-manager 收到后会**忽略**它（serde 默认放过未知字段），
    /// 即「新版 cloud-manager → 老版 agent-manager」只是不生效，不会报错。
    pub env: HashMap<String, String>,
    pub timeout_secs: u64,
    pub memory_limit_mb: u64,
}

/// 任务初始文件的引用：内容在 cloud-manager 的 blobstore，这里只带对象键。
///
/// `name` 是落盘相对路径（写入任务 workspace），`blob_key` 是 cloud-manager 侧的
/// 对象键（`agent-task/{file_uuid}`）。agent-manager 在 spawn pi 之前逐个拉取。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskFileRef {
    /// workspace 内的落盘相对路径。
    pub name: String,
    /// blobstore 对象键，形如 `agent-task/{file_uuid}`。
    pub blob_key: String,
    /// 预期字节数，仅用于日志与校验提示。
    #[serde(default)]
    pub size: u64,
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
    /// cloud-manager 的内部基地址，形如 `http://cloud-manager.wasmcloud.svc:80`。
    /// agent-manager 用它拉取任务初始文件（`GET /internal/agent-task-files`）。
    #[serde(default)]
    pub cloud_manager_url: String,
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
            cloud_manager_url: String::new(),
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
///
/// 一个 skill 目录里除 `SKILL.md` 外的所有文件都走这里（`references/x.md`、
/// `scripts/y.py`、图片等），`name` 是**相对 skill 根目录的路径**。
///
/// ⚠️ 这一个类型服务两个方向，靠字段分工区分（0.1.9 起）：
/// - **上传请求**（`POST /api/skills/upload`）带 `content`（内容内联，binary 时是 base64）；
/// - **sync manifest** 不带 `content`，只带 `blob_key` + `size` + `sha256`，内容由
///   agent-manager 走 `GET /internal/skill-attachments?key=` 按需下载。
///
/// 为什么不拆成两个类型：拆开会让**旧 agent-manager** 反序列化 `SkillEntry` 时
/// 因字段不匹配而**整条 skill 落不下来**；而现在这样（同名同型、字段全 default）
/// 旧进程至少能解析成功。⚠️ 但旧进程会把 `content` 读成空串 ⇒ **写出空附件文件**
/// —— 所以**必须先升级 agent-manager、再升级 cloud-manager**（见 README 的发布顺序）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttachmentEntry {
    /// 相对 skill 根目录的路径，如 `references/aitable.md`。
    ///
    /// ⚠️ 落盘方（agent-manager）会把它拼进 `<skills_dir>/<skill>/`，
    /// 因此**必须**先校验：不许绝对路径、不许 `..`、不许空段
    /// （否则可写到 skill 目录之外）。协议层不做校验，由两端各自把守。
    pub name: String,
    /// 文件内容（**仅上传请求使用**）。`binary = false` 时是原文；`true` 时是 **base64**。
    ///
    /// ⚠️ manifest 下发时该字段恒为空：附件动辄几百 KiB、一个 skill 可带 500 个，
    /// 内联会同时撑爆 D1 单行上限与 NATS KV 的 value 上限。
    /// 空串**不序列化**（`skip_serializing_if`）—— 让「manifest 里不该出现 content」
    /// 这条约束在产物上就看得见，而不是靠人记得。
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub content: String,
    /// 内容是否为二进制（base64 编码）。
    ///
    /// ⚠️ 必须**显式**标记：脚本/配置多为 UTF-8 文本，直接内联最省事；而 `.pyc`、
    /// 图片、字体不是合法 UTF-8，硬塞进 String 会被替换字符悄悄破坏内容。
    /// 不用「尝试解码 UTF-8 失败则当二进制」来推断 —— 那样任何一次误判都是静默的数据损坏。
    ///
    /// ⚠️ 它与 `blob_key` 指向的对象**编码无关**：那个对象里的字节永远是**落盘时的原始
    /// 字节**（不是 base64），`binary` 只描述「该文件是不是二进制」。
    /// 早期实现把它当成「内容是 base64」，走 blobstore 后该含义已不适用。
    #[serde(default)]
    pub binary: bool,
    /// blobstore 对象键（`skill-attachment/<skill_id>/<序号>`），由 cloud-manager 下发。
    ///
    /// ⚠️ 空串表示「该条目没有 blob」：要么来自**旧版 cloud-manager**（内容在 `content`
    /// 里内联），要么是 manifest 与内容不同步的异常态。读取方必须**按空前缀处理并回退到
    /// `content`** —— 不能假定它非空，否则升级期会写出空文件。
    #[serde(default)]
    pub blob_key: String,
    /// 落盘后的**原始字节数**（二进制附件即解码后的长度，不是 base64 长度）。仅用于日志与展示。
    #[serde(default)]
    pub size: u64,
    /// **原始字节**的摘要（十六进制小写）。本地已有同摘要的文件即跳过下载。
    ///
    /// ⚠️ 与 `size` 同一基准：都描述「落盘后的那个文件」，不是 blobstore 里的对象。
    #[serde(default)]
    pub sha256: String,
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

/// Sync manifest 中的可执行程序（Linux ELF 二进制）条目。
///
/// 与 skills / mcp 不同，这里**只发元数据、不发内容**：二进制动辄几十 MiB，
/// 而 manifest 要走 NATS KV（value 有大小上限，且已内联 skills/subagents 全文）。
/// agent-manager 据此判断本地是否需要重新下载，内容按需走 HTTP 拉取。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinaryEntry {
    /// 文件名，落到 `<bin_dir>/<name>`，也是 PATH 里的调用名。
    pub name: String,
    /// blobstore 对象键（`agent-binary/<uuid>`），由 cloud-manager 下发。
    pub blob_key: String,
    /// **原始（解压后）**字节数，仅用于日志与展示。
    ///
    /// ⚠️ 与 `sha256` 同一基准：两者都描述「落盘后的那个可执行文件」，
    /// 不是 blobstore 里存的字节。`stored_size` 才是传输/存储侧的体积。
    #[serde(default)]
    pub size: u64,
    /// **原始（解压后）**内容的摘要（十六进制）。本地 sidecar 与之一致即跳过下载。
    ///
    /// ⚠️ 校验发生在**解压之后**：agent-manager 拿到的是压缩内容，必须先解压再算摘要。
    #[serde(default)]
    pub sha256: String,
    /// blobstore 中实际存储的字节数（压缩后）。仅用于日志与带宽核算；0 表示未知。
    #[serde(default)]
    pub stored_size: u64,
    /// 传输/存储编码：空串 = 原样存储；`"gzip"` = blobstore 里是 gzip 流，
    /// agent-manager 必须先解压再校验 `sha256` / 落盘。
    ///
    /// ⚠️ 取值必须与 cloud-manager 侧常量一致；未知取值应按**原样**处理并告警，
    /// 不要猜测解压 —— 猜错会把压缩流当成可执行文件写进 PATH。
    #[serde(default)]
    pub encoding: String,
    #[serde(default)]
    pub updated_at: Option<i64>,
}

/// 配置同步响应（cloud-manager `GET /api/sync` → agent-manager）。
///
/// ⚠️ 新增字段一律带 `#[serde(default)]`：agent-manager 与 cloud-manager 是分别部署的，
/// 新旧版本会短暂共存（旧 agent-manager 解析新 manifest 时不能因为未知字段而失败）。
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
    /// 已启用的可执行程序清单（仅元数据）。
    #[serde(default)]
    pub binaries: Vec<BinaryEntry>,
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

#[cfg(test)]
mod attachment_compat_tests {
    use super::*;

    /// ⚠️ **升级期的兼容性守门测试**：agent-manager 与 cloud-manager 分别部署，
    /// 一定会出现「新 agent-manager 读旧 manifest」的窗口。旧形态是内容内联
    /// （`content` 有值、`blob_key` 不存在），必须仍能解析出完整条目。
    #[test]
    fn old_inline_attachment_still_deserializes() {
        let json = r#"{"name":"references/a.md","content":"hello","binary":false}"#;
        let a: AttachmentEntry = serde_json::from_str(json).unwrap();
        assert_eq!(a.name, "references/a.md");
        assert_eq!(a.content, "hello");
        assert!(!a.binary);
        // 旧形态没有 blob_key ⇒ 读取方必须回退到 content（这就是兼容窗口的关键）。
        assert!(a.blob_key.is_empty(), "旧条目不该凭空产出 blob_key");
        assert_eq!(a.size, 0);
        assert!(a.sha256.is_empty());
    }

    /// 新形态：只带元数据，`content` 缺失也必须能解析（不能因为缺字段而整条失败）。
    #[test]
    fn metadata_only_attachment_deserializes_without_content() {
        let json = r#"{"name":"scripts/run.py","blob_key":"skill-attachment/sk_1/0",
                       "size":42,"sha256":"ab","binary":false}"#;
        let a: AttachmentEntry = serde_json::from_str(json).unwrap();
        assert_eq!(a.blob_key, "skill-attachment/sk_1/0");
        assert_eq!(a.size, 42);
        assert_eq!(a.sha256, "ab");
        assert!(a.content.is_empty());
    }

    /// 极简形态（只有 `name`）不能 panic —— 早期/被裁剪的 manifest 也要能读。
    #[test]
    fn minimal_attachment_is_tolerated() {
        let a: AttachmentEntry = serde_json::from_str(r#"{"name":"x.md"}"#).unwrap();
        assert_eq!(a.name, "x.md");
        assert!(a.content.is_empty() && a.blob_key.is_empty() && a.sha256.is_empty());
    }

    /// 整个 `SkillEntry` 在两种形态下都要能读（旧/新 agent-manager 各自的视角）。
    #[test]
    fn skill_entry_reads_both_shapes() {
        let old = r#"{"name":"a","updated_at":1,"content":"---\nname: a\n---\n","attachments":[
            {"name":"r/x.md","content":"inline","binary":false}]}"#;
        let s: SkillEntry = serde_json::from_str(old).unwrap();
        assert_eq!(s.attachments[0].content, "inline");
        assert!(s.attachments[0].blob_key.is_empty());

        let new = r#"{"name":"a","updated_at":1,"content":"---\nname: a\n---\n","attachments":[
            {"name":"r/x.md","blob_key":"skill-attachment/sk_1/0","size":6,"sha256":"ff","binary":false}]}"#;
        let s: SkillEntry = serde_json::from_str(new).unwrap();
        assert_eq!(s.attachments[0].blob_key, "skill-attachment/sk_1/0");
        assert!(s.attachments[0].content.is_empty());
    }

    /// 序列化新条目时不该把 `content` 也写出去（否则 KV 里白占体积）——
    /// 这是「manifest 只发元数据」这条约束的可执行判据。
    #[test]
    fn metadata_only_entry_serializes_without_content_payload() {
        let a = AttachmentEntry {
            name: "r/x.md".into(),
            content: String::new(),
            binary: false,
            blob_key: "skill-attachment/sk_1/0".into(),
            size: 6,
            sha256: "ff".into(),
        };
        let s = serde_json::to_string(&a).unwrap();
        assert!(s.contains("skill-attachment/sk_1/0"));
        assert!(
            !s.contains("\"content\""),
            "空的 content 不该出现在 manifest 里: {s}"
        );
    }
}
