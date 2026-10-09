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
    // ── 会话复用（session reuse）──────────────────────────────────────────
    //
    // ⚠️ `task_id` 依然是**执行记录 ID**（队列去重 + 状态/产物上报键），
    // 这里额外带上「业务任务 ID」用于派生任务目录 —— 两者语义不同，**不可互换**：
    // 若把 `task_id` 换成业务任务 ID，同一任务第二次执行会被队列去重当成
    // 「已在运行」直接丢弃（`task_queue.rs` 按 `task_id` 去重）。
    //
    // 全部字段受外层 `#[serde(default)]` 保护：
    // 老版本 cloud-manager 发来的载荷缺字段也能解析；老版本 agent-manager 收到后
    // 会忽略未知字段 ⇒ 降级为「无会话」，不会报错（这是刻意选择的失败方向）。
    /// 是否启用会话复用。
    pub session_enabled: bool,
    /// 任务目录派生键（业务任务 ID，`tk_<hex>`）。
    ///
    /// ⚠️ **必须由它（而非执行 ID）派生 workspace 路径**：pi 的会话按进程 cwd 索引
    /// （实测 `~/.pi/agent/sessions/--<转义 cwd>--/`），路径一变就认不出旧会话。
    /// 留空 = 退化为按执行 ID 派生（旧行为）。
    pub session_dir_key: String,
    /// 云端上一版会话包的**内容 hash**（见 `content_hash` 的约定）。
    /// 空串 = 云端还没有会话包 ⇒ 无需比对，直接全新开始。
    pub session_hash: String,
    /// 云端会话包的对象键（`agent-session/<task_id>`）；空串 = 无包可拉。
    pub session_blob_key: String,
    /// 会话包下载的随机凭据（`st_<uuid4>`）。
    ///
    /// ⚠️ 对象键用的是**任务 ID**，而 task_id 是 `tk_<毫秒时间戳 hex>` —— **可预测**，
    /// 无法像 `tf_`/`bn_` 那样凭 uuid 的随机性承担 capability URL 的准入。
    /// 故另发一个随机凭据，worker 拉取时一并提供。
    pub session_token: String,
    // ── WebUI 在线对话的结构化 trace（§12.13.4）──────────────────────────
    //
    // ⚠️ 为何要传「会话 id + 消息 id」两个值，而不是让 agent-manager 自己推：
    // 这两个 id 的语义完全属于 cloud-manager 侧的业务模型（会话 = agent_tasks 行、
    // 消息 = chat_messages 行），agent-manager 拿不到也不该猜。留空 = 不采集 trace，
    // 老版本 cloud-manager 发来的载荷也能解析（外层已有 `#[serde(default)]`）。
    /// 对话**会话** id（= `agent_tasks.id`，`tk_<hex>`）。非空 = 本轮是对话轮次。
    pub chat_session_id: String,
    /// 本轮的 assistant **消息** id（= `chat_messages.id`，`mid_…`）。
    /// trace 以它为 blobstore 对象键 `chat-trace/<chat_session_id>/<chat_message_id>`。
    pub chat_message_id: String,
    /// 本轮那条 **user 消息**的 id（= `chat_messages.id`，`msg_…`）。
    ///
    /// ## 为什么需要它
    /// 一个轮次的 trace（= 唯一的压缩归档文件）里同时装着
    /// 「这一轮的用户输入」「助手的思考/工具/正文」「执行中追加的追问」。
    /// 用户输入与追问都以 `message_id` 标注自己的来源消息行，前端据此把
    /// 正文精确配回对应的 user 消息 —— 不用靠「第几条 followup」这种脆弱的
    /// 下标推断。
    ///
    /// 留空 = 不写 `user_message` 行（老 cloud-manager 下发的载荷仍可解析）。
    #[serde(default)]
    pub chat_user_message_id: String,
}

/// 在线对话的**追问**：本轮执行**进行中**追加的一条用户消息。
///
/// ## 为什么单独一个类型、而不是复用 `CreateTaskRequest`
/// 追问**不派发新任务**（不新建队列 task、不抢会话锁、不新建 assistant 消息行），
/// 它只是往**正在跑的这一轮**的 stdin 上再写一行 RPC。因此它需要的字段集合与
/// 「一次任务下发」完全不同：没有 model / skills / mcps / 超时 / 内存上限，
/// 却多了「属于哪一轮」这个定位信息。
///
/// ## 存放位置与生命周期
/// 存于 NATS KV `chat_followup.<chat_message_id>.<seq>`（每条一个键 ⇒ 生产者
/// 只做纯写入，没有读-改-写竞态；这是与 `chat_stream.<mid>` 的关键区别）。
/// agent-manager 消费后删除；轮次终态时 cloud-manager 前缀扫描清理残留。
///
/// ⚠️ **KV 没有 per-key TTL**（NATS `Allows Per-Message TTL: false`），
/// 所以 `created_at_ms` 不是冗余字段：它是 cron 判定孤儿键的唯一依据。
/// 没有它，进程崩溃留下的键会永远躺在 bucket 里。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatFollowup {
    /// 所属轮次的 assistant 消息 id（= `CreateTaskRequest.chat_message_id`）。
    ///
    /// ⚠️ 键里已经含了它，这里**冗余**存一份是刻意的：值因此自洽 ——
    /// dump 任意一个键就能读出「这条追问属于哪一轮」，不必去解析键名；
    /// 写路径也不必先查一次会话锁再拼键。
    pub chat_message_id: String,
    /// 这条追问对应的 `chat_messages.id`（`role='user'` 行）。
    pub user_message_id: String,
    /// 对应的 `chat_messages.seq`，同时是 KV 键的排序后缀（决定投递顺序）。
    pub seq: i64,
    /// 追问正文。附件另走 `file_refs`，正文里由 agent-manager 追加文件名提示。
    #[serde(default)]
    pub content: String,
    /// **仅本条追问**的附件引用（不含会话历史附件）。
    ///
    /// ⚠️ 不能直接复用首轮的 `CreateTaskRequest.file_refs`——那是会话文件库的
    /// 全量快照；首轮已在 spawn 前拉取过，再拉一遍会把历史附件重复覆盖写进 workspace。
    #[serde(default)]
    pub file_refs: Vec<TaskFileRef>,
    /// 写入时刻（epoch 毫秒），供孤儿键回收判定。
    #[serde(default)]
    pub created_at_ms: u64,
}

/// 追问在 KV 里的键前缀。
///
/// ⚠️ 与 `chat_stream.` / `chat_lock.` / `chat_task_map.` **互不为前缀**，
/// 否则前缀扫描会互相命中（这是 `kv.rs` 里那批常量必须成组核对的原因）。
pub const CHAT_FOLLOWUP_KV_PREFIX: &str = "chat_followup.";

/// 由 assistant 消息 id 推出追问键的**前缀**（末尾带 `.`）。
///
/// ## 为什么放在 cloud-core 而不是各自拼字符串
/// 生产者（cloud-manager 写）与消费者（agent-manager 按前缀 list）必须对前缀
/// **逐字节一致**。两边各拼一份是典型的漂移源：改了一边，另一边静默地
/// 什么都读不到 —— 表现为「追问永远投不出去」，且没有任何报错。
///
/// agent-manager 侧无需 `CreateTaskRequest` 新增字段：它已经有
/// `chat_message_id`，直接调本函数即可，协议保持向后兼容。
pub fn chat_followup_kv_prefix(chat_message_id: &str) -> String {
    format!("{CHAT_FOLLOWUP_KV_PREFIX}{chat_message_id}.")
}

/// 单条追问的完整 KV 键。
///
/// ⚠️ `seq` 用 `{:012}` 补零，使**字典序 == 数值序**。agent-manager 侧对
/// `keys()` 的返回值排序后即为投递顺序；不补零的话 `10` 会排在 `9` 前面
/// （追问顺序错乱，且这种错乱不会报错，只表现为「模型先回答后问的那句」）。
pub fn chat_followup_kv_key(chat_message_id: &str, seq: i64) -> String {
    format!("{}{chat_message_id}.{seq:012}", CHAT_FOLLOWUP_KV_PREFIX)
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

#[cfg(test)]
mod session_field_compat_tests {
    use super::*;

    /// ⚠️ **会话功能的兼容性守门测试**：cloud-manager 与 agent-manager 分别部署，
    /// 必然出现「新 payload → 老 worker」和「老 payload → 新 worker」两个窗口。
    /// 两个方向都必须**降级而非报错**（刻意选择的失败方向：丢会话能力 > 任务起不来）。
    #[test]
    fn old_payload_without_session_fields_keeps_parsing() {
        let json = r#"{"task_id":"exec-1","prompt":"hi"}"#;
        let r: CreateTaskRequest = serde_json::from_str(json).unwrap();
        assert_eq!(r.task_id, "exec-1");
        assert!(!r.session_enabled, "缺省必须是「未启用」，不能默认开");
        assert!(r.session_dir_key.is_empty());
        assert!(r.session_hash.is_empty());
        assert!(r.session_blob_key.is_empty());
    }

    /// 会话字段齐全时正常落地 —— 防止有人手滑把 `#[serde(default)]` 弄丢
    /// （那样会变成「老 payload 解析失败 ⇒ 任务直接起不来」）。
    #[test]
    fn session_fields_round_trip() {
        let r = CreateTaskRequest {
            task_id: "exec-1".into(),
            prompt: "p".into(),
            session_enabled: true,
            session_dir_key: "tk_abc123".into(),
            session_hash: "sha256hex".into(),
            session_blob_key: "agent-session/tk_abc123".into(),
            ..Default::default()
        };
        let json = serde_json::to_string(&r).unwrap();
        let back: CreateTaskRequest = serde_json::from_str(&json).unwrap();
        assert!(back.session_enabled);
        assert_eq!(back.session_dir_key, "tk_abc123");
        assert_eq!(back.session_hash, "sha256hex");
        assert_eq!(back.session_blob_key, "agent-session/tk_abc123");
    }

    /// ⚠️ 老 cloud-manager 不下发 `chat_user_message_id` ⇒ 必须降级为
    /// 「不写 user_message 行」，而不是解析失败把整个轮次搞挂。
    #[test]
    fn old_payload_without_chat_user_message_id_keeps_parsing() {
        let json = r#"{"task_id":"exec-1","prompt":"hi","chat_session_id":"tk_1","chat_message_id":"mid_1"}"#;
        let r: CreateTaskRequest = serde_json::from_str(json).unwrap();
        assert_eq!(r.chat_message_id, "mid_1");
        assert!(
            r.chat_user_message_id.is_empty(),
            "缺省必须是空串（=不写 user_message 行），不能编一个"
        );
    }

    /// ⚠️ `chat_user_message_id` 与 `chat_message_id` 是**两套 id**，不得互相顶替：
    /// 前者是 user 消息行、后者是 assistant 消息行。把它们混掉会让 trace 的
    /// `message_id` 标错来源，前端把正文配到错误的行上（且不报错）。
    #[test]
    fn chat_user_message_id_is_distinct_from_chat_message_id() {
        let json = r#"{"task_id":"exec-1","prompt":"hi","chat_session_id":"tk_1",
                       "chat_message_id":"mid_asst","chat_user_message_id":"msg_user"}"#;
        let r: CreateTaskRequest = serde_json::from_str(json).unwrap();
        assert_eq!(r.chat_message_id, "mid_asst");
        assert_eq!(r.chat_user_message_id, "msg_user");
        assert_ne!(r.chat_message_id, r.chat_user_message_id);
    }

    /// ⚠️ 会话字段**不得**顶替 `task_id`：后者是队列去重 + 状态/产物上报键。
    /// 这条一旦被违反，症状是「同一任务第二次执行被当成运行中丢弃」—— 静默吞任务。
    #[test]
    fn task_id_is_still_the_execution_id() {
        let json = r#"{"task_id":"exec-9","prompt":"hi",
                       "session_enabled":true,"session_dir_key":"tk_xyz"}"#;
        let r: CreateTaskRequest = serde_json::from_str(json).unwrap();
        assert_eq!(r.task_id, "exec-9");
        assert_eq!(r.session_dir_key, "tk_xyz");
        assert_ne!(
            r.task_id, r.session_dir_key,
            "task_id 与 session_dir_key 必须是两套 id"
        );
    }
}

#[cfg(test)]
mod chat_followup_tests {
    use super::*;

    /// 追问结构体往返 —— 两端（cloud-manager 写 / agent-manager 读）共用它，
    /// 序列化契约一旦漂移就是「键写进去了但读出来是空的」。
    #[test]
    fn followup_round_trip() {
        let f = ChatFollowup {
            chat_message_id: "mid_xyz".into(),
            user_message_id: "msg_abc".into(),
            seq: 7,
            content: "改成只回答 5".into(),
            file_refs: vec![TaskFileRef {
                name: "a.png".into(),
                blob_key: "agent-task/tf_1".into(),
                size: 12,
            }],
            created_at_ms: 1_700_000_000_000,
        };
        let json = serde_json::to_string(&f).unwrap();
        let back: ChatFollowup = serde_json::from_str(&json).unwrap();
        assert_eq!(back, f);
    }

    /// 缺省字段必须能补齐：老版本 cloud-manager 不会发这些字段，
    /// 缺失时应当降级成「无附件 / 无时间戳」，而不是解析失败把整轮搞崩。
    ///
    /// ⚠️ `chat_message_id` **故意不给 `#[serde(default)]`** —— 它是定位键，
    /// 缺了它连 KV 键都拼不出来，那种情况必须解析失败（早失败）而不是
    /// 悄悄写出一个 `chat_followup..000000000000` 的孤儿键。
    #[test]
    fn followup_tolerates_missing_optional_fields() {
        let f: ChatFollowup = serde_json::from_str(
            r#"{"chat_message_id":"mid_1","user_message_id":"msg_1","seq":3,"content":"hi"}"#,
        )
        .unwrap();
        assert!(f.file_refs.is_empty());
        assert_eq!(f.created_at_ms, 0);
        assert_eq!(f.content, "hi");
    }

    /// 缺 `chat_message_id` 必须解析失败（见上一条的注释）。
    #[test]
    fn followup_requires_chat_message_id() {
        let r: Result<ChatFollowup, _> =
            serde_json::from_str(r#"{"user_message_id":"msg_1","seq":3}"#);
        assert!(r.is_err(), "缺定位键必须早失败，不能默默拼出孤儿 KV 键");
    }

    /// ⚠️ **键序契约**：补零必须让字典序 == 数值序。
    /// 这条一旦被破坏，追问会以「先回答后问的那句」的顺序送达模型 ——
    /// 不报错、不崩溃，只是答案莫名其妙，因此必须由测试钉死。
    #[test]
    fn followup_key_orders_lexicographically_by_seq() {
        let mid = "mid_1";
        let mut keys: Vec<String> = [2i64, 10, 9, 100, 1]
            .iter()
            .map(|s| chat_followup_kv_key(mid, *s))
            .collect();
        keys.sort();
        let order: Vec<i64> = keys
            .iter()
            .map(|k| k.rsplit('.').next().unwrap().parse::<i64>().unwrap())
            .collect();
        assert_eq!(
            order,
            vec![1, 2, 9, 10, 100],
            "字典序必须等于数值序: {keys:?}"
        );
    }

    /// 前缀扫描的边界：`<mid>.` 之下必须能命中本轮全部追问，
    /// 且**不能**命中 `chat_stream.` / `chat_lock.` 那批同族键。
    #[test]
    fn followup_prefix_is_scannable_and_namespaced() {
        let mid = "mid_xyz";
        let prefix = chat_followup_kv_prefix(mid);
        assert!(chat_followup_kv_key(mid, 0).starts_with(&prefix));
        assert_eq!(prefix, "chat_followup.mid_xyz.");

        // 同族前缀互不命中（改前缀常量时重新核对这一条）。
        for other in ["chat_stream.", "chat_lock.", "chat_task_map.", "chat_del."] {
            assert!(
                !prefix.starts_with(other),
                "追问前缀不得与 {other} 互为前缀"
            );
        }
    }

    /// ⚠️ **跨 id 不串**：另一个 mid 的追问键不得被本轮的 list 命中，
    /// 否则用户 A 的追问会被投进用户 B 的对话。
    #[test]
    fn followup_prefix_does_not_cross_message_ids() {
        let mine = chat_followup_kv_prefix("mid_aaa");
        let theirs = chat_followup_kv_key("mid_bbb", 0);
        assert!(
            !theirs.starts_with(&mine),
            "mid_aaa 的前缀不得命中 mid_bbb 的键"
        );
        // 反向也要成立：id 是变长的，前缀匹配不能只比开头几个字符。
        assert!(!chat_followup_kv_key("mid_aa", 0).starts_with(&mine));
    }
}
