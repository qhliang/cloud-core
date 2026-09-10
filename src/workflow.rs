//! cloud-manager ↔ workflow-manager 协议类型。
//!
//! workflow-manager 是被 `custom:task-queue` 消费的原生 worker：cloud-manager
//! 作为 producer 把请求序列化成 task payload，workflow-manager 把执行结果
//! 序列化成 task output 回传。心跳 `info` 字段同样使用本模块的类型。
//!
//! 一次 workflow 执行可能经历多次投递：首次 `start`，之后每遇到
//! `acts.core.irq` 挂起点就由 client 再发一次 `continue`。两条路径共用
//! `exec_id` 作幂等键。

use serde::{Deserialize, Serialize};

/// 下发给 workflow-manager 的请求，按 `type` 区分启动与续跑。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkflowRequest {
    /// 启动一条新的 workflow 执行。
    Start(WorkflowStartRequest),
    /// 让挂在 `nid` 上的 workflow 继续执行。
    Continue(WorkflowContinueRequest),
}

impl WorkflowRequest {
    /// 幂等键：start 与它引发的所有 continue 共享同一个键。
    pub fn exec_id(&self) -> &str {
        match self {
            Self::Start(req) => &req.exec_id,
            Self::Continue(req) => &req.exec_id,
        }
    }
}

/// 启动一条 workflow。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct WorkflowStartRequest {
    /// 业务执行 id，兼作幂等键与 acts process id。
    pub exec_id: String,
    /// 触发该执行的自动化规则 id；用于隔离 workflow model，避免多规则互相覆盖。
    pub rule_id: String,
    /// 已转换为 acts 定义的 workflow（JSON 或 YAML 文本）。
    pub workflow_def: String,
    /// 注入流程的初始变量。
    pub vars: Vec<VarPair>,
    /// 单次执行的超时秒数；不传则由 worker 侧配置兜底。
    pub timeout_secs: u64,
}

/// 让挂起的 workflow 继续。
///
/// 副本无关：任意副本收到都能接管，前提是 workflow-manager 使用共享 store。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct WorkflowContinueRequest {
    /// 幂等键，与对应的 [`WorkflowStartRequest::exec_id`] 相同。
    pub exec_id: String,
    /// acts process id，由首次 start 回传。
    pub pid: String,
    /// 挂起 act 的 task id；`executor().act().complete(pid, tid, vars)` 的定位键。
    pub tid: String,
    /// 挂起 act 的 node id，仅用于观测与校验。
    pub nid: String,
    /// 该 act 的产出，回灌为流程变量。
    pub outputs: Vec<VarPair>,
    /// 非空表示该挂起点**失败**：worker 应使该 act 失败
    /// （`ActExecutor::fail`）而非完成它。由 cloud-manager 在智能体任务失败 /
    /// 超时 / 构建请求失败时置位；为空则正常 `complete` 续跑。
    pub error: Option<String>,
}

/// 键值对形式的流程变量。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct VarPair {
    pub key: String,
    pub value: String,
}

impl VarPair {
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// workflow-manager 回传给 cloud-manager 的执行结果信封。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct WorkflowResultEnvelope {
    pub exec_id: String,
    /// acts process id，供后续 continue 使用。
    pub pid: String,
    pub status: WorkflowStatus,
    /// 挂起点的 task id；仅 `status = Suspended` 时非空，continue 时原样回传。
    pub tid: Option<String>,
    /// 挂起点的 node id；仅 `status = Suspended` 时非空，仅用于观测。
    pub nid: Option<String>,
    /// 挂起 act 的输入，供 client 决定派发什么工作。
    pub inputs: Vec<VarPair>,
    /// 终态流程变量；仅 `status = Succeeded` 时有意义。
    pub outputs: Vec<VarPair>,
    /// 失败原因；仅 `status = Failed` 时非空。
    pub error: Option<String>,
}

/// 一次执行在本次投递结束时所处的状态。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStatus {
    /// 停在 irq 挂起点，等待 client 再次投递 continue。
    #[default]
    Suspended,
    Succeeded,
    Failed,
}

impl WorkflowStatus {
    /// 是否已无需继续推进。
    pub fn is_terminal(self) -> bool {
        !matches!(self, Self::Suspended)
    }
}

/// 心跳 `info` 载荷，用于执行过程的可观测性。
///
/// 挂起点的 `nid` 以 `WorkflowResultEnvelope` 为准（随 task output 一起回传），
/// 心跳仅作进度观察，不参与控制流。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct WorkflowProgress {
    pub exec_id: String,
    pub pid: String,
    pub phase: WorkflowPhase,
    /// 当前 act 的 node id。
    pub node: Option<String>,
}

/// 执行阶段。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowPhase {
    /// 已开始推进。
    #[default]
    Running,
    /// 停在 irq 挂起点。
    Suspended,
    /// 正常结束。
    Completed,
    /// 出错结束。
    Failed,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_request_round_trips_with_type_tag() {
        let req = WorkflowRequest::Start(WorkflowStartRequest {
            exec_id: "exec-1".to_string(),
            rule_id: "rule-1".to_string(),
            workflow_def: "{\"id\":\"automation-rule-1\"}".to_string(),
            vars: vec![VarPair::new("a", "1")],
            timeout_secs: 60,
        });
        let raw = serde_json::to_string(&req).expect("serialize");
        assert!(raw.contains("\"type\":\"start\""), "unexpected: {raw}");
        let back: WorkflowRequest = serde_json::from_str(&raw).expect("deserialize");
        assert_eq!(back.exec_id(), "exec-1");
        assert!(matches!(back, WorkflowRequest::Start(_)));
    }

    #[test]
    fn continue_request_round_trips_with_type_tag() {
        let req = WorkflowRequest::Continue(WorkflowContinueRequest {
            exec_id: "exec-1".to_string(),
            pid: "exec-1".to_string(),
            tid: "task-1".to_string(),
            nid: "step1".to_string(),
            outputs: vec![VarPair::new("result", "ok")],
            error: Some("agent task failed".to_string()),
        });
        let raw = serde_json::to_string(&req).expect("serialize");
        assert!(raw.contains("\"type\":\"continue\""), "unexpected: {raw}");
        assert!(
            raw.contains("\"error\":\"agent task failed\""),
            "unexpected: {raw}"
        );
        let back: WorkflowRequest = serde_json::from_str(&raw).expect("deserialize");
        assert_eq!(back.exec_id(), "exec-1");
    }

    /// 旧版 producer 不带 `error` 字段时应默认 None（向后兼容）。
    #[test]
    fn continue_without_error_defaults_to_none() {
        let req: WorkflowContinueRequest = serde_json::from_str(
            r#"{"exec_id":"e1","pid":"p1","tid":"t1","nid":"n1","outputs":[]}"#,
        )
        .expect("parse");
        assert!(req.error.is_none());
    }

    #[test]
    fn partial_payload_falls_back_to_defaults() {
        let req: WorkflowStartRequest = serde_json::from_str(r#"{"exec_id":"e1"}"#).expect("parse");
        assert_eq!(req.exec_id, "e1");
        assert_eq!(req.rule_id, "");
        assert_eq!(req.timeout_secs, 0);
        assert!(req.vars.is_empty());
    }

    #[test]
    fn unknown_type_is_rejected() {
        assert!(serde_json::from_str::<WorkflowRequest>(r#"{"type":"pause"}"#).is_err());
    }

    #[test]
    fn status_and_phase_strings_are_stable() {
        assert_eq!(
            serde_json::to_string(&WorkflowStatus::Suspended).expect("ser"),
            "\"suspended\""
        );
        assert_eq!(
            serde_json::to_string(&WorkflowPhase::Running).expect("ser"),
            "\"running\""
        );
        assert!(!WorkflowStatus::Suspended.is_terminal());
        assert!(WorkflowStatus::Succeeded.is_terminal());
        assert!(WorkflowStatus::Failed.is_terminal());
    }

    #[test]
    fn envelope_defaults_to_suspended() {
        let envelope = WorkflowResultEnvelope::default();
        assert_eq!(envelope.status, WorkflowStatus::Suspended);
        assert_eq!(envelope.nid, None);
    }

    #[test]
    fn var_pair_helpers_build_values() {
        let pair = VarPair::new("k", "v");
        assert_eq!(
            pair,
            VarPair {
                key: "k".to_string(),
                value: "v".to_string()
            }
        );
    }
}
