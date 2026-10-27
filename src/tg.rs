//! tokengateway ↔ cloud-manager 协议类型。
//!
//! 覆盖配置同步下发（`GET /api/tg/sync`）与调用记录上报（`POST /api/tg/calls`）。

use serde::{Deserialize, Serialize};

fn default_true() -> bool {
    true
}

// ── Tiers ─────────────────────────────────────────────────
/// Provider tier used for routing order and dashboard grouping。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    Local,
    Free,
    Paid,
}

impl std::fmt::Display for Tier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Tier::Local => write!(f, "local"),
            Tier::Free => write!(f, "free"),
            Tier::Paid => write!(f, "paid"),
        }
    }
}

impl PartialEq<&str> for Tier {
    fn eq(&self, other: &&str) -> bool {
        matches!(
            (self, *other),
            (Tier::Local, "local") | (Tier::Free, "free") | (Tier::Paid, "paid")
        )
    }
}

// ── Providers ─────────────────────────────────────────────
/// How a provider authenticates upstream。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProviderAuth {
    #[serde(alias = "apikey")]
    ApiKey {
        key: String,
    },
    None,
}

/// 服务商下的一个**具体模型**，可带若干标签。
///
/// ⚠️ 标签是**自由字符串**（用户自定义），由管理端做去重/trim；
/// 服务商组可以「按 tag」动态关联成员（见 [`GroupMembers`]）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderModel {
    pub name: String,
    #[serde(default)]
    pub tags: Vec<String>,
}

/// `Provider.models` 的兼容反序列化：同时接受
/// - 新形态 `[{"name":"gpt-4","tags":["cheap"]}]`
/// - 旧形态 `["gpt-4"]`（label 为空）
///
/// ⚠️ **兼容层是为了部署顺序**：tokengateway 与 cloud-manager 分别发版，
/// 过渡期一方可能还在读旧形状的同步清单。等两侧都升级后可以删掉。
fn models_compat<'de, D>(d: D) -> Result<Vec<ProviderModel>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Raw {
        New(Vec<ProviderModel>),
        Legacy(Vec<String>),
    }
    Ok(match Raw::deserialize(d)? {
        Raw::New(v) => v,
        Raw::Legacy(v) => v
            .into_iter()
            .map(|name| ProviderModel {
                name,
                tags: Vec::new(),
            })
            .collect(),
    })
}

impl ProviderModel {
    /// 从 `tg_providers.models` 列的 JSON 文本解析模型清单。
    ///
    /// ⚠️ **兼容旧形状** `["gpt-4"]`（无 tag）：管理端读到旧数据不会报错，
    /// 用户下次保存时会写成新形状。解析失败 / 空串 ⇒ 空清单（候选数据，不报错）。
    pub fn parse_list(raw: &str) -> Vec<ProviderModel> {
        let raw = raw.trim();
        if raw.is_empty() {
            return Vec::new();
        }
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            New(Vec<ProviderModel>),
            Legacy(Vec<String>),
        }
        match serde_json::from_str::<Raw>(raw) {
            Ok(Raw::New(v)) => v,
            Ok(Raw::Legacy(v)) => v
                .into_iter()
                .map(|name| ProviderModel {
                    name,
                    tags: Vec::new(),
                })
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    /// 该模型是否带任一指定 tag（按 tag 关联服务商组时用）。
    pub fn has_any_tag(&self, tags: &[String]) -> bool {
        self.tags.iter().any(|t| tags.contains(t))
    }
}

/// A single upstream LLM provider。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Provider {
    pub name: String,
    pub base_url: String,
    #[serde(rename = "type")]
    pub provider_type: String,
    pub auth: ProviderAuth,
    pub tier: Tier,
    /// 该服务商可用的**具体模型**（含标签）。
    ///
    /// ⚠️ 路由与「按 tag 关联的服务商组」都从这里取模型 ——
    /// 它是**选路的唯一模型来源**，不允许路由凭空写一个模型名。
    #[serde(default, deserialize_with = "models_compat")]
    pub models: Vec<ProviderModel>,
    #[serde(default)]
    pub proxy: Option<String>,
    /// 可选：覆盖发往上游的 `user-agent`。`None` / 空 ⇒ **透传调用方**的值。
    ///
    /// ⚠️ 与上面那个恒为 `None` 的 `proxy` 不同，本字段是活配置：
    ///    cloud-manager 写入、tokengateway 在 `upstream_headers()` 里消费。
    ///    未配置时保持既有行为（调用方 UA 原样透传），不补网关默认值。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_agent: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

// ── Routes ────────────────────────────────────────────────
/// How a route pattern matches an incoming model name。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RouteMatchType {
    Prefix,
    #[default]
    #[serde(alias = "Exact")]
    Exact,
}

impl std::fmt::Display for RouteMatchType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RouteMatchType::Prefix => write!(f, "prefix"),
            RouteMatchType::Exact => write!(f, "exact"),
        }
    }
}

/// A single pattern+match_type pair within a multi-rule Route。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternRule {
    pub pattern: String,
    #[serde(default)]
    pub match_type: RouteMatchType,
}

/// 路由生效时间段（可选）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ActiveWindow {
    DateRange {
        from: String,
        to: String,
    },
    WeeklyRange {
        weekdays: Vec<u8>,
        from: String,
        to: String,
    },
}

/// 一个具体的上游端点：**服务商 + 模型**。
///
/// ⚠️ 模型必须是该服务商 `models` 里配置过的名字（保存期校验）——
/// 这是「路由不再允许显式模型覆盖」的落点：上游收到的模型恒由目标决定。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelRef {
    pub provider: String,
    pub model: String,
}

/// 路由的目标：一个具体端点，或一个服务商组。
///
/// ⚠️ 用**内部标签**（`kind`）而不是 `Option<provider>` + `Option<group>`：
/// 让「二者必居其一」由类型保证，序列化后也自解释。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RouteTarget {
    /// 直连某个服务商的某个模型。
    Provider { provider: String, model: String },
    /// 交给服务商组选成员（成员各自的模型由组决定）。
    Group { group: String },
}

impl RouteTarget {
    /// 目标是「空的」——旧数据缺字段时的兜底，运行时跳过并告警。
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Provider { provider, model } => {
                provider.trim().is_empty() || model.trim().is_empty()
            }
            Self::Group { group } => group.trim().is_empty(),
        }
    }
}

/// An explicit model → target route。
#[derive(Debug, Clone, Serialize)]
pub struct Route {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub note: Option<String>,
    #[serde(default)]
    pub rules: Vec<PatternRule>,
    #[serde(default)]
    pub users: Vec<String>,
    /// 主目标（服务商+模型 或 服务商组）。
    pub target: RouteTarget,
    /// 备用目标（按顺序尝试）。空模型/空组的目标会被运行时跳过。
    #[serde(default)]
    pub fallbacks: Vec<RouteTarget>,
    #[serde(default)]
    pub supports_thinking: bool,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub active_window: Option<ActiveWindow>,
}

impl<'de> serde::Deserialize<'de> for Route {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        struct RouteRaw {
            #[serde(default)]
            id: String,
            #[serde(default)]
            name: String,
            #[serde(default)]
            note: Option<String>,
            #[serde(default)]
            pattern: Option<String>,
            #[serde(default)]
            match_type: Option<RouteMatchType>,
            #[serde(default)]
            rules: Vec<PatternRule>,
            #[serde(default)]
            users: Vec<String>,
            // 新形态
            #[serde(default)]
            target: Option<RouteTarget>,
            #[serde(default)]
            fallbacks: Vec<RouteTarget>,
            // ── 旧形态（兼容过渡期，见模块文档）──
            #[serde(default)]
            provider: Option<String>,
            #[serde(default)]
            fallback_providers: Vec<String>,
            #[serde(default)]
            rewrite_model: Option<String>,
            #[serde(default)]
            fallback_rewrite_models: Vec<Option<String>>,
            #[serde(default)]
            supports_thinking: bool,
            #[serde(default = "default_true")]
            enabled: bool,
            #[serde(default)]
            active_window: Option<ActiveWindow>,
        }
        let raw = RouteRaw::deserialize(d)?;
        let rules = if !raw.rules.is_empty() {
            raw.rules
        } else if let Some(p) = raw.pattern {
            let mt = raw.match_type.unwrap_or(RouteMatchType::Exact);
            vec![PatternRule {
                pattern: p,
                match_type: mt,
            }]
        } else {
            vec![]
        };
        // ⚠️ **兼容旧清单**：旧形态是「provider + 可选 rewrite_model」。
        //    有 rewrite_model ⇒ 它就是目标模型；没有 ⇒ 目标模型留空
        //    （运行时跳过该目标并告警，不静默改发客户端请求的模型）。
        let target = match raw.target {
            Some(t) => t,
            None => RouteTarget::Provider {
                provider: raw.provider.unwrap_or_default(),
                model: raw.rewrite_model.unwrap_or_default(),
            },
        };
        let fallbacks = if !raw.fallbacks.is_empty() {
            raw.fallbacks
        } else {
            raw.fallback_providers
                .iter()
                .enumerate()
                .map(|(i, p)| RouteTarget::Provider {
                    provider: p.clone(),
                    model: raw
                        .fallback_rewrite_models
                        .get(i)
                        .and_then(|m| m.clone())
                        .unwrap_or_default(),
                })
                .collect()
        };
        Ok(Route {
            id: raw.id,
            name: raw.name,
            note: raw.note,
            rules,
            users: raw.users,
            target,
            fallbacks,
            supports_thinking: raw.supports_thinking,
            enabled: raw.enabled,
            active_window: raw.active_window,
        })
    }
}

// ── Groups ────────────────────────────────────────────────
/// How a [`ProviderGroup`] picks a member for each call。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupStrategy {
    #[default]
    RoundRobin,
    Sequential,
    Random,
    LoadBalance,
    LeastLatency,
}

impl std::fmt::Display for GroupStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GroupStrategy::RoundRobin => write!(f, "round_robin"),
            GroupStrategy::Sequential => write!(f, "sequential"),
            GroupStrategy::Random => write!(f, "random"),
            GroupStrategy::LoadBalance => write!(f, "load_balance"),
            GroupStrategy::LeastLatency => write!(f, "least_latency"),
        }
    }
}

/// 服务商组的成员来源。**两种模式互斥**（由类型保证，不是靠校验）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GroupMembers {
    /// 显式列出 `(服务商, 模型)`。
    ProviderModels { items: Vec<ModelRef> },
    /// 按标签动态关联：成员 = 所有「模型带任一 tag」的 `(服务商, 模型)`。
    ///
    /// ⚠️ 动态成员意味着「给某个模型加 tag 后自动进组」——这正是 tag 的用途；
    /// 代价是组内成员会随服务商配置变化，管理端要展示「当前匹配 N 个」。
    Tags { tags: Vec<String> },
}

/// A virtual provider that aggregates one or more real providers。
#[derive(Debug, Clone, Serialize)]
pub struct ProviderGroup {
    pub name: String,
    /// 成员来源（显式列表 或 按 tag）。
    pub members: GroupMembers,
    #[serde(default)]
    pub strategy: GroupStrategy,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl<'de> serde::Deserialize<'de> for ProviderGroup {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        struct GroupRaw {
            #[serde(default)]
            name: String,
            #[serde(default)]
            members: Option<GroupMembers>,
            // 旧形态：只有服务商名（没有模型）
            #[serde(default)]
            providers: Vec<String>,
            #[serde(default)]
            strategy: GroupStrategy,
            #[serde(default = "default_true")]
            enabled: bool,
        }
        let raw = GroupRaw::deserialize(d)?;
        // ⚠️ 旧形态的服务商没有模型名 ⇒ 展开成空模型，运行时跳过并告警
        //    （用户需要手工重新配置 —— 不做数据迁移是既定决策）。
        let members = raw.members.unwrap_or_else(|| GroupMembers::ProviderModels {
            items: raw
                .providers
                .into_iter()
                .map(|provider| ModelRef {
                    provider,
                    model: String::new(),
                })
                .collect(),
        });
        Ok(ProviderGroup {
            name: raw.name,
            members,
            strategy: raw.strategy,
            enabled: raw.enabled,
        })
    }
}

// ── Auth ──────────────────────────────────────────────────
/// 认证密钥同步条目。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AuthKeyRow {
    pub id: String,
    pub key_hash: String,
    pub user_id: String,
}

/// 认证用户同步条目。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AuthUserRow {
    pub id: String,
    pub name: String,
    pub quota_limit: i64,
    pub window_seconds: i64,
}

/// 配置同步响应（cloud-manager `GET /api/tg/sync` → tokengateway）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TgSyncManifest {
    #[serde(default)]
    pub updated_at: i64,
    #[serde(default)]
    pub providers: Vec<Provider>,
    #[serde(default)]
    pub routes: Vec<Route>,
    #[serde(default)]
    pub groups: Vec<ProviderGroup>,
    #[serde(default)]
    pub auth_keys: Vec<AuthKeyRow>,
    #[serde(default)]
    pub auth_users: Vec<AuthUserRow>,
}

// ── Call records ──────────────────────────────────────────
/// One recorded call, produced by the gateway after each request。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallRecord {
    pub request_id: String,
    #[serde(default = "default_attempt_role")]
    pub attempt_role: String,
    #[serde(default)]
    pub parent_request_id: String,
    pub protocol: String,
    pub model: String,
    pub provider: String,
    #[serde(default)]
    pub route: String,
    #[serde(default)]
    pub auth_user: String,
    #[serde(default)]
    pub auth_restricted: bool,
    #[serde(default)]
    pub forward_strategy: String,
    #[serde(default)]
    pub forward_member: String,
    #[serde(default)]
    pub rewrite_model: String,
    pub status: u16,
    #[serde(default)]
    pub error_message: Option<String>,
    #[serde(default)]
    pub request_body: Option<String>,
    #[serde(default)]
    pub response_body: Option<String>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    #[serde(default)]
    pub prompt_cache_hit_tokens: u64,
    #[serde(default)]
    pub prompt_cache_miss_tokens: u64,
    pub latency_ms: u64,
    pub cost_usd: f64,
    #[serde(default)]
    pub timestamp: u64,
}

fn default_attempt_role() -> String {
    "primary".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `user_agent` 是**新增的可选字段**：旧清单（完全没有这个键）必须照样能读，
    /// 语义是 `None`（透传调用方 UA）；写了值则原样带出。
    ///
    /// ⚠️ 这条是部署顺序的安全带：cloud-manager 与 tokengateway 分开发版，
    ///    过渡期两边读到的清单可能是对方旧版本写的。
    #[test]
    fn provider_user_agent_is_optional_and_round_trips() {
        let legacy: Provider = serde_json::from_value(serde_json::json!({
            "name": "p1", "base_url": "http://x/v1", "type": "openai",
            "auth": {"kind": "none"}, "tier": "paid", "models": []
        }))
        .expect("旧清单（无 user_agent）必须能解析");
        assert_eq!(legacy.user_agent, None);

        let with_ua: Provider = serde_json::from_value(serde_json::json!({
            "name": "p1", "base_url": "http://x/v1", "type": "openai",
            "auth": {"kind": "none"}, "tier": "paid", "models": [],
            "user_agent": "curl/8.5.0"
        }))
        .expect("带 user_agent 的清单必须能解析");
        assert_eq!(with_ua.user_agent.as_deref(), Some("curl/8.5.0"));

        // 未配置时不往清单里写 `"user_agent": null`（旧网关也不认识这个键）。
        let json = serde_json::to_value(&legacy).unwrap();
        assert!(json.get("user_agent").is_none());
    }

    /// ⚠️ **兼容层是部署顺序的安全带**：tokengateway 与 cloud-manager 分别发版，
    /// 过渡期可能读到旧形状的同步清单。旧形状必须能解析，且语义不能跑偏。
    #[test]
    fn provider_models_accepts_legacy_string_array() {
        let p: Provider = serde_json::from_value(serde_json::json!({
            "name": "p1", "base_url": "http://x/v1", "type": "openai",
            "auth": {"kind": "none"}, "tier": "paid",
            "models": ["gpt-4", "gpt-4o"]
        }))
        .unwrap();
        assert_eq!(p.models.len(), 2);
        assert_eq!(p.models[0].name, "gpt-4");
        assert!(p.models[0].tags.is_empty(), "旧形态没有 tag");
    }

    #[test]
    fn provider_models_accepts_new_shape_with_tags() {
        let p: Provider = serde_json::from_value(serde_json::json!({
            "name": "p1", "base_url": "http://x/v1", "type": "openai",
            "auth": {"kind": "none"}, "tier": "paid",
            "models": [{"name": "gpt-4", "tags": ["cheap", "fast"]}]
        }))
        .unwrap();
        assert_eq!(
            p.models[0].tags,
            vec!["cheap".to_string(), "fast".to_string()]
        );
    }

    /// 旧路由（provider + rewrite_model）⇒ 目标是 (服务商, rewrite_model)。
    /// 没有 rewrite_model ⇒ 模型留空（运行时跳过，**不**静默透传客户端模型）。
    #[test]
    fn route_accepts_legacy_provider_and_rewrite_model() {
        let r: Route = serde_json::from_value(serde_json::json!({
            "id": "r1", "provider": "openrouter-free", "rewrite_model": "openrouter/free",
            "rules": [{"pattern": "agent-manager-model", "match_type": "exact"}]
        }))
        .unwrap();
        assert_eq!(
            r.target,
            RouteTarget::Provider {
                provider: "openrouter-free".into(),
                model: "openrouter/free".into()
            }
        );
        assert!(r.fallbacks.is_empty());

        let no_rewrite: Route = serde_json::from_value(serde_json::json!({
            "id": "r2", "provider": "p1"
        }))
        .unwrap();
        assert_eq!(
            no_rewrite.target,
            RouteTarget::Provider {
                provider: "p1".into(),
                model: String::new()
            }
        );
        assert!(no_rewrite.target.is_empty(), "空模型的目标应被视为无效");
    }

    /// 旧 fallback（服务商列表 + 逐项 rewrite）⇒ `Vec<RouteTarget>`。
    #[test]
    fn route_accepts_legacy_fallbacks() {
        let r: Route = serde_json::from_value(serde_json::json!({
            "id": "r1", "provider": "p1", "rewrite_model": "m1",
            "fallback_providers": ["p2", "p3"],
            "fallback_rewrite_models": ["m2", ""]
        }))
        .unwrap();
        assert_eq!(
            r.fallbacks,
            vec![
                RouteTarget::Provider {
                    provider: "p2".into(),
                    model: "m2".into()
                },
                RouteTarget::Provider {
                    provider: "p3".into(),
                    model: String::new()
                },
            ]
        );
    }

    /// 新路由目标的原生形态（含 fallback 指向组）。
    #[test]
    fn route_accepts_native_targets() {
        let r: Route = serde_json::from_value(serde_json::json!({
            "id": "r1",
            "target": {"kind": "group", "group": "fast"},
            "fallbacks": [{"kind": "provider", "provider": "p1", "model": "m1"}]
        }))
        .unwrap();
        assert_eq!(
            r.target,
            RouteTarget::Group {
                group: "fast".into()
            }
        );
        assert_eq!(
            r.fallbacks,
            vec![RouteTarget::Provider {
                provider: "p1".into(),
                model: "m1".into()
            }]
        );
    }

    /// 旧组（providers: ["p"]）⇒ 显式成员，模型为空（需人工重新配置）。
    #[test]
    fn group_accepts_legacy_provider_list() {
        let g: ProviderGroup = serde_json::from_value(serde_json::json!({
            "name": "g1", "providers": ["deepseek"], "strategy": "load_balance"
        }))
        .unwrap();
        match g.members {
            GroupMembers::ProviderModels { items } => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].provider, "deepseek");
                assert!(items[0].model.is_empty(), "旧组成员没有模型");
            }
            _ => panic!("旧形态应映射为显式成员"),
        }
    }

    /// 组的两种模式必须能正确序列化/反序列化（tagged union）。
    #[test]
    fn group_members_tagged_union_roundtrip() {
        let tags = GroupMembers::Tags {
            tags: vec!["cheap".into()],
        };
        let json = serde_json::to_value(&tags).unwrap();
        assert_eq!(json["kind"], "tags");
        assert_eq!(serde_json::from_value::<GroupMembers>(json).unwrap(), tags);

        let items = GroupMembers::ProviderModels {
            items: vec![ModelRef {
                provider: "p".into(),
                model: "m".into(),
            }],
        };
        let json = serde_json::to_value(&items).unwrap();
        assert_eq!(json["kind"], "provider_models");
        assert_eq!(serde_json::from_value::<GroupMembers>(json).unwrap(), items);
    }
}
