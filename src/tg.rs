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

/// A single upstream LLM provider。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Provider {
    pub name: String,
    pub base_url: String,
    #[serde(rename = "type")]
    pub provider_type: String,
    pub auth: ProviderAuth,
    pub tier: Tier,
    #[serde(default)]
    pub models: Vec<String>,
    #[serde(default)]
    pub proxy: Option<String>,
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

/// Serialize Vec<Option<String>> as Vec<String> (None -> "") so TOML can represent it。
fn serialize_fallback_rewrite_models<S: serde::Serializer>(
    val: &[Option<String>],
    ser: S,
) -> Result<S::Ok, S::Error> {
    let mapped: Vec<&str> = val.iter().map(|m| m.as_deref().unwrap_or("")).collect();
    mapped.serialize(ser)
}

/// An explicit model → provider route。
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
    pub provider: String,
    #[serde(default)]
    pub fallback_providers: Vec<String>,
    #[serde(default)]
    pub rewrite_model: Option<String>,
    #[serde(default, serialize_with = "serialize_fallback_rewrite_models")]
    pub fallback_rewrite_models: Vec<Option<String>>,
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
            provider: String,
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
        Ok(Route {
            id: raw.id,
            name: raw.name,
            note: raw.note,
            rules,
            users: raw.users,
            provider: raw.provider,
            fallback_providers: raw.fallback_providers,
            rewrite_model: raw.rewrite_model,
            fallback_rewrite_models: raw.fallback_rewrite_models,
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

/// A virtual provider that aggregates one or more real providers。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderGroup {
    pub name: String,
    pub providers: Vec<String>,
    #[serde(default)]
    pub strategy: GroupStrategy,
    #[serde(default = "default_true")]
    pub enabled: bool,
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
