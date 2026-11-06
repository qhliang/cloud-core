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

/// 一条「新增请求头」配置。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeaderPair {
    /// 请求头名（写进上游时的大小写以此为准）。
    pub name: String,
    /// 值：**字面量**，或混入 `{头名:起-止}` 引用调用方**原始请求头**（大小写无关）。
    ///
    /// 例：
    /// - `xxx_{cookie:2-}` —— `cookie` 头从第 2 个字符到末尾；
    /// - `{authorization}` —— 整个 `authorization` 头；
    /// - `Bearer {x-token:0-8}` —— 取前 8 个字符；
    /// - `static-value` —— 纯字面量。
    pub value: String,
}

/// 发往上游前的请求头增删配置。
///
/// ## 执行顺序：**先新增、后删除**
///
/// 这是刻意的 —— 否则「引用一个马上要被删掉的头」会取不到值：
/// 新增阶段先按引用把值**拷贝**出来，删除阶段再把这个原头摘掉，
/// 两条规则就能同时生效。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeaderConfig {
    /// 新增 / 覆盖（同名头会被替换，保留一份）。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub add: Vec<HeaderPair>,
    /// 删除（**大小写无关**，只写名字）。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub remove: Vec<String>,
}

impl HeaderConfig {
    pub fn is_empty(&self) -> bool {
        self.add.is_empty() && self.remove.is_empty()
    }

    /// 把配置应用到一组「待发往上游」的请求头上。
    ///
    /// `original` 是调用方**原始**请求头（协议转换前的那份），供
    /// `${header.<名>}` 取值；它的值不会被本次增删影响。
    ///
    /// ⚠️ 只做增删，不负责合法性 —— 名字/值的校验在写入端（cloud-manager）
    /// 完成，放到这里会让「测试探测」这类没有原始头的场景也被拒。
    pub fn apply(&self, headers: &mut Vec<(String, String)>, original: &[(String, String)]) {
        for pair in &self.add {
            let name = pair.name.trim();
            if name.is_empty() {
                continue;
            }
            let value = resolve_header_refs(&pair.value, original);
            // 同名头**覆盖**而不是追加：HTTP 单值头重复会让上游判非法。
            headers.retain(|(k, _)| !k.eq_ignore_ascii_case(name));
            headers.push((name.to_string(), value));
        }
        if self.remove.iter().any(|r| !r.trim().is_empty()) {
            headers.retain(|(k, _)| {
                !self
                    .remove
                    .iter()
                    .any(|r| !r.trim().is_empty() && r.trim().eq_ignore_ascii_case(k))
            });
        }
    }
}

/// 把 `value` 里的 `{头名:起-止}` 替换成 `original` 中同名头的**切片**。
///
/// ## 语法
///
/// | 写法 | 含义 |
/// |---|---|
/// | `{cookie}` | 整个 `cookie` 头 |
/// | `{cookie:2-}` | 从第 2 个字符到末尾 |
/// | `{cookie:2-5}` | `[2, 5)`（左闭右开） |
/// | `{cookie:-5}` | 前 5 个字符（`[0, 5)`） |
/// | `{cookie:3}` | 只取第 3 个字符 |
/// | `xxx_{cookie:2-}` | 与字面量混用：得到 `xxx_` + 切片 |
///
/// ## 规则
///
/// - 头名**大小写无关**；
/// - 下标按**字符**（不是字节）算，越界自动收敛到合法区间（不 panic）；
/// - 头不存在 ⇒ 空串（不能把 `{cookie:2-}` 这种占位符原样发给上游）；
/// - `{...}` 里不像引用时（空、名字含非法字符、括号没闭合）**原样保留**，
///   这样 `{"a":1}` 这类字面量不会被误吃。
pub fn resolve_header_refs(value: &str, original: &[(String, String)]) -> String {
    let mut out = String::with_capacity(value.len());
    let mut rest = value;
    while let Some(start) = rest.find('{') {
        out.push_str(&rest[..start]);
        let after = &rest[start + 1..];
        let Some(end) = after.find('}') else {
            // 没有闭合 ⇒ 剩余部分原样保留。
            out.push_str(&rest[start..]);
            return out;
        };
        let expr = &after[..end];
        match resolve_one(expr, original) {
            Some(v) => out.push_str(&v),
            None => {
                out.push('{');
                out.push_str(expr);
                out.push('}');
            }
        }
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    out
}

/// 合法的 HTTP 请求头名：RFC 7230 token，长度 ≤ 64。
///
/// 两处共用，避免「管理端放行、运行端拒绝」的漂移：
/// - cloud-manager 保存智能体/服务商配置时校验；
/// - agent-manager 写 `models.json` 的 `headers` 前再做一次兜底。
pub fn is_valid_header_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "!#$%&'*+-.^_`|~".contains(c))
}

/// 解析单个 `头名[:起-止]`；`None` = 不像引用，调用方原样保留。
fn resolve_one(expr: &str, original: &[(String, String)]) -> Option<String> {
    let (name, range) = match expr.split_once(':') {
        Some((n, r)) => (n.trim(), Some(r.trim())),
        None => (expr.trim(), None),
    };
    // 头名必须是合法 token —— 顺便把 `{"a":1}` 这种字面量排除掉。
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "!#$%&'*+-.^_`|~".contains(c))
    {
        return None;
    }
    // ⚠️ 先把区间语法解析干净，**再**去查头：否则 `{cookie:b}` 这种语法错误
    //    会因为「头不存在」被解析成空串，而不是原样保留。
    let (start, end) = match range {
        None => (0, None),
        // `{h:}` 等价于整值（用户清空了区间输入框）。
        Some("") => (0, None),
        Some(r) => match r.split_once('-') {
            Some((a, b)) => {
                let start = if a.trim().is_empty() {
                    0
                } else {
                    a.trim().parse::<usize>().ok()?
                };
                let end = if b.trim().is_empty() {
                    None
                } else {
                    Some(b.trim().parse::<usize>().ok()?)
                };
                (start, end)
            }
            // 没有 `-`：当成单字符下标。
            None => {
                let i = r.parse::<usize>().ok()?;
                (i, Some(i.saturating_add(1)))
            }
        },
    };
    let Some((_, raw)) = original.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)) else {
        // 头不存在：引用结果为空串。
        return Some(String::new());
    };
    let chars: Vec<char> = raw.chars().collect();
    let len = chars.len();
    let start = start.min(len);
    // 越界 / 起止颠倒都收敛成空区间，不 panic。
    let end = end.unwrap_or(len).min(len).max(start);
    Some(chars[start..end].iter().collect())
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
    /// 发往上游前的请求头增删（先新增、后删除）。
    ///
    /// ⚠️ 与 `user_agent` 不同，这里是**批量**规则：`add` 可以覆盖任意头
    /// （含 `authorization`），`remove` 按名字丢弃（大小写无关）。
    /// 留空 ⇔ 完全不干预，行为与升级前一致。
    #[serde(default, skip_serializing_if = "HeaderConfig::is_empty")]
    pub headers: HeaderConfig,
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

    /// 请求头配置的执行顺序：**先新增、后删除**。
    ///
    /// 这是需求里点名的行为：`add` 里引用了一个「同时被 `remove` 掉的原始头」，
    /// 也必须能取到值 —— 因为新增阶段先把它拷出来，删除阶段删的是**原头**。
    #[test]
    fn header_config_adds_before_removing() {
        let cfg = HeaderConfig {
            add: vec![HeaderPair {
                name: "x-copied".into(),
                value: "prefix_{X-Source:2-}".into(),
            }],
            remove: vec!["x-source".into()],
        };
        let original = vec![("X-Source".to_string(), "01value".to_string())];
        let mut headers = vec![
            ("x-source".to_string(), "01value".to_string()),
            ("accept".to_string(), "*/*".to_string()),
        ];
        cfg.apply(&mut headers, &original);
        assert_eq!(
            headers,
            vec![
                ("accept".to_string(), "*/*".to_string()),
                // 新增的头取到了切片（跳过前 2 个字符）……
                ("x-copied".to_string(), "prefix_value".to_string()),
                // ……而原头最后被删掉。
            ]
        );
    }

    /// 删除是**大小写无关**的，新增则覆盖同名头（只留一份）。
    #[test]
    fn header_config_is_case_insensitive() {
        let cfg = HeaderConfig {
            add: vec![HeaderPair {
                name: "Authorization".into(),
                value: "Bearer x".into(),
            }],
            remove: vec!["ACCEPT".into(), "x-gone".into()],
        };
        let mut headers = vec![
            ("authorization".to_string(), "Bearer old".to_string()),
            ("accept".to_string(), "*/*".to_string()),
            ("X-Gone".to_string(), "1".to_string()),
            ("x-keep".to_string(), "2".to_string()),
        ];
        cfg.apply(&mut headers, &[]);
        assert_eq!(
            headers,
            vec![
                ("x-keep".to_string(), "2".to_string()),
                ("Authorization".to_string(), "Bearer x".to_string()),
            ]
        );
    }

    /// 引用语法 `{头名:起-止}`：大小写无关、可混在字面量里、按字符切片。
    #[test]
    fn header_refs_slice_by_char_index() {
        let original = vec![
            ("cookie".to_string(), "session=abcdef".to_string()),
            ("user-agent".to_string(), "curl/8.5.0".to_string()),
        ];
        // 需求里的例子：从第 2 个字符到末尾。
        assert_eq!(
            resolve_header_refs("xxx_{cookie:2-}", &original),
            "xxx_ssion=abcdef"
        );
        // 整个头。
        assert_eq!(resolve_header_refs("{Cookie}", &original), "session=abcdef");
        // 左闭右开。
        assert_eq!(resolve_header_refs("{cookie:0-7}", &original), "session");
        // 只有止：相当于前缀。
        assert_eq!(resolve_header_refs("{cookie:-7}", &original), "session");
        // 单个下标。
        assert_eq!(resolve_header_refs("{cookie:1}", &original), "e");
        // 越界收敛（不 panic、不截断错位）。
        assert_eq!(resolve_header_refs("{cookie:100-}", &original), "");
        assert_eq!(resolve_header_refs("{cookie:5-2}", &original), "");
        assert_eq!(
            resolve_header_refs("{cookie:-100}", &original),
            "session=abcdef"
        );
    }

    /// 头名合法性：正常头名通过，空格/冒号/中文/超长拒绝（两端共用同一判据）。
    #[test]
    fn header_name_token_rule_is_shared() {
        for ok in [
            "x-session-affinity",
            "session_id",
            "Authorization",
            "a!#$%&'*+-.^_`|~",
        ] {
            assert!(is_valid_header_name(ok), "{ok} 应合法");
        }
        for bad in ["", "bad name", "x:y", "会话", &"a".repeat(65)] {
            assert!(!is_valid_header_name(bad), "{bad:?} 应拒绝");
        }
    }

    /// 不是引用的花括号必须**原样保留**（避免把 JSON 字面量吃掉），
    /// 引用不存在的头则解析成空串。
    #[test]
    fn header_refs_keep_literals_and_degrade_missing() {
        let original = vec![("x-token".to_string(), "abc".to_string())];
        assert_eq!(resolve_header_refs(r#"{"a":1}"#, &original), r#"{"a":1}"#);
        assert_eq!(resolve_header_refs("{cookie:b}", &original), "{cookie:b}");
        assert_eq!(resolve_header_refs("no-ref", &original), "no-ref");
        // 头不存在 ⇒ 空串（不能把占位符原样发给上游）。
        assert_eq!(resolve_header_refs("[{nope:2-}]", &original), "[]");
        // 没有闭合 `}` ⇒ 原样保留。
        assert_eq!(resolve_header_refs("a_{x-token", &original), "a_{x-token");
    }

    /// `headers` 与 `user_agent` 一样是**可选字段**：旧清单不写它时按空配置处理。
    #[test]
    fn provider_headers_are_optional() {
        let legacy: Provider = serde_json::from_value(serde_json::json!({
            "name": "p1", "base_url": "http://x/v1", "type": "openai",
            "auth": {"kind": "none"}, "tier": "paid", "models": []
        }))
        .expect("旧清单（无 headers）必须能解析");
        assert!(legacy.headers.is_empty());
        // 空配置序列化时整个键都省略（清单体积与旧版一致）。
        let v = serde_json::to_value(&legacy).unwrap();
        assert!(v.get("headers").is_none());
    }

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
