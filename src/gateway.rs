//! `gateway-proxy` ↔ `cloud-manager` 的配置契约。
//!
//! # 为什么要有这个模块
//!
//! 这份 JSON 此前**两端各写一份**：控制面 `cloud-manager-core/src/gateway_proxy.rs`
//! 负责拼装与保存时校验，网关 `gateway-proxy/src/{config,routes}.rs` 负责解析与
//! 加载时校验。两边的规则一旦不同步，故障模式是**最难查的一种**：
//!
//! > 控制面「保存成功」→ 网关 30 秒后热加载失败 → 继续跑旧路由 →
//! > 界面上一切正常，没有任何 5xx。
//!
//! 所以类型与校验都放在这里，两端只保留各自的运行时类型（网关的 `Target`、
//! 控制面的 D1 行）。
//!
//! # feature 门控
//!
//! 整个模块在 `gateway` feature 后面：需要 `http` crate 做 URI / header 校验，
//! 而 `cloud-core` 的根包必须保持可在 `wasm32-wasip2` 编译（CI 的 `wasm-root`
//! job）。默认不启用，只有两个网关相关服务打开。

use std::collections::{BTreeMap, HashSet};

use serde::{Deserialize, Serialize};

/// 不允许由配置设置的请求头：它们由传输层/框架管理，覆盖会导致协议错乱。
pub const RESERVED_HEADERS: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "content-length",
];

/// 下发/拉取的完整路由文档。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireDocument {
    /// `cloud_proxy_route.updated_at_ms` 的单调版本号，供网关判断是否需要重载。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
    pub routes: Vec<WireRoute>,
}

/// 单条路由。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireRoute {
    pub token: String,
    /// 为空表示兜底路由（匹配所有路径）。
    #[serde(default)]
    pub prefix: String,
    pub upstreams: Vec<String>,
    /// 命中 prefix 后把路径前缀换成它；`Some("")` 表示去掉前缀。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rewrite: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers: Option<HeaderOps>,
}

/// 发往上游的请求头增删配置。
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HeaderOps {
    /// 追加（同名头会被保留，形成多值）。
    #[serde(default)]
    pub add: BTreeMap<String, String>,
    /// 覆盖（同名头只留这一个值）。
    #[serde(default)]
    pub set: BTreeMap<String, String>,
    /// 删除。
    #[serde(default)]
    pub remove: Vec<String>,
}

impl HeaderOps {
    pub fn is_empty(&self) -> bool {
        self.add.is_empty() && self.set.is_empty() && self.remove.is_empty()
    }
}

/// 一条路由的校验错误。
///
/// ⚠️ 带 `index` 与 `field`：控制面据此返回「第 3 条路由的 prefix 非法」，
/// 而不是一句笼统的「配置错误」——后者会让人去翻整份文档。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteError {
    /// 在 `WireDocument::routes` 里的下标。
    pub index: usize,
    /// 字段路径，如 `prefix` / `upstreams[1]` / `headers.add`。
    pub field: String,
    pub message: String,
}

impl std::fmt::Display for RouteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "routes[{}].{}: {}", self.index, self.field, self.message)
    }
}

/// 校验并**规范化**整份文档。
///
/// 规范化是必须的：`/apps/x/` 与 `/apps/x` 在匹配上等价，但如果两端规范化
/// 方式不同，控制面按前者去重、网关按后者匹配，就会出现「两条不同 prefix
/// 实际命中同一个路径」而双方都没报错。
///
/// 返回全部错误而不是第一个：配置一次改多条时，逐个修比来回试快。
pub fn validate(doc: WireDocument) -> Result<WireDocument, Vec<RouteError>> {
    let mut errors = Vec::new();
    let mut out = Vec::with_capacity(doc.routes.len());
    // (token, prefix) 全局唯一 —— 网关按这两个键定位路由。
    let mut seen: HashSet<(String, String)> = HashSet::new();

    for (index, route) in doc.routes.into_iter().enumerate() {
        match normalize_route(route) {
            Ok(route) => {
                if !seen.insert((route.token.clone(), route.prefix.clone())) {
                    errors.push(RouteError {
                        index,
                        field: "prefix".to_string(),
                        message: format!(
                            "重复的 (token, prefix)：token={:?} prefix={:?}",
                            route.token, route.prefix
                        ),
                    });
                }
                out.push(route);
            }
            Err(mut route_errors) => {
                for e in &mut route_errors {
                    e.index = index;
                }
                errors.extend(route_errors);
            }
        }
    }

    if errors.is_empty() {
        Ok(WireDocument {
            version: doc.version,
            routes: out,
        })
    } else {
        Err(errors)
    }
}

fn normalize_route(route: WireRoute) -> Result<WireRoute, Vec<RouteError>> {
    let mut errors = Vec::new();

    let token = match normalize_token(&route.token) {
        Ok(v) => v,
        Err(message) => {
            errors.push(RouteError {
                index: 0,
                field: "token".to_string(),
                message,
            });
            String::new()
        }
    };

    let prefix = match normalize_prefix(&route.prefix) {
        Ok(v) => v,
        Err(message) => {
            errors.push(RouteError {
                index: 0,
                field: "prefix".to_string(),
                message,
            });
            String::new()
        }
    };

    if route.upstreams.is_empty() {
        errors.push(RouteError {
            index: 0,
            field: "upstreams".to_string(),
            message: "至少需要一个上游地址".to_string(),
        });
    }
    for (i, raw) in route.upstreams.iter().enumerate() {
        if let Err(message) = validate_upstream(raw) {
            errors.push(RouteError {
                index: 0,
                field: format!("upstreams[{i}]"),
                message,
            });
        }
    }

    let rewrite = match normalize_rewrite(route.rewrite.as_deref()) {
        Ok(v) => v,
        Err(message) => {
            errors.push(RouteError {
                index: 0,
                field: "rewrite".to_string(),
                message,
            });
            None
        }
    };
    if rewrite.is_some() && prefix.is_empty() {
        errors.push(RouteError {
            index: 0,
            field: "rewrite".to_string(),
            message: "兜底路由（prefix 为空）不能设置 rewrite".to_string(),
        });
    }

    let headers = match normalize_headers(route.headers) {
        Ok(v) => v,
        Err(mut e) => {
            errors.append(&mut e);
            None
        }
    };

    if !errors.is_empty() {
        return Err(errors);
    }

    Ok(WireRoute {
        token,
        prefix,
        upstreams: route.upstreams,
        rewrite,
        headers,
    })
}

fn normalize_token(raw: &str) -> Result<String, String> {
    let token = raw.trim();
    if token.is_empty() {
        return Err("token 不能为空".to_string());
    }
    if token.chars().count() > 256 {
        return Err("token 过长（上限 256 字符）".to_string());
    }
    if token.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err("token 不能包含空白或控制字符".to_string());
    }
    Ok(token.to_string())
}

fn normalize_prefix(raw: &str) -> Result<String, String> {
    let raw = raw.trim();
    if raw.is_empty() || raw == "/" {
        return Ok(String::new());
    }
    if !raw.starts_with('/') {
        return Err(format!("prefix 必须以 / 开头：{raw}"));
    }
    if raw.contains('?') || raw.contains('#') {
        return Err(format!("prefix 不能包含查询串或 fragment：{raw}"));
    }
    let mut prefix = raw.to_string();
    while prefix.len() > 1 && prefix.ends_with('/') {
        prefix.pop();
    }
    Ok(prefix)
}

fn normalize_rewrite(raw: Option<&str>) -> Result<Option<String>, String> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let raw = raw.trim();
    if raw.is_empty() || raw == "/" {
        return Ok(Some(String::new()));
    }
    if !raw.starts_with('/') {
        return Err(format!("rewrite 必须为空或以 / 开头：{raw}"));
    }
    if raw.contains('?') || raw.contains('#') {
        return Err(format!("rewrite 不能包含查询串或 fragment：{raw}"));
    }
    let mut rewrite = raw.to_string();
    while rewrite.len() > 1 && rewrite.ends_with('/') {
        rewrite.pop();
    }
    Ok(Some(rewrite))
}

fn validate_upstream(raw: &str) -> Result<(), String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err("上游地址不能为空".to_string());
    }
    if raw.contains('#') {
        return Err(format!("上游地址不能包含 fragment：{raw}"));
    }
    let (scheme_raw, rest) = match raw.split_once("://") {
        Some((scheme, rest)) => (scheme, rest),
        None => ("http", raw),
    };
    let scheme = match scheme_raw.to_ascii_lowercase().as_str() {
        "http" | "ws" => "http",
        "https" | "wss" => "https",
        other => return Err(format!("不支持的上游 scheme：{other}")),
    };
    let uri = format!("{scheme}://{rest}")
        .parse::<http::Uri>()
        .map_err(|e| format!("上游地址非法：{raw}（{e}）"))?;
    let authority = uri
        .authority()
        .ok_or_else(|| format!("上游地址缺少主机名：{raw}"))?;
    if authority.host().is_empty() {
        return Err(format!("上游地址缺少主机名：{raw}"));
    }
    if authority.as_str().contains('@') {
        return Err(format!("上游地址不能包含 userinfo：{raw}"));
    }
    if uri.query().is_some() {
        return Err(format!("上游地址不能包含查询串：{raw}"));
    }
    Ok(())
}

fn normalize_headers(headers: Option<HeaderOps>) -> Result<Option<HeaderOps>, Vec<RouteError>> {
    let Some(headers) = headers else {
        return Ok(None);
    };
    if headers.is_empty() {
        return Ok(None);
    }

    let mut errors = Vec::new();
    let mut seen = HashSet::new();
    let mut add = BTreeMap::new();
    let mut set = BTreeMap::new();
    let mut remove = Vec::new();

    for (section, map) in [("add", &headers.add), ("set", &headers.set)] {
        for (raw_name, raw_value) in map {
            match validate_header_pair(raw_name, raw_value, &mut seen) {
                Ok((name, value)) => {
                    if section == "add" {
                        add.insert(name, value);
                    } else {
                        set.insert(name, value);
                    }
                }
                Err(message) => errors.push(RouteError {
                    index: 0,
                    field: format!("headers.{section}"),
                    message,
                }),
            }
        }
    }
    for raw_name in &headers.remove {
        match validate_header_name(raw_name, &mut seen) {
            Ok(name) => remove.push(name),
            Err(message) => errors.push(RouteError {
                index: 0,
                field: "headers.remove".to_string(),
                message,
            }),
        }
    }

    if !errors.is_empty() {
        return Err(errors);
    }
    Ok(Some(HeaderOps { add, set, remove }))
}

fn validate_header_pair(
    raw_name: &str,
    raw_value: &str,
    seen: &mut HashSet<String>,
) -> Result<(String, String), String> {
    let name = validate_header_name(raw_name, seen)?;
    let value = raw_value.trim();
    http::HeaderValue::from_str(value).map_err(|e| format!("header「{name}」的值非法：{e}"))?;
    Ok((name, value.to_string()))
}

fn validate_header_name(raw_name: &str, seen: &mut HashSet<String>) -> Result<String, String> {
    let raw_name = raw_name.trim();
    let name = http::HeaderName::from_bytes(raw_name.as_bytes())
        .map_err(|e| format!("header 名非法「{raw_name}」：{e}"))?;
    let lower = name.as_str().to_string();
    if RESERVED_HEADERS.contains(&lower.as_str()) {
        return Err(format!("header「{lower}」由传输层管理，不能由配置设置"));
    }
    if !seen.insert(lower.clone()) {
        return Err(format!("header「{lower}」在 add / set / remove 中重复出现"));
    }
    Ok(lower)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(prefix: &str) -> WireRoute {
        WireRoute {
            token: "t".to_string(),
            prefix: prefix.to_string(),
            upstreams: vec!["http://127.0.0.1:8080".to_string()],
            rewrite: None,
            headers: None,
        }
    }

    fn doc(routes: Vec<WireRoute>) -> WireDocument {
        WireDocument {
            version: Some(1),
            routes,
        }
    }

    #[test]
    fn normalizes_prefix_and_rewrite() {
        let mut r = route("/apps/x/");
        r.rewrite = Some("/apps/x/".to_string());
        let out = validate(doc(vec![r])).expect("valid");
        assert_eq!(out.routes[0].prefix, "/apps/x");
        assert_eq!(out.routes[0].rewrite.as_deref(), Some("/apps/x"));
    }

    #[test]
    fn empty_prefix_is_fallback_and_normalized() {
        let out = validate(doc(vec![route("/")])).expect("valid");
        assert_eq!(out.routes[0].prefix, "");
    }

    #[test]
    fn fallback_route_must_not_rewrite() {
        let mut r = route("");
        r.rewrite = Some("/x".to_string());
        let err = validate(doc(vec![r])).expect_err("must reject");
        assert_eq!(err[0].field, "rewrite");
    }

    #[test]
    fn duplicate_token_and_prefix_is_rejected() {
        let err = validate(doc(vec![route("/a"), route("/a")])).expect_err("must reject");
        assert!(err.iter().any(|e| e.index == 1 && e.field == "prefix"));
    }

    #[test]
    fn reserved_header_is_rejected() {
        let mut r = route("/a");
        r.headers = Some(HeaderOps {
            add: BTreeMap::from([("content-length".to_string(), "1".to_string())]),
            ..Default::default()
        });
        let err = validate(doc(vec![r])).expect_err("must reject");
        assert!(err[0].message.contains("传输层管理"), "{}", err[0].message);
    }

    #[test]
    fn header_name_is_lowercased() {
        let mut r = route("/a");
        r.headers = Some(HeaderOps {
            add: BTreeMap::from([("X-Tenant".to_string(), "acme".to_string())]),
            ..Default::default()
        });
        let out = validate(doc(vec![r])).expect("valid");
        let headers = out.routes[0].headers.clone().expect("headers kept");
        assert!(headers.add.contains_key("x-tenant"));
    }

    #[test]
    fn upstream_requires_host_and_known_scheme() {
        let mut r = route("/a");
        r.upstreams = vec!["ftp://x/y".to_string()];
        let err = validate(doc(vec![r])).expect_err("must reject");
        assert!(err[0].message.contains("scheme"), "{}", err[0].message);

        let mut r = route("/a");
        r.upstreams = vec!["http://user:pw@host".to_string()];
        let err = validate(doc(vec![r])).expect_err("must reject");
        assert!(err[0].message.contains("userinfo"), "{}", err[0].message);
    }

    #[test]
    fn upstream_without_scheme_is_treated_as_http() {
        let mut r = route("/a");
        r.upstreams = vec!["127.0.0.1:9000".to_string()];
        assert!(validate(doc(vec![r])).is_ok());
    }

    #[test]
    fn unknown_fields_are_rejected_at_deserialize() {
        let json = r#"{"routes":[{"token":"t","prefix":"/a","upstreams":["http://h"],"extra":1}]}"#;
        assert!(serde_json::from_str::<WireDocument>(json).is_err());
    }

    #[test]
    fn version_round_trips_and_is_optional() {
        let json = serde_json::to_string(&doc(vec![route("/a")])).expect("serialize");
        assert!(json.contains("\"version\":1"), "{json}");

        let no_version = WireDocument {
            version: None,
            routes: vec![route("/a")],
        };
        let json = serde_json::to_string(&no_version).expect("serialize");
        assert!(!json.contains("version"), "{json}");
    }
}
