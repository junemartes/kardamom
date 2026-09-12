//! The Consul agent HTTP client: service registration with a TTL check,
//! check passes, deregistration, and blocking health queries filtered on
//! service metadata.
//!
//! Every call goes to the local agent. Consul is the discovery control
//! plane only: no message, no per-message lookup, no ordering decision
//! goes through it.

use std::collections::BTreeMap;
use std::net::IpAddr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::catalog::{Query, QueryResult, RegistrationSpec};
use super::record::{ServiceEntry, ServiceId};
use crate::config::DiscoveryConfig;
use crate::error::LogError;

/// The token header Consul reads. The value is never logged.
const TOKEN_HEADER: &str = "X-Consul-Token";
const INDEX_HEADER: &str = "X-Consul-Index";
/// The environment variables that carry the token when no token file is
/// configured, in order: Consul's own name, then the name Nomad sets on a
/// task with a Consul workload identity.
const TOKEN_ENV: [&str; 2] = ["CONSUL_HTTP_TOKEN", "CONSUL_TOKEN"];

#[derive(Clone)]
pub struct ConsulClient {
    http: reqwest::Client,
    base: String,
    token: Option<String>,
    datacenter: Option<String>,
    request_timeout: Duration,
}

/// Consul's service registration body.
#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct RegisterBody<'a> {
    #[serde(rename = "ID")]
    id: &'a str,
    name: &'a str,
    address: IpAddr,
    port: u16,
    meta: &'a BTreeMap<String, String>,
    check: RegisterCheck,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct RegisterCheck {
    #[serde(rename = "CheckID")]
    check_id: String,
    #[serde(rename = "TTL")]
    ttl: String,
    deregister_critical_service_after: String,
}

/// One element of Consul's `/v1/health/service/<name>` answer, reduced to
/// the service fields this crate reads.
#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct HealthEntry {
    service: HealthService,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct HealthService {
    #[serde(rename = "ID")]
    id: String,
    service: String,
    address: String,
    port: u16,
    #[serde(default)]
    meta: BTreeMap<String, String>,
}

impl HealthService {
    fn into_entry(self) -> Result<ServiceEntry, LogError> {
        let address = self.address.parse().map_err(|e| {
            LogError::Discovery(format!(
                "record {}: address {} is not an IP: {e}",
                self.id, self.address
            ))
        })?;
        Ok(ServiceEntry {
            id: ServiceId::new(self.id),
            name: self.service,
            address,
            port: self.port,
            meta: self.meta,
        })
    }
}

/// Consul's duration syntax for a whole number of seconds, rounded up so
/// a sub-second setting never becomes `0s`. Every duration sent here
/// comes from a non-zero config value.
fn consul_seconds(d: Duration) -> String {
    format!("{}s", d.as_secs() + u64::from(d.subsec_nanos() > 0))
}

/// The `filter` expression of a health query: every `meta_equals` pair
/// as an equality on the service metadata.
fn filter_expression(meta_equals: &BTreeMap<String, String>) -> String {
    meta_equals
        .iter()
        .map(|(k, v)| format!("Service.Meta.{k} == \"{}\"", v.replace('"', "\\\"")))
        .collect::<Vec<_>>()
        .join(" and ")
}

impl ConsulClient {
    /// Build the client from the `[discovery]` section, reading the token
    /// file once.
    ///
    /// # Errors
    ///
    /// Returns an error if the token file cannot be read or the HTTP
    /// client fails to build.
    pub fn new(cfg: &DiscoveryConfig) -> Result<Self, LogError> {
        let token = resolve_token(cfg, |name| std::env::var(name).ok())?;
        let http = reqwest::Client::builder()
            .connect_timeout(cfg.request_timeout())
            .build()
            .map_err(|e| LogError::Discovery(format!("build http client: {e}")))?;
        Ok(Self {
            http,
            base: cfg.consul_http_addr.trim_end_matches('/').to_string(),
            token,
            datacenter: Some(cfg.datacenter.clone()).filter(|dc| !dc.is_empty()),
            request_timeout: cfg.request_timeout(),
        })
    }

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let req = self.http.request(method, format!("{}{path}", self.base));
        match &self.token {
            Some(token) => req.header(TOKEN_HEADER, token),
            None => req,
        }
    }

    /// Send a request whose only interesting outcome is success.
    async fn send_ok(&self, req: reqwest::RequestBuilder, what: &str) -> Result<(), LogError> {
        let resp = req
            .timeout(self.request_timeout)
            .send()
            .await
            .map_err(|e| LogError::Discovery(format!("{what}: {e}")))?;
        let status = resp.status();
        if status.is_success() {
            return Ok(());
        }
        let body = resp.text().await.unwrap_or_default();
        Err(LogError::Discovery(format!("{what}: {status}: {body}")))
    }

    /// # Errors
    ///
    /// Returns an error if the agent rejects the registration or is
    /// unreachable.
    pub async fn register(&self, spec: &RegistrationSpec) -> Result<(), LogError> {
        let body = RegisterBody {
            id: spec.entry.id.as_str(),
            name: &spec.entry.name,
            address: spec.entry.address,
            port: spec.entry.port,
            meta: &spec.entry.meta,
            check: RegisterCheck {
                check_id: spec.check_id(),
                ttl: consul_seconds(spec.ttl),
                deregister_critical_service_after: consul_seconds(spec.deregister_after),
            },
        };
        let req = self
            .request(reqwest::Method::PUT, "/v1/agent/service/register")
            .json(&body);
        self.send_ok(req, &format!("register {}", spec.entry.id))
            .await
    }

    /// # Errors
    ///
    /// Returns an error if the check is unknown (the registration is
    /// gone) or the agent is unreachable.
    pub async fn pass(&self, service: &ServiceId) -> Result<(), LogError> {
        let path = format!("/v1/agent/check/pass/{}", super::catalog::check_id(service));
        let req = self.request(reqwest::Method::PUT, &path);
        self.send_ok(req, &format!("pass check of {service}")).await
    }

    /// # Errors
    ///
    /// Returns an error if the agent is unreachable.
    pub async fn deregister(&self, service: &ServiceId) -> Result<(), LogError> {
        let path = format!("/v1/agent/service/deregister/{service}");
        let req = self.request(reqwest::Method::PUT, &path);
        self.send_ok(req, &format!("deregister {service}")).await
    }

    /// # Errors
    ///
    /// Returns an error if the agent is unreachable or denies the read,
    /// if the answer is not the expected JSON, or if it carries no index.
    pub async fn query(&self, query: &Query) -> Result<QueryResult, LogError> {
        let mut params: Vec<(&str, String)> = vec![
            ("passing", "true".into()),
            ("filter", filter_expression(&query.meta_equals)),
        ];
        if query.index > 0 {
            params.push(("index", query.index.to_string()));
            params.push(("wait", consul_seconds(query.wait)));
        }
        if let Some(dc) = &self.datacenter {
            params.push(("dc", dc.clone()));
        }
        // A blocking query holds for `wait` plus the agent's jitter (up to
        // a sixteenth of `wait`), so its budget is the wait plus the
        // ordinary request budget.
        let timeout = query.wait + query.wait / 16 + self.request_timeout;
        let what = format!("query {}", query.service);
        let path = format!("/v1/health/service/{}", query.service);
        let resp = self
            .request(reqwest::Method::GET, &path)
            .query(&params)
            .timeout(timeout)
            .send()
            .await
            .map_err(|e| LogError::Discovery(format!("{what}: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(LogError::Discovery(format!("{what}: {status}: {body}")));
        }
        let index = resp
            .headers()
            .get(INDEX_HEADER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .ok_or_else(|| LogError::Discovery(format!("{what}: no {INDEX_HEADER} header")))?;
        let entries: Vec<HealthEntry> = resp
            .json()
            .await
            .map_err(|e| LogError::Discovery(format!("{what}: malformed body: {e}")))?;
        let entries = entries
            .into_iter()
            .map(|e| e.service.into_entry())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(QueryResult { index, entries })
    }
}

/// The ACL token: the configured file when set, else the first non-empty
/// variable of [`TOKEN_ENV`], else none. `env` is the variable lookup.
fn resolve_token(
    cfg: &DiscoveryConfig,
    env: impl Fn(&str) -> Option<String>,
) -> Result<Option<String>, LogError> {
    if let Some(path) = cfg.consul_token_file.as_deref() {
        return std::fs::read_to_string(path)
            .map(|s| Some(s.trim().to_string()))
            .map_err(|e| {
                LogError::Discovery(format!("read consul token file {}: {e}", path.display()))
            });
    }
    Ok(TOKEN_ENV
        .iter()
        .filter_map(|name| env(name))
        .map(|value| value.trim().to_string())
        .find(|value| !value.is_empty()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_comes_from_the_file_before_the_environment() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token");
        std::fs::write(&path, "from-file\n").unwrap();
        let cfg = DiscoveryConfig {
            consul_token_file: Some(path),
            ..DiscoveryConfig::default()
        };
        let token = resolve_token(&cfg, |_| Some("from-env".into())).unwrap();
        assert_eq!(token.as_deref(), Some("from-file"));
    }

    #[test]
    fn token_falls_back_to_the_environment_in_order() {
        let cfg = DiscoveryConfig::default();
        let nomad_only = |name: &str| (name == "CONSUL_TOKEN").then(|| " nomad ".to_string());
        assert_eq!(
            resolve_token(&cfg, nomad_only).unwrap().as_deref(),
            Some("nomad")
        );
        let both = |name: &str| Some(format!("{name}-value"));
        assert_eq!(
            resolve_token(&cfg, both).unwrap().as_deref(),
            Some("CONSUL_HTTP_TOKEN-value")
        );
        assert_eq!(resolve_token(&cfg, |_| Some(String::new())).unwrap(), None);
        assert_eq!(resolve_token(&cfg, |_| None).unwrap(), None);
    }

    #[test]
    fn filter_joins_every_pair_with_and() {
        let meta = BTreeMap::from([
            ("topic".to_string(), "tx_data".to_string()),
            ("cluster_id".to_string(), "c1".to_string()),
        ]);
        assert_eq!(
            filter_expression(&meta),
            "Service.Meta.cluster_id == \"c1\" and Service.Meta.topic == \"tx_data\""
        );
    }

    #[test]
    fn consul_seconds_rounds_up_and_never_yields_zero() {
        assert_eq!(consul_seconds(Duration::from_millis(1500)), "2s");
        assert_eq!(consul_seconds(Duration::from_secs(10)), "10s");
        assert_eq!(consul_seconds(Duration::from_millis(200)), "1s");
    }
}
