//! TOML configuration deserialized by the `kardamom-executor` binary.
//!
//! Historically the executor took all runtime tuning via CLI flags and only
//! presence-checked `--config`. This module adds the top-level config the
//! binary deserializes from that TOML; every field has a default so an empty
//! config file (the existing deployment shape) still parses. The first real
//! field is the optional `[cluster]` section — default disabled, leaving the
//! Aeron IPC/MDC tx_ordering path unchanged.
//!
//! Note: this is distinct from [`crate::ExecutorConfig`] (in `actor.rs`), which
//! is the in-process runtime tuning passed to [`crate::Executor::run`]. This
//! struct is the deserialization target for the operator-supplied TOML file.

use serde::Deserialize;

/// Top-level config the `kardamom-executor` binary deserializes from `--config`.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct ExecutorFileConfig {
    /// Aeron Cluster (Raft) sealer client config. tx_ordering ALWAYS comes from
    /// the cluster egress — there is no longer a non-cluster path.
    pub cluster: ClusterConfig,
}

/// Aeron Cluster (Raft) sealer client config.
#[derive(Debug, Clone, serde::Deserialize, Default)]
#[serde(default)]
pub struct ClusterConfig {
    /// Retained for config-file backward compatibility; IGNORED — cluster mode
    /// is the only mode, so the executor always connects to the cluster.
    pub enabled: bool,
    /// "memberId=host:port,…" cluster ingress endpoints.
    pub ingress_endpoints: String,
    pub initial_leader_member_id: i32,
    pub ingress_stream_id: i32,
    /// This client's egress (response) channel URI, e.g. "aeron:udp?endpoint=<ip>:<port>".
    pub egress_channel: String,
    pub egress_stream_id: i32,
    pub keep_alive_interval_ms: u64,
}

impl ClusterConfig {
    /// Sane stream-id / keepalive defaults when the TOML omits them.
    pub fn defaults_applied(mut self) -> Self {
        if self.ingress_stream_id == 0 {
            self.ingress_stream_id = 101;
        }
        if self.egress_stream_id == 0 {
            self.egress_stream_id = 102;
        }
        if self.keep_alive_interval_ms == 0 {
            self.keep_alive_interval_ms = 1000;
        }
        self
    }

    pub fn to_live(&self) -> kardamom_cluster_adapter::LiveClusterConfig {
        let c = self.clone().defaults_applied();
        kardamom_cluster_adapter::LiveClusterConfig {
            ingress_endpoints: c.ingress_endpoints,
            initial_leader_member_id: c.initial_leader_member_id,
            ingress_stream_id: c.ingress_stream_id,
            egress_channel: c.egress_channel,
            egress_stream_id: c.egress_stream_id,
            keep_alive_interval_ms: c.keep_alive_interval_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_toml_parses_to_disabled_cluster() {
        let cfg: ExecutorFileConfig = toml::from_str("").unwrap();
        assert!(!cfg.cluster.enabled);
    }

    #[test]
    fn comment_only_toml_parses() {
        // Matches the deployed executor.toml shape (comment-only file).
        let cfg: ExecutorFileConfig = toml::from_str("# just a comment\n").unwrap();
        assert!(!cfg.cluster.enabled);
    }

    #[test]
    fn cluster_section_parses() {
        let toml = r#"
            [cluster]
            enabled = true
            ingress_endpoints = "0=h0:9000,1=h1:9001"
            initial_leader_member_id = 0
            egress_channel = "aeron:udp?endpoint=127.0.0.1:9050"
        "#;
        let cfg: ExecutorFileConfig = toml::from_str(toml).unwrap();
        assert!(cfg.cluster.enabled);
        assert_eq!(cfg.cluster.ingress_endpoints, "0=h0:9000,1=h1:9001");
        // Stream-id / keepalive defaults fill in when omitted.
        let c = cfg.cluster.defaults_applied();
        assert_eq!(c.ingress_stream_id, 101);
        assert_eq!(c.egress_stream_id, 102);
        assert_eq!(c.keep_alive_interval_ms, 1000);
    }
}
