//! TOML configuration deserialized by the `kardamom-executor` binary.
//!
//! Historically the executor took all runtime tuning via CLI flags and only
//! presence-checked `--config`. This module adds the top-level config the
//! binary deserializes from that TOML; every field has a default so an empty
//! config file (the existing deployment shape) still parses. The first real
//! field is the `[cluster]` section — cluster mode is the only mode, so the
//! section must be populated for the binary to connect (an empty section
//! fails at cluster connect time with a config error).
//!
//! Note: this is distinct from [`crate::ExecutorConfig`] (in `actor.rs`), which
//! is the in-process runtime tuning passed to [`crate::Executor::run`]. This
//! struct is the deserialization target for the operator-supplied TOML file.

use serde::Deserialize;

// The `[cluster]` section type lives beside the cluster reader in the engine
// (the validator parses the same section); re-exported here so existing
// `kardamom_executor::config::ClusterConfig` paths keep resolving.
pub use kardamom_engine::reader::cluster::ClusterConfig;

/// Top-level config the `kardamom-executor` binary deserializes from `--config`.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct ExecutorFileConfig {
    /// Aeron Cluster (Raft) sealer client config. tx_ordering ALWAYS comes from
    /// the cluster egress — there is no longer a non-cluster path.
    pub cluster: ClusterConfig,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_toml_parses_to_default_cluster() {
        let cfg: ExecutorFileConfig = toml::from_str("").unwrap();
        assert!(cfg.cluster.ingress_endpoints.is_empty());
    }

    #[test]
    fn comment_only_toml_parses() {
        // Matches the deployed executor.toml shape (comment-only file).
        let cfg: ExecutorFileConfig = toml::from_str("# just a comment\n").unwrap();
        assert!(cfg.cluster.ingress_endpoints.is_empty());
    }

    #[test]
    fn cluster_section_parses() {
        // `enabled` is a legacy key (removed knob) — must still be tolerated.
        let toml = r#"
            [cluster]
            enabled = true
            ingress_endpoints = "0=h0:9000,1=h1:9001"
            initial_leader_member_id = 0
            egress_channel = "aeron:udp?endpoint=127.0.0.1:9050"
        "#;
        let cfg: ExecutorFileConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.cluster.ingress_endpoints, "0=h0:9000,1=h1:9001");
        // Stream-id / keepalive defaults fill in when omitted.
        let c = cfg.cluster.defaults_applied();
        assert_eq!(c.ingress_stream_id, 101);
        assert_eq!(c.egress_stream_id, 102);
        assert_eq!(c.keep_alive_interval_ms, 1000);
    }
}
