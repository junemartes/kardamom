//! TOML configuration that the `kardamom-executor` binary deserializes.
//!
//! In the past, the executor took all runtime tuning through CLI flags. It
//! only checked that `--config` was present. This module adds the top-level
//! config that the binary reads from that TOML file. Every field has a
//! default, so an empty config file (the current deployment shape) still
//! parses. The first real field is the `[cluster]` section. Cluster mode is
//! the only mode, so this section must be filled in before the binary can
//! connect. An empty section fails at cluster connect time with a config
//! error.
//!
//! Note: this differs from [`crate::ExecutorConfig`] (in `actor.rs`), which
//! is the in-process runtime tuning passed to [`crate::Executor::run`]. This
//! struct is the target for deserializing the operator-supplied TOML file.

use serde::Deserialize;

// The `[cluster]` section type lives next to the cluster reader in the
// engine (the validator parses the same section). It is re-exported here so
// existing `kardamom_executor::config::ClusterConfig` paths still resolve.
pub use kardamom_engine::reader::cluster::ClusterConfig;

/// Top-level config the `kardamom-executor` binary deserializes from `--config`.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct ExecutorFileConfig {
    /// Aeron Cluster (Raft) sealer client config. tx_ordering always comes
    /// from the cluster egress. There is no other path.
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
        // This matches the deployed executor.toml shape: a comment-only file.
        let cfg: ExecutorFileConfig = toml::from_str("# just a comment\n").unwrap();
        assert!(cfg.cluster.ingress_endpoints.is_empty());
    }

    #[test]
    fn cluster_section_parses() {
        // `enabled` is an old key. The setting is removed, but the parser
        // must still accept it.
        let toml = r#"
            [cluster]
            enabled = true
            ingress_endpoints = "0=h0:9000,1=h1:9001"
            initial_leader_member_id = 0
            egress_channel = "aeron:udp?endpoint=127.0.0.1:9050"
        "#;
        let cfg: ExecutorFileConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.cluster.ingress_endpoints, "0=h0:9000,1=h1:9001");
        // Stream ID and keep-alive defaults fill in when they are omitted.
        let c = cfg.cluster.defaults_applied();
        assert_eq!(c.ingress_stream_id, 101);
        assert_eq!(c.egress_stream_id, 102);
        assert_eq!(c.keep_alive_interval_ms, 1000);
    }
}
