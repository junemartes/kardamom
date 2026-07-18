//! The `[cluster]` TOML section shared by every cluster client.
//!
//! One definition (here, next to [`LiveClusterConfig`] it maps onto) consumed
//! by every role that connects to the Aeron Cluster (Raft) sealer: the
//! sequencer (tx_ordering publish), the executor/validator (canonical-stream
//! consume, via `kardamom_engine::reader::cluster`), and ingress (on-quorum
//! watermark). The service config modules re-export this type so the schema —
//! including the magic stream-id/keepalive defaults — can only be changed in
//! one place.

use serde::{Deserialize, Serialize};

use crate::live::LiveClusterConfig;

/// Aeron Cluster (Raft) sealer client config — the `[cluster]` TOML section.
///
/// Cluster mode is the only mode: every service that parses this section
/// always connects to the cluster (there is no `enabled` knob; a legacy
/// `enabled = true` key in an old config file is ignored by serde). An empty
/// / missing section is rejected when the connection is actually opened —
/// [`crate::live::connect`] fails startup on empty `ingress_endpoints` /
/// `egress_channel`.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(default)]
pub struct ClusterConfig {
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

    pub fn to_live(&self) -> LiveClusterConfig {
        let c = self.clone().defaults_applied();
        LiveClusterConfig {
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
    fn defaults_fill_in_when_omitted() {
        let cfg: ClusterConfig = toml::from_str(
            r#"
            ingress_endpoints = "0=h0:9000,1=h1:9001"
            initial_leader_member_id = 0
            egress_channel = "aeron:udp?endpoint=127.0.0.1:9050"
            "#,
        )
        .unwrap();
        let c = cfg.defaults_applied();
        assert_eq!(c.ingress_stream_id, 101);
        assert_eq!(c.egress_stream_id, 102);
        assert_eq!(c.keep_alive_interval_ms, 1000);
    }

    #[test]
    fn legacy_enabled_key_is_tolerated() {
        // Older deploy configs carried a (never-consulted) `enabled = true`;
        // the field is gone but old files must keep parsing.
        let cfg: ClusterConfig = toml::from_str(
            r#"
            enabled = true
            ingress_endpoints = "0=h0:9000"
            "#,
        )
        .unwrap();
        assert_eq!(cfg.ingress_endpoints, "0=h0:9000");
    }

    #[test]
    fn to_live_maps_all_fields() {
        let cfg = ClusterConfig {
            ingress_endpoints: "0=h0:9000".into(),
            initial_leader_member_id: 2,
            ingress_stream_id: 111,
            egress_channel: "aeron:udp?endpoint=1.2.3.4:9050".into(),
            egress_stream_id: 112,
            keep_alive_interval_ms: 250,
        };
        let live = cfg.to_live();
        assert_eq!(live.ingress_endpoints, cfg.ingress_endpoints);
        assert_eq!(live.initial_leader_member_id, cfg.initial_leader_member_id);
        assert_eq!(live.ingress_stream_id, 111);
        assert_eq!(live.egress_channel, cfg.egress_channel);
        assert_eq!(live.egress_stream_id, 112);
        assert_eq!(live.keep_alive_interval_ms, 250);
    }
}
