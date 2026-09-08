//! The `[cluster]` TOML section shared by every cluster client.
//!
//! This is one definition, next to [`LiveClusterConfig`] which it maps onto.
//! Every role that connects to the Aeron Cluster (Raft) sealer uses it: the
//! sequencer (`tx_ordering` publish), the executor and validator
//! (canonical-stream consume, through `kardamom_engine::reader::cluster`),
//! and ingress (on-quorum watermark). The service config modules re-export
//! this type. This way, the schema, including the default stream ID and
//! keep-alive values, can change in only one place.

use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};

use crate::live::LiveClusterConfig;

/// Default Aeron ingress stream ID, used when the TOML omits `ingress_stream_id`.
const DEFAULT_INGRESS_STREAM_ID: i32 = 101;
/// Default Aeron egress stream ID, used when the TOML omits `egress_stream_id`.
const DEFAULT_EGRESS_STREAM_ID: i32 = 102;
/// Default keep-alive interval, used when the TOML omits `keep_alive_interval_ms`.
const DEFAULT_KEEP_ALIVE_INTERVAL_MS: NonZeroU64 = NonZeroU64::new(1000).expect("1000 != 0");

/// Aeron Cluster (Raft) sealer client config: the `[cluster]` TOML section.
///
/// Cluster mode is the only mode. Every service that parses this section
/// always connects to the cluster. There is no `enabled` knob; unknown
/// keys are ignored. An empty or missing section is rejected only when
/// the connection actually opens: [`crate::live::connect`] fails startup
/// on an empty `ingress_endpoints` or `egress_channel`.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(default)]
pub struct ClusterConfig {
    /// "memberId=host:port,…" cluster ingress endpoints.
    pub ingress_endpoints: String,
    pub initial_leader_member_id: i32,
    /// `None` means the TOML omitted this key: fall back to
    /// [`DEFAULT_INGRESS_STREAM_ID`]. This is `Option<i32>`, not a
    /// `0`-as-unset sentinel, so an explicit `ingress_stream_id = 0` (if
    /// that is ever a real Aeron stream ID) is never silently replaced.
    pub ingress_stream_id: Option<i32>,
    /// This client's egress (response) channel URI, e.g. "aeron:udp?endpoint=<ip>:<port>".
    pub egress_channel: String,
    /// `None` means the TOML omitted this key: fall back to
    /// [`DEFAULT_EGRESS_STREAM_ID`]. See `ingress_stream_id` for why this
    /// is `Option<i32>`.
    pub egress_stream_id: Option<i32>,
    /// `None` means the TOML omitted this key: fall back to
    /// [`DEFAULT_KEEP_ALIVE_INTERVAL_MS`]. Never zero when set: a
    /// zero-length keep-alive interval is not a meaningful setting.
    pub keep_alive_interval_ms: Option<NonZeroU64>,
}

impl ClusterConfig {
    /// Default stream ID and keep-alive values, used when the TOML omits them.
    #[must_use]
    pub fn defaults_applied(mut self) -> Self {
        self.ingress_stream_id
            .get_or_insert(DEFAULT_INGRESS_STREAM_ID);
        self.egress_stream_id
            .get_or_insert(DEFAULT_EGRESS_STREAM_ID);
        self.keep_alive_interval_ms
            .get_or_insert(DEFAULT_KEEP_ALIVE_INTERVAL_MS);
        self
    }

    #[must_use]
    pub fn to_live(&self) -> LiveClusterConfig {
        self.clone().into()
    }
}

impl From<ClusterConfig> for LiveClusterConfig {
    /// Maps the six `[cluster]` TOML fields onto their `LiveClusterConfig`
    /// counterparts, filling in defaults for whichever were omitted.
    fn from(cfg: ClusterConfig) -> Self {
        Self {
            ingress_endpoints: cfg.ingress_endpoints,
            initial_leader_member_id: cfg.initial_leader_member_id,
            ingress_stream_id: cfg.ingress_stream_id.unwrap_or(DEFAULT_INGRESS_STREAM_ID),
            egress_channel: cfg.egress_channel,
            egress_stream_id: cfg.egress_stream_id.unwrap_or(DEFAULT_EGRESS_STREAM_ID),
            keep_alive_interval_ms: cfg
                .keep_alive_interval_ms
                .unwrap_or(DEFAULT_KEEP_ALIVE_INTERVAL_MS)
                .get(),
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
        assert_eq!(c.ingress_stream_id, Some(101));
        assert_eq!(c.egress_stream_id, Some(102));
        assert_eq!(c.keep_alive_interval_ms, NonZeroU64::new(1000));
    }

    #[test]
    fn legacy_enabled_key_is_tolerated() {
        // Older deploy configs had an `enabled = true` key that was never
        // used. The field is gone, but old files must still parse.
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
            ingress_stream_id: Some(111),
            egress_channel: "aeron:udp?endpoint=1.2.3.4:9050".into(),
            egress_stream_id: Some(112),
            keep_alive_interval_ms: Some(NonZeroU64::new(250).unwrap()),
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
