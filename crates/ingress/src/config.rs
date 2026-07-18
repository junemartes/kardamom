//! Static configuration for an `IngressProxy` instance.

use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::time::Duration;

use kardamom_types::AckPolicy;

/// Static configuration for an `IngressProxy` instance.
///
/// All fields are required; pick a `IngressConfig::default()` for tests.
#[derive(Debug, Clone)]
pub struct IngressConfig {
    /// HTTP+WS jsonrpsee server bind address.
    pub jsonrpc_bind: SocketAddr,
    /// Optional TCP bind for the binary line protocol.
    pub binary_tcp_bind: Option<SocketAddr>,
    /// Optional UDS path for the binary line protocol.
    pub binary_uds_path: Option<PathBuf>,
    /// Number of sequencer partitions (M); routes `keccak(sender) % M`.
    pub partition_count_m: u32,
    /// Stable identity of this ingress replica (active/active deployments run N
    /// of them). Namespaces `correlation_id` so the `(replica, sequence)` pair
    /// is globally unique: `correlation_id = (ingress_id << 48) | (seq & 2^48-1)`.
    /// Logged at startup. Single-instance deployments use `0`.
    pub ingress_id: u16,
    /// Per-IP token-bucket replenishment rate (tokens/sec).
    pub rate_limit_per_ip_per_sec: NonZeroU32,
    /// Per-IP token-bucket burst capacity.
    pub rate_limit_burst: NonZeroU32,
    /// Batched sig-verify ring depth (spec calls for 64).
    pub sig_verify_batch_depth: usize,
    /// Batched sig-verify flush window (spec calls for 50µs).
    pub sig_verify_flush_window: Duration,
    /// Max time the proxy waits for receipt + watermark before timing out the
    /// client.
    pub pending_receipt_timeout: Duration,
    /// L2 chain id (returned by `eth_chainId`).
    pub chain_id: u64,
    /// Receipt-cache capacity (FIFO-evicted).
    pub receipt_cache_capacity: usize,
    /// Which durability gate the proxy waits on before acking a tx. See
    /// [`kardamom_types::AckPolicy`] for the four modes.
    pub ack_policy: AckPolicy,
    /// Max concurrent JSON-RPC connections. `submit_raw` parks each
    /// submission's request until its receipt arrives, so steady-state
    /// concurrent connections ≈ offered rate × receipt latency — which grows
    /// exactly when the pipeline is slowest. jsonrpsee's default (100) capped
    /// end-to-end throughput at 100/latency and turned overload into
    /// connection refusals for every client of the replica. Sized so the
    /// connection table is never the binding limit.
    pub rpc_max_connections: u32,
}

impl Default for IngressConfig {
    fn default() -> Self {
        use nonzero_ext::nonzero;
        Self {
            jsonrpc_bind: "127.0.0.1:0".parse().unwrap(),
            binary_tcp_bind: None,
            binary_uds_path: None,
            partition_count_m: 8,
            ingress_id: 0,
            rate_limit_per_ip_per_sec: nonzero!(10_000u32),
            rate_limit_burst: nonzero!(1_000u32),
            sig_verify_batch_depth: 64,
            sig_verify_flush_window: Duration::from_micros(50),
            pending_receipt_timeout: Duration::from_secs(30),
            chain_id: 1,
            receipt_cache_capacity: 64 * 1024,
            ack_policy: AckPolicy::default(),
            rpc_max_connections: 8192,
        }
    }
}

/// TOML file the `kardamom-ingress` binary parses from `--config` for the
/// Aeron Cluster (Raft) client connection. The rest of the ingress runtime
/// tuning still comes from CLI flags + [`IngressConfig::default`].
#[derive(Debug, Clone, serde::Deserialize, Default)]
#[serde(default)]
pub struct IngressFileConfig {
    /// Aeron Cluster (Raft) sealer client config. The on-quorum ack gate
    /// derives its durable watermark from this cluster's egress progress.
    pub cluster: ClusterConfig,
}

/// Aeron Cluster (Raft) client config (mirrors the executor/sequencer shape).
#[derive(Debug, Clone, serde::Deserialize, Default)]
#[serde(default)]
pub struct ClusterConfig {
    /// Retained for config-file backward compatibility; IGNORED — cluster mode
    /// is the only mode.
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
    fn defaults_applied(mut self) -> Self {
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
    fn default_matches_spec() {
        let cfg = IngressConfig::default();
        assert_eq!(cfg.partition_count_m, 8);
        assert_eq!(cfg.sig_verify_batch_depth, 64);
        assert_eq!(cfg.sig_verify_flush_window, Duration::from_micros(50));
    }
}
