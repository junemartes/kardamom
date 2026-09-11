//! Static configuration for an `IngressProxy` instance.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::time::Duration;

use kardamom_types::AckPolicy;

/// Static configuration for an `IngressProxy` instance.
///
/// All fields are required. Use `IngressConfig::default()` for tests.
#[derive(Debug, Clone)]
pub struct IngressConfig {
    /// HTTP and WS jsonrpsee server bind address.
    pub jsonrpc_bind: SocketAddr,
    /// Optional TCP bind for the binary line protocol.
    pub binary_tcp_bind: Option<SocketAddr>,
    /// Optional UDS path for the binary line protocol.
    pub binary_uds_path: Option<PathBuf>,
    /// Number of sequencer partitions (M). Routes on `keccak(sender) % M`
    /// when `shard_map` is `None`.
    pub partition_count_m: NonZeroU32,
    /// The versioned vslot-to-lane map. `None` means the identity map
    /// `lane = vslot % M`, the legacy rule. A resize installs a map
    /// through the ingress config. See
    /// `docs/specs/dynamic-sequencer-sizing.md`, section 3.2.
    pub shard_map: Option<kardamom_types::shard_map::ShardMap>,
    /// The stable identity of this ingress replica. An active/active
    /// deployment runs N replicas. This id namespaces `correlation_id`, so
    /// the `(replica, sequence)` pair stays unique:
    /// `correlation_id = (ingress_id << 48) | (seq & 2^48-1)`.
    /// The proxy logs this id at startup. A single-instance deployment
    /// uses `0`.
    pub ingress_id: u16,
    /// Per-IP token-bucket replenishment rate (tokens/sec).
    pub rate_limit_per_ip_per_sec: NonZeroU32,
    /// Per-IP token-bucket burst capacity.
    pub rate_limit_burst: NonZeroU32,
    /// Batched sig-verify ring depth (spec calls for 64).
    pub sig_verify_batch_depth: NonZeroUsize,
    /// Batched sig-verify flush window (spec calls for 50µs).
    pub sig_verify_flush_window: Duration,
    /// Max time the proxy waits for a receipt and a watermark before it
    /// times out the client.
    pub pending_receipt_timeout: Duration,
    /// L2 chain id (returned by `eth_chainId`). EIP-155 forbids chain id
    /// 0.
    pub chain_id: NonZeroU64,
    /// Receipt-cache capacity. Eviction order is arbitrary; see
    /// [`crate::receipt_cache::ReceiptCache`].
    pub receipt_cache_capacity: NonZeroUsize,
    /// Which durability gate the proxy waits on before acking a tx. See
    /// [`kardamom_types::AckPolicy`] for the four modes.
    pub ack_policy: AckPolicy,
    /// Max concurrent JSON-RPC connections. `submit_raw` parks each
    /// submission's request until its receipt arrives. So, at steady
    /// state, concurrent connections are about the offered rate times the
    /// receipt latency, and this count grows most when the pipeline is
    /// slowest. This value must exceed the offered rate times the receipt
    /// latency, so the connection table is never the limit and a
    /// replica never turns overload into connection refusals.
    pub rpc_max_connections: u32,
    /// Pending-registry depth. Past this depth, new submissions get an
    /// explicit retryable `Overloaded` error instead of being parked. A
    /// registry this deep means the pipeline is not draining. Parking more
    /// submits would only make the backlog worse, since a parked submit
    /// pins its connection and its sender's later nonces. A depth of 0
    /// sheds everything, as a test hook.
    pub pending_shed_depth: usize,
}

impl IngressConfig {
    /// The `tx_data` lane of `sender`: the shard map, or the identity rule
    /// over `partition_count_m`.
    #[inline]
    #[must_use]
    pub fn lane_for(&self, sender: alloy_primitives::Address) -> u32 {
        match &self.shard_map {
            Some(map) => u32::from(map.lane_for(sender)),
            None => crate::routing::partition_for(sender, self.partition_count_m),
        }
    }
}

impl Default for IngressConfig {
    fn default() -> Self {
        use nonzero_ext::nonzero;
        Self {
            jsonrpc_bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            binary_tcp_bind: None,
            binary_uds_path: None,
            partition_count_m: nonzero!(8u32),
            shard_map: None,
            ingress_id: 0,
            rate_limit_per_ip_per_sec: nonzero!(10_000u32),
            rate_limit_burst: nonzero!(1_000u32),
            sig_verify_batch_depth: nonzero!(64usize),
            sig_verify_flush_window: Duration::from_micros(50),
            pending_receipt_timeout: Duration::from_secs(30),
            chain_id: nonzero!(1u64),
            // 128k gives about a 27s query horizon at 4,800 tx/s (about
            // 77MB across both indexes at bench-receipt sizes). Eviction
            // order is arbitrary (DashMap), so the horizon is a lower
            // bound for only part of the entries. Fallbacks must poll
            // well inside it.
            receipt_cache_capacity: nonzero!(128 * 1024usize),
            ack_policy: AckPolicy::default(),
            rpc_max_connections: 8192,
            pending_shed_depth: 16_384,
        }
    }
}

/// TOML file that the `kardamom-ingress` binary parses from `--config`, for
/// the Aeron Cluster (Raft) client connection. The rest of the ingress
/// runtime tuning still comes from CLI flags and
/// [`IngressConfig::default`].
#[derive(Debug, Clone, serde::Deserialize, Default)]
#[serde(default)]
pub struct IngressFileConfig {
    /// Aeron Cluster (Raft) sealer client config. The on-quorum ack gate
    /// derives its durable watermark from this cluster's egress progress.
    pub cluster: ClusterConfig,
}

// The `[cluster]` TOML section has one definition. It mirrors the
// executor/sequencer shape by design, and every cluster client shares it.
// `kardamom-cluster-adapter` re-exports it here.
pub use kardamom_cluster_adapter::ClusterConfig;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_matches_spec() {
        let cfg = IngressConfig::default();
        assert_eq!(cfg.partition_count_m.get(), 8);
        assert_eq!(cfg.sig_verify_batch_depth.get(), 64);
        assert_eq!(cfg.sig_verify_flush_window, Duration::from_micros(50));
    }
}
