//! Static configuration for an `IngressProxy` instance.

use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::time::Duration;

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
}

impl Default for IngressConfig {
    fn default() -> Self {
        use nonzero_ext::nonzero;
        Self {
            jsonrpc_bind: "127.0.0.1:0".parse().unwrap(),
            binary_tcp_bind: None,
            binary_uds_path: None,
            partition_count_m: 8,
            rate_limit_per_ip_per_sec: nonzero!(10_000u32),
            rate_limit_burst: nonzero!(1_000u32),
            sig_verify_batch_depth: 64,
            sig_verify_flush_window: Duration::from_micros(50),
            pending_receipt_timeout: Duration::from_secs(30),
            chain_id: 1,
            receipt_cache_capacity: 64 * 1024,
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
