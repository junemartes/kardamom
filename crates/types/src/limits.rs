//! Protocol limits shared by ingress validation and the exec core.

use core::num::NonZeroU32;

/// The number of shards a deployment partitions transactions across.
/// Always at least 1, so a routing computation that divides by it can
/// never divide by zero.
///
/// Parse the CLI `shards` argument into this type at the boundary
/// (`ingress/src/bin/kardamom-ingress/main.rs`,
/// `executor/src/bin/kardamom-executor/args.rs`), and read `.get()`
/// downstream, so a zero shard count fails at startup instead of reaching
/// a routing computation that divides by it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ShardCount(NonZeroU32);

impl ShardCount {
    /// Returns `None` if `count` is 0.
    #[must_use]
    pub const fn new(count: u32) -> Option<Self> {
        match NonZeroU32::new(count) {
            Some(n) => Some(Self(n)),
            None => None,
        }
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// EIP-7825 gas limit cap for one transaction (2^24). This applies from Osaka.
///
/// revm enforces this cap during transaction validation from Osaka onward.
/// A transaction with a higher gas limit is invalid, even if the block gas
/// limit (30M) is larger. The ingress rejects such a transaction at
/// submission with a clear error. This stops total derivation from burning
/// the transaction into a `status=false` skip receipt.
///
/// This value mirrors `revm::primitives::eip7825::TX_GAS_LIMIT_CAP`. This way
/// the ingress does not need a revm dependency. The `cfg_pinning` test in
/// `kardamom-exec-core` checks that the two values stay equal.
pub const TX_GAS_LIMIT_CAP: u64 = 16_777_216;
