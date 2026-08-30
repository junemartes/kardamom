//! Protocol limits shared by ingress validation and the exec core.

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
