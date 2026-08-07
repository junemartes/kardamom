//! Chain-wide protocol limits shared by ingress validation and the exec core.

/// EIP-7825 per-transaction gas limit cap (2^24), in force since Osaka.
///
/// revm enforces this cap during tx validation whenever the spec is Osaka or
/// later — a tx with a higher gas limit is *invalid*, even though the block
/// gas limit (30M) is larger. The ingress rejects such txs at submission with
/// a clear error instead of letting total derivation burn them into a
/// `status=false` skip receipt.
///
/// This is a mirror of `revm::primitives::eip7825::TX_GAS_LIMIT_CAP` so the
/// ingress does not need a revm dependency; `kardamom-exec-core`'s
/// `cfg_pinning` test asserts the two stay equal.
pub const TX_GAS_LIMIT_CAP: u64 = 16_777_216;
