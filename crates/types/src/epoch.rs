//! L1 **epoch** derivation — the single definition of "which deposits does
//! L1 block N contain, and in what order".
//!
//! This lives in `kardamom-types`, not in the DA watcher, on purpose. Under
//! `docs/agents/l1-origin-deposit-derivation-spec.md` the same rule is run by
//! two parties for opposite reasons: the **producer** (the DA watcher) emits
//! an epoch's deposits into the canonical stream, and the **verifier** (the
//! validator, later the executor) re-derives them from L1 and rejects a chain
//! that disagrees. A verifier running a second, subtly different copy of the
//! rule would verify nothing — so there is exactly one copy, here.
//!
//! Contents:
//!
//!   * [`source_hash`] — keccak over `(domain || keccak(rlp[l1_block_hash,
//!     l1_log_index]))`. Domain 0 = user deposit. The canonical id used by
//!     downstream consumers to dedup deposits.
//!   * [`alias_l1_address`] — `L1 + 0x1111...1111` mod 2^160. Avoids
//!     collisions between L1 contracts and L2 contracts at the same address.
//!     EOAs round-trip harmlessly (their L2 alias is just a different EOA
//!     address).
//!   * [`DepositLog`] — the decoded shape of one `DepositInitiated` L1 log.
//!   * [`deposit_from_log`] / [`derive_epoch`] — log(s) → [`Deposit`](crate::Deposit)s.
//!   * [`EpochRecord`] — an epoch as it travels on the canonical stream.
//!
//! Ported verbatim from PR #10's `crates/node/src/deposit.rs`; the
//! algorithm is OP-compatible and pinned by the contracts' bytecode-hash
//! CI gate, so the implementation here must stay byte-identical.

use alloy_primitives::{Address, B256, Bytes as AlloyBytes, U256, keccak256};
use alloy_rlp::{Encodable, Header};
use bytes::Bytes;
use rkyv::{Archive, Deserialize, Serialize};

use crate::deposit::Deposit;
use crate::wire;

/// Compute the OP-style source hash for a user deposit:
///
/// ```text
///   deposit_id_hash = keccak256(rlp([l1_block_hash, l1_log_index]))
///   source_hash     = keccak256(rlp([domain = 0u64, deposit_id_hash]))
/// ```
///
/// Domain 0 = user deposit; domain 1 (reserved) = L1-attributes / system tx.
pub fn source_hash(l1_block_hash: B256, l1_log_index: u64) -> B256 {
    let inner = encode_list_two(&l1_block_hash, &l1_log_index);
    let deposit_id_hash = keccak256(&inner);

    // domain = 0u64 encodes as RLP 0x80 (canonical empty-string form for
    // integer zero).
    let domain: u64 = 0;
    let outer = encode_list_two(&domain, &deposit_id_hash);
    keccak256(&outer)
}

fn encode_list_two<A: Encodable, B: Encodable>(a: &A, b: &B) -> Vec<u8> {
    let mut buf = Vec::new();
    let payload_len = a.length() + b.length();
    Header {
        list: true,
        payload_length: payload_len,
    }
    .encode(&mut buf);
    a.encode(&mut buf);
    b.encode(&mut buf);
    buf
}

/// Add the OP-style aliasing offset `0x1111...1111` to an L1 sender,
/// wrapping at uint160. EOAs round-trip harmlessly; L1 contracts shift to
/// avoid colliding with L2 contracts at the same address. The DA watcher
/// applies this — the L2 executor never sees the un-aliased L1 sender.
pub fn alias_l1_address(l1: Address) -> Address {
    const OFFSET: [u8; 20] = [
        0x11, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x11, 0x11,
    ];

    let mut a = [0u8; 32];
    a[12..].copy_from_slice(l1.as_slice());
    let mut b = [0u8; 32];
    b[12..].copy_from_slice(&OFFSET);

    let sum = U256::from_be_bytes(a).wrapping_add(U256::from_be_bytes(b));
    let sum_bytes = sum.to_be_bytes::<32>();

    let mut out = [0u8; 20];
    out.copy_from_slice(&sum_bytes[12..32]);
    Address::from(out)
}

/// One decoded `DepositInitiated` L1 log.
///
/// Pure data: the transport that produced it (an RPC `eth_getLogs`, an
/// archive replay, a test fixture) is not this type's business, which is what
/// lets the producer and the verifier share [`derive_epoch`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DepositLog {
    /// Number of the L1 block this log was emitted in. Not part of
    /// `source_hash` — it is the key the watcher groups logs by when it
    /// splits a multi-block log query into one epoch per L1 block.
    pub block_number: u64,
    /// Hash of the L1 block this log was emitted in. Feeds `source_hash`.
    pub block_hash: B256,
    /// Position of this log within the L1 block. Feeds `source_hash`, and is
    /// the ORDERING key within an epoch.
    pub log_index: u64,
    /// L1 sender (un-aliased).
    pub from: Address,
    /// L2 recipient of the credited mint.
    pub to: Address,
    /// Amount minted on L2 (and forwarded as `value` in the inner EVM call).
    /// Wire type on L1 is `uint256`; decoders reject `mint > u128::MAX`.
    pub mint: u128,
    /// Gas limit for the inner EVM call.
    pub gas_limit: u64,
    /// Optional calldata for the inner EVM call. Alloy's `Bytes` — this is
    /// the shape the L1 ABI decoder yields; `Deposit::input` is the wire
    /// shape and `deposit_from_log` converts.
    pub data: AlloyBytes,
}

/// One L1 epoch's contribution to the L2 chain, as it travels on the
/// canonical stream.
///
/// Atomic by construction: the origin and its deposits are one record, so a
/// partially-delivered epoch (origin ordered, deposits lost) cannot happen —
/// which is what lets the no-skipping rule be enforced. Carrying the deposits
/// by VALUE rather than as refs also removes the ref↔envelope join that makes
/// a lost `tx_deposits` envelope fatal today.
#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct EpochRecord {
    /// L1 block number this epoch corresponds to. Becomes the `l1_origin` of
    /// the L2 blocks it opens.
    pub l1_number: u64,
    /// Hash of that L1 block. Only meaningful because the origin is required
    /// to be at or below L1 finality, so a number maps to one hash forever
    /// and no L1-reorg handling is needed.
    #[rkyv(with = wire::B256Bytes)]
    pub l1_hash: B256,
    /// The epoch's deposits in log-index order. EMPTY IS NORMAL and must
    /// still be emitted: the no-skipping rule is only enforceable if every
    /// epoch appears.
    pub deposits: Vec<Deposit>,
}

impl EpochRecord {
    /// Canonical id for cluster dedup. Racing producers that observe the same
    /// L1 block derive byte-identical records, so first-seen dedup collapses
    /// them — the property the cluster already relies on for `DepositRef`.
    pub fn canonical_id(&self) -> B256 {
        keccak256(self.l1_hash.as_slice())
    }
}

/// Build the L2 deposit for one L1 log.
pub fn deposit_from_log(log: &DepositLog) -> Deposit {
    Deposit {
        source_hash: source_hash(log.block_hash, log.log_index),
        from: alias_l1_address(log.from),
        // Deposits from the ETHLockbox always target an L2 recipient
        // (CREATE deposits would need a different L1 event shape and are
        // not currently produced by the lockbox).
        to: Some(log.to),
        mint: log.mint,
        // For ETH-bridge deposits `value` == `mint` (the minted ETH is
        // forwarded to the recipient). When the lockbox evolves to support
        // mint-without-forward, decouple here.
        value: U256::from(log.mint),
        gas_limit: log.gas_limit,
        is_system_transaction: false,
        input: Bytes::copy_from_slice(log.data.as_ref()),
    }
}

/// THE derivation rule: L1 block `(l1_number, l1_hash)` plus its
/// `DepositInitiated` logs → the epoch the L2 chain must contain.
///
/// Deposits are ordered by **log index**, which is also what `source_hash`
/// commits to, so the order is a property of L1 rather than of whoever
/// happened to read it. `logs` may arrive in any order; callers need not
/// pre-sort. Logs from a different block than `l1_hash` are a programming
/// error and are rejected rather than silently mixed in.
pub fn derive_epoch(
    l1_number: u64,
    l1_hash: B256,
    logs: &[DepositLog],
) -> Result<EpochRecord, EpochError> {
    let mut ordered: Vec<&DepositLog> = Vec::with_capacity(logs.len());
    for log in logs {
        if log.block_hash != l1_hash {
            return Err(EpochError::ForeignLog {
                expected: l1_hash,
                found: log.block_hash,
            });
        }
        ordered.push(log);
    }
    ordered.sort_by_key(|l| l.log_index);
    if let Some(dup) = ordered
        .windows(2)
        .find(|w| w[0].log_index == w[1].log_index)
    {
        return Err(EpochError::DuplicateLogIndex {
            log_index: dup[0].log_index,
        });
    }
    Ok(EpochRecord {
        l1_number,
        l1_hash,
        deposits: ordered.into_iter().map(deposit_from_log).collect(),
    })
}

/// Why an epoch could not be derived. Both variants indicate the caller fed
/// logs that do not belong together, which for a VERIFIER is a rejection and
/// for a PRODUCER is a bug.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EpochError {
    #[error("log from L1 block {found} in an epoch for {expected}")]
    ForeignLog { expected: B256, found: B256 },
    #[error("two deposit logs share log index {log_index} in one L1 block")]
    DuplicateLogIndex { log_index: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, address, b256};

    #[test]
    fn alias_zero_address_is_offset() {
        let l1 = Address::ZERO;
        let expected = address!("1111000000000000000000000000000000001111");
        assert_eq!(alias_l1_address(l1), expected);
    }

    #[test]
    fn alias_wraps_at_uint160() {
        // 2^160 - 1 + 0x1111...1111 (mod 2^160) = 0x1111...1110.
        let l1 = address!("ffffffffffffffffffffffffffffffffffffffff");
        let expected = address!("1111000000000000000000000000000000001110");
        assert_eq!(alias_l1_address(l1), expected);
    }

    #[test]
    fn source_hash_is_deterministic() {
        let block = B256::repeat_byte(0x11);
        let h1 = source_hash(block, 0);
        let h2 = source_hash(block, 0);
        assert_eq!(h1, h2);
    }

    #[test]
    fn source_hash_differs_for_different_log_indices() {
        let block = B256::repeat_byte(0xAB);
        let a = source_hash(block, 0);
        let b = source_hash(block, 1);
        assert_ne!(a, b);
    }

    fn log(block: B256, index: u64, to_byte: u8, mint: u128) -> DepositLog {
        DepositLog {
            block_number: 0,
            block_hash: block,
            log_index: index,
            from: Address::repeat_byte(0xA1),
            to: Address::repeat_byte(to_byte),
            mint,
            gas_limit: 200_000,
            data: AlloyBytes::new(),
        }
    }

    #[test]
    fn epoch_orders_by_log_index_regardless_of_input_order() {
        // The order is a property of L1, not of whoever read the logs — a
        // verifier that fetched them in a different order must derive the
        // same epoch, byte for byte.
        let block = B256::repeat_byte(0x77);
        let forward =
            derive_epoch(9, block, &[log(block, 0, 1, 10), log(block, 1, 2, 20)]).unwrap();
        let reversed =
            derive_epoch(9, block, &[log(block, 1, 2, 20), log(block, 0, 1, 10)]).unwrap();
        assert_eq!(forward, reversed);
        assert_eq!(forward.deposits.len(), 2);
        assert_eq!(forward.deposits[0].mint, 10);
        assert_eq!(forward.deposits[1].mint, 20);
    }

    #[test]
    fn empty_epoch_is_valid_and_must_still_exist() {
        // Rule 2 (no skipping) is only enforceable if depositless epochs are
        // still emitted, so deriving one is explicitly not an error.
        let block = B256::repeat_byte(0x33);
        let e = derive_epoch(12, block, &[]).unwrap();
        assert!(e.deposits.is_empty());
        assert_eq!(e.l1_number, 12);
        assert_eq!(e.l1_hash, block);
    }

    #[test]
    fn foreign_log_is_rejected_not_silently_mixed_in() {
        let block = B256::repeat_byte(0x01);
        let other = B256::repeat_byte(0x02);
        let err = derive_epoch(1, block, &[log(other, 0, 1, 5)]).unwrap_err();
        assert!(matches!(err, EpochError::ForeignLog { .. }), "got {err:?}");
    }

    #[test]
    fn duplicate_log_index_is_rejected() {
        // Two logs at the same index would produce two deposits with the
        // SAME source_hash — a dedup collision that must never reach the
        // canonical stream.
        let block = B256::repeat_byte(0x04);
        let err = derive_epoch(1, block, &[log(block, 3, 1, 5), log(block, 3, 2, 6)]).unwrap_err();
        assert!(
            matches!(err, EpochError::DuplicateLogIndex { log_index: 3 }),
            "got {err:?}"
        );
    }

    #[test]
    fn epoch_derivation_is_deterministic_across_callers() {
        // The producer/verifier equality this whole design rests on.
        let block = B256::repeat_byte(0x55);
        let logs = [log(block, 2, 9, 7), log(block, 0, 8, 3)];
        assert_eq!(
            derive_epoch(5, block, &logs).unwrap(),
            derive_epoch(5, block, &logs).unwrap()
        );
    }

    #[test]
    fn canonical_id_is_stable_per_l1_block() {
        let a = derive_epoch(1, B256::repeat_byte(0xAA), &[]).unwrap();
        let b = derive_epoch(1, B256::repeat_byte(0xAA), &[]).unwrap();
        let c = derive_epoch(2, B256::repeat_byte(0xBB), &[]).unwrap();
        assert_eq!(a.canonical_id(), b.canonical_id());
        assert_ne!(a.canonical_id(), c.canonical_id());
    }

    #[test]
    fn deposits_carry_aliased_sender_and_derived_source_hash() {
        let block = B256::repeat_byte(0x66);
        let l = log(block, 4, 7, 100);
        let d = &derive_epoch(3, block, std::slice::from_ref(&l))
            .unwrap()
            .deposits[0];
        assert_eq!(d.source_hash, source_hash(block, 4));
        assert_eq!(d.from, alias_l1_address(l.from));
        assert_eq!(d.to, Some(l.to));
        assert_eq!(d.value, U256::from(100u64));
        assert!(!d.is_system_transaction);
    }

    #[test]
    fn source_hash_known_vector_matches_op_form() {
        // Anchored output for a fixed (l1_block_hash, log_index) pair —
        // changes to the algorithm flip this and force a code-review
        // conversation. The value below was captured from the first run
        // against the OP-aligned algorithm ported from PR #10's
        // `crates/node/src/deposit.rs` (whose own conformance is pinned by
        // the contracts' bytecode-hash CI gate).
        let h = source_hash(
            b256!("0000000000000000000000000000000000000000000000000000000000000001"),
            42,
        );
        assert_eq!(
            h,
            b256!("fce50386841795079cfbaa39a7061f9f746945afa35650f060fa5935c4462c61")
        );
    }
}
