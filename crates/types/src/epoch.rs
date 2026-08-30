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
//!   * [`source_hash`] / [`source_hash_system`] — keccak over `(domain ||
//!     keccak(rlp[l1_block_hash, l1_log_index]))`. Domain 0 = user deposit,
//!     domain 1 = system tx. The canonical id used by downstream consumers to
//!     dedup deposits.
//!   * [`alias_l1_address`] — `L1 + 0x1111...1111` mod 2^160. Avoids
//!     collisions between L1 contracts and L2 contracts at the same address.
//!     EOAs round-trip harmlessly (their L2 alias is just a different EOA
//!     address).
//!   * [`DepositLog`] — the decoded shape of one `DepositInitiated` L1 log.
//!   * [`UpgradeLog`] — the decoded shape of one `UpgradeInitiated` L1 log (an
//!     **upgrade transaction**), and [`LockboxLog`], the union the watcher
//!     reads off the single lockbox address.
//!   * [`deposit_from_log`] / [`upgrade_from_log`] / [`derive_epoch`] —
//!     log(s) → [`Deposit`](crate::Deposit)s.
//!   * [`EpochRecord`] — an epoch as it travels on the canonical stream.
//!
//! Ported verbatim from PR #10's `crates/node/src/deposit.rs`; the
//! algorithm is OP-compatible and pinned by the contracts' bytecode-hash
//! CI gate, so the implementation here must stay byte-identical.

use alloc::vec::Vec;

use alloy_primitives::{Address, B256, Bytes as AlloyBytes, U256, keccak256};
use alloy_rlp::{Encodable, Header};
use bytes::Bytes;
use rkyv::{Archive, Deserialize, Serialize};

use crate::deposit::Deposit;
use crate::wire;

/// Source-hash domain for an ordinary user deposit (`ETHLockbox.depositETH`).
pub const DOMAIN_USER_DEPOSIT: u64 = 0;

/// Source-hash domain for a **system transaction** — today, the upgrade
/// transaction (`ETHLockbox.initiateUpgrade`).
///
/// Domain separation is what keeps a system deposit's id disjoint from a user
/// deposit's even when both are derived from the same `(block_hash, log_index)`
/// position, which is exactly what the downstream first-seen dedup keys on.
pub const DOMAIN_SYSTEM_TX: u64 = 1;

/// Compute the OP-style source hash for a user deposit:
///
/// ```text
///   deposit_id_hash = keccak256(rlp([l1_block_hash, l1_log_index]))
///   source_hash     = keccak256(rlp([domain = 0u64, deposit_id_hash]))
/// ```
pub fn source_hash(l1_block_hash: B256, l1_log_index: u64) -> B256 {
    source_hash_in_domain(DOMAIN_USER_DEPOSIT, l1_block_hash, l1_log_index)
}

/// Source hash for a system transaction — same construction as [`source_hash`]
/// under [`DOMAIN_SYSTEM_TX`].
pub fn source_hash_system(l1_block_hash: B256, l1_log_index: u64) -> B256 {
    source_hash_in_domain(DOMAIN_SYSTEM_TX, l1_block_hash, l1_log_index)
}

/// The shared construction behind both domains. One body so a change to the
/// hashing scheme cannot apply to user deposits and system txs unevenly.
fn source_hash_in_domain(domain: u64, l1_block_hash: B256, l1_log_index: u64) -> B256 {
    let inner = encode_list_two(&l1_block_hash, &l1_log_index);
    let deposit_id_hash = keccak256(&inner);

    // domain = 0u64 encodes as RLP 0x80 (canonical empty-string form for
    // integer zero); domain = 1u64 encodes as the single byte 0x01.
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

/// One decoded `UpgradeInitiated` L1 log — the **upgrade transaction**.
///
/// Emitted by `ETHLockbox.initiateUpgrade`, which is gated to the L1 factory
/// owner, so merely being in this list means the instruction was authorized on
/// L1. Derivation reproduces it verbatim; it never re-checks the authority,
/// because L1 already did and every node sees the same finalized logs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpgradeLog {
    /// Number of the L1 block this log was emitted in.
    pub block_number: u64,
    /// Hash of the L1 block this log was emitted in. Feeds `source_hash`.
    pub block_hash: B256,
    /// Position of this log within the L1 block. Feeds `source_hash`, and is
    /// the ORDERING key within an epoch — shared with deposits, since both
    /// kinds come from the same contract and interleave in one log stream.
    pub log_index: u64,
    /// The feature flag to schedule.
    pub feature_id: U256,
    /// Activation time in epoch-**milliseconds** (this chain's block-timestamp
    /// unit); `0` means "activate immediately", resolved on L2 to the
    /// activating block's timestamp.
    pub activation_timestamp: u64,
}

/// A decoded log from the lockbox — the one L1 contract the watcher reads.
///
/// Both variants derive into a [`Deposit`]; they differ in the source-hash
/// domain, the sender, and whether `is_system_transaction` is set. Keeping them
/// in one ordered list is what makes deposits and upgrades interleave by L1 log
/// index, so an upgrade cannot jump ahead of a deposit that L1 ordered first.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LockboxLog {
    /// `DepositInitiated` — a user bridging ETH.
    Deposit(DepositLog),
    /// `UpgradeInitiated` — an authorized upgrade transaction.
    Upgrade(UpgradeLog),
}

impl LockboxLog {
    /// Number of the L1 block this log was emitted in.
    pub fn block_number(&self) -> u64 {
        match self {
            Self::Deposit(l) => l.block_number,
            Self::Upgrade(l) => l.block_number,
        }
    }

    /// Hash of the L1 block this log was emitted in.
    pub fn block_hash(&self) -> B256 {
        match self {
            Self::Deposit(l) => l.block_hash,
            Self::Upgrade(l) => l.block_hash,
        }
    }

    /// Position of this log within its L1 block — the epoch ordering key.
    pub fn log_index(&self) -> u64 {
        match self {
            Self::Deposit(l) => l.log_index,
            Self::Upgrade(l) => l.log_index,
        }
    }
}

impl From<DepositLog> for LockboxLog {
    fn from(l: DepositLog) -> Self {
        Self::Deposit(l)
    }
}

impl From<UpgradeLog> for LockboxLog {
    fn from(l: UpgradeLog) -> Self {
        Self::Upgrade(l)
    }
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

/// Build the L2 system deposit for one L1 upgrade log.
///
/// Deliberately unlike [`deposit_from_log`] in three ways, each load-bearing:
///
///  * the sender is the fixed [`SYSTEM_UPGRADER`](crate::upgrades::SYSTEM_UPGRADER)
///    and is **not** aliased — aliasing exists to keep L1 senders from colliding
///    with L2 addresses, whereas this sender is *defined* on L2 and is the thing
///    the predeploy authorizes against;
///  * `mint`/`value` are zero: an upgrade moves no ETH (and the lockbox's
///    `initiateUpgrade` is not payable, so there is none to move);
///  * `is_system_transaction` is set — the first real use of the field, which
///    the wire type has carried as `false` since v0.
pub fn upgrade_from_log(log: &UpgradeLog) -> Deposit {
    Deposit {
        source_hash: source_hash_system(log.block_hash, log.log_index),
        from: crate::upgrades::SYSTEM_UPGRADER,
        to: Some(crate::upgrades::CHAIN_STATE),
        mint: 0,
        value: U256::ZERO,
        gas_limit: crate::upgrades::UPGRADE_TX_GAS_LIMIT,
        is_system_transaction: true,
        input: Bytes::copy_from_slice(
            crate::upgrades::encode_set_feature(log.feature_id, log.activation_timestamp).as_ref(),
        ),
    }
}

/// Build the L2 deposit for one lockbox log, whichever kind it is.
pub fn deposit_from_lockbox_log(log: &LockboxLog) -> Deposit {
    match log {
        LockboxLog::Deposit(l) => deposit_from_log(l),
        LockboxLog::Upgrade(l) => upgrade_from_log(l),
    }
}

/// THE derivation rule: L1 block `(l1_number, l1_hash)` plus its lockbox logs
/// (`DepositInitiated` **and** `UpgradeInitiated`) → the epoch the L2 chain
/// must contain.
///
/// Entries are ordered by **log index**, which is also what `source_hash`
/// commits to, so the order is a property of L1 rather than of whoever
/// happened to read it. `logs` may arrive in any order; callers need not
/// pre-sort. Logs from a different block than `l1_hash` are a programming
/// error and are rejected rather than silently mixed in.
///
/// Both log kinds share one index space because they are emitted by one
/// contract: an upgrade and a deposit in the same L1 block land in the L2 block
/// in the order L1 put them in, and `DuplicateLogIndex` catches a caller that
/// merged two log sets incorrectly — including one that fetched deposits and
/// upgrades in separate queries and double-counted a position.
pub fn derive_epoch(
    l1_number: u64,
    l1_hash: B256,
    logs: &[LockboxLog],
) -> Result<EpochRecord, EpochError> {
    let mut ordered: Vec<&LockboxLog> = Vec::with_capacity(logs.len());
    for log in logs {
        if log.block_hash() != l1_hash {
            return Err(EpochError::ForeignLog {
                expected: l1_hash,
                found: log.block_hash(),
            });
        }
        ordered.push(log);
    }
    ordered.sort_by_key(|l| l.log_index());
    if let Some(dup) = ordered
        .windows(2)
        .find(|w| w[0].log_index() == w[1].log_index())
    {
        return Err(EpochError::DuplicateLogIndex {
            log_index: dup[0].log_index(),
        });
    }
    Ok(EpochRecord {
        l1_number,
        l1_hash,
        deposits: ordered.into_iter().map(deposit_from_lockbox_log).collect(),
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

    fn deposit_log(block: B256, index: u64, to_byte: u8, mint: u128) -> DepositLog {
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

    fn log(block: B256, index: u64, to_byte: u8, mint: u128) -> LockboxLog {
        LockboxLog::Deposit(deposit_log(block, index, to_byte, mint))
    }

    fn upgrade_log(block: B256, index: u64, feature: u64, activation: u64) -> LockboxLog {
        LockboxLog::Upgrade(UpgradeLog {
            block_number: 0,
            block_hash: block,
            log_index: index,
            feature_id: U256::from(feature),
            activation_timestamp: activation,
        })
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
        let l = deposit_log(block, 4, 7, 100);
        let d = &derive_epoch(3, block, &[l.clone().into()])
            .unwrap()
            .deposits[0];
        assert_eq!(d.source_hash, source_hash(block, 4));
        assert_eq!(d.from, alias_l1_address(l.from));
        assert_eq!(d.to, Some(l.to));
        assert_eq!(d.value, U256::from(100u64));
        assert!(!d.is_system_transaction);
    }

    // ---------------------------------------------------------------------
    // Upgrade transactions (system deposits)
    // ---------------------------------------------------------------------

    #[test]
    fn system_domain_separates_from_the_user_domain_at_the_same_position() {
        // The whole point of the domain byte: a deposit and an upgrade at the
        // same (block, log_index) — impossible today, but the dedup key must
        // not rely on that — get different ids.
        let block = B256::repeat_byte(0x91);
        assert_ne!(source_hash(block, 3), source_hash_system(block, 3));
    }

    #[test]
    fn upgrade_derives_a_system_deposit_targeting_the_chain_state() {
        let block = B256::repeat_byte(0x21);
        let e = derive_epoch(4, block, &[upgrade_log(block, 2, 1, 0)]).unwrap();
        let d = &e.deposits[0];

        assert_eq!(d.source_hash, source_hash_system(block, 2));
        assert_eq!(d.from, crate::upgrades::SYSTEM_UPGRADER);
        assert_eq!(d.to, Some(crate::upgrades::CHAIN_STATE));
        assert!(d.is_system_transaction);
        // An upgrade moves no ETH.
        assert_eq!(d.mint, 0);
        assert_eq!(d.value, U256::ZERO);
        // Calldata is exactly setFeature(1, 0).
        assert_eq!(
            d.input.as_ref(),
            crate::upgrades::encode_set_feature(U256::from(1u64), 0).as_ref()
        );
    }

    #[test]
    fn upgrade_sender_is_not_aliased() {
        // Aliasing exists to separate L1 senders from L2 addresses; the system
        // sender is defined ON L2 and is what the predeploy authorizes against,
        // so aliasing it would break every upgrade.
        let block = B256::repeat_byte(0x22);
        let e = derive_epoch(1, block, &[upgrade_log(block, 0, 1, 0)]).unwrap();
        assert_eq!(e.deposits[0].from, crate::upgrades::SYSTEM_UPGRADER);
        assert_ne!(
            e.deposits[0].from,
            alias_l1_address(crate::upgrades::SYSTEM_UPGRADER)
        );
    }

    #[test]
    fn upgrade_carries_its_activation_timestamp_into_the_calldata() {
        let block = B256::repeat_byte(0x23);
        let ts = 1_700_000_004_000u64;
        let e = derive_epoch(1, block, &[upgrade_log(block, 0, 9, ts)]).unwrap();
        assert_eq!(
            e.deposits[0].input.as_ref(),
            crate::upgrades::encode_set_feature(U256::from(9u64), ts).as_ref()
        );
    }

    #[test]
    fn deposits_and_upgrades_interleave_in_l1_log_order() {
        // One contract, one log stream: an upgrade must not jump ahead of a
        // deposit L1 ordered first, in either input order.
        let block = B256::repeat_byte(0x24);
        let forward = derive_epoch(
            6,
            block,
            &[
                log(block, 0, 1, 10),
                upgrade_log(block, 1, 1, 0),
                log(block, 2, 2, 20),
            ],
        )
        .unwrap();
        let shuffled = derive_epoch(
            6,
            block,
            &[
                log(block, 2, 2, 20),
                log(block, 0, 1, 10),
                upgrade_log(block, 1, 1, 0),
            ],
        )
        .unwrap();

        assert_eq!(forward, shuffled);
        assert_eq!(forward.deposits.len(), 3);
        assert!(!forward.deposits[0].is_system_transaction);
        assert!(forward.deposits[1].is_system_transaction);
        assert!(!forward.deposits[2].is_system_transaction);
    }

    #[test]
    fn an_upgrade_colliding_with_a_deposit_log_index_is_rejected() {
        let block = B256::repeat_byte(0x25);
        let err = derive_epoch(
            1,
            block,
            &[log(block, 3, 1, 5), upgrade_log(block, 3, 1, 0)],
        )
        .unwrap_err();
        assert!(
            matches!(err, EpochError::DuplicateLogIndex { log_index: 3 }),
            "got {err:?}"
        );
    }

    #[test]
    fn foreign_upgrade_log_is_rejected() {
        let block = B256::repeat_byte(0x26);
        let other = B256::repeat_byte(0x27);
        let err = derive_epoch(1, block, &[upgrade_log(other, 0, 1, 0)]).unwrap_err();
        assert!(matches!(err, EpochError::ForeignLog { .. }), "got {err:?}");
    }

    #[test]
    fn upgrade_derivation_is_deterministic_across_callers() {
        // Same producer/verifier equality the user-deposit path relies on: the
        // validator re-derives upgrades from L1 and fail-stops on any diff.
        let block = B256::repeat_byte(0x28);
        let logs = [upgrade_log(block, 1, 3, 999), log(block, 0, 4, 1)];
        assert_eq!(
            derive_epoch(2, block, &logs).unwrap(),
            derive_epoch(2, block, &logs).unwrap()
        );
    }

    #[test]
    fn system_source_hash_known_vector() {
        // Anchored like the user-deposit vector above: the id a system deposit
        // dedups on is consensus-visible, so a change must be deliberate.
        //
        // Cross-checked against a hand-built RLP encoding rather than captured
        // from this implementation, so the vector is independent evidence:
        //   inner = e2 a0 <32-byte block hash> 2a          (list, payload 34)
        //   outer = e2 01 a0 keccak(inner)                 (domain 1 is a bare
        //                                                   0x01 byte, not 0x81 0x01)
        // The same procedure with domain 0 (encoded 0x80) reproduces the
        // user-domain vector below, which confirms the method.
        let h = source_hash_system(
            b256!("0000000000000000000000000000000000000000000000000000000000000001"),
            42,
        );
        assert_eq!(
            h,
            b256!("60a7cd0721ff0987010cb857c9439ed4078bec21893625b554f27c03787047cc")
        );
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
