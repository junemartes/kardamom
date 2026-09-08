//! THE derivation rule: a batch of observed outbox messages to the record
//! the destination chain must contain next.

use alloc::vec::Vec;

use bytes::Bytes;

use alloy_primitives::B256;

use super::ids::{remote_source_hash, xchain_anchor_hash};
use super::message::{OutboxMessage, RemoteEpochRecord, XChainMessage};
use super::{MAX_DATA_BYTES, MAX_MESSAGE_GAS};

/// `expected_first_seq` is the deriving side's cursor for the pair (one past
/// the last message already canonicalised). Messages may arrive in any order;
/// callers need not pre-sort. Every violation is an error rather than a
/// repair: for a VERIFIER these are chain faults, for a PRODUCER bugs or a
/// malicious feed — either way, fail stop, never skip or reorder.
///
/// # Errors
///
/// Returns [`XChainError::Empty`] if `msgs` is empty; [`XChainError::DuplicateSeq`]
/// if two messages share a `seq`; [`XChainError::SeqSkipped`] or
/// [`XChainError::SeqRegressed`] if the batch does not start at, or is not
/// dense from, `expected_first_seq`; [`XChainError::SeqOverflow`] if a
/// `seq` this close to `u64::MAX` would overflow computing the next
/// expected value; and [`XChainError::ForeignDestination`] if a message
/// addresses a chain other than `self_chain_id`; and
/// [`XChainError::MultiBlockBatch`] if the batch spans more than one origin
/// block.
pub fn derive_remote_epoch(
    self_chain_id: u64,
    origin_chain_id: u64,
    expected_first_seq: u64,
    msgs: &[OutboxMessage],
) -> Result<RemoteEpochRecord, XChainError> {
    let mut ordered: Vec<&OutboxMessage> = msgs.iter().collect();
    ordered.sort_by_key(|m| m.seq);

    // Parsed once: `split_first` proves `ordered` non-empty, and hands back
    // both halves, so the last-element access below reuses that same proof
    // (`rest.last()`, falling back to `first_msg` for a one-element batch)
    // instead of re-deriving it.
    let (first_msg, rest) = ordered.split_first().ok_or(XChainError::Empty)?;
    let first = first_msg.seq;

    if let Some(dup) = ordered.windows(2).find(|w| w[0].seq == w[1].seq) {
        return Err(XChainError::DuplicateSeq { seq: dup[0].seq });
    }
    if first != expected_first_seq {
        return Err(if first < expected_first_seq {
            XChainError::SeqRegressed {
                expected: expected_first_seq,
                found: first,
            }
        } else {
            XChainError::SeqSkipped {
                expected: expected_first_seq,
                found: first,
            }
        });
    }
    for w in ordered.windows(2) {
        let expected_next = w[0]
            .seq
            .checked_add(1)
            .ok_or(XChainError::SeqOverflow { seq: w[0].seq })?;
        if w[1].seq != expected_next {
            return Err(XChainError::SeqSkipped {
                expected: expected_next,
                found: w[1].seq,
            });
        }
    }
    // The record's seq range must fit in u64. The validator computes
    // `last_seq + 1` as its next cursor, so the last seq must leave room
    // for one more.
    let len = u64::try_from(ordered.len()).map_err(|_| XChainError::SeqOverflow { seq: first })?;
    if first.checked_add(len).is_none() {
        return Err(XChainError::SeqOverflow { seq: first });
    }
    if let Some(m) = ordered.iter().find(|m| m.dest_chain_id != self_chain_id) {
        return Err(XChainError::ForeignDestination {
            expected: self_chain_id,
            found: m.dest_chain_id,
        });
    }
    ordered.iter().try_for_each(|m| check_outbox_bounds(m))?;

    // One record is one origin block. The record's anchor is the block of
    // its LAST message, so a batch that spans two blocks would anchor its
    // earlier messages at a later block. The watcher cuts batches at block
    // edges, so a spanning batch is a producer bug or a malicious feed.
    let first_block = first_msg.origin_block_number;
    if let Some(m) = rest.iter().find(|m| m.origin_block_number != first_block) {
        return Err(XChainError::MultiBlockBatch {
            first_block,
            found_block: m.origin_block_number,
        });
    }

    let last = rest.last().unwrap_or(first_msg);
    Ok(RemoteEpochRecord {
        origin_chain_id,
        anchor_number: last.origin_block_number,
        anchor_hash: last.origin_block_hash,
        first_seq: first,
        messages: ordered
            .into_iter()
            .map(|m| XChainMessage {
                source_hash: remote_source_hash(origin_chain_id, m.seq),
                seq: m.seq,
                origin_sender: m.sender,
                target: m.target,
                value: m.value,
                gas_limit: m.gas_limit,
                input: Bytes::copy_from_slice(m.data.as_ref()),
                callback: m.callback,
            })
            .collect(),
    })
}

/// Producer-side mirror of the `Outbox.sendMessage` checks. An honest
/// origin can never trip these. A feed that does is malicious or corrupt,
/// and the record must not reach the canonical stream: the destination
/// budgets delivery from these bounds.
fn check_outbox_bounds(m: &OutboxMessage) -> Result<(), XChainError> {
    if m.value != 0 {
        return Err(XChainError::ValueNotAllowed {
            seq: m.seq,
            value: m.value,
        });
    }
    if m.gas_limit > MAX_MESSAGE_GAS {
        return Err(XChainError::GasLimitAboveCap {
            seq: m.seq,
            gas_limit: m.gas_limit,
            cap: MAX_MESSAGE_GAS,
        });
    }
    if m.data.len() > MAX_DATA_BYTES {
        return Err(XChainError::DataAboveCap {
            seq: m.seq,
            len: m.data.len(),
            cap: MAX_DATA_BYTES,
        });
    }
    Ok(())
}

/// Why a remote epoch could not be derived. All variants are fail-stop for a
/// verifier and bugs (or a malicious feed) for a producer.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum XChainError {
    #[error("remote epochs advance only with messages; an empty batch is invalid")]
    Empty,
    #[error("two outbox messages share seq {seq} for one origin")]
    DuplicateSeq { seq: u64 },
    #[error("outbox seq skipped: expected {expected}, found {found}")]
    SeqSkipped { expected: u64, found: u64 },
    #[error("outbox seq regressed: expected {expected}, found {found}")]
    SeqRegressed { expected: u64, found: u64 },
    #[error("outbox seq {seq} has no successor (u64 overflow)")]
    SeqOverflow { seq: u64 },
    #[error("message for chain {found} in a batch derived by chain {expected}")]
    ForeignDestination { expected: u64, found: u64 },
    /// v1 messaging carries no value. The `Outbox` rejects a nonzero value,
    /// so this can only come from a malicious or corrupt feed.
    #[error("outbox message seq {seq} carries value {value}; v1 delivery is value-free")]
    ValueNotAllowed { seq: u64, value: u128 },
    /// `gas_limit` above [`MAX_MESSAGE_GAS`], which the `Outbox` rejects.
    #[error("outbox message seq {seq} gas limit {gas_limit} is above the cap {cap}")]
    GasLimitAboveCap { seq: u64, gas_limit: u64, cap: u64 },
    /// `data` longer than [`MAX_DATA_BYTES`], which the `Outbox` rejects.
    #[error("outbox message seq {seq} data length {len} is above the cap {cap}")]
    DataAboveCap { seq: u64, len: usize, cap: usize },
    /// A batch spans more than one origin block. One record anchors at one
    /// block, so the watcher must cut the batch at the block edge.
    #[error("batch spans origin blocks {first_block} and {found_block}; one record is one block")]
    MultiBlockBatch { first_block: u64, found_block: u64 },
    /// A feed message carries an `origin_block_hash` that is not
    /// [`xchain_anchor_hash`] of its `(origin, block)`. The anchor is a pure
    /// function of the position, so the feed must not choose it.
    #[error(
        "message seq {seq} at origin block {block} carries anchor {carried}, expected {expected}"
    )]
    AnchorMismatch {
        seq: u64,
        block: u64,
        carried: B256,
        expected: B256,
    },
}

/// Reject a feed message whose `origin_block_hash` is not the anchor that
/// [`xchain_anchor_hash`] computes for its position. The watcher runs this
/// on every message BEFORE derivation, so the feed never chooses the anchor.
///
/// # Errors
///
/// Returns [`XChainError::AnchorMismatch`] if the carried anchor differs
/// from the computed one.
pub fn check_anchor(origin_chain_id: u64, msg: &OutboxMessage) -> Result<(), XChainError> {
    let expected = xchain_anchor_hash(origin_chain_id, msg.origin_block_number);
    if msg.origin_block_hash == expected {
        Ok(())
    } else {
        Err(XChainError::AnchorMismatch {
            seq: msg.seq,
            block: msg.origin_block_number,
            carried: msg.origin_block_hash,
            expected,
        })
    }
}
