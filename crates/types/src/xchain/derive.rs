//! THE derivation rule: a batch of observed outbox messages to the record
//! the destination chain must contain next.

use alloc::vec::Vec;

use bytes::Bytes;

use super::ids::remote_source_hash;
use super::message::{OutboxMessage, RemoteEpochRecord, XChainMessage};

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
/// addresses a chain other than `self_chain_id`.
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
    if let Some(m) = ordered.iter().find(|m| m.dest_chain_id != self_chain_id) {
        return Err(XChainError::ForeignDestination {
            expected: self_chain_id,
            found: m.dest_chain_id,
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
}
