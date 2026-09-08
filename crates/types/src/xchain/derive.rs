//! THE derivation rule: a batch of observed outbox messages to the record
//! the destination chain must contain next.

use alloc::vec::Vec;

use bytes::Bytes;

use alloy_primitives::B256;

use super::ids::{Anchor, remote_source_hash};
use super::message::{BoundsFault, NonEmptyVec, OutboxMessage, RemoteEpochRecord, XChainMessage};

/// The producer/verifier batch, sorted by `seq`. Each method below checks
/// one verdict; [`derive_remote_epoch`] chains them into the derivation
/// rule.
///
/// `first` is the batch's own state (every check reads it), so it lives on
/// the struct instead of traveling as a parameter to each method.
struct Batch<'a> {
    expected_first_seq: u64,
    first: &'a OutboxMessage,
    ordered: Vec<&'a OutboxMessage>,
}

impl<'a> Batch<'a> {
    /// Sorts `msgs` by `seq` and names its first message. Parsed once:
    /// this is the batch's only non-empty check, and every later step
    /// reuses `first` instead of re-deriving it.
    ///
    /// # Errors
    ///
    /// Returns [`XChainError::Empty`] if `msgs` is empty.
    fn new(expected_first_seq: u64, msgs: &'a [OutboxMessage]) -> Result<Self, XChainError> {
        let mut ordered: Vec<&OutboxMessage> = msgs.iter().collect();
        ordered.sort_by_key(|m| m.seq);
        let first = *ordered.first().ok_or(XChainError::Empty)?;
        Ok(Self {
            expected_first_seq,
            first,
            ordered,
        })
    }

    /// Every message after the first, in seq order.
    fn rest(&self) -> &[&'a OutboxMessage] {
        &self.ordered[1..]
    }

    /// No two messages share a `seq`.
    fn check_no_duplicates(&self) -> Result<(), XChainError> {
        if let Some(dup) = self.ordered.windows(2).find(|w| w[0].seq == w[1].seq) {
            return Err(XChainError::DuplicateSeq { seq: dup[0].seq });
        }
        Ok(())
    }

    /// The batch starts at, and is dense from, `expected_first_seq`.
    fn check_starts_at_expected(&self) -> Result<(), XChainError> {
        let first = self.first.seq;
        if first == self.expected_first_seq {
            return Ok(());
        }
        Err(if first < self.expected_first_seq {
            XChainError::SeqRegressed {
                expected: self.expected_first_seq,
                found: first,
            }
        } else {
            XChainError::SeqSkipped {
                expected: self.expected_first_seq,
                found: first,
            }
        })
    }

    /// Every consecutive pair advances by exactly 1: no internal skip.
    fn check_dense(&self) -> Result<(), XChainError> {
        self.ordered.windows(2).try_for_each(|w| {
            let expected_next = w[0]
                .seq
                .checked_add(1)
                .ok_or(XChainError::SeqOverflow { seq: w[0].seq })?;
            if w[1].seq == expected_next {
                Ok(())
            } else {
                Err(XChainError::SeqSkipped {
                    expected: expected_next,
                    found: w[1].seq,
                })
            }
        })
    }

    /// The record's seq range fits in `u64`, with room for the next cursor
    /// (`last_seq + 1`), which the validator computes as its lane position.
    fn check_range_fits(&self) -> Result<(), XChainError> {
        let first = self.first.seq;
        let len = u64::try_from(self.ordered.len())
            .map_err(|_| XChainError::SeqOverflow { seq: first })?;
        if first.checked_add(len).is_some() {
            Ok(())
        } else {
            Err(XChainError::SeqOverflow { seq: first })
        }
    }

    /// Every message addresses `self_chain_id`.
    fn check_destination(&self, self_chain_id: u64) -> Result<(), XChainError> {
        if let Some(m) = self
            .ordered
            .iter()
            .find(|m| m.dest_chain_id != self_chain_id)
        {
            return Err(XChainError::ForeignDestination {
                expected: self_chain_id,
                found: m.dest_chain_id,
            });
        }
        Ok(())
    }

    /// One record is one origin block: the record's anchor is the block of
    /// its LAST message, so a batch that spans two blocks would anchor its
    /// earlier messages at a later block. The watcher cuts batches at block
    /// edges, so a spanning batch is a producer bug or a malicious feed.
    fn check_one_block(&self) -> Result<(), XChainError> {
        let first_block = self.first.origin_block_number;
        if let Some(m) = self
            .ordered
            .iter()
            .find(|m| m.origin_block_number != first_block)
        {
            return Err(XChainError::MultiBlockBatch {
                first_block,
                found_block: m.origin_block_number,
            });
        }
        Ok(())
    }
}

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
/// expected value; [`XChainError::ForeignDestination`] if a message
/// addresses a chain other than `self_chain_id`; [`XChainError::MultiBlockBatch`]
/// if the batch spans more than one origin block; and [`XChainError::Bounds`]
/// if a message violates the Outbox's own send-time bounds.
pub fn derive_remote_epoch(
    self_chain_id: u64,
    origin_chain_id: u64,
    expected_first_seq: u64,
    msgs: &[OutboxMessage],
) -> Result<RemoteEpochRecord, XChainError> {
    let batch = Batch::new(expected_first_seq, msgs)?;

    batch.check_no_duplicates()?;
    batch.check_starts_at_expected()?;
    batch.check_dense()?;
    batch.check_range_fits()?;
    batch.check_destination(self_chain_id)?;
    batch.ordered.iter().try_for_each(|m| m.check_bounds())?;
    batch.check_one_block()?;

    let to_xchain_message = |m: &OutboxMessage| XChainMessage {
        source_hash: remote_source_hash(origin_chain_id, m.seq),
        seq: m.seq,
        origin_sender: m.sender,
        target: m.target,
        value: m.value,
        gas_limit: m.gas_limit,
        input: Bytes::copy_from_slice(m.data.as_ref()),
        callback: m.callback,
    };
    let messages = NonEmptyVec::new(
        to_xchain_message(batch.first),
        batch.rest().iter().map(|m| to_xchain_message(m)).collect(),
    );

    let last = batch.rest().last().copied().unwrap_or(batch.first);
    Ok(RemoteEpochRecord {
        origin_chain_id,
        anchor_number: last.origin_block_number,
        anchor_hash: last.origin_block_hash,
        first_seq: batch.first.seq,
        messages,
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
    /// A message violates the Outbox's own send-time bounds (value, gas
    /// limit, or data length). The `Outbox` rejects each of these at send
    /// time, so a message that carries one did not come from the shared
    /// derivation rule.
    #[error("outbox {0}")]
    Bounds(#[from] BoundsFault),
    /// A batch spans more than one origin block. One record anchors at one
    /// block, so the watcher must cut the batch at the block edge.
    #[error("batch spans origin blocks {first_block} and {found_block}; one record is one block")]
    MultiBlockBatch { first_block: u64, found_block: u64 },
    /// A feed message carries an `origin_block_hash` that is not
    /// [`Anchor::hash`] of its `(origin, block)`. The anchor is a pure
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

impl OutboxMessage {
    /// Reject a feed message whose `origin_block_hash` is not the anchor
    /// that [`Anchor::hash`] computes for its position. The watcher runs
    /// this on every message BEFORE derivation, so the feed never chooses
    /// the anchor.
    ///
    /// # Errors
    ///
    /// Returns [`XChainError::AnchorMismatch`] if the carried anchor
    /// differs from the computed one.
    pub fn check_anchor(&self, origin_chain_id: u64) -> Result<(), XChainError> {
        let expected = Anchor {
            origin_chain_id,
            block_number: self.origin_block_number,
        }
        .hash();
        if self.origin_block_hash == expected {
            Ok(())
        } else {
            Err(XChainError::AnchorMismatch {
                seq: self.seq,
                block: self.origin_block_number,
                carried: self.origin_block_hash,
                expected,
            })
        }
    }
}
