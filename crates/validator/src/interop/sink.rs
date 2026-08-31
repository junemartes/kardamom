//! The extraction seam: a [`TxReceiptsPublication`] wrapper that collects
//! each block's `MessageSent` receipts off the (locally recomputed) receipt
//! stream and, at the block boundary, cross-checks them against the
//! executor's BAL claims and feeds the serving store.
//!
//! The [`crate::attester::AttestingReceiptSink`] shape: receipts buffer per
//! block, the boundary flushes. Extraction happens AFTER the inner sink
//! accepted the message, so a block whose receipts diverged never reaches
//! the feed — the sink chain's inner error propagates first and the flush
//! for that block never runs ("a validator whose verification halts must
//! stop serving").
//!
//! Claims come from the sink's OWN [`ClaimBuffer`] (the BAL pump inserts
//! into both this one and the parallel engine's, sharing the `Arc`): the
//! engine's buffer is CONSUMED by the whole-block strategy, and in streaming
//! mode it is never drained at all, so neither can serve two readers. A
//! block whose claims never arrive is extracted UNCHECKED and counted —
//! the `bal_missing` posture: the messages are still this validator's own
//! re-execution, and the missing frame already surfaced on the write-set
//! path. A claim MISMATCH is a divergence halt.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use kardamom_engine::{CMessage, ExecutorError, TxReceiptsPublication};
use kardamom_types::Receipt;
use kardamom_types::xchain::OUTBOX;

use crate::buffers::ClaimBuffer;
use crate::interop::extract::collect_outbox_messages;
use crate::interop::store::FeedStore;
use crate::{Divergence, metrics};

/// How long the flush waits for a block's claims before extracting
/// unchecked. The BAL frame lands around the boundary (the write-set
/// cross-check on the exec thread already waited for the same frame), so
/// this is a margin, not an expected wait.
pub const CLAIM_WAIT: Duration = Duration::from_secs(2);

pub struct ExtractingReceiptSink<P: TxReceiptsPublication> {
    inner: P,
    chain_id: u64,
    claims: Arc<ClaimBuffer>,
    store: Arc<FeedStore>,
    divergence: Arc<Divergence>,
    /// Blocks' outbox-relevant receipts accumulated since the last boundary.
    pending: BTreeMap<u64, Vec<Receipt>>,
    claim_wait: Duration,
}

impl<P: TxReceiptsPublication> ExtractingReceiptSink<P> {
    pub fn new(
        inner: P,
        chain_id: u64,
        claims: Arc<ClaimBuffer>,
        store: Arc<FeedStore>,
        divergence: Arc<Divergence>,
    ) -> Self {
        Self {
            inner,
            chain_id,
            claims,
            store,
            divergence,
            pending: BTreeMap::new(),
            claim_wait: CLAIM_WAIT,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_claim_wait(mut self, wait: Duration) -> Self {
        self.claim_wait = wait;
        self
    }

    /// Extract every pending block up to and including `block`; every block
    /// advances the store's retention head even when it sent nothing.
    fn flush_through(&mut self, block: u64) -> Result<(), ExecutorError> {
        let tail = self.pending.split_off(&(block + 1));
        let flushed = std::mem::replace(&mut self.pending, tail);
        for (b, receipts) in flushed {
            let claims = self.claims.take(b, self.claim_wait);
            if claims.is_none() {
                metrics::counter_outbox_unchecked();
                tracing::warn!(
                    block = b,
                    "no BAL claims for an outbox-sending block; messages served UNCHECKED \
                     (own re-execution; the write-set path already flagged the missing frame)"
                );
            }
            let msgs = match collect_outbox_messages(
                self.chain_id,
                b,
                &receipts,
                claims.as_ref().map(|(g, idx)| (*g, idx.as_ref())),
            ) {
                Ok(msgs) => msgs,
                Err(fault) => {
                    let reason = format!("outbox extraction failed: {fault}");
                    self.divergence.record(reason.clone());
                    return Err(ExecutorError::Divergence(reason));
                }
            };
            metrics::counter_outbox_extracted(msgs.len());
            self.store.append_block(b, msgs);
        }
        // The boundary block itself may have had no outbox receipts —
        // retention still advances.
        self.store.append_block(block, Vec::new());
        Ok(())
    }
}

impl<P: TxReceiptsPublication> TxReceiptsPublication for ExtractingReceiptSink<P> {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
        // Buffer BEFORE forwarding (the message moves into the inner sink),
        // but FLUSH only after the inner chain accepted the boundary — a
        // diverging block must never reach the feed.
        let mut boundary = None;
        match &msg {
            CMessage::Receipt(r) => {
                if r.logs.iter().any(|l| l.address == OUTBOX) {
                    self.pending
                        .entry(r.block_number)
                        .or_default()
                        .push(r.clone());
                }
            }
            CMessage::BlockBoundary(b) => boundary = Some(b.block_number),
        }
        self.inner.publish(msg)?;
        if let Some(b) = boundary {
            self.flush_through(b)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{B256, U256};
    use kardamom_types::{BlockBoundary, WireLog};

    use crate::interop::extract::sent_messages_slot;
    use crate::interop::extract::tests_support::{honest_sent_log, log_msg_hash};
    use crate::parallel::ClaimIndex;

    struct OkSink;
    impl TxReceiptsPublication for OkSink {
        fn publish(&mut self, _msg: CMessage) -> Result<(), ExecutorError> {
            Ok(())
        }
    }

    fn receipt(block: u64, tx_index: u64, logs: Vec<WireLog>) -> CMessage {
        CMessage::Receipt(Receipt {
            block_number: block,
            transaction_index: tx_index,
            logs,
            ..Default::default()
        })
    }

    fn boundary(block: u64) -> CMessage {
        CMessage::BlockBoundary(BlockBoundary {
            block_number: block,
            end_tx_idx: kardamom_types::BPosition::from_index(0),
            l2_timestamp: 0,
            l1_origin: 0,
        })
    }

    const CHAIN: u64 = 412_346;

    #[test]
    fn extracts_at_the_boundary_with_matching_claims() {
        let claims = ClaimBuffer::new();
        let store = Arc::new(FeedStore::new(CHAIN, 100));
        let div = Divergence::new();
        let mut sink =
            ExtractingReceiptSink::new(OkSink, CHAIN, claims.clone(), store.clone(), div.clone());

        let log = honest_sent_log(CHAIN, 412_347, 0, &[0xCA]);
        let mut idx = ClaimIndex::default();
        idx.storage
            .entry((OUTBOX, sent_messages_slot(log_msg_hash(&log))))
            .or_default()
            .push((1, U256::ONE));
        claims.insert(5, 1, idx);

        sink.publish(receipt(5, 0, vec![log])).unwrap();
        sink.publish(boundary(5)).unwrap();

        assert!(!div.is_halted());
        let (msgs, _) = store.from_seq(412_347, 0);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].seq, 0);
    }

    #[test]
    fn a_claim_mismatch_halts_and_never_serves() {
        let claims = ClaimBuffer::new();
        let store = Arc::new(FeedStore::new(CHAIN, 100));
        let div = Divergence::new();
        let mut sink =
            ExtractingReceiptSink::new(OkSink, CHAIN, claims.clone(), store.clone(), div.clone())
                .with_claim_wait(Duration::from_millis(50));

        let log = honest_sent_log(CHAIN, 412_347, 0, &[0xCA]);
        // Claims for the block EXIST but lack the sentMessages slot.
        let mut idx = ClaimIndex::default();
        idx.storage
            .entry((OUTBOX, B256::repeat_byte(0x77)))
            .or_default()
            .push((1, U256::ONE));
        claims.insert(5, 1, idx);

        sink.publish(receipt(5, 0, vec![log])).unwrap();
        let err = sink.publish(boundary(5)).unwrap_err();
        assert!(matches!(err, ExecutorError::Divergence(_)));
        assert!(div.is_halted());
        assert!(div.reason().unwrap().contains("outbox extraction failed"));
        let (msgs, _) = store.from_seq(412_347, 0);
        assert!(msgs.is_empty(), "a diverging block must never be served");
    }

    #[test]
    fn missing_claims_serve_unchecked_not_halt() {
        let claims = ClaimBuffer::new();
        let store = Arc::new(FeedStore::new(CHAIN, 100));
        let div = Divergence::new();
        let mut sink =
            ExtractingReceiptSink::new(OkSink, CHAIN, claims, store.clone(), div.clone())
                .with_claim_wait(Duration::from_millis(50));

        let log = honest_sent_log(CHAIN, 412_347, 3, &[]);
        sink.publish(receipt(9, 0, vec![log])).unwrap();
        sink.publish(boundary(9)).unwrap();

        assert!(
            !div.is_halted(),
            "bal_missing posture: unchecked, not halted"
        );
        let (msgs, _) = store.from_seq(412_347, 0);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].seq, 3);
    }

    #[test]
    fn boundaries_advance_retention_for_empty_blocks() {
        let claims = ClaimBuffer::new();
        let store = Arc::new(FeedStore::new(CHAIN, 2));
        let div = Divergence::new();
        let mut sink =
            ExtractingReceiptSink::new(OkSink, CHAIN, claims.clone(), store.clone(), div)
                .with_claim_wait(Duration::from_millis(10));

        let log = honest_sent_log(CHAIN, 412_347, 0, &[]);
        sink.publish(receipt(1, 0, vec![log])).unwrap();
        sink.publish(boundary(1)).unwrap();
        for b in 2..=8 {
            sink.publish(boundary(b)).unwrap();
        }
        // Retention 2, head 8: the block-1 message aged out.
        let (msgs, floor) = store.from_seq(412_347, 0);
        assert!(msgs.is_empty());
        assert_eq!(floor, 1);
    }
}
