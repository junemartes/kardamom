//! The `tx_receipts` floor feed: the rows of this replica's senders into
//! the local account layer, and each receipt and rollback into the floor
//! channel of the publish loop.

use std::ops::ControlFlow;

use kardamom_cache::LiveAccountsWriter;
use kardamom_log::aeron_live::{ReceiptFrame, TxReceiptsReceiver, TxReceiptsSubscriberHandle};
use kardamom_sequencer::resync::{FloorEvent, FloorUpdate};
use kardamom_sequencer::sequencer::Shutdown;
use kardamom_types::shard_map::{VslotSet, vslot_for};
use kardamom_types::{AccountRow, ReceiptBatch, ReceiptRollback};

/// The `tx_receipts`-to-per-sender executed-truth floor feed. Only the
/// senders of this replica's vslots reach the floor channel. The set is
/// the replica's whole set, shadow slots included: floors must advance
/// for the incoming senders during the warm-up too. The batch frame's
/// account rows of the same vslots go into the local account layer,
/// before the receipts forward.
///
/// A rollback frame of an executor clears the local layer and goes into
/// the floor channel in stream order, so the publish loop forgets the
/// floors of the void receipts before it reads the receipts after it.
///
/// The receipts handle already fans in over a tokio unbounded channel. The
/// Aeron poll thread is the producer. So this is a plain async task. It
/// awaits `recv_frame()` and `Shutdown::cancelled`, with no idle-sleep
/// polling. `floor_tx` stays a std channel, because its consumer is the
/// sync `ResyncController` in the publish loop.
pub(crate) struct ReceiptFloorFeed {
    vslots: VslotSet,
    floor_tx: crossbeam_channel::Sender<FloorEvent>,
    /// The local account layer's one writer.
    writer: LiveAccountsWriter,
}

impl ReceiptFloorFeed {
    pub(crate) fn new(
        vslots: VslotSet,
        floor_tx: crossbeam_channel::Sender<FloorEvent>,
        writer: LiveAccountsWriter,
    ) -> Self {
        Self {
            vslots,
            floor_tx,
            writer,
        }
    }

    /// Spawn as a plain async task. The handle reduces to its receiver
    /// (`into_receiver`), so the task holds no `AeronRuntime` clone.
    /// `floor_tx` is bounded by `resync::ResyncChannel::open` to the
    /// cluster's dedup capacity, the resync window this feed exists to
    /// cover.
    pub(crate) fn spawn(
        self,
        sub: TxReceiptsSubscriberHandle,
        shutdown: Shutdown,
    ) -> tokio::task::JoinHandle<()> {
        let rx = sub.into_receiver();
        tokio::spawn(self.run(rx, shutdown))
    }

    async fn run(mut self, mut rx: TxReceiptsReceiver, shutdown: Shutdown) {
        loop {
            match self.tick(&mut rx, &shutdown).await {
                ControlFlow::Break(()) => return,
                ControlFlow::Continue(()) => (),
            }
        }
    }

    /// One [`Self::run`] pass: wait for the next frame or shutdown, then
    /// apply it. `Break` means the task should stop: shutdown, the
    /// subscription closed, or the publish loop is gone.
    async fn tick(&mut self, rx: &mut TxReceiptsReceiver, shutdown: &Shutdown) -> ControlFlow<()> {
        let frame = tokio::select! {
            biased;
            () = shutdown.cancelled() => return ControlFlow::Break(()),
            msg = rx.recv_frame() => match msg {
                Some((_, frame)) => frame,
                // The subscription closed. The runtime shut down.
                // Nothing more to feed.
                None => return ControlFlow::Break(()),
            },
        };
        match frame {
            ReceiptFrame::Batch(batch) => self.forward_batch(batch),
            ReceiptFrame::Rollback(rollback) => self.forward_rollback(rollback),
        }
    }

    /// An executor rolled back. The local layer holds rows of the void
    /// blocks at positions above the new stream, and the monotone rule
    /// would keep them, so the layer starts empty. The rollback then goes
    /// into the floor channel. A dropped rollback leaves floors above the
    /// chain, so this send waits for room, as the lookup answers do.
    /// `Break` means the publish loop is gone.
    fn forward_rollback(&mut self, rollback: ReceiptRollback) -> ControlFlow<()> {
        self.writer.clear();
        tracing::warn!(
            to_block = rollback.to_block,
            "an executor rolled back; the floors above the block are void"
        );
        let event = FloorEvent::Rollback {
            to_block: rollback.to_block,
        };
        match self.floor_tx.send(event) {
            Ok(()) => ControlFlow::Continue(()),
            Err(_) => ControlFlow::Break(()),
        }
    }

    /// The rows of this replica's vslots into the local layer at the
    /// batch's end position, then every receipt through the floor
    /// filter. The rows go first, so a lookup request the receipt's
    /// park raises finds the entry.
    fn forward_batch(&mut self, batch: ReceiptBatch) -> ControlFlow<()> {
        if let Some(end) = batch.end_tx_idx() {
            let rows: Vec<AccountRow> = batch
                .accounts
                .into_iter()
                .filter(|row| self.vslots.contains(vslot_for(row.address)))
                .collect();
            self.writer.apply(end, &rows);
        }
        for receipt in &batch.receipts {
            self.forward_one_receipt(receipt)?;
        }
        ControlFlow::Continue(())
    }

    /// Filter, build, and forward one receipt, for [`Self::tick`].
    /// `Break` means the publish loop is gone and the task should stop.
    fn forward_one_receipt(&self, receipt: &kardamom_types::Receipt) -> ControlFlow<()> {
        // Forward every receipt of this replica's vslots, nonce 0
        // included. A deposit receipt carries `Receipt::tx_type ==
        // TX_TYPE_DEPOSIT`, so the controller tells it apart from a
        // genuine nonce-0 transaction receipt. It excludes deposits and
        // skip receipts from floor evidence (they consume no L2 nonce),
        // and counts skip receipts as publish confirmations (ordering is
        // the claim). See `ResyncController::drain_floor_updates`.
        //
        // Only this replica's senders can appear in its publish stream,
        // so the floor map stays bounded to them.
        if !self.vslots.contains(vslot_for(receipt.from)) {
            return ControlFlow::Continue(());
        }
        match self
            .floor_tx
            .try_send(FloorUpdate::of_receipt(receipt).into())
        {
            Ok(()) => ControlFlow::Continue(()),
            // The publish loop is stalled past the resync window already.
            // Dropping is safe: an unproven skip just falls through to
            // publish, the side every degraded mode already degrades
            // toward (see the module doc on `crate::resync`).
            Err(crossbeam_channel::TrySendError::Full(_)) => {
                tracing::warn!("receipt-floor channel full; dropping");
                ControlFlow::Continue(())
            }
            // The publish loop is gone. Exit.
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => ControlFlow::Break(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, U256};
    use kardamom_cache::{LiveAccounts, LiveAccountsConfig};
    use kardamom_types::{BPosition, Receipt};

    use super::*;

    /// The receipts of a batch go into the floor channel with their block,
    /// and a rollback after them follows in the same order. The rollback
    /// also empties the local account layer.
    #[test]
    fn a_rollback_follows_the_receipts_and_empties_the_local_layer() {
        let (floor_tx, floor_rx) = crossbeam_channel::unbounded();
        let (live, writer) = LiveAccounts::new(&LiveAccountsConfig::default());
        let mut feed = ReceiptFloorFeed::new(VslotSet::full(), floor_tx, writer);
        let sender = Address::repeat_byte(0x31);
        let receipt = Receipt {
            from: sender,
            nonce: 4,
            block_number: 9,
            tx_idx: BPosition::from_index(20),
            ..Receipt::default()
        };
        let batch = ReceiptBatch {
            receipts: vec![receipt.clone()],
            accounts: vec![AccountRow {
                address: sender,
                nonce: 5,
                balance: U256::from(1u64),
            }],
        };
        assert_eq!(feed.forward_batch(batch), ControlFlow::Continue(()));
        assert_eq!(live.get(sender).map(|view| view.nonce), Some(5));

        let rollback = ReceiptRollback { to_block: 7 };
        assert_eq!(feed.forward_rollback(rollback), ControlFlow::Continue(()));
        assert!(live.is_empty(), "the rows of the void blocks are gone");

        let events: Vec<FloorEvent> = floor_rx.try_iter().collect();
        assert_eq!(
            events,
            vec![
                FloorUpdate::of_receipt(&receipt).into(),
                FloorEvent::Rollback { to_block: 7 },
            ]
        );
        assert_eq!(FloorUpdate::of_receipt(&receipt).block, Some(9));
    }
}
