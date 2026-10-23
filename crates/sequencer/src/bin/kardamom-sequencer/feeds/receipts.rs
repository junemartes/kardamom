//! The `tx_receipts` floor feed: the rows of this replica's senders into
//! the local account layer, and each receipt and rollback into the floor
//! channel of the publish loop.

use std::ops::ControlFlow;

use kardamom_cache::LiveAccountsWriter;
use kardamom_log::aeron_live::{ReceiptFrame, TxReceiptsReceiver, TxReceiptsSubscriberHandle};
use kardamom_obs::pipeline::{LoopMeter, LoopState};
use kardamom_sequencer::admission::AccountState;
use kardamom_sequencer::resync::{FloorEvent, FloorUpdate};
use kardamom_sequencer::sequencer::Shutdown;
use kardamom_sequencer::v2::AccountEvent;
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
    sink: ReceiptSink,
    /// The local account layer's one writer.
    writer: LiveAccountsWriter,
    /// Idle while the task waits for a frame, busy while it applies one,
    /// and blocked while the account events wait for room in the queue of
    /// the v2 loop.
    meter: LoopMeter,
}

/// Where the receipts of this replica's senders go: the floor channel of
/// the v1 loop, or the account events of the v2 loop.
pub(crate) enum ReceiptSink {
    Floors(crossbeam_channel::Sender<FloorEvent>),
    Accounts(crossbeam_channel::Sender<AccountEvent>),
}

impl ReceiptFloorFeed {
    pub(crate) fn new(vslots: VslotSet, sink: ReceiptSink, writer: LiveAccountsWriter) -> Self {
        Self {
            vslots,
            sink,
            writer,
            meter: LoopMeter::new("sequencer.receipt_feed"),
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
        self.meter.mark(LoopState::Idle);
        let flow = match frame {
            ReceiptFrame::Batch(batch) => self.forward_batch(batch),
            ReceiptFrame::Rollback(rollback) => self.forward_rollback(rollback),
        };
        self.meter.mark(LoopState::Busy);
        flow
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
        let sent = match &self.sink {
            ReceiptSink::Floors(floor_tx) => floor_tx.send(FloorEvent::Rollback(rollback)).is_ok(),
            ReceiptSink::Accounts(accounts) => accounts.send(AccountEvent::Rollback).is_ok(),
        };
        if sent {
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(())
        }
    }

    /// The rows of this replica's vslots into the local layer at the
    /// batch's end position, then every receipt through the floor
    /// filter. The rows go first, so a lookup request the receipt's
    /// park raises finds the entry.
    fn forward_batch(&mut self, batch: ReceiptBatch) -> ControlFlow<()> {
        let block = batch.receipts.last().map(|r| r.block_number);
        if let Some(end) = batch.end_tx_idx() {
            let rows: Vec<AccountRow> = batch
                .accounts
                .into_iter()
                .filter(|row| self.vslots.contains(vslot_for(row.address)))
                .collect();
            self.writer.apply(end, &rows);
            if let (ReceiptSink::Accounts(accounts), Some(block)) = (&self.sink, block) {
                let event = rows_event(block, end.as_index(), &rows);
                self.meter.mark(LoopState::Busy);
                let sent = accounts.send(event);
                self.meter.mark(LoopState::Blocked);
                return if sent.is_ok() {
                    ControlFlow::Continue(())
                } else {
                    ControlFlow::Break(())
                };
            }
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
        let ReceiptSink::Floors(floor_tx) = &self.sink else {
            return ControlFlow::Continue(());
        };
        match floor_tx.try_send(FloorUpdate::of_receipt(receipt).into()) {
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

/// The rows of one batch as the state of each account at `tx_idx`, the
/// end of the batch, for the v2 loop.
fn rows_event(block: u64, tx_idx: u64, rows: &[AccountRow]) -> AccountEvent {
    let rows = rows
        .iter()
        .map(|row| {
            let state = AccountState {
                nonce: row.nonce,
                balance: row.balance,
                delegated: false,
                tx_idx,
            };
            (row.address, state)
        })
        .collect();
    AccountEvent::Rows { block, rows }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, U256};
    use kardamom_cache::{LiveAccounts, LiveAccountsConfig};
    use kardamom_sequencer::resync::Evidence;
    use kardamom_types::{BPosition, Receipt};

    use super::*;

    /// The receipts of a batch go into the floor channel with their block,
    /// and a rollback after them follows in the same order. The rollback
    /// also empties the local account layer.
    #[test]
    fn a_rollback_follows_the_receipts_and_empties_the_local_layer() {
        let (floor_tx, floor_rx) = crossbeam_channel::unbounded();
        let (live, writer) = LiveAccounts::new(&LiveAccountsConfig::default());
        let mut feed =
            ReceiptFloorFeed::new(VslotSet::full(), ReceiptSink::Floors(floor_tx), writer);
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

        let rollback = ReceiptRollback {
            to_block: 7,
            ..Default::default()
        };
        assert_eq!(feed.forward_rollback(rollback), ControlFlow::Continue(()));
        assert!(live.is_empty(), "the rows of the void blocks are gone");

        let events: Vec<FloorEvent> = floor_rx.try_iter().collect();
        assert_eq!(
            events,
            vec![
                FloorUpdate::of_receipt(&receipt).into(),
                FloorEvent::Rollback(rollback),
            ]
        );
        assert_eq!(
            FloorUpdate::of_receipt(&receipt).evidence,
            Evidence::Receipt { block: 9 }
        );
    }
}
