//! One `kardamom_subscribeReceipts` session: the deduped receipt and
//! tx-error feeds, the sender filter, and the sink the events go out on.

use alloy_primitives::Address;
use jsonrpsee::server::SubscriptionSink;
use tokio::sync::{broadcast, watch};

use kardamom_types::TxError;

use crate::channels::ReceiptItem;
use crate::json_rpc::{ReceiptEvent, RpcReceipt};
use crate::rollback_log::RollbackLog;

/// A sender allow-list for `kardamom_subscribeReceipts`. `None` streams
/// every sender.
pub(crate) struct SenderFilter(Option<std::collections::HashSet<Address>>);

impl SenderFilter {
    /// Build the filter from the `senders` param. `None` or an empty
    /// list streams every sender.
    pub(crate) fn new(senders: Option<Vec<Address>>) -> Self {
        Self(
            senders
                .filter(|s| !s.is_empty())
                .map(|s| s.into_iter().collect()),
        )
    }

    fn allows(&self, addr: Address) -> bool {
        self.0.as_ref().is_none_or(|f| f.contains(&addr))
    }
}

/// One feed poll's outcome, for [`ReceiptSubscription::next_event`]'s
/// `select!` arms: an event to send, a filtered-out item to skip, or the
/// feed closing, which ends the subscription.
enum NextEvent {
    Event(ReceiptEvent),
    FilteredOut,
    FeedClosed,
    /// An executor voided the receipts above this block. The session ends
    /// with an error, so the client reads its receipts again.
    RolledBack(u64),
}

/// The feeds of one session: the deduped receipts with their rollbacks,
/// the log of the applied rollbacks, and the tx errors.
pub(crate) struct Feeds {
    pub(crate) receipts: broadcast::Receiver<ReceiptItem>,
    pub(crate) rollbacks: watch::Receiver<RollbackLog>,
    pub(crate) errors: broadcast::Receiver<TxError>,
}

/// What one session makes of each feed item: the sender filter, and the
/// rollbacks since the session started.
struct Rules {
    filter: SenderFilter,
    rollbacks: watch::Receiver<RollbackLog>,
    /// The rollback generation when the session started. A rollback after
    /// it, that the session missed on a gap of the receipt feed, ends the
    /// session too.
    started_at: u64,
}

/// Owns one `kardamom_subscribeReceipts` session end to end: the
/// upstream feeds, the rules of the session, and the sink the events go
/// out on.
pub(crate) struct ReceiptSubscription {
    receipts: broadcast::Receiver<ReceiptItem>,
    errors: broadcast::Receiver<TxError>,
    rules: Rules,
    sink: SubscriptionSink,
}

impl ReceiptSubscription {
    pub(crate) fn new(feeds: Feeds, filter: SenderFilter, sink: SubscriptionSink) -> Self {
        let Feeds {
            receipts,
            rollbacks,
            errors,
        } = feeds;
        Self {
            receipts,
            errors,
            rules: Rules::new(filter, rollbacks),
            sink,
        }
    }

    /// Forwards events to the sink until the sink closes, a feed
    /// disconnects, or the subscriber goes away. A rollback ends the
    /// session with an error that names its block.
    pub(crate) async fn run(mut self) -> Result<(), String> {
        while self.step().await?.is_continue() {}
        Ok(())
    }

    /// Wait for one event and send it. `Break` ends the session: the sink
    /// or a feed closed, or the subscriber went away.
    async fn step(&mut self) -> Result<std::ops::ControlFlow<()>, String> {
        let event = match self.next_event().await {
            NextEvent::Event(event) => event,
            NextEvent::RolledBack(to_block) => return Err(Self::rolled_back(to_block)),
            NextEvent::FilteredOut | NextEvent::FeedClosed => {
                return Ok(std::ops::ControlFlow::Break(()));
            }
        };
        Ok(if self.send_event(&event).await? {
            std::ops::ControlFlow::Continue(())
        } else {
            std::ops::ControlFlow::Break(())
        })
    }

    /// The error that ends a session on a rollback.
    fn rolled_back(to_block: u64) -> String {
        format!(
            "receipts rolled back: an executor voided the receipts above block {to_block}; \
             read them again with eth_getTransactionReceipt and subscribe again"
        )
    }

    /// Waits for the next receipt or tx-error frame, applying the
    /// filter, or a lag marker if either feed fell behind. Returns
    /// [`NextEvent::FeedClosed`] once the sink closes or a feed
    /// disconnects, and [`NextEvent::RolledBack`] on a rollback. Both
    /// end the subscription. It never returns [`NextEvent::FilteredOut`].
    async fn next_event(&mut self) -> NextEvent {
        loop {
            let next = tokio::select! {
                () = self.sink.closed() => return NextEvent::FeedClosed,
                r = self.receipts.recv() => self.rules.on_receipt_result(r),
                e = self.errors.recv() => self.rules.on_error_result(e),
            };
            if !matches!(next, NextEvent::FilteredOut) {
                return next;
            }
        }
    }

    /// Serializes and sends one event to the subscriber. Returns
    /// `Ok(false)` if the subscriber went away, so the caller ends the
    /// loop instead of treating a closed sink as an error.
    async fn send_event(&self, event: &ReceiptEvent) -> Result<bool, String> {
        let msg = serde_json::value::to_raw_value(event)
            .map_err(|e| format!("serialize subscription event: {e}"))?;
        Ok(self.sink.send(msg).await.is_ok())
    }
}

impl Rules {
    fn new(filter: SenderFilter, rollbacks: watch::Receiver<RollbackLog>) -> Self {
        let started_at = rollbacks.borrow().generation();
        Self {
            filter,
            rollbacks,
            started_at,
        }
    }

    /// One `receipts` feed poll, for [`ReceiptSubscription::next_event`]'s `select!` arm.
    fn on_receipt_result(&self, r: Result<ReceiptItem, broadcast::error::RecvError>) -> NextEvent {
        match r {
            Ok(ReceiptItem::Rollback(rollback)) => NextEvent::RolledBack(rollback.to_block),
            Ok(ReceiptItem::Receipt(rcpt)) if !self.filter.allows(rcpt.from) => {
                NextEvent::FilteredOut
            }
            Ok(ReceiptItem::Receipt(rcpt)) => NextEvent::Event(ReceiptEvent::Receipt {
                receipt: Box::new(RpcReceipt::from(&rcpt).0),
            }),
            Err(broadcast::error::RecvError::Lagged(n)) => self.missed_rollback().map_or(
                NextEvent::Event(ReceiptEvent::Lagged { skipped: n }),
                NextEvent::RolledBack,
            ),
            Err(broadcast::error::RecvError::Closed) => NextEvent::FeedClosed,
        }
    }

    /// The lowest block of a rollback after the start of the session. The
    /// receipt feed dropped items, and a rollback can be among them.
    fn missed_rollback(&self) -> Option<u64> {
        self.rollbacks
            .borrow()
            .since(self.started_at)
            .map(|r| r.to_block)
            .min()
    }

    /// One `errors` feed poll, for [`ReceiptSubscription::next_event`]'s `select!` arm.
    fn on_error_result(&self, e: Result<TxError, broadcast::error::RecvError>) -> NextEvent {
        match e {
            Ok(err) if !self.filter.allows(err.sender) => NextEvent::FilteredOut,
            Ok(err) => NextEvent::Event(ReceiptEvent::TxError {
                sender: err.sender,
                nonce: err.nonce,
                reason: err.reason.word().to_string(),
                expected_nonce: err.reason.expected_nonce(),
            }),
            Err(broadcast::error::RecvError::Lagged(n)) => {
                NextEvent::Event(ReceiptEvent::Lagged { skipped: n })
            }
            Err(broadcast::error::RecvError::Closed) => NextEvent::FeedClosed,
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::B256;
    use kardamom_types::ReceiptRollback;

    use super::*;

    fn rollback(to_block: u64, id: u8) -> ReceiptRollback {
        ReceiptRollback {
            to_block,
            id: B256::repeat_byte(id),
        }
    }

    /// A gap on the receipt feed ends the session when a rollback came
    /// after the start of the session, also one that the gap dropped. A
    /// rollback from before the start does not end it.
    #[test]
    fn a_rollback_missed_on_a_gap_ends_the_session() {
        let (log, rollbacks) = watch::channel(RollbackLog::default());
        log.send_modify(|l| {
            l.push(rollback(2, 1));
        });
        let rules = Rules::new(SenderFilter::new(None), rollbacks);
        let lagged = || Err(broadcast::error::RecvError::Lagged(9));
        assert!(matches!(
            rules.on_receipt_result(lagged()),
            NextEvent::Event(ReceiptEvent::Lagged { skipped: 9 })
        ));
        log.send_modify(|l| {
            l.push(rollback(6, 2));
            l.push(rollback(4, 3));
        });
        assert!(matches!(
            rules.on_receipt_result(lagged()),
            NextEvent::RolledBack(4)
        ));
    }
}
