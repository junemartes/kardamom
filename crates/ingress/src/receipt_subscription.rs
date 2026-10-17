//! One `kardamom_subscribeReceipts` session: the deduped receipt and
//! tx-error feeds, the sender filter, and the sink the events go out on.

use alloy_primitives::Address;
use jsonrpsee::server::SubscriptionSink;
use tokio::sync::broadcast;

use kardamom_types::TxError;

use crate::channels::ReceiptItem;
use crate::json_rpc::{ReceiptEvent, RpcReceipt};

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

/// Owns one `kardamom_subscribeReceipts` session end to end: the two
/// upstream feeds, the sender filter, and the sink the events go out on.
pub(crate) struct ReceiptSubscription {
    receipts: broadcast::Receiver<ReceiptItem>,
    errors: broadcast::Receiver<TxError>,
    filter: SenderFilter,
    sink: SubscriptionSink,
}

impl ReceiptSubscription {
    pub(crate) fn new(
        receipts: broadcast::Receiver<ReceiptItem>,
        errors: broadcast::Receiver<TxError>,
        filter: SenderFilter,
        sink: SubscriptionSink,
    ) -> Self {
        Self {
            receipts,
            errors,
            filter,
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
                r = self.receipts.recv() => self.on_receipt_result(r),
                e = self.errors.recv() => self.on_error_result(e),
            };
            if !matches!(next, NextEvent::FilteredOut) {
                return next;
            }
        }
    }

    /// One `receipts` feed poll, for [`Self::next_event`]'s `select!` arm.
    fn on_receipt_result(&self, r: Result<ReceiptItem, broadcast::error::RecvError>) -> NextEvent {
        match r {
            Ok(ReceiptItem::Rollback(rollback)) => NextEvent::RolledBack(rollback.to_block),
            Ok(ReceiptItem::Receipt(rcpt)) if !self.filter.allows(rcpt.from) => {
                NextEvent::FilteredOut
            }
            Ok(ReceiptItem::Receipt(rcpt)) => NextEvent::Event(ReceiptEvent::Receipt {
                receipt: Box::new(RpcReceipt::from(&rcpt).0),
            }),
            Err(broadcast::error::RecvError::Lagged(n)) => {
                NextEvent::Event(ReceiptEvent::Lagged { skipped: n })
            }
            Err(broadcast::error::RecvError::Closed) => NextEvent::FeedClosed,
        }
    }

    /// One `errors` feed poll, for [`Self::next_event`]'s `select!` arm.
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

    /// Serializes and sends one event to the subscriber. Returns
    /// `Ok(false)` if the subscriber went away, so the caller ends the
    /// loop instead of treating a closed sink as an error.
    async fn send_event(&self, event: &ReceiptEvent) -> Result<bool, String> {
        let msg = serde_json::value::to_raw_value(event)
            .map_err(|e| format!("serialize subscription event: {e}"))?;
        Ok(self.sink.send(msg).await.is_ok())
    }
}
