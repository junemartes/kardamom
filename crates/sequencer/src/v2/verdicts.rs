//! The verdicts of the v2 loop: the answer to each transaction, to the
//! ingress that published it.
//!
//! The ingress answers `eth_sendRawTransaction` on the verdict, as an
//! Ethereum node answers when its mempool takes a transaction:
//! - `Accepted` when the filter forwards the transaction, or holds it for
//!   a nonce gap or a busy run;
//! - `Rejected` with the reason when the filter or the fee gate refuses it.
//!
//! A transaction of a sender with no state waits for the lookup, then gets
//! its verdict. A later outcome of an answered transaction, such as the
//! expiry of a parked one, is not a verdict: the client sees it through
//! the egress. Both twins send a verdict, and the ingress answers on the
//! first one that decides.

use kardamom_types::{TxError, TxErrorReason, TxStatus};
use tokio::sync::mpsc;

use crate::admission::Ack;
use crate::metrics::record_verdicts;

/// One verdict for the verdict task: `status` goes to the ingress `ingress`.
#[derive(Debug)]
pub struct VerdictOut {
    pub ingress: u8,
    pub status: TxStatus,
}

/// Where the loop sends its verdicts. With no queue, the loop sends none.
pub(super) struct Verdicts {
    out: Option<mpsc::Sender<VerdictOut>>,
    partition: u32,
}

impl Verdicts {
    pub(super) fn off(partition: u32) -> Self {
        Self {
            out: None,
            partition,
        }
    }

    pub(super) fn to(&mut self, out: mpsc::Sender<VerdictOut>) {
        self.out = Some(out);
    }

    /// The filter took the transaction of `ack` into its mempool.
    pub(super) fn accept(&self, ack: Ack) {
        self.send(
            ack.ingress,
            TxStatus::accepted(ack.tx_hash, ack.sender, ack.nonce),
        );
    }

    /// The filter or the fee gate refused the transaction `ack.tx_hash`.
    pub(super) fn reject(&self, ack: Ack, reason: TxErrorReason) {
        let error = TxError {
            sender: ack.sender,
            nonce: ack.nonce,
            reason,
        };
        self.send(ack.ingress, TxStatus::rejected(ack.tx_hash, &error));
    }

    /// Queue one verdict. A full queue drops it: the verdict task is
    /// behind, and the loop never waits for it. The client then gets the
    /// ack timeout of its ingress.
    fn send(&self, ingress: u16, status: TxStatus) {
        let Some(out) = &self.out else {
            return;
        };
        let Ok(ingress) = u8::try_from(ingress) else {
            record_verdicts(self.partition, "wide", 1);
            return;
        };
        let outcome = match out.try_send(VerdictOut { ingress, status }) {
            Ok(()) => "sent",
            Err(mpsc::error::TrySendError::Full(_)) => "full",
            Err(mpsc::error::TrySendError::Closed(_)) => "closed",
        };
        record_verdicts(self.partition, outcome, 1);
    }
}
