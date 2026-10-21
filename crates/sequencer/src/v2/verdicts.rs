//! The verdicts of the v2 loop: the receipt of each forwarded transaction,
//! to the ingress that published it.
//!
//! The filter keeps the ingress of each forwarded transaction until a row
//! confirms it. The receipts of a batch reach the loop before the rows of
//! the same batch, so the match finds the ingress first. A receipt that
//! the filter does not hold forwarded gets no verdict: the other replica
//! forwarded it, or the filter forgot the run. The client then waits until
//! the timeout of its ingress.
//!
//! A frame also carries the account rows of its senders, so the ingress
//! answers the next nonce and the balance of its own senders from its local
//! layer.

use alloy_primitives::Address;
use kardamom_types::{AccountRow, Receipt, ReceiptBatch, ReceiptRollback, TX_TYPE_DEPOSIT};
use tokio::sync::mpsc;

use crate::admission::Admission;
use crate::metrics::record_verdicts;

/// One frame for the verdict task.
#[derive(Debug)]
pub enum VerdictOut {
    /// The receipts of the transactions that `ingress` published.
    Receipts { ingress: u8, batch: ReceiptBatch },
    /// An executor rolled back: each ingress drops its receipts above the
    /// block.
    Rollback(ReceiptRollback),
}

/// The receipts of one batch, grouped by the ingress of their transactions.
type Groups = Vec<(u16, Vec<Receipt>)>;

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

    /// Match the receipts of one batch to the ingress of each forwarded
    /// transaction, and queue one frame for each ingress, with the rows of
    /// its senders. A deposit carries no L2 nonce, so it matches no
    /// forwarded transaction.
    pub(super) fn on_receipts<T>(
        &self,
        admission: &Admission<T>,
        receipts: Vec<Receipt>,
        mut rows: Vec<AccountRow>,
    ) {
        if self.out.is_none() {
            return;
        }
        rows.sort_unstable_by_key(|row| row.address);
        receipts
            .into_iter()
            .filter(|r| r.tx_type != TX_TYPE_DEPOSIT)
            .filter_map(|r| Some((admission.ingress_of(r.from, r.nonce)?, r)))
            .fold(Groups::new(), Self::group)
            .into_iter()
            .for_each(|(ingress, receipts)| self.send_receipts(ingress, receipts, &rows));
    }

    /// Queue `rollback` for every ingress.
    pub(super) fn on_rollback(&self, rollback: ReceiptRollback) {
        let outcome = self.queue(VerdictOut::Rollback(rollback));
        if outcome != "sent" && outcome != "off" {
            tracing::warn!(
                outcome,
                to_block = rollback.to_block,
                "the verdict rollback did not queue"
            );
        }
    }

    fn group(mut groups: Groups, (ingress, receipt): (u16, Receipt)) -> Groups {
        match groups.iter_mut().find(|(id, _)| *id == ingress) {
            Some((_, receipts)) => receipts.push(receipt),
            None => groups.push((ingress, vec![receipt])),
        }
        groups
    }

    fn send_receipts(&self, ingress: u16, receipts: Vec<Receipt>, rows: &[AccountRow]) {
        let n = receipts.len();
        let Ok(ingress) = u8::try_from(ingress) else {
            record_verdicts(self.partition, "wide", n);
            return;
        };
        let batch = ReceiptBatch {
            accounts: Self::rows_of(&receipts, rows),
            receipts,
        };
        let outcome = self.queue(VerdictOut::Receipts { ingress, batch });
        record_verdicts(self.partition, outcome, n);
    }

    /// The rows of the senders of `receipts`. `rows` is sorted by address.
    fn rows_of(receipts: &[Receipt], rows: &[AccountRow]) -> Vec<AccountRow> {
        let mut senders: Vec<Address> = receipts.iter().map(|r| r.from).collect();
        senders.sort_unstable();
        senders.dedup();
        senders
            .iter()
            .filter_map(|sender| {
                let at = rows.binary_search_by_key(sender, |row| row.address).ok()?;
                rows.get(at).cloned()
            })
            .collect()
    }

    /// Queue one frame, and return the outcome. A full queue drops it: the
    /// verdict task is behind, and the loop never waits for it.
    fn queue(&self, frame: VerdictOut) -> &'static str {
        let Some(out) = &self.out else {
            return "off";
        };
        match out.try_send(frame) {
            Ok(()) => "sent",
            Err(mpsc::error::TrySendError::Full(_)) => "full",
            Err(mpsc::error::TrySendError::Closed(_)) => "closed",
        }
    }
}
