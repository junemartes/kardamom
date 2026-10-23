//! The submit of the validated ack. `eth_sendRawTransaction` answers when
//! this ingress validated the transaction and published it on `tx_data`.
//! The sequencer sends nothing back to the ingress. A later drop reaches
//! the client through the egress.
//!
//! The steps, in order:
//! 1. The intake checks: the overload valve, the drain, the pause, the
//!    rate limit, and the size cap.
//! 2. The duplicate cache: a hash that the cache holds answers with the
//!    hash at once. It costs no decode, no recovery, and no publication.
//! 3. The decode, the protocol limits, and the sender recovery.
//! 4. The nonce check: a nonce below the known nonce of the sender answers
//!    `nonce too low`.
//! 5. The offer on `tx_data` of the lane of the sender. A failed offer
//!    answers `txpool is full`, and the client can send again.
//!
//! The nonce check reads the local account layer, then Redis when
//! `[cache]` is on. Both hold the committed rows of the executors, and a
//! row only moves forward. So the view can lag the chain but never lead
//! it: a rejection is always true, and a stale view only misses some
//! rejections. A miss admits. The check asks no executor, so no call waits
//! for a query. The check reads no balance: a balance can grow, so a stale
//! balance could refuse a valid transaction. The sequencer checks it.
//!
//! No waiter stays after the answer: no pending receipt and no verdict.

use std::net::IpAddr;
use std::ops::ControlFlow;

use alloy_primitives::{B256, Bytes as AlloyBytes, keccak256};
use kardamom_cache::AccountView;

use crate::channels::{IngressPublication, IngressSubscription};
use crate::error::IngressError;
use crate::submit_steps::Timing;

use super::{IngressProxy, ValidatedSubmission};

impl<P, S> IngressProxy<P, S>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    /// The submit of the validated ack: the hash once the transaction is
    /// valid here and on `tx_data`. See the module doc for the steps.
    ///
    /// # Errors
    ///
    /// Returns an `IngressError` when a check refuses the transaction:
    /// `NonceTooLow` for a nonce below the account view, and `PoolFull`
    /// when the offer fails.
    pub async fn submit_validated(
        &self,
        client_ip: IpAddr,
        raw_tx: AlloyBytes,
    ) -> Result<B256, IngressError> {
        let _timing = Timing::start(&self.steps.submit);
        let v = match self.validate_unseen(client_ip, &raw_tx).await? {
            ControlFlow::Break(tx_hash) => return Ok(tx_hash),
            ControlFlow::Continue(v) => v,
        };
        self.check_nonce(&v).await?;
        self.offer(&v, raw_tx).await.map_err(|e| {
            tracing::debug!(error = %e, "tx_data offer failed");
            v.refuse("txpool-full", IngressError::PoolFull)
        })?;
        self.seen.insert(v.tx_hash, v.sender);
        metrics::counter!(crate::metrics::TX_ACCEPTED_TOTAL).increment(1);
        Ok(v.tx_hash)
    }

    /// The head of the validated ack. `Break` carries the hash of a
    /// transaction that the duplicate cache holds: the call answers it at
    /// once, and counts it as accepted.
    async fn validate_unseen(
        &self,
        client_ip: IpAddr,
        raw_tx: &AlloyBytes,
    ) -> Result<ControlFlow<B256, ValidatedSubmission>, IngressError> {
        let _timing = Timing::start(&self.steps.validate);
        self.check_intake(client_ip, raw_tx)?;
        let tx_hash = keccak256(raw_tx);
        if let Some(sender) = self.seen.get(&tx_hash) {
            metrics::counter!(crate::metrics::TX_ACCEPTED_TOTAL).increment(1);
            tracing::debug!(%sender, %tx_hash, "submit answered from the duplicate cache");
            return Ok(ControlFlow::Break(tx_hash));
        }
        self.recover_submission(raw_tx)
            .await
            .map(ControlFlow::Continue)
    }

    /// The nonce check against the account view. Off with
    /// `admission_checks` off. A sender with no row admits.
    async fn check_nonce(&self, v: &ValidatedSubmission) -> Result<(), IngressError> {
        if !self.cfg.admission_checks {
            return Ok(());
        }
        self.cached_view(v.sender)
            .await
            .map_or(Ok(()), |view| v.check_nonce(&view))
    }
}

impl ValidatedSubmission {
    /// A nonce below the known nonce of the sender is `nonce too low`. An
    /// equal or higher nonce admits: the sequencer orders it or holds it
    /// for a gap.
    fn check_nonce(&self, view: &AccountView) -> Result<(), IngressError> {
        if self.nonce >= view.nonce {
            return Ok(());
        }
        Err(self.refuse(
            "nonce-too-low",
            IngressError::NonceTooLow {
                address: self.sender,
                tx_nonce: self.nonce,
                state_nonce: view.nonce,
            },
        ))
    }
}
