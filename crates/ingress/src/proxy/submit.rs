//! The client-facing submit path: `submit_raw` (blocking, park-until-receipt)
//! and `submit_raw_async` (ack-on-publish), plus their shared validate /
//! cache-answer / publish stages.

use std::net::IpAddr;

use alloy_consensus::TxEnvelope as ConsensusEnvelope;
use alloy_consensus::transaction::Transaction;
use alloy_primitives::{B256, Bytes as AlloyBytes};
use alloy_rlp::Decodable;

use kardamom_types::{Receipt, TxEnvelope};

use crate::channels::{IngressPublication, IngressSubscription};
use crate::error::IngressError;
use crate::metrics::count_reject;
use crate::pending::ReceiptResponse;
use crate::routing::partition_for;

use super::{IngressProxy, ValidatedSubmission};

impl<P, S> IngressProxy<P, S>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    /// Hot path for both JSON-RPC and binary submissions. Returns the
    /// receipt once both `(sender, nonce, receipt)` and the quorum watermark
    /// are satisfied.
    pub async fn submit_raw(
        &self,
        client_ip: IpAddr,
        raw_tx: AlloyBytes,
    ) -> Result<ReceiptResponse, IngressError> {
        let v = self.validate_submission(client_ip, &raw_tx).await?;
        if let Some(answer) = self.answer_from_cache(&v) {
            return answer.map(|receipt| ReceiptResponse { receipt });
        }

        // Park *before* publishing — the receipt can arrive on the cache
        // channel before we'd otherwise have registered, especially under load.
        // The queue-depth gauge is maintained by the registry itself (every
        // insert/remove path, including a cancelled handler's Drop).
        let wait = self.pending.register(v.sender, v.nonce);

        self.publish_validated(&v, raw_tx).await?;

        let result = wait
            .await_with_timeout(self.cfg.pending_receipt_timeout)
            .await;

        // Count accepted/rejected on the terminal outcome (not at publish
        // time) so a single submission never increments both.
        match &result {
            Ok(_) => {
                metrics::counter!(crate::metrics::TX_ACCEPTED_TOTAL).increment(1);
            }
            Err(IngressError::Timeout) => count_reject("timeout"),
            Err(IngressError::Duplicate(_)) => count_reject("duplicate"),
            Err(_) => count_reject("internal"),
        }

        // Identity check on the fulfilled receipt: the pending map is keyed
        // (sender, nonce), so with racing replicas — or any upstream
        // receipt mix-up — the receipt that releases this waiter can belong
        // to a DIFFERENT tx. Echoing its hash as the submit response is how
        // #156 surfaced (in-cluster nonce-unordered: "returned hash !=
        // locally computed"). Fail loudly and NAME both hashes instead: the
        // error attributes the mix-up to the component that produced it.
        if let Ok(resp) = &result
            && resp.receipt.tx_hash != v.tx_hash
        {
            tracing::error!(
                sender = %v.sender,
                nonce = v.nonce,
                submitted = %v.tx_hash,
                receipted = %resp.receipt.tx_hash,
                "receipt fulfilled (sender, nonce) with a DIFFERENT tx — refusing to \
                 answer the submit with a foreign identity"
            );
            count_reject("receipt-identity");
            return Err(IngressError::Internal(format!(
                "receipt for ({}, {}) belongs to a different tx: submitted {} got {}",
                v.sender, v.nonce, v.tx_hash, resp.receipt.tx_hash
            )));
        }

        result
    }

    /// Fire-and-observe submission for subscription-mode clients
    /// (`kardamom_sendRawTransactionAsync`): validates and publishes exactly
    /// like [`Self::submit_raw`], but acks with the tx hash as soon as the
    /// envelope is on tx_data instead of parking the caller until the receipt
    /// arrives. Receipt delivery happens out-of-band — on
    /// `kardamom_subscribeReceipts` or by polling
    /// `eth_getTransactionReceipt`. On this path "accepted" in the metrics
    /// means *published*, not *receipted*; the
    /// `received == accepted + rejected` invariant is unchanged.
    pub async fn submit_raw_async(
        &self,
        client_ip: IpAddr,
        raw_tx: AlloyBytes,
    ) -> Result<B256, IngressError> {
        let v = self.validate_submission(client_ip, &raw_tx).await?;
        if let Some(answer) = self.answer_from_cache(&v) {
            return answer.map(|receipt| receipt.tx_hash);
        }
        self.publish_validated(&v, raw_tx).await?;
        metrics::counter!(crate::metrics::TX_ACCEPTED_TOTAL).increment(1);
        Ok(v.tx_hash)
    }

    /// The single home of the cached-receipt identity rule (#156), shared by
    /// both submit paths. `None` means no cache hit — continue to publish.
    /// On a hit: a resubmission is only a resubmission if it is the SAME tx —
    /// the cache is keyed (sender, nonce), so a DIFFERENT tx reusing a
    /// receipted nonce would otherwise be answered with the previous tx's
    /// receipt — and its hash — as if it had landed (#156: the submit
    /// response must never carry another tx's identity).
    fn answer_from_cache(&self, v: &ValidatedSubmission) -> Option<Result<Receipt, IngressError>> {
        let prev = v.cached.as_ref()?;
        if prev.tx_hash != v.tx_hash {
            count_reject("nonce-conflict");
            return Some(Err(IngressError::Duplicate((v.sender, v.nonce))));
        }
        // Count it so received == accepted + rejected holds on every path.
        metrics::counter!(crate::metrics::TX_ACCEPTED_TOTAL).increment(1);
        Some(Ok(prev.clone()))
    }

    /// Shared head of both submit paths: rate-limit, decode, batch
    /// sig-verify, and receipt-cache lookup. Does not publish.
    async fn validate_submission(
        &self,
        client_ip: IpAddr,
        raw_tx: &AlloyBytes,
    ) -> Result<ValidatedSubmission, IngressError> {
        metrics::counter!(crate::metrics::TX_RECEIVED_TOTAL).increment(1);

        // Overload valve: when the pending registry is this deep the pipeline
        // is not draining — shed new submissions with an explicit retryable
        // error instead of parking them into a wedge (the pre-#86 failure
        // mode: parked submits pinned every connection slot and the ingress
        // refused ALL clients). Applies to both submit paths; depth 0 sheds
        // everything (used by tests).
        let depth = self.pending.len();
        if depth >= self.cfg.pending_shed_depth {
            count_reject("overloaded");
            return Err(IngressError::Overloaded(depth));
        }

        if let Err(e) = self.rate_limiter.check(client_ip) {
            let _ = e; // unit error
            count_reject("rate-limited");
            return Err(IngressError::RateLimited(client_ip.to_string()));
        }

        let env = ConsensusEnvelope::decode(&mut raw_tx.as_ref()).map_err(|e| {
            count_reject("decode-error");
            IngressError::Decode(e.to_string())
        })?;

        // Protocol-limit checks (W1b, docs/agents/l1-client-suite-port-spec.md)
        // — before sig-verify, so a tx that can never execute is refused with
        // a clear error instead of costing a signature recovery and then
        // burning into a `status=false` skip receipt downstream.
        if let ConsensusEnvelope::Eip4844(_) = env {
            count_reject("unsupported-type");
            return Err(IngressError::UnsupportedTxType(0x03));
        }
        if env.gas_limit() > kardamom_types::limits::TX_GAS_LIMIT_CAP {
            count_reject("gas-cap");
            return Err(IngressError::GasLimitExceedsCap(env.gas_limit()));
        }

        let nonce = env.nonce();

        //: the proxy is the *only* place `sender` and
        // `tx_hash` are computed. Both fields are stamped into the envelope
        // before any downstream consumer observes the tx, and the sig-verify
        // failure path returns *before* we publish to Aeron.
        let (sender, tx_hash) = self
            .verifier
            .recover(env, raw_tx.clone())
            .await
            .map_err(|e| {
                if matches!(e, IngressError::SignatureInvalid) {
                    count_reject("signature-invalid");
                } else {
                    count_reject("internal");
                }
                e
            })?;

        let cached = self.cache.lookup(sender, nonce);
        Ok(ValidatedSubmission {
            sender,
            nonce,
            tx_hash,
            cached,
        })
    }

    /// Publish a validated envelope onto tx_data[shard]. The shard is
    /// selected by sender-address hash (`partition_for(sender, K)`) so every
    /// tx from a given sender lands on the same shard's A stream, which lets
    /// the P sequencers per shard nonce-order them consistently. The envelope
    /// carries the canonical `tx_hash` so downstream consumers can dedup and
    /// re-emit it without recomputing (S0).
    async fn publish_validated(
        &self,
        v: &ValidatedSubmission,
        raw_tx: AlloyBytes,
    ) -> Result<(), IngressError> {
        let shard = partition_for(v.sender, self.cfg.partition_count_m) as usize;
        let correlation_id = self.next_correlation_id();
        self.publication
            .publish_tx_data(
                shard,
                TxEnvelope {
                    correlation_id,
                    raw_tx: raw_tx.0.clone(),
                    sender: v.sender,
                    tx_hash: v.tx_hash,
                },
            )
            .await
            .inspect_err(|_| {
                count_reject("partition-unavailable");
            })
    }
}
