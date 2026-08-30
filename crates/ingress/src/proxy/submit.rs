//! The client-facing submit path. `submit_raw` blocks and parks until a
//! receipt arrives. `submit_raw_async` acks on publish. Both share the
//! validate, cache-answer, and publish stages.

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

        // Park before publishing. Under load, the receipt can arrive on
        // the cache channel before this code would otherwise register the
        // wait. The registry itself maintains the queue-depth gauge, on
        // every insert and remove path, including a cancelled handler's
        // Drop.
        let wait = self.pending.register(v.sender, v.nonce);

        self.publish_validated(&v, raw_tx).await?;

        let result = wait
            .await_with_timeout(self.cfg.pending_receipt_timeout)
            .await;

        // Count accepted or rejected on the terminal outcome, not at
        // publish time, so a single submission never increments both.
        match &result {
            Ok(_) => {
                metrics::counter!(crate::metrics::TX_ACCEPTED_TOTAL).increment(1);
            }
            Err(IngressError::Timeout) => count_reject("timeout"),
            Err(IngressError::Duplicate(_)) => count_reject("duplicate"),
            Err(_) => count_reject("internal"),
        }

        // Identity check on the fulfilled receipt. The pending map is
        // keyed by (sender, nonce). So, with racing replicas, or any
        // upstream receipt mix-up, the receipt that releases this waiter
        // can belong to a different tx. Echoing its hash as the submit
        // response is how issue #156 surfaced: an in-cluster
        // nonce-unordered case where the returned hash did not match the
        // locally computed hash. Instead, this code fails loudly and
        // names both hashes, so the error attributes the mix-up to the
        // component that produced it.
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
    /// (`kardamom_sendRawTransactionAsync`). This method validates and
    /// publishes exactly like [`Self::submit_raw`], but it acks with the tx
    /// hash as soon as the envelope is on tx_data, instead of parking the
    /// caller until the receipt arrives. Receipt delivery happens
    /// separately, through `kardamom_subscribeReceipts` or by polling
    /// `eth_getTransactionReceipt`. On this path, "accepted" in the
    /// metrics means published, not receipted. The
    /// `received == accepted + rejected` invariant still holds.
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

    /// The one place for the cached-receipt identity rule (issue #156),
    /// shared by both submit paths. `None` means no cache hit, so the
    /// caller should continue to publish. On a hit, a resubmission counts
    /// as a resubmission only if it is the same tx. The cache is keyed by
    /// (sender, nonce), so a different tx that reuses a receipted nonce
    /// would otherwise get answered with the previous tx's receipt, and
    /// hash, as if it had landed. The submit response must never carry
    /// another tx's identity.
    fn answer_from_cache(&self, v: &ValidatedSubmission) -> Option<Result<Receipt, IngressError>> {
        let prev = v.cached.as_ref()?;
        if prev.tx_hash != v.tx_hash {
            count_reject("nonce-conflict");
            return Some(Err(IngressError::Duplicate((v.sender, v.nonce))));
        }
        // Count it, so received == accepted + rejected holds on every path.
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

        // Overload valve. When the pending registry reaches this depth,
        // the pipeline is not draining. This code sheds new submissions
        // with an explicit retryable error, instead of parking them into
        // a backlog. Before issue #86 fixed this, parked submits pinned
        // every connection slot and the ingress refused every client.
        // This applies to both submit paths. A depth of 0 sheds
        // everything, and tests use this.
        let depth = self.pending.len();
        if depth >= self.cfg.pending_shed_depth {
            count_reject("overloaded");
            return Err(IngressError::Overloaded(depth));
        }

        if let Err(e) = self.rate_limiter.check(client_ip) {
            let _ = e; // This error carries no data.
            count_reject("rate-limited");
            return Err(IngressError::RateLimited(client_ip.to_string()));
        }

        let env = ConsensusEnvelope::decode(&mut raw_tx.as_ref()).map_err(|e| {
            count_reject("decode-error");
            IngressError::Decode(e.to_string())
        })?;

        // Protocol-limit checks (W1b,
        // docs/agents/l1-client-suite-port-spec.md) run before sig-verify.
        // This way, a tx that can never execute gets a clear error, and
        // does not cost a signature recovery or turn into a
        // `status=false` skip receipt downstream.
        if let ConsensusEnvelope::Eip4844(_) = env {
            count_reject("unsupported-type");
            return Err(IngressError::UnsupportedTxType(0x03));
        }
        if env.gas_limit() > kardamom_types::limits::TX_GAS_LIMIT_CAP {
            count_reject("gas-cap");
            return Err(IngressError::GasLimitExceedsCap(env.gas_limit()));
        }

        let nonce = env.nonce();

        // Identity guarantee: the proxy is the only place that computes
        // `sender` and `tx_hash`. This code stamps both fields into the
        // envelope before any other consumer sees the tx. The sig-verify
        // failure path returns before this code publishes to Aeron.
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

    /// Publishes a validated envelope onto tx_data[shard]. The shard comes
    /// from the sender-address hash, `partition_for(sender, K)`, so every
    /// tx from a given sender lands on the same shard's A stream. This
    /// lets the P sequencers per shard nonce-order them consistently. The
    /// envelope carries the canonical `tx_hash`, so downstream consumers
    /// can dedup and re-emit it without recomputing it.
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
