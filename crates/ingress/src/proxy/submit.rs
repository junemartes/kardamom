//! The client-facing submit path. `submit_raw` blocks and parks until a
//! receipt arrives. `submit_raw_async` acks on publish. Both share the
//! validate, cache-answer, admission-check, and publish stages.
//!
//! The admission checks read the local account layer, then Redis on a
//! miss when `[cache]` is on, then the executor answer for a cold sender
//! (see `crate::cold_accounts`). Only a fresh answer refuses: a stale,
//! failed or shed query admits. A past nonce answers from a receipt:
//! the local receipt cache, then the Redis receipt index. The same tx
//! gets its receipt, across an ingress restart when Redis holds it;
//! another tx at a nonce that a receipt proves used is `Duplicate`. A
//! skip receipt proves no used nonce, so another tx at its nonce
//! publishes, unless the skip burned its nonce
//! (`Receipt::burns_nonce`). With no receipt in either index, it publishes and the
//! sequencer decides. An unfunded sender rejects as `InsufficientFunds`,
//! only when the balance is fresh. Each refusal is counted by reason and
//! logged at debug level with the sender and the nonce.

use std::net::IpAddr;
use std::time::Instant;

use alloy_consensus::TxEnvelope as ConsensusEnvelope;
use alloy_consensus::transaction::Transaction;
use alloy_primitives::{Address, B256, Bytes as AlloyBytes, U256};
use alloy_rlp::Decodable;

use kardamom_cache::metrics as cache_metrics;
use kardamom_types::{Receipt, TxEnvelope};

use crate::channels::{IngressPublication, IngressSubscription};
use crate::config::AckMode;
use crate::error::IngressError;
use crate::metrics::count_reject;
use crate::pending::ReceiptResponse;
use crate::submit_steps::Timing;

use super::{AccountState, IngressProxy, ValidatedSubmission};

impl<P, S> IngressProxy<P, S>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    /// Hot path for both JSON-RPC and binary submissions. Returns the
    /// receipt once both `(sender, nonce, receipt)` and the quorum watermark
    /// are satisfied.
    ///
    /// # Errors
    ///
    /// Returns an `IngressError` if the submission is rejected (rate
    /// limit, decode, signature, protocol limits, overload) or times out
    /// waiting for a receipt.
    pub async fn submit_raw(
        &self,
        client_ip: IpAddr,
        raw_tx: AlloyBytes,
    ) -> Result<ReceiptResponse, IngressError> {
        let _timing = Timing::start(&self.steps.submit);
        let v = self.validate_submission(client_ip, &raw_tx).await?;
        if let Some(answer) = v.answer_from_cache() {
            return answer.map(|receipt| ReceiptResponse { receipt });
        }
        if let Some(receipt) = self.admit(&v).await? {
            return Ok(ReceiptResponse { receipt });
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
        let result = self.answer_executed_duplicate(client_ip, &v, result).await;

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
        // can belong to a different tx. The response must never carry
        // another tx's hash. This code fails loudly and names both
        // hashes, so the error attributes the mix-up to the component
        // that produced it.
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

    /// The submit of `eth_sendRawTransaction` and of the binary protocol:
    /// the transaction hash. The ack mode decides when it answers: with the
    /// receipt ([`Self::submit_raw`]), when the sequencer takes the
    /// transaction into its mempool, as on Ethereum ([`crate::acks`]), or
    /// when this ingress validated and published it
    /// ([`Self::submit_validated`]).
    ///
    /// # Errors
    ///
    /// Returns an `IngressError` if the submission is rejected, here or by
    /// the sequencer, or if no verdict comes in the ack timeout.
    pub async fn submit(
        &self,
        client_ip: IpAddr,
        raw_tx: AlloyBytes,
    ) -> Result<B256, IngressError> {
        match self.cfg.ack {
            AckMode::Receipt => self
                .submit_raw(client_ip, raw_tx)
                .await
                .map(|resp| resp.receipt.tx_hash),
            AckMode::Admission(_) => self.submit_admitted(client_ip, raw_tx).await,
            AckMode::Validated => self.submit_validated(client_ip, raw_tx).await,
        }
    }

    /// The ack at admission: publish, then wait for the verdict of the
    /// sequencer.
    async fn submit_admitted(
        &self,
        client_ip: IpAddr,
        raw_tx: AlloyBytes,
    ) -> Result<B256, IngressError> {
        let _timing = Timing::start(&self.steps.submit);
        let v = self.validate_submission(client_ip, &raw_tx).await?;
        let wait = self
            .acks
            .register(v.tx_hash)
            .inspect_err(|_| count_reject("already-known"))?;
        self.publish_validated(&v, raw_tx).await?;
        let result = wait.answer().await;
        match &result {
            Ok(_) => metrics::counter!(crate::metrics::TX_ACCEPTED_TOTAL).increment(1),
            Err(IngressError::NotAcknowledged(_)) => count_reject("not-acknowledged"),
            Err(_) => count_reject("sequencer-rejected"),
        }
        result
    }

    /// Fire-and-observe submission for subscription-mode clients
    /// (`kardamom_sendRawTransactionAsync`). This method validates and
    /// publishes exactly like [`Self::submit_raw`], but it acks with the tx
    /// hash as soon as the envelope is on `tx_data`, instead of parking the
    /// caller until the receipt arrives. Receipt delivery happens
    /// separately, through `kardamom_subscribeReceipts` or by polling
    /// `eth_getTransactionReceipt`. On this path, "accepted" in the
    /// metrics means published, not receipted. The
    /// `received == accepted + rejected` invariant still holds. With the
    /// validated ack, this call is [`Self::submit_validated`], the same as
    /// `eth_sendRawTransaction`.
    ///
    /// # Errors
    ///
    /// Returns an `IngressError` if the submission is rejected. See
    /// [`Self::submit_raw`].
    pub async fn submit_raw_async(
        &self,
        client_ip: IpAddr,
        raw_tx: AlloyBytes,
    ) -> Result<B256, IngressError> {
        if self.cfg.ack == AckMode::Validated {
            return self.submit_validated(client_ip, raw_tx).await;
        }
        let _timing = Timing::start(&self.steps.submit);
        let v = self.validate_submission(client_ip, &raw_tx).await?;
        if let Some(answer) = v.answer_from_cache() {
            return answer.map(|receipt| receipt.tx_hash);
        }
        if let Some(receipt) = self.admit(&v).await? {
            return Ok(receipt.tx_hash);
        }
        self.publish_validated(&v, raw_tx).await?;
        metrics::counter!(crate::metrics::TX_ACCEPTED_TOTAL).increment(1);
        Ok(v.tx_hash)
    }

    /// Shared head of both submit paths: rate-limit, size cap, decode, batch
    /// sig-verify, and receipt-cache lookup. Does not publish and rejects
    /// nothing on account state; see [`Self::admit`].
    /// A refused copy of a transaction that executed answers the receipt of
    /// that transaction. The sequencer refuses a copy whose nonce a
    /// transaction used, and this ingress can miss the receipt: it reads
    /// only its own verdicts, or it started after the receipt. The lookup
    /// by hash finds the receipt only when the submitted transaction is the
    /// one that executed.
    async fn answer_executed_duplicate(
        &self,
        client_ip: IpAddr,
        v: &ValidatedSubmission,
        result: Result<ReceiptResponse, IngressError>,
    ) -> Result<ReceiptResponse, IngressError> {
        let Err(IngressError::Duplicate(_)) = &result else {
            return result;
        };
        match self.receipt_by_hash(client_ip, v.tx_hash).await {
            Some(receipt) => Ok(ReceiptResponse { receipt }),
            None => result,
        }
    }

    async fn validate_submission(
        &self,
        client_ip: IpAddr,
        raw_tx: &AlloyBytes,
    ) -> Result<ValidatedSubmission, IngressError> {
        let _timing = Timing::start(&self.steps.validate);
        self.check_intake(client_ip, raw_tx)?;
        self.recover_submission(raw_tx).await
    }

    /// The checks before the decode: the overload valve, the drain, the
    /// pause, the rate limit, and the size cap. Each refusal is counted.
    pub(super) fn check_intake(
        &self,
        client_ip: IpAddr,
        raw_tx: &AlloyBytes,
    ) -> Result<(), IngressError> {
        metrics::counter!(crate::metrics::TX_RECEIVED_TOTAL).increment(1);

        // Overload valve. When the pending registry reaches this depth,
        // the pipeline is not draining. This code sheds new submissions
        // with an explicit retryable error, instead of parking them into
        // a backlog: a parked submit pins its connection, so parking
        // more would only make the backlog worse. This applies to both
        // submit paths. A depth of 0 sheds everything, and tests use
        // this.
        let depth = self.pending.len() + self.acks.len();
        if depth >= self.cfg.pending_shed_depth {
            count_reject("overloaded");
            return Err(IngressError::Overloaded(depth));
        }
        if self.is_draining() {
            count_reject("draining");
            return Err(IngressError::Draining);
        }
        // A paused ingress refuses submits: a root upstream (the sealer's
        // DA-lag guard or lost quorum, or every executor halted) cannot
        // take the transaction, or an operator paused it. The sealer
        // would refuse a record of a DA lag too; answering here saves the
        // round trip and names the root. A state one tick stale costs one
        // resubmit, nothing more.
        if let Some(pause) = kardamom_obs::lifecycle::process().slots().pause {
            count_reject("paused");
            return Err(self.paused_error(pause));
        }

        if let Err(e) = self.rate_limiter.check(client_ip) {
            let _ = e; // This error carries no data.
            count_reject("rate-limited");
            return Err(IngressError::RateLimited(client_ip.to_string()));
        }

        // The size cap runs before the decode: an oversized transaction
        // costs no decode and no recovery.
        self.cfg.check_tx_size(raw_tx)
    }

    /// The decode, the protocol limits, the sender recovery, and the
    /// receipt-cache lookup.
    pub(super) async fn recover_submission(
        &self,
        raw_tx: &AlloyBytes,
    ) -> Result<ValidatedSubmission, IngressError> {
        let env = ConsensusEnvelope::decode(&mut raw_tx.as_ref()).map_err(|e| {
            count_reject("decode-error");
            IngressError::Decode(e.to_string())
        })?;

        // Protocol-limit checks run before sig-verify, to save a
        // recovery: a tx that can never execute gets a clear error
        // instead of turning into a `status=false` skip receipt
        // downstream.
        env.check_protocol_limits()?;

        let nonce = env.nonce();
        let cost = env.worst_case_cost();

        // Identity guarantee: the proxy is the only place that computes
        // `sender` and `tx_hash`. This code stamps both fields into the
        // envelope before any other consumer sees the tx. The sig-verify
        // failure path returns before this code publishes to Aeron.
        let started = Instant::now();
        let recovered = self.verifier.recover(env, raw_tx.clone()).await;
        self.steps.recover.since(started);
        let (sender, tx_hash) = recovered.map_err(|e| {
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
            cost,
        })
    }

    /// The admission checks, after the local receipt cache missed. `Some`
    /// carries the receipt the Redis index holds for a landed tx of the
    /// same identity: the caller answers with it, as a cache hit. `Ok(None)`
    /// admits: no known state, or the checks pass.
    ///
    /// A nonce below the known one is a past nonce: `Duplicate`, the same
    /// error the sequencer's rejection becomes, so a client sees one shape
    /// for one condition. A fresh balance below the worst-case cost is
    /// `InsufficientFunds`. A stale balance, a cost overflow, or no entry
    /// admits: the executor's own checks are the bound.
    async fn admit(&self, v: &ValidatedSubmission) -> Result<Option<Receipt>, IngressError> {
        if !self.cfg.admission_checks {
            return Ok(None);
        }
        let Some(state) = self.account_state(v.sender).await else {
            return Ok(None);
        };
        if v.nonce < state.view.nonce
            && let Some(receipt) = self.answer_past_nonce(v).await?
        {
            return Ok(Some(receipt));
        }
        v.check_balance(&state)?;
        Ok(None)
    }

    /// The sender's latest known state: the local layer, then Redis on a
    /// miss, then the executor answer for a cold sender. Each read is
    /// counted.
    async fn account_state(&self, sender: Address) -> Option<AccountState> {
        if let Some(view) = self.live.get(sender) {
            cache_metrics::record_lookup("live", "hit");
            return Some(AccountState { view, fresh: true });
        }
        cache_metrics::record_lookup("live", "miss");
        if let Some(redis) = &self.redis
            && let Some(view) = redis.account(sender).await
        {
            return Some(AccountState {
                view,
                fresh: redis.fresh(),
            });
        }
        let view = self.cold.fresh_state(sender).await?;
        Some(AccountState { view, fresh: true })
    }

    /// A past nonce: a receipt index may hold the landed tx. The local
    /// receipt cache is read again, since its receipt may have arrived
    /// after the submit's first read, then the Redis receipt index. The
    /// same identity answers with its receipt, which also fills the local
    /// cache. A different identity is `Duplicate`.
    ///
    /// No receipt in either index is `None`: the submit publishes and the
    /// sequencer decides. The account layer can lead both indexes. The
    /// pump applies a batch's rows at once, the receipt bus drops
    /// receipts for a lagging watcher, and the mirror is one more hop
    /// from the same source. A reject there would answer a retry of a
    /// landed tx with `Duplicate` for good.
    async fn answer_past_nonce(
        &self,
        v: &ValidatedSubmission,
    ) -> Result<Option<Receipt>, IngressError> {
        let indexed = match (self.cache.lookup(v.sender, v.nonce), &self.redis) {
            (Some(local), _) => Some(local),
            (None, Some(redis)) => redis.receipt(v.sender, v.nonce).await,
            (None, None) => None,
        };
        let Some(receipt) = indexed else {
            // A receipt-index miss, not a degraded read: the chaos cases
            // read the degraded count as "Redis is dark".
            cache_metrics::record_lookup("receipt", "miss");
            return Ok(None);
        };
        self.cache.insert(receipt.clone());
        v.answer_from(&receipt).transpose()
    }

    /// The newest sealed block this proxy knows of, the base of the
    /// inclusion deadline: the newest executor boundary, or the sealed
    /// head of the sealer's status, whichever is higher. Both count the
    /// sealer's own blocks. The sealer sends its status on every boundary
    /// tick, so the value moves while every executor is down. A deadline
    /// from the executor boundaries alone stops at the outage, and the
    /// sealer then refuses every new transaction as past its deadline.
    fn deadline_clock(&self) -> u64 {
        self.latest_block_number()
            .max(self.cluster_status().sealed_head)
    }

    /// [`Self::offer`], with a failure counted as `partition-unavailable`.
    async fn publish_validated(
        &self,
        v: &ValidatedSubmission,
        raw_tx: AlloyBytes,
    ) -> Result<(), IngressError> {
        self.offer(v, raw_tx).await.inspect_err(|_| {
            count_reject("partition-unavailable");
        })
    }

    /// Publishes a validated envelope onto `tx_data[shard]`. The shard comes
    /// from the sender-address hash, `partition_for(sender, K)`, so every
    /// tx from a given sender lands on the same shard's A stream. This
    /// lets the P sequencers per shard nonce-order them consistently. The
    /// envelope carries the canonical `tx_hash`, so downstream consumers
    /// can dedup and re-emit it without recomputing it. A failure is not
    /// counted here: each caller counts its own refusal.
    pub(super) async fn offer(
        &self,
        v: &ValidatedSubmission,
        raw_tx: AlloyBytes,
    ) -> Result<(), IngressError> {
        let shard = kardamom_types::num::u32_to_usize(self.cfg.lane_for(v.sender));
        let correlation_id = self.next_correlation_id();
        let started = Instant::now();
        let published = self
            .publication
            .publish_tx_data(
                shard,
                TxEnvelope {
                    correlation_id,
                    raw_tx: raw_tx.0.clone(),
                    sender: v.sender,
                    tx_hash: v.tx_hash,
                    max_inclusion_block: self.cfg.inclusion_deadline(self.deadline_clock()),
                },
            )
            .await;
        self.steps.publish_round_trip.since(started);
        published
    }
}

/// Rejects a tx this proxy can never execute: an EIP-4844 blob tx, or one
/// over the per-tx gas cap. `ConsensusEnvelope` (`alloy_consensus::TxEnvelope`)
/// is foreign, so this attaches the check as a local extension trait
/// instead of a free function taking the envelope as a loose parameter.
/// The worst-case cost lives here for the same reason.
trait CheckProtocolLimits {
    fn check_protocol_limits(&self) -> Result<(), IngressError>;
    /// `gas_limit * max_fee_per_gas + value`, the most the tx can take
    /// from the sender. `max_fee_per_gas` is `gas_price` on a legacy
    /// envelope. `None` on overflow.
    fn worst_case_cost(&self) -> Option<U256>;
}

impl CheckProtocolLimits for ConsensusEnvelope {
    fn worst_case_cost(&self) -> Option<U256> {
        U256::from(self.gas_limit())
            .checked_mul(U256::from(self.max_fee_per_gas()))
            .and_then(|gas| gas.checked_add(self.value()))
    }

    fn check_protocol_limits(&self) -> Result<(), IngressError> {
        if let Self::Eip4844(_) = self {
            count_reject("unsupported-type");
            return Err(IngressError::UnsupportedTxType(0x03));
        }
        if self.gas_limit() > kardamom_types::limits::TX_GAS_LIMIT_CAP {
            count_reject("gas-cap");
            return Err(IngressError::GasLimitExceedsCap(self.gas_limit()));
        }
        Ok(())
    }
}

impl ValidatedSubmission {
    /// The one place for the cached-receipt identity rule, shared by
    /// both submit paths. `None` means no cache hit, so the
    /// caller should continue to publish. On a hit, a resubmission counts
    /// as a resubmission only if it is the same tx. The cache is keyed by
    /// (sender, nonce), so a different tx that reuses a receipted nonce
    /// would otherwise get answered with the previous tx's receipt, and
    /// hash, as if it had landed. The submit response must never carry
    /// another tx's identity.
    fn answer_from_cache(&self) -> Option<Result<Receipt, IngressError>> {
        self.answer_from(self.cached.as_ref()?)
    }

    /// The answer a receipt at the submission's `(sender, nonce)` gives.
    /// The same tx gets the receipt. Another tx at a nonce that the
    /// receipt proves used is `Duplicate`: an executed tx, or a skip that
    /// burned its nonce. Any other skip receipt, or a deposit's, proves no
    /// used nonce: the chain still expects the nonce, so another tx at it
    /// gets `None` and publishes.
    fn answer_from(&self, prev: &Receipt) -> Option<Result<Receipt, IngressError>> {
        if prev.tx_hash == self.tx_hash {
            // Count it, so received == accepted + rejected holds on every
            // path.
            metrics::counter!(crate::metrics::TX_ACCEPTED_TOTAL).increment(1);
            return Some(Ok(prev.clone()));
        }
        prev.uses_nonce().then(|| {
            Err(self.refuse(
                "nonce-conflict",
                IngressError::Duplicate((self.sender, self.nonce)),
            ))
        })
    }

    /// Count a refusal of this submission by `reason`, log it at debug
    /// level with the sender and the nonce, and return `err`.
    pub(super) fn refuse(&self, reason: &'static str, err: IngressError) -> IngressError {
        count_reject(reason);
        tracing::debug!(
            sender = %self.sender,
            nonce = self.nonce,
            reason,
            error = %err,
            "submit refused"
        );
        err
    }

    /// The balance check against a known state. A stale balance admits
    /// and counts a degraded check: a stale-low balance would reject a
    /// funded sender.
    fn check_balance(&self, state: &AccountState) -> Result<(), IngressError> {
        let Some(want) = self.cost else {
            return Ok(());
        };
        if !state.fresh {
            cache_metrics::record_degraded("stale");
            return Ok(());
        }
        if state.view.balance < want {
            return Err(self.refuse(
                "insufficient-funds",
                IngressError::InsufficientFunds {
                    address: self.sender,
                    have: state.view.balance,
                    want,
                },
            ));
        }
        Ok(())
    }
}
