//! secp256k1 ECDSA recovery for transaction sender addresses + canonical
//! `tx_hash`.
//!
//!: the proxy is the **only** component that computes
//! either field. Both are produced together (`recover_signer()` + a single
//! `keccak256(raw_tx)` pass) so downstream consumers may trust
//! `TxEnvelope.{sender, tx_hash}` unconditionally. Failure ⇒ caller MUST
//! reject at the RPC boundary (returns `IngressError::SignatureInvalid`,
//! which maps to JSON-RPC `-32602`) before any publish to an `ingress[i]`
//! channel.
//!
//! Two paths:
//! - [`recover_single`] — minimal, used either when no batching is active or
//!   as the correctness reference for the batched path.
//! - [`BatchVerifier`] — 64-deep ring with a 50µs flush window. The "batch"
//!   amortizes wakeups + task hops, not vectorized math (secp256k1 doesn't
//!   expose that).

use std::sync::Arc;
use std::time::Duration;

use alloy_consensus::TxEnvelope;
use alloy_consensus::transaction::SignerRecoverable;
use alloy_primitives::{Address, B256, Bytes, keccak256};
use tokio::sync::{Mutex, Notify, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::error::IngressError;

/// Recover the sender address from a fully-decoded `TxEnvelope` and compute
/// the canonical `tx_hash = keccak256(raw_tx)` in the same pass.
///
/// Returns `(sender, tx_hash)`. On recovery failure, returns
/// `IngressError::SignatureInvalid` — callers MUST reject the tx at the RPC
/// boundary before publishing.
pub fn recover_single(env: &TxEnvelope, raw_tx: &Bytes) -> Result<(Address, B256), IngressError> {
    let sender = env
        .recover_signer()
        .map_err(|_| IngressError::SignatureInvalid)?;
    let tx_hash = keccak256(raw_tx.as_ref());
    Ok((sender, tx_hash))
}

/// A request submitted to `BatchVerifier::recover`.
struct VerifyRequest {
    env: TxEnvelope,
    raw_tx: Bytes,
    respond: oneshot::Sender<Result<(Address, B256), IngressError>>,
}

/// 64-deep recovery ring with a 50µs flush window.
///
/// Submitted requests park on a `oneshot` until the ring is flushed: either
/// because depth was reached, or because the flush timer fires.
///
///: each `recover` call returns `(sender, tx_hash)`. The
/// keccak256 over `raw_tx` is computed alongside ECDSA recovery in the same
/// batch slot (essentially free vs. the ECDSA cost). Failure ⇒ caller rejects
/// at the RPC boundary.
/// Batches smaller than this are recovered on a single blocking thread —
/// splitting a 1-3 tx batch costs more in hops than the 42µs/tx it saves.
/// At deployed rates the ring almost always holds ONE transaction (a 50µs
/// window at 3.8k tx/s sees 0.2 arrivals), so this is the common path.
const PARALLEL_THRESHOLD: usize = 4;

pub struct BatchVerifier {
    inner: Arc<Mutex<Vec<VerifyRequest>>>,
    notify: Arc<Notify>,
    depth: usize,
    /// Chunks a full ring is split into. Defaults to the machine's
    /// parallelism, capped: ingress shares its cores with the RPC
    /// reactor, so recovery must not monopolize them.
    parallelism: usize,
    _flush_task: JoinHandle<()>,
}

impl BatchVerifier {
    pub fn new(depth: usize, flush_window: Duration) -> Self {
        let parallelism = std::thread::available_parallelism()
            .map(|n| n.get().clamp(1, 8))
            .unwrap_or(1);
        Self::with_parallelism(depth, flush_window, parallelism)
    }

    pub fn with_parallelism(depth: usize, flush_window: Duration, parallelism: usize) -> Self {
        assert!(depth > 0);
        let parallelism = parallelism.max(1);
        let inner: Arc<Mutex<Vec<VerifyRequest>>> = Arc::new(Mutex::new(Vec::with_capacity(depth)));
        let notify = Arc::new(Notify::new());
        let inner_for_task = inner.clone();
        let notify_for_task = notify.clone();
        let flush = tokio::spawn(async move {
            let mut scratch: Vec<VerifyRequest> = Vec::new();
            loop {
                // Wait until at least one request is queued (or the verifier is
                // dropped; in that case nothing notifies and the task just
                // hangs harmlessly).
                notify_for_task.notified().await;
                // Bound the flush window. If `depth` requests showed up first
                // the synchronous fast-path in `recover` already drained them,
                // so this sleep just resolves remaining stragglers.
                let deadline = Instant::now() + flush_window;
                tokio::time::sleep_until(deadline).await;
                // Swap with a reusable scratch: the old drain().collect()
                // allocated a fresh Vec per flush window (the ring grows
                // once to steady-state capacity and is then reused).
                scratch.clear();
                {
                    let mut g = inner_for_task.lock().await;
                    std::mem::swap(&mut *g, &mut scratch);
                }
                Self::process_batch(std::mem::take(&mut scratch), parallelism).await;
            }
        });
        Self {
            inner,
            notify,
            depth,
            parallelism,
            _flush_task: flush,
        }
    }

    /// Recover a batch OFF the async runtime, fanning out across the
    /// blocking pool.
    ///
    /// Two properties this fixes, both measured:
    ///
    /// 1. Recovery is 42µs of pure CPU per transaction. Running it inside
    ///    the flush task blocked a tokio worker that is also serving RPC
    ///    connections — the classic async anti-pattern, and it shows up as
    ///    tail latency rather than throughput.
    /// 2. It is embarrassingly parallel — 23.3k tx/s on one thread, 45.3k
    ///    on two, 85.7k on four (see `tests/stage_costs.rs`) — but the old
    ///    loop was strictly sequential, so a full 64-deep ring was 2.7ms
    ///    of serial work.
    ///
    /// The ECDSA math itself cannot be batched: recovery produces a
    /// DISTINCT public key per signature, so there is no random-linear-
    /// combination trick as with Ed25519 batch verification or BLS
    /// aggregation. Parallelism is the whole win available here.
    async fn process_batch(batch: Vec<VerifyRequest>, parallelism: usize) {
        if batch.is_empty() {
            return;
        }
        // Below the threshold, splitting costs more than it saves: one
        // chunk is 42µs of work against a spawn_blocking hop.
        let chunks = if batch.len() < PARALLEL_THRESHOLD || parallelism <= 1 {
            1
        } else {
            parallelism.min(batch.len())
        };
        let per = batch.len().div_ceil(chunks);
        let mut handles = Vec::with_capacity(chunks);
        let mut rest = batch;
        while !rest.is_empty() {
            let take = per.min(rest.len());
            let tail = rest.split_off(take);
            let head = std::mem::replace(&mut rest, tail);
            handles.push(tokio::task::spawn_blocking(move || {
                Self::recover_chunk(head);
            }));
        }
        for h in handles {
            // A panicking chunk drops its senders, so the awaiting
            // callers see the verifier as gone rather than hanging.
            let _ = h.await;
        }
    }

    /// The synchronous core: recover each request and answer its caller.
    fn recover_chunk(batch: Vec<VerifyRequest>) {
        for req in batch {
            let res = recover_single(&req.env, &req.raw_tx);
            let _ = req.respond.send(res);
        }
    }

    /// Submit a tx envelope (plus its raw bytes) and await `(sender, tx_hash)`.
    /// Flushes immediately if the ring fills.
    pub async fn recover(
        &self,
        env: TxEnvelope,
        raw_tx: Bytes,
    ) -> Result<(Address, B256), IngressError> {
        let (tx, rx) = oneshot::channel();
        let should_flush_now = {
            let mut g = self.inner.lock().await;
            g.push(VerifyRequest {
                env,
                raw_tx,
                respond: tx,
            });
            g.len() >= self.depth
        };
        if should_flush_now {
            // Ring full: recover now rather than waiting out the timer —
            // but on the blocking pool, never on this task's thread.
            let drained: Vec<VerifyRequest> = {
                let mut g = self.inner.lock().await;
                g.drain(..).collect()
            };
            Self::process_batch(drained, self.parallelism).await;
        } else {
            self.notify.notify_one();
        }
        rx.await
            .map_err(|_| IngressError::Internal("verifier dropped".into()))?
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use alloy_consensus::{SignableTransaction, TxLegacy};
    use alloy_primitives::{Signature, TxKind, U256};
    use alloy_rlp::Encodable;
    use alloy_signer_local::PrivateKeySigner;
    use k256::ecdsa::{RecoveryId, signature::hazmat::PrehashSigner};

    /// Produce a freshly-signed legacy tx along with its RLP bytes and the
    /// signer address. Shared by every sig-verify and routing test in the
    /// crate.
    pub(crate) fn signed_legacy_envelope() -> (TxEnvelope, Bytes, Address) {
        let signer = PrivateKeySigner::random();
        let addr = signer.address();
        let tx = TxLegacy {
            chain_id: Some(1),
            nonce: 0,
            gas_price: 1_000_000_000,
            gas_limit: 21_000,
            to: TxKind::Call(Address::ZERO),
            value: U256::ZERO,
            input: Default::default(),
        };
        let (k256_sig, rid): (k256::ecdsa::Signature, RecoveryId) = signer
            .credential()
            .sign_prehash(tx.signature_hash().as_slice())
            .unwrap();
        let alloy_sig = Signature::from_signature_and_parity(k256_sig, rid.is_y_odd());
        let signed = tx.into_signed(alloy_sig);
        let env: TxEnvelope = signed.into();
        let mut buf = Vec::new();
        env.encode(&mut buf);
        (env, Bytes::from(buf), addr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovers_legacy_sender_and_hash() {
        let (env, raw, expected) = super::test_support::signed_legacy_envelope();
        let (recovered, tx_hash) = recover_single(&env, &raw).unwrap();
        assert_eq!(recovered, expected);
        // tx_hash MUST equal keccak256(raw_tx) — the canonical hash defined
        // by S0.
        assert_eq!(tx_hash, keccak256(raw.as_ref()));
    }
}

#[cfg(test)]
mod batch_tests {
    use super::*;
    use crate::sig_verify::test_support::signed_legacy_envelope;

    #[tokio::test]
    async fn batched_matches_single_for_random_corpus() {
        let v = BatchVerifier::new(64, Duration::from_micros(50));
        let mut futs = Vec::new();
        let mut expected = Vec::new();
        for _ in 0..100 {
            let (env, raw, addr) = signed_legacy_envelope();
            let expected_hash = keccak256(raw.as_ref());
            expected.push((addr, expected_hash));
            futs.push(v.recover(env, raw));
        }
        let actual = futures::future::join_all(futs).await;
        for (i, res) in actual.into_iter().enumerate() {
            assert_eq!(res.unwrap(), expected[i], "mismatch at index {i}");
        }
    }

    #[tokio::test]
    async fn flushes_on_depth_without_waiting_for_timer() {
        let v = BatchVerifier::new(8, Duration::from_secs(60));
        let mut futs = Vec::new();
        for _ in 0..8 {
            let (env, raw, _) = signed_legacy_envelope();
            futs.push(v.recover(env, raw));
        }
        let start = Instant::now();
        let _ = futures::future::join_all(futs).await;
        // 60s timer never fires; depth-flush must complete in <500ms on slow
        // CI runners.
        assert!(start.elapsed() < Duration::from_millis(500));
    }
}
