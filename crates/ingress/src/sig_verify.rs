//! secp256k1 ECDSA recovery for transaction sender addresses, and the
//! canonical `tx_hash`.
//!
//! Identity guarantee: the proxy is the only component that computes
//! either field. Both come from one pass: `recover_signer()` plus one
//! `keccak256(raw_tx)`. So downstream consumers can trust
//! `TxEnvelope.{sender, tx_hash}` without a check. On failure, the caller
//! must reject the tx at the RPC boundary. It returns
//! `IngressError::SignatureInvalid`, which maps to JSON-RPC `-32602`,
//! before any publish to an `ingress[i]` channel.
//!
//! Two paths:
//! - [`recover_single`]: minimal, used when no batching is active, and as
//!   the correctness reference for the batched path.
//! - [`BatchVerifier`]: a 64-deep ring with a 50µs flush window. The
//!   "batch" spreads out wakeups and task hops. It is not vectorized
//!   math, since secp256k1 does not expose that.

use std::sync::Arc;
use std::time::Duration;

use alloy_consensus::TxEnvelope;
use alloy_consensus::transaction::SignerRecoverable;
use alloy_primitives::{Address, B256, Bytes, keccak256};
use tokio::sync::{Mutex, Notify, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::error::IngressError;

/// Recovers the sender address from a fully decoded `TxEnvelope`, and
/// computes the canonical `tx_hash = keccak256(raw_tx)` in the same pass.
///
/// Returns `(sender, tx_hash)`. On recovery failure, returns
/// `IngressError::SignatureInvalid`. The caller must reject the tx at the
/// RPC boundary before publishing it.
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

/// A 64-deep recovery ring with a 50µs flush window.
///
/// A submitted request parks on a `oneshot` until the ring flushes,
/// either because it reached depth, or because the flush timer fired.
/// Each `recover` call returns `(sender, tx_hash)`. This computes the
/// keccak256 over `raw_tx` alongside ECDSA recovery in the same batch
/// slot, at almost no extra cost next to the ECDSA work. On failure, the
/// caller rejects the tx at the RPC boundary.
///
/// A batch smaller than this threshold is recovered on a single blocking
/// thread. Splitting a 1-3 tx batch costs more in hops than the 42µs per
/// tx it would save. At deployed rates, the ring almost always holds one
/// transaction (a 50µs window at 3.8k tx/s sees 0.2 arrivals on
/// average), so this is the common path.
const PARALLEL_THRESHOLD: usize = 4;

pub struct BatchVerifier {
    inner: Arc<Mutex<Vec<VerifyRequest>>>,
    notify: Arc<Notify>,
    depth: usize,
    /// Number of chunks a full ring splits into. Defaults to the
    /// machine's parallelism, capped, since ingress shares its cores with
    /// the RPC reactor and recovery must not take them all.
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
                // Wait until at least one request is queued. If the
                // verifier is dropped, nothing notifies, and this task
                // just hangs harmlessly.
                notify_for_task.notified().await;
                // This bounds the flush window. If `depth` requests
                // showed up first, the synchronous fast path in
                // `recover` already drained them, so this sleep only
                // resolves the remaining stragglers.
                let deadline = Instant::now() + flush_window;
                tokio::time::sleep_until(deadline).await;
                // This swaps with a reusable scratch buffer. The old
                // drain().collect() allocated a fresh Vec on every flush
                // window. The ring grows once to steady-state capacity,
                // then this buffer is reused.
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

    /// Recovers a batch off the async runtime, fanned out across the
    /// blocking pool.
    ///
    /// This fixes two measured problems:
    ///
    /// 1. Recovery costs 42µs of pure CPU per transaction. Running it
    ///    inside the flush task blocked a tokio worker that also serves
    ///    RPC connections. This is a classic async anti-pattern, and it
    ///    shows up as tail latency rather than lost throughput.
    /// 2. The work parallelizes easily: 23.3k tx/s on one thread, 45.3k
    ///    on two, 85.7k on four (see `tests/stage_costs.rs`). But the old
    ///    loop ran strictly in sequence, so a full 64-deep ring took
    ///    2.7ms of serial work.
    ///
    /// The ECDSA math itself cannot be batched. Recovery produces a
    /// distinct public key for each signature, so there is no
    /// random-linear-combination trick like Ed25519 batch verification
    /// or BLS aggregation uses. Parallelism is the only gain available
    /// here.
    async fn process_batch(batch: Vec<VerifyRequest>, parallelism: usize) {
        if batch.is_empty() {
            return;
        }
        // Below the threshold, splitting costs more than it saves: one
        // chunk is 42µs of work against the cost of a spawn_blocking hop.
        let workers = if batch.len() < PARALLEL_THRESHOLD || parallelism <= 1 {
            1
        } else {
            parallelism.min(batch.len())
        };
        if workers == 1 {
            let _ = tokio::task::spawn_blocking(move || Self::recover_chunk(batch)).await;
            return;
        }
        // This uses a shared cursor, not a split. `split_off` reallocates
        // and copies the remaining tail once per chunk. Cutting a 64-deep
        // ring into 8 chunks moved 224 requests through 7 progressively
        // smaller allocations, which the CI allocation gate caught as
        // +1374 B/op over the 4254 B/op baseline. Allocs/op stayed the
        // same: few allocations, but large ones. Handing every worker the
        // same iterator moves each request exactly once, and allocates
        // one Arc for the whole batch. It also load-balances: recovery
        // costs the same per signature, but the blocking pool's threads
        // do not start at the same time, so a worker that starts late
        // simply takes fewer requests.
        //
        // Lock cost does not matter here: one uncontended acquire per
        // request, against 42µs of ECDSA work on the other side of it.
        let cursor = Arc::new(std::sync::Mutex::new(batch.into_iter()));
        let mut handles = Vec::with_capacity(workers);
        for _ in 0..workers {
            let cursor = cursor.clone();
            handles.push(tokio::task::spawn_blocking(move || {
                loop {
                    let next = cursor
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .next();
                    let Some(req) = next else { break };
                    let res = recover_single(&req.env, &req.raw_tx);
                    let _ = req.respond.send(res);
                }
            }));
        }
        for h in handles {
            // A panicking worker drops its senders, so the awaiting
            // callers see the verifier as gone, instead of hanging.
            let _ = h.await;
        }
    }

    /// The synchronous core. Recovers each request and answers its
    /// caller.
    fn recover_chunk(batch: Vec<VerifyRequest>) {
        for req in batch {
            let res = recover_single(&req.env, &req.raw_tx);
            let _ = req.respond.send(res);
        }
    }

    /// Submits a tx envelope, with its raw bytes, and awaits
    /// `(sender, tx_hash)`. Flushes right away if the ring fills.
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
            // The ring is full, so recover now instead of waiting out
            // the timer. This still runs on the blocking pool, never on
            // this task's own thread.
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

    /// Produces a freshly signed legacy tx, its RLP bytes, and the signer
    /// address. Every sig-verify and routing test in the crate shares
    /// this.
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
        // tx_hash must equal keccak256(raw_tx), the canonical hash
        // definition.
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
        // The 60s timer never fires, so the depth flush must complete in
        // under 500ms, even on slow CI runners.
        assert!(start.elapsed() < Duration::from_millis(500));
    }
}
