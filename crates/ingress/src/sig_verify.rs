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
pub struct BatchVerifier {
    inner: Arc<Mutex<Vec<VerifyRequest>>>,
    notify: Arc<Notify>,
    depth: usize,
    _flush_task: JoinHandle<()>,
}

impl BatchVerifier {
    pub fn new(depth: usize, flush_window: Duration) -> Self {
        assert!(depth > 0);
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
                if scratch.is_empty() {
                    continue;
                }
                scratch = Self::process_batch_blocking(scratch).await;
            }
        });
        Self {
            inner,
            notify,
            depth,
            _flush_task: flush,
        }
    }

    /// Run the CPU-bound recovery for `batch` on the blocking pool
    /// (`spawn_blocking`), so ECDSA never stalls the tokio workers. Each
    /// request's result is sent on its `oneshot` from the blocking thread.
    /// Returns the (drained) Vec so the caller can reuse its capacity.
    async fn process_batch_blocking(mut batch: Vec<VerifyRequest>) -> Vec<VerifyRequest> {
        match tokio::task::spawn_blocking(move || {
            for req in batch.drain(..) {
                // Per-tx recovery + keccak; the "batch" amortizes wakeups,
                // not math.
                let res = recover_single(&req.env, &req.raw_tx);
                let _ = req.respond.send(res);
            }
            batch
        })
        .await
        {
            Ok(batch) => batch,
            // The blocking task panicked: the pending `oneshot` senders are
            // dropped with it, so every waiter sees "verifier dropped".
            Err(e) => {
                tracing::error!(error = %e, "sig_verify: recovery batch panicked");
                Vec::new()
            }
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
            // Drain and process now to avoid waiting for the timer.
            let drained: Vec<VerifyRequest> = {
                let mut g = self.inner.lock().await;
                g.drain(..).collect()
            };
            let _ = Self::process_batch_blocking(drained).await;
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
