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

use std::time::Duration;

use alloy_consensus::TxEnvelope;
use alloy_consensus::transaction::SignerRecoverable;
use alloy_primitives::{Address, B256, Bytes, keccak256};
use tokio::sync::{mpsc, oneshot};
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

/// Batched recovery: `depth`-sized batches with a `flush_window` cap.
///
/// One consumer task pulls requests from a bounded mpsc channel with
/// `recv_many`. A batch flushes when it reaches `depth`, or when the
/// window ends with a partial batch. The old shape was this channel built
/// by hand: a `Mutex<Vec>` as the queue, a `Notify` as the wake, and a
/// race where two `recover` callers both saw `len() >= depth` and the
/// second drained an empty Vec.
///
/// Each `recover` call returns `(sender, tx_hash)`. The keccak256 over
/// `raw_tx` is computed alongside ECDSA recovery in the same batch slot
/// (essentially free vs. the ECDSA cost). Failure ⇒ caller rejects at the
/// RPC boundary.
///
/// The queue is bounded at `depth * 4`: an overloaded verifier
/// backpressures the RPC handlers instead of growing without limit. The
/// consumer task exits when the verifier drops (all senders gone).
pub struct BatchVerifier {
    tx: mpsc::Sender<VerifyRequest>,
}

impl BatchVerifier {
    pub fn new(depth: usize, flush_window: Duration) -> Self {
        assert!(depth > 0);
        let (tx, mut rx) = mpsc::channel::<VerifyRequest>(depth * 4);
        tokio::spawn(async move {
            let mut batch: Vec<VerifyRequest> = Vec::with_capacity(depth);
            loop {
                // Park until at least one request arrives. `recv_many`
                // returns 0 only when every sender is gone — the verifier
                // dropped, so the task exits.
                if rx.recv_many(&mut batch, depth).await == 0 {
                    return;
                }
                // Top the batch up inside the flush window. A full batch
                // skips the wait entirely — `recv_many` already returned
                // everything queued, and the loop condition is false.
                let deadline = Instant::now() + flush_window;
                while batch.len() < depth {
                    let want = depth - batch.len();
                    match tokio::time::timeout_at(deadline, rx.recv_many(&mut batch, want)).await {
                        // Window over, or senders gone with a partial
                        // batch: flush what we have.
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
                batch = Self::process_batch_blocking(batch).await;
            }
        });
        Self { tx }
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
    /// A full batch flushes at once; a partial batch flushes at the window.
    pub async fn recover(
        &self,
        env: TxEnvelope,
        raw_tx: Bytes,
    ) -> Result<(Address, B256), IngressError> {
        let (otx, orx) = oneshot::channel();
        self.tx
            .send(VerifyRequest {
                env,
                raw_tx,
                respond: otx,
            })
            .await
            .map_err(|_| IngressError::Internal("verifier task gone".into()))?;
        orx.await
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
