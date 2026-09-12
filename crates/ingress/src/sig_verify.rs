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
//! - [`BatchVerifier`]: a bounded ring with a flush window. The "batch"
//!   spreads out wakeups and task hops. It is not vectorized math, since
//!   secp256k1 does not expose that.

use std::num::NonZeroUsize;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

use alloy_consensus::TxEnvelope;
use alloy_consensus::transaction::SignerRecoverable;
use alloy_primitives::{Address, B256, Bytes, keccak256};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::error::IngressError;
use crate::sync_util::LockIgnorePoison;

/// Recovers the sender address from a fully decoded `TxEnvelope`, and
/// computes the canonical `tx_hash = keccak256(raw_tx)` in the same pass.
///
/// Returns `(sender, tx_hash)`.
///
/// # Errors
///
/// Returns `IngressError::SignatureInvalid` if recovery fails. The caller
/// must reject the tx at the RPC boundary before publishing it.
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

impl VerifyRequest {
    /// Recovers this request and sends the result to its waiting caller.
    /// A dropped receiver (the caller went away) is not an error here.
    fn recover_and_respond(self) {
        let res = recover_single(&self.env, &self.raw_tx);
        let _ = self.respond.send(res);
    }
}

/// One batch's shared work cursor. Every blocking worker pulls from the
/// same iterator, so each request moves exactly once and a worker that
/// starts late simply takes fewer requests.
struct RequestCursor {
    requests: std::sync::Mutex<std::vec::IntoIter<VerifyRequest>>,
}

impl RequestCursor {
    fn new(batch: Vec<VerifyRequest>) -> Self {
        Self {
            requests: std::sync::Mutex::new(batch.into_iter()),
        }
    }

    /// Take the next request off the cursor. The lock guard dies with
    /// this call, before the caller starts recovery, so a worker never
    /// holds the shared cursor lock across the 42µs of ECDSA work.
    fn next(&self) -> Option<VerifyRequest> {
        self.requests.lock_ignore_poison().next()
    }

    /// One worker's whole life: recover requests until the cursor runs
    /// dry.
    fn drain(&self) {
        while let Some(req) = self.next() {
            req.recover_and_respond();
        }
    }
}

/// A recovery ring with a bounded depth and a flush window.
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
    tx: mpsc::UnboundedSender<VerifyRequest>,
    _flush_task: JoinHandle<()>,
}

/// Owns one flush task's channel and knobs, and its two loops:
/// [`Self::run`] drains the channel into batches, and
/// [`Self::fill_until_deadline`] is its inner loop, racing the flush
/// window's deadline against further arrivals. Splitting these into two
/// methods on this struct, instead of one function with a loop nested
/// inside a loop, means neither loop's body needs to thread the other's
/// state through as loose parameters.
struct FlushLoop {
    rx: mpsc::UnboundedReceiver<VerifyRequest>,
    depth: NonZeroUsize,
    flush_window: Duration,
    parallelism: NonZeroUsize,
}

impl FlushLoop {
    fn new(
        rx: mpsc::UnboundedReceiver<VerifyRequest>,
        depth: NonZeroUsize,
        flush_window: Duration,
        parallelism: NonZeroUsize,
    ) -> Self {
        Self {
            rx,
            depth,
            flush_window,
            parallelism,
        }
    }

    /// Drains the channel into batches, one flush per loop iteration.
    ///
    /// `recv_many` blocks until at least one request arrives, then grabs
    /// everything already queued, up to `depth`. If the ring is not yet
    /// full, [`Self::fill_until_deadline`] races the flush window's
    /// deadline against further arrivals, so a burst that fills the ring
    /// mid-window still flushes at depth, instead of waiting out the rest
    /// of the window. This ends when the channel closes, which happens
    /// when the owning `BatchVerifier` drops.
    async fn run(mut self) {
        loop {
            match self.run_one_batch().await {
                ControlFlow::Break(()) => return,
                ControlFlow::Continue(()) => {}
            }
        }
    }

    /// One [`Self::run`] pass: fill a batch (racing the flush window once
    /// it holds at least one request), then flush it. `Break` means the
    /// channel closed.
    async fn run_one_batch(&mut self) -> ControlFlow<()> {
        let depth = self.depth.get();
        let mut buf = Vec::with_capacity(depth);
        if self.rx.recv_many(&mut buf, depth).await == 0 {
            return ControlFlow::Break(());
        }
        if buf.len() < depth {
            self.fill_until_deadline(&mut buf).await;
        }
        self.process_batch(buf).await;
        ControlFlow::Continue(())
    }

    /// Recovers a batch off the async runtime, fanned out across the
    /// blocking pool.
    ///
    /// Recovery costs 42µs of pure CPU per transaction. Running it inline
    /// on the flush task would block a tokio worker that also serves RPC
    /// connections. The work parallelizes easily: 23.3k tx/s on one
    /// thread, 45.3k on two, 85.7k on four (see `tests/stage_costs.rs`).
    ///
    /// The ECDSA math itself cannot be batched. Recovery produces a
    /// distinct public key for each signature, so there is no
    /// random-linear-combination trick like Ed25519 batch verification
    /// or BLS aggregation uses. Parallelism is the only gain available
    /// here.
    async fn process_batch(&self, batch: Vec<VerifyRequest>) {
        let Some(batch_len) = NonZeroUsize::new(batch.len()) else {
            return;
        };
        // Below the threshold, splitting costs more than it saves: one
        // chunk is 42µs of work against the cost of a spawn_blocking hop.
        let workers = if batch.len() < PARALLEL_THRESHOLD || self.parallelism == NonZeroUsize::MIN {
            1
        } else {
            self.parallelism.min(batch_len).get()
        };
        if workers == 1 {
            let _ = tokio::task::spawn_blocking(move || {
                batch
                    .into_iter()
                    .for_each(VerifyRequest::recover_and_respond);
            })
            .await;
            return;
        }
        // This uses a shared cursor, not a split. `split_off` reallocates
        // and copies the remaining tail once per chunk. Handing every
        // worker the same iterator moves each request exactly once, and
        // allocates one Arc for the whole batch. It also load-balances:
        // recovery costs the same per signature, but the blocking pool's
        // threads do not start at the same time, so a worker that starts
        // late simply takes fewer requests.
        //
        // Lock cost does not matter here: one uncontended acquire per
        // request, against 42µs of ECDSA work on the other side of it.
        let cursor = Arc::new(RequestCursor::new(batch));
        let handles: Vec<_> = (0..workers)
            .map(|_| {
                let cursor = cursor.clone();
                tokio::task::spawn_blocking(move || cursor.drain())
            })
            .collect();
        for h in handles {
            // A panicking worker drops its senders, so the awaiting
            // callers see the verifier as gone, instead of hanging.
            let _ = h.await;
        }
    }

    /// Races the flush window's deadline against further arrivals until
    /// `buf` reaches `self.depth`, or the deadline passes.
    async fn fill_until_deadline(&mut self, buf: &mut Vec<VerifyRequest>) {
        let depth = self.depth.get();
        // `Instant + Duration` panics on overflow (R12). `checked_add`
        // guards it; on overflow, flush right away instead of panicking
        // or waiting out a window that can never elapse. The only
        // producer of `flush_window` today is the 50µs default, so this
        // branch is unreached in practice.
        let Some(deadline) = Instant::now().checked_add(self.flush_window) else {
            return;
        };
        loop {
            let remaining = depth - buf.len();
            tokio::select! {
                biased;
                () = tokio::time::sleep_until(deadline) => return,
                n = self.rx.recv_many(buf, remaining) => {
                    if n == 0 || buf.len() >= depth {
                        return;
                    }
                }
            }
        }
    }
}

impl BatchVerifier {
    #[must_use]
    pub fn new(depth: NonZeroUsize, flush_window: Duration) -> Self {
        use nonzero_ext::nonzero;
        let parallelism = std::thread::available_parallelism()
            .map_or(NonZeroUsize::MIN, |n| n.min(nonzero!(8usize)));
        Self::with_parallelism(depth, flush_window, parallelism)
    }

    #[must_use]
    pub fn with_parallelism(
        depth: NonZeroUsize,
        flush_window: Duration,
        parallelism: NonZeroUsize,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let flush_task = tokio::spawn(FlushLoop::new(rx, depth, flush_window, parallelism).run());
        Self {
            tx,
            _flush_task: flush_task,
        }
    }

    /// Submits a tx envelope, with its raw bytes, and awaits
    /// `(sender, tx_hash)`.
    ///
    /// # Errors
    ///
    /// Returns `IngressError::SignatureInvalid` if recovery fails, or
    /// `IngressError::Internal` if the flush task is gone.
    pub async fn recover(
        &self,
        env: TxEnvelope,
        raw_tx: Bytes,
    ) -> Result<(Address, B256), IngressError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(VerifyRequest {
                env,
                raw_tx,
                respond: tx,
            })
            .map_err(|_| IngressError::Internal("verifier dropped".into()))?;
        rx.await
            .map_err(|_| IngressError::Internal("verifier dropped".into()))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{SignedTx, sign_legacy_tx};
    use alloy_signer_local::PrivateKeySigner;

    #[test]
    fn recovers_legacy_sender_and_hash() {
        let SignedTx {
            env,
            raw,
            sender: expected,
        } = sign_legacy_tx(&PrivateKeySigner::random(), 0);
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
    use crate::test_support::{SignedTx, sign_legacy_tx};
    use alloy_signer_local::PrivateKeySigner;

    fn depth(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).unwrap()
    }

    #[tokio::test]
    async fn batched_matches_single_for_random_corpus() {
        let v = BatchVerifier::new(depth(64), Duration::from_micros(50));
        let mut futs = Vec::new();
        let mut expected = Vec::new();
        for _ in 0..100 {
            let SignedTx { env, raw, sender } = sign_legacy_tx(&PrivateKeySigner::random(), 0);
            let expected_hash = keccak256(raw.as_ref());
            expected.push((sender, expected_hash));
            futs.push(v.recover(env, raw));
        }
        let actual = futures::future::join_all(futs).await;
        for (i, res) in actual.into_iter().enumerate() {
            assert_eq!(res.unwrap(), expected[i], "mismatch at index {i}");
        }
    }

    #[tokio::test]
    async fn flushes_on_depth_without_waiting_for_timer() {
        let v = BatchVerifier::new(depth(8), Duration::from_secs(60));
        let mut futs = Vec::new();
        for _ in 0..8 {
            let SignedTx { env, raw, .. } = sign_legacy_tx(&PrivateKeySigner::random(), 0);
            futs.push(v.recover(env, raw));
        }
        let start = Instant::now();
        let _ = futures::future::join_all(futs).await;
        // The 60s timer never fires, so the depth flush must complete in
        // under 500ms, even on slow CI runners.
        assert!(start.elapsed() < Duration::from_millis(500));
    }

    // Regression test for the mpsc/recv_many rewrite: a ring that fills
    // gradually, one submit at a time with a yield between each, must
    // still flush at depth instead of waiting out the whole window. This
    // exercises the `select!` race in `flush_loop`, which
    // `flushes_on_depth_without_waiting_for_timer` does not: that test
    // sends every request before the flush task can run once, so the
    // first `recv_many` call already sees a full ring.
    #[tokio::test]
    async fn flushes_at_depth_even_when_the_ring_fills_gradually() {
        let v = BatchVerifier::new(depth(8), Duration::from_secs(60));
        let mut futs = Vec::new();
        for _ in 0..8 {
            let SignedTx { env, raw, .. } = sign_legacy_tx(&PrivateKeySigner::random(), 0);
            futs.push(v.recover(env, raw));
            tokio::task::yield_now().await;
        }
        let start = Instant::now();
        let _ = futures::future::join_all(futs).await;
        assert!(start.elapsed() < Duration::from_millis(500));
    }
}
