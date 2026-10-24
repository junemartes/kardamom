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
use std::time::{Duration, Instant};

use alloy_consensus::TxEnvelope;
use alloy_consensus::transaction::SignerRecoverable;
use alloy_primitives::{Address, B256, Bytes, keccak256};
use kardamom_obs::pipeline::{Batch, Queue, Step};
use nonzero_ext::nonzero;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

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
    /// When the caller submitted it, for the wait before its batch.
    queued: Instant,
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
    /// holds the shared cursor lock across the ECDSA work (42 µs in the
    /// bench, 76-120 µs under load on staging).
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
/// The fewest recoveries that one blocking worker gets. A batch of `n`
/// requests uses `n / MIN_PER_WORKER` workers, at most the parallelism and
/// at least one.
///
/// Each extra worker costs the wake of a pool thread. A worker pays for its
/// wake only when its share of the work costs more than the wake. Staging
/// measured these costs on a shared 4-vCPU node under load:
/// - One recovery: 76-120 µs.
/// - The fixed cost of a batch (a wake): 0.17-0.30 ms.
///
/// A split over two workers saves at most half of the work, that is the
/// share of the second worker:
/// - A batch of 4 saves 2 recoveries, 0.15-0.24 ms. That is not more than
///   the wake, so 4 is break-even or a loss.
/// - In the worst case a worker needs 0.30 ms / 76 µs = 3.9 recoveries to
///   pay for its wake. So 5 is the first count with a margin.
/// - A batch of 10 on two workers saves 5 recoveries, 0.38-0.60 ms. That
///   is more than the wake in every measured case.
///
/// A load that sends in bursts every 10 ms makes batches of about 10 at
/// 1,700 tx/s, so the common batch splits over two workers. The rule is
/// per worker, not per batch. So on an 8-core node a batch of 10 also
/// wakes 2 workers, not 4.
const MIN_PER_WORKER: NonZeroUsize = nonzero!(5usize);

/// Intake capacity, as a multiple of the ring depth. A full intake
/// makes `recover` wait, so overload turns into backpressure on the RPC
/// handlers instead of unbounded queue growth. Four rings of slack
/// absorb a burst while one batch is on the blocking pool.
const INTAKE_RINGS: usize = 4;

pub struct BatchVerifier {
    tx: mpsc::Sender<VerifyRequest>,
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
    rx: mpsc::Receiver<VerifyRequest>,
    depth: NonZeroUsize,
    flush_window: Duration,
    parallelism: NonZeroUsize,
    meters: FlushMeters,
}

/// The pipeline metrics of the flush task, recorded once per batch.
struct FlushMeters {
    /// The requests in each flushed batch.
    batch: Batch,
    /// The recovery of one batch on the blocking pool, from the hop to the
    /// end of its last worker.
    recover: Step,
    /// The requests that queued in the intake while the batch before ran.
    /// The flush task reads it once per batch, before it waits for the
    /// next request. So while the verifier is idle, the gauge holds the
    /// last backlog that it read (sample and hold), not a live depth.
    intake: Queue,
    /// The wait of each request from its submit to the take of its batch.
    wait: Step,
}

impl FlushMeters {
    fn new() -> Self {
        Self {
            batch: Batch::new("ingress.recover_batch"),
            recover: Step::new("ingress.recover_batch"),
            intake: Queue::new("ingress.recover_intake"),
            wait: Step::new("ingress.recover_wait"),
        }
    }
}

impl FlushLoop {
    fn new(
        rx: mpsc::Receiver<VerifyRequest>,
        depth: NonZeroUsize,
        flush_window: Duration,
        parallelism: NonZeroUsize,
    ) -> Self {
        Self {
            rx,
            depth,
            flush_window,
            parallelism,
            meters: FlushMeters::new(),
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
        while self.run_one_batch().await.is_continue() {}
    }

    /// One [`Self::run`] pass: fill a batch (racing the flush window once
    /// it holds at least one request), then flush it. `Break` means the
    /// channel closed.
    async fn run_one_batch(&mut self) -> ControlFlow<()> {
        let depth = self.depth.get();
        let mut buf = Vec::with_capacity(depth);
        self.meters.intake.set(self.rx.len());
        if self.rx.recv_many(&mut buf, depth).await == 0 {
            return ControlFlow::Break(());
        }
        if buf.len() < depth {
            self.fill_until_deadline(&mut buf).await;
        }
        for req in &buf {
            self.meters.wait.since(req.queued);
        }
        self.meters.batch.record(buf.len());
        let started = Instant::now();
        self.process_batch(buf).await;
        self.meters.recover.since(started);
        ControlFlow::Continue(())
    }

    /// Recovers a batch off the async runtime, fanned out across the
    /// blocking pool.
    ///
    /// Recovery costs 42 µs of pure CPU per transaction in the bench, and
    /// 76-120 µs under load on staging. Running it inline
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
        let workers = self.workers(batch.len());
        if workers == NonZeroUsize::MIN {
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
        // request, against the ECDSA work on the other side of it (42 µs
        // in the bench, 76-120 µs under load on staging).
        let cursor = Arc::new(RequestCursor::new(batch));
        let handles: Vec<_> = (0..workers.get())
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

    /// The blocking workers for a batch of `batch_len` requests. Each
    /// worker gets at least `MIN_PER_WORKER` recoveries, so that its share
    /// of the work pays for its wake. The count is at most the parallelism,
    /// and at least one.
    fn workers(&self, batch_len: usize) -> NonZeroUsize {
        NonZeroUsize::new(batch_len / MIN_PER_WORKER)
            .unwrap_or(NonZeroUsize::MIN)
            .min(self.parallelism)
    }

    /// Takes further arrivals until `buf` reaches `self.depth`, or the
    /// flush window passes.
    ///
    /// The window is shorter than one tick of the tokio timer (1 ms), so a
    /// timer would hold each batch for a full millisecond, not for the
    /// window. Each pass yields once instead, so a handler that runs on
    /// the same runtime can queue its request, then takes what is queued
    /// without a wait. A partial batch costs at most the window in passes.
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
        while buf.len() < depth && Instant::now() < deadline {
            tokio::task::yield_now().await;
            buf.extend(std::iter::from_fn(|| self.rx.try_recv().ok()).take(depth - buf.len()));
        }
    }
}

impl BatchVerifier {
    #[must_use]
    pub fn new(depth: NonZeroUsize, flush_window: Duration) -> Self {
        // Half of the cores, at most 8: the other half stays for the tokio
        // workers and the Aeron threads, which serve the same requests.
        let parallelism = std::thread::available_parallelism().map_or(NonZeroUsize::MIN, |n| {
            NonZeroUsize::new(n.get() / 2)
                .unwrap_or(NonZeroUsize::MIN)
                .min(nonzero!(8usize))
        });
        Self::with_parallelism(depth, flush_window, parallelism)
    }

    #[must_use]
    pub fn with_parallelism(
        depth: NonZeroUsize,
        flush_window: Duration,
        parallelism: NonZeroUsize,
    ) -> Self {
        let (tx, rx) = mpsc::channel(depth.get().saturating_mul(INTAKE_RINGS));
        let flush_task = tokio::spawn(FlushLoop::new(rx, depth, flush_window, parallelism).run());
        Self {
            tx,
            _flush_task: flush_task,
        }
    }

    /// Submits a tx envelope, with its raw bytes, and awaits
    /// `(sender, tx_hash)`. When the intake is full, this waits for a
    /// slot, so an overloaded verifier slows its callers down instead of
    /// growing a queue without bound.
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
                queued: Instant::now(),
            })
            .await
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
    use crate::rendered_metrics_tests::Rendered;
    use crate::test_support::{SignedTx, sign_legacy_tx};
    use alloy_signer_local::PrivateKeySigner;

    fn depth(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).unwrap()
    }

    /// A request for a new signed tx. Its caller is gone, so the answer
    /// goes nowhere.
    fn request() -> VerifyRequest {
        let SignedTx { env, raw, .. } = sign_legacy_tx(&PrivateKeySigner::random(), 0);
        VerifyRequest {
            env,
            raw_tx: raw,
            respond: oneshot::channel().0,
            queued: Instant::now(),
        }
    }

    // A worker joins a batch only for 5 or more recoveries, and the
    // parallelism caps the count. A batch of 4 stays on one worker.
    #[test]
    fn each_worker_gets_at_least_five_recoveries() {
        let flush = FlushLoop::new(mpsc::channel(1).1, depth(64), Duration::ZERO, depth(4));
        let workers = [1, 4, 9, 10, 14, 15, 64].map(|n| flush.workers(n).get());
        assert_eq!(workers, [1, 1, 1, 2, 2, 3, 4]);
    }

    // Regression test for the intake gauge: it read the channel after
    // the take, so it was always 0. The requests that queue while a
    // batch runs must show in the gauge when the flush task comes back
    // for the next batch.
    #[test]
    fn the_intake_gauge_reads_the_requests_that_queued_while_a_batch_ran() {
        let text = Rendered::of(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            runtime.block_on(async {
                let (tx, rx) = mpsc::channel(16);
                let mut flush = FlushLoop::new(rx, depth(4), Duration::ZERO, NonZeroUsize::MIN);
                tx.send(request()).await.unwrap();
                // `join!` polls the batch first. It takes the one request
                // and waits for the blocking pool. Then the second arm
                // queues 3 more requests while that batch runs.
                tokio::join!(
                    async { assert!(flush.run_one_batch().await.is_continue()) },
                    async {
                        for _ in 0..3 {
                            tx.send(request()).await.unwrap();
                        }
                    },
                );
                assert!(flush.run_one_batch().await.is_continue());
            });
        });
        assert_eq!(
            text.value(r#"kardamom_queue_depth{queue="ingress.recover_intake"}"#),
            3.0
        );
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
        let v = BatchVerifier::new(depth(8), Duration::from_mins(1));
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
        let v = BatchVerifier::new(depth(8), Duration::from_mins(1));
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
