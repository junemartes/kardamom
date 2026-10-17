use super::*;
use crate::load::accounting::tests::base_input;
use crate::load::accounting::{EvalInput, evaluate};
use crate::load::plan::PlannedTx;
use crate::load::scrape::MetricsSnapshot;
use crate::load::tracker::PendingCounts;
use alloy_primitives::Bytes;
use jsonrpsee::RpcModule;
use jsonrpsee::http_client::HttpClientBuilder;
use jsonrpsee::server::{Server, ServerHandle};
use jsonrpsee::types::ErrorObjectOwned;
use std::sync::atomic::{AtomicU64, Ordering};

fn tx(sender: usize, nonce: u64) -> PlannedTx {
    PlannedTx {
        raw: Bytes::new(),
        hash: B256::with_last_byte(u8::try_from(nonce).unwrap_or(0)),
        sender,
        nonce,
    }
}

#[test]
fn queues_round_robin_preserve_per_sender_order() {
    let mut q = Queues::new(vec![vec![tx(0, 0), tx(0, 1)], vec![tx(1, 0), tx(1, 1)]]);
    assert_eq!(q.remaining(), 4);
    // In rotation, the order is s0n0, s1n0, s0n1, s1n1.
    let order: Vec<(usize, u64)> = std::iter::from_fn(|| q.pop_next())
        .map(|t| (t.sender, t.nonce))
        .collect();
    assert_eq!(order, vec![(0, 0), (1, 0), (0, 1), (1, 1)]);
    assert_eq!(q.remaining(), 0);
}

#[test]
fn queues_skip_drained_sender() {
    let mut q = Queues::new(vec![vec![tx(0, 0)], vec![tx(1, 0), tx(1, 1)]]);
    let mut got = vec![];
    while let Some(t) = q.pop_next() {
        got.push((t.sender, t.nonce));
    }
    assert_eq!(got, vec![(0, 0), (1, 0), (1, 1)]);
}

#[test]
fn tracker_counts_start_zero() {
    let t = Tracker::new().unwrap();
    let c = t.counts();
    assert_eq!(
        (c.offered, c.accepted, c.receipted, c.bad_status),
        (0, 0, 0, 0)
    );
    assert_eq!(t.remaining_pending(), PendingCounts::default());
}

/// A running [`mock_ingress`] server: the client to submit through,
/// the count of `eth_sendRawTransaction` calls it has seen, and its
/// handle, which must stay alive for the server's lifetime.
struct MockIngress {
    client: Arc<HttpClient>,
    send_calls: Arc<AtomicU64>,
    _handle: ServerHandle,
}

/// How a mock ingress answers `eth_sendRawTransaction`.
#[derive(Clone, Copy)]
enum Submits {
    /// Accept every submit with a hash.
    Accept,
    /// Fail every submit with a generic server error.
    Fail,
    /// Refuse this many submits as a paused ingress, then accept.
    PausedFirst(u64),
}

impl Submits {
    /// The answer to the submit with the 0-based number `call`.
    fn answer(self, call: u64) -> Result<B256, ErrorObjectOwned> {
        match self {
            Self::Accept => Ok(B256::repeat_byte(0x11)),
            Self::PausedFirst(n) if call >= n => Ok(B256::repeat_byte(0x11)),
            Self::PausedFirst(_) => Err(ErrorObjectOwned::owned(
                kardamom_ingress::error::CHAIN_HALTED_CODE,
                "chain halted",
                None::<()>,
            )),
            Self::Fail => Err(ErrorObjectOwned::owned(-32000, "boom", None::<()>)),
        }
    }
}

/// A mock ingress. It counts `eth_sendRawTransaction` calls, and
/// answers each one as `submits` says. It serves a fixed value for `eth_getTransactionReceipt`;
/// `Null` means the receipt is never found.
async fn mock_ingress(submits: Submits, receipt: serde_json::Value) -> MockIngress {
    let send_calls = Arc::new(AtomicU64::new(0));
    let mut module = RpcModule::new(Arc::clone(&send_calls));
    module
        .register_method("eth_sendRawTransaction", move |_, calls, _| {
            submits.answer(calls.fetch_add(1, Ordering::Relaxed))
        })
        .unwrap();
    module
        .register_method("eth_getTransactionReceipt", move |_, _, _| receipt.clone())
        .unwrap();
    let server = Server::builder().build("127.0.0.1:0").await.unwrap();
    let addr = server.local_addr().unwrap();
    let handle = server.start(module);
    let client = Arc::new(
        HttpClientBuilder::default()
            .build(format!("http://{addr}"))
            .unwrap(),
    );
    MockIngress {
        client,
        send_calls,
        _handle: handle,
    }
}

async fn permit() -> OwnedSemaphorePermit {
    Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap()
}

/// Regression test: an ingress that acks a submit whose receipt never
/// materializes is the exact bug class the must-deliver gate exists to
/// catch. Such a transaction must flow into `Pending { accepted: true }`,
/// survive the drain, surface as `missing`, and fire the
/// `assert_all_delivered` gate.
#[tokio::test]
async fn accepted_but_unreceipted_tx_counts_missing_and_fires_must_deliver() {
    let ingress = mock_ingress(Submits::Accept, serde_json::Value::Null).await;
    let tracker = Arc::new(Tracker::new().unwrap());
    submit_task(
        Arc::clone(&ingress.client),
        Arc::clone(&tracker),
        tx(0, 0),
        permit().await,
        SubmitOpts {
            retry: 0,
            pause_wait: Duration::ZERO,
            mode: SubmitMode::Blocking,
            feed_confirm: false,
        },
    )
    .await;

    let c = tracker.counts();
    assert_eq!((c.offered, c.accepted, c.receipted), (1, 1, 0));

    // The drain re-polls it once, since the deadline is already due, and
    // still finds no receipt, so it stays pending as accepted.
    Drainer::new(vec![Arc::clone(&ingress.client)], Arc::clone(&tracker))
        .drain(Instant::now())
        .await;
    let pending = tracker.remaining_pending();
    assert_eq!(
        pending,
        PendingCounts {
            missing: 1,
            unlanded: 0
        },
        "accepted-but-unreceipted must surface as missing"
    );

    // Check that the verdict gate actually fires on it.
    let base = MetricsSnapshot::default();
    let fin = MetricsSnapshot::default();
    let v = evaluate(&EvalInput {
        counts: tracker.counts(),
        missing: pending.missing,
        unlanded: pending.unlanded,
        ..base_input(&base, &fin)
    });
    assert!(!v.pass);
    assert!(v.failures.iter().any(|f| f.contains("must-deliver")));
}

/// Regression test: a submit whose RPC errored, but whose transaction
/// actually landed, with a receipt that exists, must not be
/// resubmitted. A duplicate would be counted by the sequencer as a
/// past-nonce drop and fail the run.
#[tokio::test]
async fn landed_tx_is_not_resubmitted_after_submit_error() {
    let ingress = mock_ingress(Submits::Fail, serde_json::json!({"status": "0x1"})).await;
    let tracker = Arc::new(Tracker::new().unwrap());
    submit_task(
        ingress.client,
        Arc::clone(&tracker),
        tx(0, 0),
        permit().await,
        SubmitOpts {
            retry: 3,
            pause_wait: Duration::ZERO,
            mode: SubmitMode::Blocking,
            feed_confirm: false,
        },
    )
    .await;
    assert_eq!(
        ingress.send_calls.load(Ordering::Relaxed),
        1,
        "no duplicate resubmit once the receipt is found"
    );
    let c = tracker.counts();
    assert_eq!(
        (c.offered, c.accepted, c.receipted, c.bad_status),
        (1, 0, 1, 0)
    );
    assert_eq!(tracker.remaining_pending(), PendingCounts::default());
}

/// A paused ingress refuses a submit. The submit waits it out past its
/// retry count, with the same transaction, and lands when the pause
/// ends.
#[tokio::test]
async fn a_paused_refusal_waits_out_the_pause_with_the_same_nonce() {
    let ingress = mock_ingress(Submits::PausedFirst(3), serde_json::Value::Null).await;
    let tracker = Arc::new(Tracker::new().unwrap());
    submit_task(
        Arc::clone(&ingress.client),
        Arc::clone(&tracker),
        tx(0, 7),
        permit().await,
        SubmitOpts {
            retry: 0,
            pause_wait: Duration::from_secs(30),
            mode: SubmitMode::Blocking,
            feed_confirm: false,
        },
    )
    .await;
    assert_eq!(ingress.send_calls.load(Ordering::Relaxed), 4);
    let c = tracker.counts();
    assert_eq!((c.offered, c.accepted), (1, 1));
}

/// With no pause wait, a paused refusal counts as an attempt, and the
/// submit gives up after its retry count.
#[tokio::test]
async fn a_paused_refusal_with_no_wait_counts_as_an_attempt() {
    let ingress = mock_ingress(Submits::PausedFirst(3), serde_json::Value::Null).await;
    let tracker = Arc::new(Tracker::new().unwrap());
    submit_task(
        Arc::clone(&ingress.client),
        Arc::clone(&tracker),
        tx(0, 7),
        permit().await,
        SubmitOpts {
            retry: 0,
            pause_wait: Duration::ZERO,
            mode: SubmitMode::Blocking,
            feed_confirm: false,
        },
    )
    .await;
    assert_eq!(ingress.send_calls.load(Ordering::Relaxed), 1);
    let c = tracker.counts();
    assert_eq!((c.offered, c.accepted), (1, 0));
}

/// Regression test: the verdict must not be read while submit tasks
/// are still in flight. `join_submit_tasks` waits for them, up to a bound.
#[tokio::test]
async fn join_submit_tasks_waits_for_in_flight_tasks() {
    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(async { tokio::time::sleep(Duration::from_millis(50)).await });
    join_submit_tasks(&mut tasks, Instant::now() + Duration::from_secs(5)).await;
    assert!(tasks.is_empty());
}

#[tokio::test]
async fn join_submit_tasks_gives_up_at_deadline() {
    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(async { tokio::time::sleep(Duration::from_mins(1)).await });
    join_submit_tasks(&mut tasks, Instant::now() + Duration::from_millis(50)).await;
    assert_eq!(tasks.len(), 1, "wedged task left behind after the deadline");
}
