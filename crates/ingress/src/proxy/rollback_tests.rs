//! The proxy's reaction to a rollback on the receipt bus: the cache, the
//! first-wins dedup, the fee history, the head, and the deduped feed. And
//! the count of the receipts that a gap of the bus drops.

use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use alloy_primitives::{Address, B256};
use kardamom_types::{BlockBoundary, Receipt, ReceiptRollback};

use crate::channels::{MockChannels, ReceiptItem};
use crate::config::IngressConfig;
use crate::json_rpc::{IngressHandlers, IngressKardamomApiServer};
use crate::proxy::IngressProxy;
use crate::test_support::receipt;

type MockProxy = IngressProxy<MockChannels, MockChannels>;

/// The watchers run on their own tasks: give them time to drain.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(50)).await;
}

fn in_block(nonce: u64, hash: u8, block_number: u64) -> Receipt {
    Receipt {
        block_number,
        ..receipt(
            Address::repeat_byte(0x33),
            nonce,
            B256::repeat_byte(hash),
            0,
        )
    }
}

fn boundary(block_number: u64) -> BlockBoundary {
    BlockBoundary {
        block_number,
        ..BlockBoundary::default()
    }
}

/// A proxy over fresh mock channels. The channels keep their buses.
fn proxy() -> (MockChannels, MockProxy) {
    let (mock, _shard_rx) = MockChannels::new(std::num::NonZeroUsize::new(1).unwrap());
    let cfg = IngressConfig {
        chain_id: NonZeroU64::new(1).unwrap(),
        ..IngressConfig::default()
    };
    let proxy = IngressProxy::new(cfg, mock.clone(), mock.clone());
    (mock, proxy)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rollback_drops_the_receipts_and_the_head_above_its_block() {
    let (mock, proxy) = proxy();
    let mut feed = proxy.subscribe_receipt_feed();
    let (kept, lost) = (B256::repeat_byte(0x11), B256::repeat_byte(0x22));
    mock.receipt_bus.send(in_block(1, 0x11, 4).into()).unwrap();
    mock.receipt_bus.send(in_block(2, 0x22, 5).into()).unwrap();
    mock.block_boundary_bus.send(boundary(4)).unwrap();
    mock.block_boundary_bus.send(boundary(5)).unwrap();
    settle().await;
    assert_eq!(proxy.latest_block_number(), 5);
    assert_eq!(proxy.fee_history().latest(), 5);

    let rollback = ReceiptRollback {
        to_block: 4,
        ..Default::default()
    };
    mock.receipt_bus
        .send(ReceiptItem::Rollback(rollback))
        .unwrap();
    settle().await;
    assert!(proxy.lookup_receipt_by_hash(kept).is_some());
    assert!(proxy.lookup_receipt_by_hash(lost).is_none());
    assert_eq!(proxy.latest_block_number(), 4);
    assert_eq!(proxy.fee_history().latest(), 4);
    assert_eq!(feed.recv().await.unwrap(), in_block(1, 0x11, 4).into());
    assert_eq!(feed.recv().await.unwrap(), in_block(2, 0x22, 5).into());
    assert_eq!(feed.recv().await.unwrap(), ReceiptItem::Rollback(rollback));

    // The transaction comes again with its old hash in a new block. The
    // first-wins dedup forgot the hash, so the new receipt passes.
    mock.receipt_bus.send(in_block(2, 0x22, 6).into()).unwrap();
    settle().await;
    assert_eq!(
        proxy.lookup_receipt_by_hash(lost).map(|r| r.block_number),
        Some(6)
    );
    assert_eq!(feed.recv().await.unwrap(), in_block(2, 0x22, 6).into());
}

/// The next raw subscription frame, within 5 s.
async fn next_frame(
    frames: &mut tokio::sync::mpsc::Receiver<Box<serde_json::value::RawValue>>,
) -> String {
    tokio::time::timeout(Duration::from_secs(5), frames.recv())
        .await
        .expect("a frame within 5 s")
        .expect("the subscription sends a frame")
        .get()
        .to_string()
}

/// A rollback ends each receipt subscription with an error frame that
/// names the rollback block. The receipt before it still arrives.
#[tokio::test(flavor = "multi_thread")]
async fn a_rollback_ends_each_receipt_subscription_with_an_error() {
    let (mock, proxy) = proxy();
    let module = IngressKardamomApiServer::into_rpc(
        IngressHandlers::<(MockChannels, MockChannels)>::new(proxy),
    );
    let request =
        r#"{"jsonrpc":"2.0","id":1,"method":"kardamom_subscribeReceipts","params":[null]}"#;
    let (_answer, mut frames) = module.raw_json_request(request, 16).await.unwrap();
    mock.receipt_bus.send(in_block(1, 0x11, 4).into()).unwrap();
    mock.receipt_bus
        .send(ReceiptItem::Rollback(ReceiptRollback {
            to_block: 4,
            ..Default::default()
        }))
        .unwrap();
    let first = next_frame(&mut frames).await;
    assert!(first.contains(r#""type":"receipt""#), "{first}");
    let close = next_frame(&mut frames).await;
    assert!(
        close.contains(r#""error""#)
            && close.contains("rolled back")
            && close.contains("above block 4"),
        "the close names the rollback: {close}"
    );
}

fn rollback(to_block: u64, id: u8) -> ReceiptRollback {
    ReceiptRollback {
        to_block,
        id: B256::repeat_byte(id),
    }
}

/// The receipt watcher falls behind the bus, and the bus drops the
/// rollback. The watcher reads it from the rollback log of the
/// subscription and applies it once.
#[tokio::test]
async fn a_rollback_lost_on_bus_lag_is_still_applied() {
    let (mock, proxy) = proxy();
    mock.receipt_bus.send(in_block(1, 0x22, 5).into()).unwrap();
    settle().await;
    assert!(
        proxy
            .lookup_receipt_by_hash(B256::repeat_byte(0x22))
            .is_some()
    );

    // The test task does not yield, so the watcher reads none of these
    // until the bus has dropped the oldest. The rollback goes only into
    // the log, as if the bus dropped it: only the gap applies it.
    let lost = rollback(4, 7);
    mock.rollback_log.send_modify(|log| {
        log.push(lost);
    });
    send_fillers(&mock, crate::channels::BUS_CAPACITY + 8);
    settle().await;
    assert!(
        proxy
            .lookup_receipt_by_hash(B256::repeat_byte(0x22))
            .is_none(),
        "the receipt above the block is gone"
    );
    let applied = proxy.subscribe_rollbacks();
    assert_eq!(applied.borrow().generation(), 1, "applied once");
}

/// A watcher that falls behind the bus counts each receipt that the bus
/// dropped. The waiter of a dropped receipt waits until its timeout, so
/// the count names the cause of that wait.
#[tokio::test]
async fn a_gap_counts_the_receipts_that_the_bus_dropped() {
    let lagged = LaggedReceipts::default();
    // The test runtime has one thread, so the watcher task records here.
    let _recorder = metrics::set_default_local_recorder(&lagged);
    let (mock, _proxy) = proxy();

    // The test task does not yield, so the watcher reads none of these
    // until the bus has dropped the oldest eight.
    send_fillers(&mock, crate::channels::BUS_CAPACITY + 8);
    settle().await;
    assert_eq!(lagged.0.load(Ordering::Relaxed), 8);
}

/// Records the count of [`crate::metrics::BUS_LAGGED_TOTAL`] for the
/// `tx_receipts` bus, and nothing else.
#[derive(Clone, Default)]
struct LaggedReceipts(Arc<AtomicU64>);

impl metrics::CounterFn for LaggedReceipts {
    fn increment(&self, value: u64) {
        self.0.fetch_add(value, Ordering::Relaxed);
    }

    fn absolute(&self, value: u64) {
        self.0.store(value, Ordering::Relaxed);
    }
}

impl metrics::Recorder for LaggedReceipts {
    fn describe_counter(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn describe_gauge(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn describe_histogram(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        let receipts = key.name() == crate::metrics::BUS_LAGGED_TOTAL
            && key
                .labels()
                .any(|l| l.key() == "bus" && l.value() == "tx_receipts");
        if receipts {
            metrics::Counter::from_arc(Arc::new(self.clone()))
        } else {
            metrics::Counter::noop()
        }
    }

    fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::noop()
    }

    fn register_histogram(
        &self,
        _: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

/// Send `count` receipts of block 3, each with a hash of its own.
fn send_fillers(mock: &MockChannels, count: usize) {
    (0..count).for_each(|i| {
        let filler = Receipt {
            tx_hash: B256::from(alloy_primitives::U256::from(i + 1_000)),
            ..in_block(u64::try_from(i).unwrap() + 10, 0x00, 3)
        };
        mock.receipt_bus.send(filler.into()).unwrap();
    });
}

/// The receipt watcher falls behind the bus, but the bus still holds the
/// rollback, and the log holds it too. The rollback applies in bus order:
/// it drops the receipt above its block that comes before it on the bus,
/// and keeps the receipt of the new stream after it.
#[tokio::test]
async fn a_rollback_still_on_the_bus_after_a_gap_applies_in_bus_order() {
    let (mock, proxy) = proxy();
    let (void, new) = (B256::repeat_byte(0x44), B256::repeat_byte(0x55));
    let held = rollback(5, 8);

    // The test task does not yield, so the watcher reads none of these
    // until the bus has dropped the oldest fillers. The pump puts the
    // rollback on the bus, then in the log.
    send_fillers(&mock, crate::channels::BUS_CAPACITY);
    mock.receipt_bus.send(in_block(1, 0x44, 10).into()).unwrap();
    mock.receipt_bus.send(ReceiptItem::Rollback(held)).unwrap();
    mock.rollback_log.send_modify(|log| {
        log.push(held);
    });
    mock.receipt_bus.send(in_block(2, 0x55, 6).into()).unwrap();
    settle().await;
    assert!(
        proxy.lookup_receipt_by_hash(void).is_none(),
        "the receipt before the rollback is void"
    );
    assert!(
        proxy.lookup_receipt_by_hash(new).is_some(),
        "the receipt of the new stream stays"
    );
    assert_eq!(proxy.subscribe_rollbacks().borrow().generation(), 1);
}

/// A late copy of one rollback event from another executor changes
/// nothing: the receipts of the new stream stay, and the feed carries the
/// rollback once.
#[tokio::test(flavor = "multi_thread")]
async fn a_repeated_rollback_id_is_a_no_op() {
    let (mock, proxy) = proxy();
    let mut feed = proxy.subscribe_receipt_feed();
    mock.receipt_bus.send(in_block(1, 0x22, 5).into()).unwrap();
    mock.receipt_bus
        .send(ReceiptItem::Rollback(rollback(4, 1)))
        .unwrap();
    mock.receipt_bus.send(in_block(1, 0x33, 5).into()).unwrap();
    mock.receipt_bus
        .send(ReceiptItem::Rollback(rollback(4, 1)))
        .unwrap();
    settle().await;
    assert!(
        proxy
            .lookup_receipt_by_hash(B256::repeat_byte(0x22))
            .is_none()
    );
    assert!(
        proxy
            .lookup_receipt_by_hash(B256::repeat_byte(0x33))
            .is_some(),
        "the receipt of the new stream stays"
    );
    assert_eq!(feed.recv().await.unwrap(), in_block(1, 0x22, 5).into());
    assert_eq!(
        feed.recv().await.unwrap(),
        ReceiptItem::Rollback(rollback(4, 1))
    );
    assert_eq!(feed.recv().await.unwrap(), in_block(1, 0x33, 5).into());
    assert!(feed.try_recv().is_err(), "no second rollback on the feed");
}
