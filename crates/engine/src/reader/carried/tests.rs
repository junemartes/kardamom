//! The frame rules of the carried reader on one source: the request ids,
//! the cursor, the slots, the gap, the resend, and the replay end.

use std::time::{Duration, Instant};

use alloy_primitives::Address;
use alloy_signer_local::PrivateKeySigner;
use kardamom_cluster_adapter::wire::{
    CanonicalTx, EGRESS_KIND_REPLAY_AHEAD, EGRESS_KIND_REPLAY_DONE, EGRESS_KIND_REPLAY_UNAVAILABLE,
    RT_EPOCH, encode_egress_boundary, encode_egress_record, encode_feed_answer, encode_feed_gap,
};
use kardamom_types::{Deposit, EpochRecord, TxEnvelope, TxOrderingMessage};

use super::*;

/// Short waits, so the timer tests run in well under a second each.
pub(super) const TIMING: Timing = Timing {
    resend: Duration::from_millis(100),
    silence: Duration::from_millis(300),
    ready: Duration::from_millis(150),
    poll: Duration::from_millis(5),
};

/// A reader of two in-memory sources, and the test's side of each.
pub(super) struct Rig {
    pub(super) sub: CarriedSubscription<MemoryTransport, MemoryTransport>,
    pub(super) feed: MemoryRemote,
    pub(super) service: MemoryRemote,
}

impl Rig {
    pub(super) fn new(next_index: u64, next_block: u64) -> Self {
        let (feed, feed_remote) = MemoryTransport::pair();
        let (service, service_remote) = MemoryTransport::pair();
        Self {
            sub: CarriedSubscription::with_timing(
                feed,
                service,
                ReplayCursor::new(next_index, next_block),
                TIMING,
            ),
            feed: feed_remote,
            service: service_remote,
        }
    }

    pub(super) fn remote(&self, source: Source) -> &MemoryRemote {
        match source {
            Source::Feed => &self.feed,
            Source::Service => &self.service,
        }
    }

    /// Run the reader until `probe` yields a value, and return it.
    fn poll_until<T>(&mut self, what: &str, mut probe: impl FnMut(&mut Self) -> Option<T>) -> T {
        let deadline = Instant::now() + Duration::from_secs(5);
        std::iter::repeat_with(|| {
            let got = probe(self);
            if got.is_none() {
                assert!(Instant::now() < deadline, "{what}");
                self.sub.step().expect("a step of the reader");
            }
            got
        })
        .find_map(|got| got)
        .expect("a value")
    }

    /// Run the reader until `source` gets a request, and return it.
    pub(super) fn request(&mut self, source: Source) -> Request {
        self.poll_until("no request", |rig| {
            rig.remote(source).request(Duration::ZERO)
        })
    }

    /// Run the reader for `steps` polls, and label each item that it
    /// delivers.
    pub(super) fn deliver(&mut self, steps: usize) -> Vec<String> {
        let mut got = Vec::new();
        (0..steps).for_each(|_| {
            got.extend(std::iter::from_fn(|| self.sub.reorder.try_deliver().unwrap()).map(label));
            self.sub.step().expect("a step of the reader");
        });
        got.extend(std::iter::from_fn(|| self.sub.reorder.try_deliver().unwrap()).map(label));
        got
    }

    /// Run the reader for `wait`, and drop what it delivers.
    pub(super) fn run_for(&mut self, wait: Duration) {
        let end = Instant::now() + wait;
        std::iter::repeat_with(|| self.deliver(1))
            .take_while(|_| Instant::now() < end)
            .for_each(drop);
    }

    /// Run the reader until `done` holds.
    pub(super) fn run_until(&mut self, done: impl Fn(&Self) -> bool) {
        self.poll_until("the condition did not hold", |rig| done(rig).then_some(()));
    }

    /// Run the reader until it delivers one item.
    pub(super) fn next_item(&mut self) -> (kardamom_types::BPosition, Ordered) {
        self.poll_until("the reader delivered nothing", |rig| {
            rig.sub.reorder.try_deliver().unwrap()
        })
    }

    /// Run the reader until it fails, and return the error.
    pub(super) fn error(&mut self) -> ExecutorError {
        let deadline = Instant::now() + Duration::from_secs(5);
        std::iter::repeat_with(|| {
            assert!(Instant::now() < deadline, "the reader did not fail");
            self.sub.step()
        })
        .find_map(Result::err)
        .expect("an error")
    }
}

/// A short tag of one delivered item: `t<index>` for an entry, `b<block>`
/// for a boundary, `e<index>` for an epoch.
pub(super) fn label((pos, item): (kardamom_types::BPosition, Ordered)) -> String {
    match item {
        Ordered::Tx(_) => format!("t{}", pos.as_index()),
        Ordered::Message(TxOrderingMessage::BoundaryStart(b)) => format!("b{}", b.block_number),
        Ordered::Message(TxOrderingMessage::Epoch(_)) => format!("e{}", pos.as_index()),
        Ordered::Message(other) => format!("?{other:?}"),
    }
}

/// A signed transfer with nonce `nonce`, as a canonical entry.
pub(in crate::reader) fn entry(nonce: u64) -> CanonicalTx {
    let signer = PrivateKeySigner::from_bytes(&[0x11; 32].into()).expect("a test key");
    let envelope = crate::actor::test_support::legacy(&signer, Address::from([0x22; 20]), nonce, 1);
    CanonicalTx {
        canonical_id: envelope.tx_hash,
        lane: 0,
        envelope,
    }
}

/// The relayed frame of `tx` at `index`.
pub(super) fn tx_frame(index: u64, tx: &CanonicalTx) -> Vec<u8> {
    encode_egress_record(index, &tx.payload().unwrap()).unwrap()
}

/// The relayed frame of a valid entry at `index`.
pub(in crate::reader) fn tx_at(index: u64) -> Vec<u8> {
    tx_frame(index, &entry(index))
}

/// The relayed frame of a valid entry at `index` as a full reference, as a
/// sealer below decision version 5 relays it.
pub(in crate::reader) fn full_ref_at(index: u64) -> Vec<u8> {
    encode_egress_record(index, &entry(index).full_ref_payload().unwrap()).unwrap()
}

/// An entry whose id is the hash in its envelope, but whose raw
/// transaction hashes to another id: bytes changed on the way.
pub(super) fn forged_at(index: u64) -> Vec<u8> {
    let good = entry(index);
    let envelope = TxEnvelope {
        raw_tx: bytes::Bytes::from_static(b"not the signed transaction"),
        ..good.envelope.clone()
    };
    tx_frame(index, &CanonicalTx { envelope, ..good })
}

/// The relayed frame of a transaction reference at `index`: an entry with
/// no bytes.
pub(super) fn ref_at(index: u64) -> Vec<u8> {
    let tx_ref = kardamom_types::TxRef::new(
        alloy_primitives::B256::repeat_byte(0x33),
        0,
        kardamom_types::BPosition::from_index(index),
        0,
    );
    let ingress = kardamom_cluster_adapter::wire::encode_ingress_txref(
        &tx_ref,
        kardamom_cluster_adapter::wire::GuardHeader::EXEMPT,
    );
    let (_, relayed) = kardamom_cluster_adapter::wire::split_ingress(&ingress).unwrap();
    encode_egress_record(index, relayed).unwrap()
}

pub(super) fn boundary(block: u64, end: u64) -> Vec<u8> {
    encode_egress_boundary(block, end, 0, 0)
}

pub(super) fn done(id: u64, index: u64, block: u64) -> Vec<u8> {
    encode_feed_answer(EGRESS_KIND_REPLAY_DONE, index, block, id)
}

pub(super) fn unavailable(id: u64, index: u64, block: u64) -> Vec<u8> {
    encode_feed_answer(EGRESS_KIND_REPLAY_UNAVAILABLE, index, block, id)
}

/// The relayed frame of an epoch with `deposits` deposits at `index`. The
/// epoch takes one slot for its marker and one for each deposit.
pub(super) fn epoch_at(index: u64, deposits: usize) -> Vec<u8> {
    let epoch = EpochRecord {
        l1_number: 7,
        l1_hash: alloy_primitives::B256::repeat_byte(7),
        deposits: vec![Deposit::default(); deposits],
    };
    let body = rkyv::to_bytes::<rkyv::rancor::Error>(&epoch).unwrap();
    let payload = [
        epoch.canonical_id().as_slice(),
        &[RT_EPOCH],
        body.as_slice(),
    ]
    .concat();
    encode_egress_record(index, &payload).unwrap()
}

#[test]
fn the_first_request_names_the_cursor() {
    let mut rig = Rig::new(5, 3);
    let request = rig.request(Source::Feed);
    assert_eq!((request.from_index, request.from_block), (5, 3));
    assert_eq!(rig.sub.source(), Source::Feed);
}

#[test]
fn no_frame_is_delivered_before_the_replay_done_of_the_newest_request() {
    let mut rig = Rig::new(0, 1);
    let id = rig.request(Source::Feed).request_id;
    rig.feed.send(tx_at(0));
    rig.feed.send(tx_at(1));
    assert_eq!(rig.deliver(5), [] as [String; 0], "the open block waits");
    rig.feed.send(done(id, 2, 1));
    assert_eq!(rig.deliver(5), ["t0", "t1"]);
    rig.feed.send(tx_at(2));
    assert_eq!(rig.deliver(5), ["t2"], "live after the replay done");
}

#[test]
fn a_relayed_full_reference_is_delivered_as_its_entry() {
    let mut rig = Rig::new(0, 1);
    let id = rig.request(Source::Feed).request_id;
    rig.feed.send(tx_at(0));
    rig.feed.send(full_ref_at(1));
    rig.feed.send(done(id, 2, 1));
    assert_eq!(rig.deliver(5), ["t0", "t1"]);
}

#[test]
fn an_answer_with_an_old_id_is_dropped() {
    let mut rig = Rig::new(0, 1);
    let id = rig.request(Source::Feed).request_id;
    let old = id.wrapping_sub(1);
    rig.feed.send(tx_at(0));
    rig.feed.send(done(old, 1, 1));
    rig.feed.send(encode_feed_gap(0, 1, old));
    rig.feed.send(unavailable(old, 9, 9));
    rig.feed
        .send(encode_feed_answer(EGRESS_KIND_REPLAY_AHEAD, 0, 0, old));
    assert_eq!(rig.deliver(5), [] as [String; 0]);
    assert_eq!(rig.feed.requests(), [], "an old gap asks for nothing");
    assert_eq!(
        rig.sub.source(),
        Source::Feed,
        "an old refusal changes nothing"
    );
    rig.feed.send(done(id, 1, 1));
    assert_eq!(rig.deliver(5), ["t0"]);
}

#[test]
fn frames_below_the_cursor_are_dropped() {
    let mut rig = Rig::new(5, 3);
    let id = rig.request(Source::Feed).request_id;
    rig.feed.send(tx_at(3));
    rig.feed.send(boundary(2, 4));
    rig.feed.send(tx_at(4));
    rig.feed.send(tx_at(5));
    rig.feed.send(boundary(3, 6));
    rig.feed.send(done(id, 6, 4));
    assert_eq!(rig.deliver(5), ["t5", "b3"]);
    assert_eq!(
        rig.feed.requests(),
        [],
        "a frame below the cursor is no hole"
    );
}

#[test]
fn a_hole_asks_again_from_the_cursor_and_skips_the_frame() {
    let mut rig = Rig::new(0, 1);
    let first = rig.request(Source::Feed);
    rig.feed.send(tx_at(0));
    rig.feed.send(tx_at(2));
    let again = rig.request(Source::Feed);
    assert_ne!(again.request_id, first.request_id);
    assert_eq!((again.from_index, again.from_block), (0, 1));
    rig.feed.send(tx_at(0));
    rig.feed.send(tx_at(1));
    rig.feed.send(boundary(1, 2));
    rig.feed.send(done(again.request_id, 2, 2));
    assert_eq!(rig.deliver(5), ["t0", "t1", "b1"]);
}

#[test]
fn a_boundary_that_does_not_close_the_cursor_is_a_hole() {
    let mut rig = Rig::new(0, 1);
    let first = rig.request(Source::Feed);
    rig.feed.send(tx_at(0));
    rig.feed.send(boundary(1, 2));
    let again = rig.request(Source::Feed);
    assert_ne!(again.request_id, first.request_id);
    rig.feed.send(tx_at(0));
    rig.feed.send(boundary(2, 1));
    let third = rig.request(Source::Feed);
    assert_ne!(
        third.request_id, again.request_id,
        "a skipped block is a hole"
    );
}

#[test]
fn an_origin_record_moves_the_next_index_by_its_slots() {
    let mut rig = Rig::new(0, 1);
    let id = rig.request(Source::Feed).request_id;
    rig.feed.send(epoch_at(0, 2));
    rig.feed.send(tx_at(3));
    rig.feed.send(boundary(1, 4));
    rig.feed.send(done(id, 4, 2));
    assert_eq!(rig.deliver(5), ["e0", "t3", "b1"]);
    assert_eq!(rig.feed.requests(), [], "a jump after an epoch is no hole");
}

#[test]
fn a_feed_gap_asks_again_and_stays_on_the_feed() {
    let mut rig = Rig::new(0, 1);
    let first = rig.request(Source::Feed);
    rig.feed.send(tx_at(0));
    rig.feed.send(boundary(1, 1));
    rig.feed.send(done(first.request_id, 1, 2));
    assert_eq!(rig.deliver(5), ["t0", "b1"]);
    rig.feed.send(tx_at(1));
    rig.feed.send(encode_feed_gap(2, 2, first.request_id));
    let again = rig.request(Source::Feed);
    assert_ne!(again.request_id, first.request_id);
    assert_eq!(
        (again.from_index, again.from_block),
        (1, 2),
        "the request starts at the reader's own cursor"
    );
    assert_eq!(rig.sub.source(), Source::Feed);
    assert_eq!(rig.service.requests(), []);
    rig.feed.send(tx_at(1));
    rig.feed.send(done(again.request_id, 2, 2));
    assert_eq!(rig.deliver(5), ["t1"]);
}

#[test]
fn a_request_with_no_answer_is_sent_again_with_a_new_id() {
    let mut rig = Rig::new(4, 2);
    let first = rig.request(Source::Feed);
    let started = Instant::now();
    let again = rig.request(Source::Feed);
    assert!(started.elapsed() >= TIMING.resend.saturating_sub(TIMING.poll));
    assert_ne!(again.request_id, first.request_id);
    assert_eq!((again.from_index, again.from_block), (4, 2));
}

#[test]
fn a_live_source_sends_no_resend() {
    let mut rig = Rig::new(0, 1);
    let id = rig.request(Source::Feed).request_id;
    rig.feed.send(done(id, 0, 1));
    rig.deliver(1);
    std::thread::sleep(TIMING.resend);
    rig.feed.send(boundary(1, 0));
    assert_eq!(rig.deliver(3), ["b1"]);
    assert_eq!(rig.feed.requests(), []);
}

#[test]
fn a_new_request_holds_records_until_their_boundary() {
    let mut rig = Rig::new(0, 1);
    let first = rig.request(Source::Feed);
    rig.feed.send(done(first.request_id, 0, 1));
    rig.feed.send(tx_at(0));
    assert_eq!(rig.deliver(3), ["t0"]);
    rig.feed.send(tx_at(2));
    let again = rig.request(Source::Feed);
    rig.feed.send(tx_at(1));
    assert_eq!(
        rig.deliver(3),
        [] as [String; 0],
        "catch-up again after the hole"
    );
    rig.feed.send(boundary(1, 2));
    assert_eq!(rig.deliver(3), ["t1", "b1"]);
    rig.feed.send(tx_at(2));
    rig.feed.send(done(again.request_id, 3, 2));
    assert_eq!(rig.deliver(3), ["t2"]);
}

#[test]
fn an_unknown_kind_is_dropped() {
    let mut rig = Rig::new(0, 1);
    let id = rig.request(Source::Feed).request_id;
    rig.feed.send(vec![200, 1, 2, 3]);
    rig.feed.send(tx_at(0));
    rig.feed.send(done(id, 1, 1));
    assert_eq!(rig.deliver(5), ["t0"]);
    assert_eq!(rig.sub.source(), Source::Feed);
}

#[test]
fn a_new_image_is_a_new_connection_with_a_new_request() {
    let mut rig = Rig::new(0, 1);
    let first = rig.request(Source::Feed);
    rig.feed.send(done(first.request_id, 0, 1));
    rig.deliver(1);
    rig.feed.reconnect();
    let again = rig.request(Source::Feed);
    assert_ne!(again.request_id, first.request_id);
    assert_eq!((again.from_index, again.from_block), (0, 1));
    assert_eq!(rig.sub.source(), Source::Feed, "a new image is no fault");
    rig.feed.send(done(first.request_id, 0, 1));
    rig.feed.send(tx_at(0));
    assert_eq!(rig.deliver(3), [] as [String; 0], "the old answer is old");
    rig.feed.send(done(again.request_id, 1, 1));
    assert_eq!(rig.deliver(3), ["t0"]);
}

#[test]
fn a_hole_and_the_frames_after_it_send_one_request() {
    let mut rig = Rig::new(0, 1);
    rig.request(Source::Feed);
    rig.feed.send(tx_at(2));
    (3..43).for_each(|index| rig.feed.send(tx_at(index)));
    let started = Instant::now();
    rig.deliver(45);
    assert!(
        started.elapsed() < TIMING.resend,
        "the test ran past a resend"
    );
    assert_eq!(rig.feed.requests().len(), 1, "one request for the hole");
}

#[test]
fn the_cursor_gauge_names_the_newest_delivered_record() {
    let text = crate::meter::tests::render(|| {
        let Rig { sub, feed, service } = Rig::new(0, 1);
        let mut rig = Rig {
            sub: sub.with_cursor_gauge("test_egress_cursor_index"),
            feed,
            service,
        };
        let request = rig.request(Source::Feed);
        rig.feed.send(tx_at(0));
        rig.feed.send(tx_at(1));
        rig.feed.send(boundary(1, 2));
        rig.feed.send(done(request.request_id, 2, 2));
        let labels: Vec<String> = (0..3)
            .map(|_| label(TxOrderingSubscription::next(&mut rig.sub).unwrap()))
            .collect();
        assert_eq!(labels, ["t0", "t1", "b1"]);
    });
    assert_eq!(
        crate::meter::tests::value(&text, "test_egress_cursor_index"),
        1.0,
        "a boundary does not move the gauge"
    );
}

/// The text of the gauges after a reader at the cursor `(7, 4)` gets the
/// `REPLAY_DONE` of its first request at that cursor, with no frame before
/// it. `suppress` makes it the reader of a validator or a batcher.
fn head_at_the_cursor(suppress: bool) -> String {
    crate::meter::tests::render(|| {
        let Rig { sub, feed, service } = Rig::new(7, 4);
        let mut rig = Rig {
            sub: if suppress {
                sub.suppress_sealer_metrics()
            } else {
                sub
            },
            feed,
            service,
        };
        let request = rig.request(Source::Feed);
        rig.feed.send(done(request.request_id, 7, 4));
        assert_eq!(rig.deliver(3), [] as [String; 0]);
    })
}

#[test]
fn a_suppressed_reader_at_the_head_exports_the_validator_head() {
    let text = head_at_the_cursor(true);
    assert_eq!(
        crate::meter::tests::value(&text, crate::metrics::VALIDATOR_SEALER_BLOCK_NUMBER),
        3.0,
        "the head is the block before the next block of the answer"
    );
    assert!(
        !text.contains(crate::metrics::SEALER_BLOCK_NUMBER),
        "only the executor exports the sealer series"
    );
}

#[test]
fn the_executor_reader_at_the_head_exports_the_sealer_head() {
    let text = head_at_the_cursor(false);
    assert_eq!(
        crate::meter::tests::value(&text, crate::metrics::SEALER_BLOCK_NUMBER),
        3.0
    );
    assert!(!text.contains(crate::metrics::VALIDATOR_SEALER_BLOCK_NUMBER));
}
