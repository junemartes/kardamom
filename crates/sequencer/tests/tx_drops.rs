//! Each path that drops a transaction publishes one `TxDrop` on
//! `tx_drops`, with the hash, the sender, the nonce and the drop reason.
//! A drop that a receipt proves, which gives the client no error, still
//! publishes `nonce too low` for the egress.

use std::num::NonZeroU64;
use std::time::Duration;

use alloy_primitives::B256;
use alloy_signer_local::PrivateKeySigner;
use kardamom_types::{BlockBoundary, DropReason, TxDataLoc, TxDrop, TxEnvelope, TxErrorReason};

use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::fees::{FeeGate, LatestBaseFee};
use kardamom_sequencer::resync::{
    FloorEvent, FloorUpdate, ResyncChannel, ResyncConfig, SealerRefusal,
};
use kardamom_sequencer::sequencer::Sequencer;
use kardamom_sequencer::testkit::{EnvelopeSpec, Rig, envelope_with, one_partition_cfg, pos};

/// One sequencer, its fakes, and the resync channels that a test feeds.
struct Lane {
    seq: Sequencer,
    rig: Rig,
    signer: PrivateKeySigner,
    floor_tx: crossbeam_channel::Sender<FloorEvent>,
    deadline_tx: crossbeam_channel::Sender<SealerRefusal>,
    /// The next free `tx_data` offset.
    offset: i32,
}

impl Lane {
    fn new(cfg: SequencerConfig, seed: u64) -> Self {
        let mut seq = Sequencer::new(cfg).unwrap();
        let ResyncChannel {
            controller,
            floor_tx,
            deadline_tx,
            ..
        } = ResyncChannel::open(ResyncConfig::default(), 0).unwrap();
        seq.enable_resync(controller);
        Self {
            seq,
            rig: Rig::default(),
            signer: kardamom_sequencer::testkit::signer(seed),
            floor_tx,
            deadline_tx,
            offset: 0,
        }
    }

    /// The client submits `nonce`, and the sequencer takes one step.
    /// Returns the hash of the transaction.
    fn submit(&mut self, nonce: u64) -> B256 {
        let envelope = self.envelope(nonce);
        let tx_hash = envelope.tx_hash;
        self.rig.push(TxDataLoc::new(0, pos(self.offset)), envelope);
        self.offset += 64;
        self.step();
        tx_hash
    }

    /// An envelope of `nonce` with its real hash. The offset makes each
    /// envelope, and so each hash, distinct.
    fn envelope(&self, nonce: u64) -> TxEnvelope {
        let spec = EnvelopeSpec {
            real_hash: true,
            ..EnvelopeSpec::default()
        };
        envelope_with(
            &self.signer,
            nonce,
            u64::try_from(self.offset).unwrap(),
            spec,
        )
    }

    fn step(&mut self) {
        self.rig.step(&mut self.seq).unwrap();
    }

    /// The receipt of the executed transaction at `nonce` arrives.
    fn execute(&self, nonce: u64) {
        self.floor_tx
            .send(FloorUpdate::executed(self.signer.address(), nonce).into())
            .unwrap();
    }

    /// The drop of `tx_hash` at `nonce`, as this lane publishes it.
    fn drop_of(&self, tx_hash: B256, nonce: u64, reason: DropReason) -> TxDrop {
        TxDrop {
            tx_hash,
            sender: self.signer.address(),
            nonce,
            reason,
        }
    }
}

#[test]
fn a_duplicate_publishes_one_nonce_too_low_drop() {
    let mut lane = Lane::new(one_partition_cfg(), 1);
    lane.submit(0);
    let duplicate = lane.submit(0);
    assert_eq!(
        lane.rig.drops(),
        vec![lane.drop_of(duplicate, 0, DropReason::NonceTooLow)]
    );
    assert_eq!(lane.rig.errors().len(), 1, "the client still hears of it");
}

#[test]
fn an_expired_entry_publishes_one_expired_drop() {
    let cfg = SequencerConfig {
        tx_ttl_ms: NonZeroU64::new(20).unwrap(),
        ..one_partition_cfg()
    };
    let mut lane = Lane::new(cfg, 2);
    let parked = lane.submit(1);
    assert_eq!(lane.rig.drops(), vec![], "no drop before tx_ttl");
    std::thread::sleep(Duration::from_millis(40));
    lane.step();
    assert_eq!(
        lane.rig.drops(),
        vec![lane.drop_of(parked, 1, DropReason::Expired)]
    );
}

#[test]
fn an_evicted_entry_publishes_one_txpool_full_drop() {
    let cfg = SequencerConfig {
        max_pending_per_sender: 2,
        ..one_partition_cfg()
    };
    let mut lane = Lane::new(cfg, 3);
    let furthest = lane.submit(3);
    lane.submit(2);
    lane.submit(1);
    assert_eq!(
        lane.rig.drops(),
        vec![lane.drop_of(furthest, 3, DropReason::TxpoolFull)],
        "the lower nonce evicts the furthest one"
    );
}

#[test]
fn a_fee_rejection_publishes_one_base_fee_drop() {
    let mut lane = Lane::new(one_partition_cfg(), 4);
    let view = LatestBaseFee::new();
    view.on_boundary(&BlockBoundary {
        base_fee: 100_000_000_000,
        gas_used: 15_000_000,
        ..BlockBoundary::default()
    });
    lane.seq.enable_fees(FeeGate::on(view, None));
    let refused = lane.submit(0);
    assert_eq!(
        lane.rig.drops(),
        vec![lane.drop_of(refused, 0, DropReason::FeeCapBelowBaseFee)]
    );
}

#[test]
fn a_sealer_refusal_publishes_one_expired_drop() {
    let mut lane = Lane::new(one_partition_cfg(), 5);
    let offered = lane.submit(0);
    lane.deadline_tx
        .send(SealerRefusal {
            sender: lane.signer.address(),
            nonce: 0,
            reason: TxErrorReason::PastDeadline {
                max_inclusion_block: u64::MAX,
                at_block: 65,
            },
        })
        .unwrap();
    lane.step();
    assert_eq!(
        lane.rig.drops(),
        vec![lane.drop_of(offered, 0, DropReason::Expired)]
    );
}

#[test]
fn a_receipt_proven_duplicate_publishes_nonce_too_low_and_no_error() {
    let mut lane = Lane::new(one_partition_cfg(), 6);
    lane.execute(1);
    let stale = lane.submit(1);
    assert_eq!(
        lane.rig.drops(),
        vec![lane.drop_of(stale, 1, DropReason::NonceTooLow)]
    );
    assert_eq!(
        lane.rig.errors(),
        vec![],
        "a proven duplicate is no client error"
    );
}

#[test]
fn a_receipt_floor_publishes_a_drop_for_each_buffered_entry_below_it() {
    let mut lane = Lane::new(one_partition_cfg(), 7);
    let three = lane.submit(3);
    let four = lane.submit(4);
    let five = lane.submit(5);
    lane.execute(4);
    lane.step();
    assert_eq!(
        lane.rig.drops(),
        vec![
            lane.drop_of(three, 3, DropReason::NonceTooLow),
            lane.drop_of(four, 4, DropReason::NonceTooLow),
        ]
    );
    assert_eq!(lane.rig.errors(), vec![]);
    assert!(
        lane.rig.refs().iter().any(|r| r.tx_hash == five),
        "the entry at the floor publishes"
    );
}

/// With `tx_events` on, each offered reference adds one `sent` record, and
/// a duplicate adds none, after the frames go out.
#[test]
fn tx_events_carry_one_sent_record_for_each_offer() {
    let offers = kardamom_sequencer::outbound::fakes::CountedOffers::default();
    let mut lane = Lane::new(one_partition_cfg(), 7);
    lane.rig.errors = std::mem::take(&mut lane.rig.errors).with_tx_events(offers.clone());
    lane.submit(0);
    lane.submit(0);
    lane.submit(1);
    std::thread::sleep(kardamom_log::aeron_live::FRAME_MAX_AGE);
    lane.step();
    let count = |c: &std::sync::atomic::AtomicUsize| c.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(count(&offers.sent), 2);
}
