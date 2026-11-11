//! resize-scale-out-in on the real sequencer core and the real shard map.
//!
//! The lanes are production `Sequencer` replicas with their production
//! vslot sets, old-lane reads and shadow warm-up. The ingress routes each
//! sender by the production `ShardMap` of its map version. The sealer is
//! the first-seen dedup of the cluster over every replica's offers, and
//! each ordered transaction returns to every replica as a receipt floor.
//! The test scales 2 -> 3 lanes, kills one replica of the new lane during
//! the overlap, then scales 3 -> 2. Every sender, moved or not, must reach
//! the canonical log with every nonce exactly once, in order.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::num::NonZeroU64;
use std::time::{Duration, Instant};

use alloy_primitives::{Address, B256};
use alloy_signer_local::PrivateKeySigner;
use crossbeam_channel::Sender;
use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::inbound::{Inbound, TxDataSubscriber};
use kardamom_sequencer::outbound::fakes::{InMemorySidePublisher, InMemoryTxOrderingRefPublisher};
use kardamom_sequencer::resync::{
    Evidence, FloorEvent, FloorUpdate, ResyncChannel, ResyncConfig, SharedWatermark,
};
use kardamom_sequencer::sequencer::Ports;
use kardamom_sequencer::testkit::{EnvelopeSpec, envelope_with, signer};
use kardamom_sequencer::{Sequencer, SequencerError};
use kardamom_types::shard_map::{ShardMap, VslotSet};
use kardamom_types::{BPosition, TxDataLoc, TxRef};

/// The transaction lifetime of every replica, and the shadow warm-up of a
/// gaining lane: one lifetime and a margin.
const TX_TTL_MS: u64 = 300;
const WARM_MS: u64 = 500;
/// The senders of the load. Some move at each resize, some stay.
const SENDERS: u64 = 24;
/// The transactions each sender sends in one phase.
const PER_PHASE: u64 = 3;
/// The bound of one settle: every submitted transaction ordered.
const SETTLE: Duration = Duration::from_secs(20);

/// The `tx_data` lanes a replica reads, in arrival order.
#[derive(Default)]
struct LaneFeed(VecDeque<Inbound>);

impl TxDataSubscriber for LaneFeed {
    fn poll(&mut self) -> Result<Option<Inbound>, SequencerError> {
        Ok(self.0.pop_front())
    }

    fn queued(&self) -> usize {
        self.0.len()
    }
}

/// One sequencer replica and its ports.
struct Replica {
    name: String,
    seq: Sequencer,
    lanes: Vec<u8>,
    feed: LaneFeed,
    refs: InMemoryTxOrderingRefPublisher,
    side: InMemorySidePublisher,
    floors: Sender<FloorEvent>,
    watermark: SharedWatermark,
    /// The offers already taken to the sealer.
    taken: usize,
    /// The offers this replica made while its incoming slots were in
    /// shadow mode.
    shadow_offers: Vec<TxRef>,
}

/// The layout of one replica.
struct Layout {
    name: String,
    lane: u8,
    vslots: VslotSet,
    old_lanes: Vec<u8>,
    shadow: VslotSet,
}

impl Replica {
    fn start(layout: Layout) -> Self {
        let cfg = SequencerConfig {
            partition_index: u32::from(layout.lane),
            sequencer_id: layout.lane,
            lane: Some(layout.lane),
            vslots: Some(layout.vslots),
            extra_lanes: layout.old_lanes.clone(),
            shadow_vslots: layout.shadow,
            shadow_warm_ms: Some(WARM_MS),
            tx_ttl_ms: NonZeroU64::new(TX_TTL_MS).unwrap(),
            resync: ResyncConfig {
                dedup_capacity: NonZeroU64::new(4096).unwrap(),
                exit_hold_ms: 50,
                ..ResyncConfig::default()
            },
            ..SequencerConfig::default()
        };
        cfg.validate().unwrap();
        let channel = ResyncChannel::open(cfg.resync.clone(), cfg.partition_index).unwrap();
        let mut seq = Sequencer::new(cfg).unwrap();
        seq.enable_resync(channel.controller);
        Self {
            name: layout.name,
            seq,
            lanes: std::iter::once(layout.lane)
                .chain(layout.old_lanes)
                .collect(),
            feed: LaneFeed::default(),
            refs: InMemoryTxOrderingRefPublisher::default(),
            side: InMemorySidePublisher::default(),
            floors: channel.floor_tx,
            watermark: channel.watermark,
            taken: 0,
            shadow_offers: Vec::new(),
        }
    }

    /// Run the core until it has nothing more to do, and return its new
    /// offers.
    fn drive(&mut self) -> Vec<TxRef> {
        let in_shadow = !self.seq.shadow_vslots().is_empty();
        let mut ports = Ports {
            tx_data: &mut self.feed,
            refs: &mut self.refs,
            errors: &mut self.side,
        };
        while self.seq.run_once(&mut ports).unwrap() {}
        let all = self.refs.refs();
        let new: Vec<TxRef> = all[self.taken..].to_vec();
        self.taken = all.len();
        if in_shadow {
            self.shadow_offers.extend(new.iter().copied());
        }
        new
    }
}

/// The ingress, the sealer and the executors around the replicas.
struct Chain {
    signers: Vec<PrivateKeySigner>,
    next_nonce: Vec<u64>,
    map: ShardMap,
    replicas: Vec<Replica>,
    /// The ordered transactions, first seen first.
    canonical: Vec<TxRef>,
    /// The replica whose offer the sealer took first, per transaction.
    origin: HashMap<B256, String>,
    ordered: HashSet<B256>,
    sent: HashMap<B256, (Address, u64)>,
    submitted: usize,
    position: i32,
}

impl Chain {
    fn new(map: ShardMap) -> Self {
        let signers: Vec<PrivateKeySigner> = (1..=SENDERS).map(signer).collect();
        Self {
            next_nonce: vec![0; signers.len()],
            signers,
            map,
            replicas: Vec::new(),
            canonical: Vec::new(),
            origin: HashMap::new(),
            ordered: HashSet::new(),
            sent: HashMap::new(),
            submitted: 0,
            position: 0,
        }
    }

    /// One replica of `lane` with the vslots of `map`, reading `old_lanes`
    /// with `shadow` in warm-up.
    fn start(&mut self, name: &str, lane: u8, map: &ShardMap, old_lanes: &[u8], shadow: VslotSet) {
        let replica = Replica::start(Layout {
            name: name.to_string(),
            lane,
            vslots: map.vslot_set(lane),
            old_lanes: old_lanes.to_vec(),
            shadow,
        });
        self.catch_up(&replica);
        self.replicas.push(replica);
    }

    /// A new replica learns the committed floors of every sender, as the
    /// executor nonce lookup answers a restarted replica.
    fn catch_up(&self, replica: &Replica) {
        let mut floors: BTreeMap<Address, u64> = BTreeMap::new();
        self.canonical.iter().for_each(|r| {
            let (sender, nonce) = self.sent[&r.tx_hash];
            floors.insert(sender, nonce);
        });
        for (sender, nonce) in floors {
            replica
                .floors
                .send(FloorUpdate::executed(sender, nonce).into())
                .unwrap();
        }
        replica
            .watermark
            .store(u64::try_from(self.canonical.len()).unwrap());
    }

    /// Stop every replica whose name starts with `prefix`.
    fn stop(&mut self, prefix: &str) {
        self.replicas.retain(|r| !r.name.starts_with(prefix));
    }

    /// Every sender submits `PER_PHASE` transactions through the ingress,
    /// routed by the ingress map.
    fn submit(&mut self) {
        (0..PER_PHASE).for_each(|_| (0..self.signers.len()).for_each(|i| self.submit_one(i)));
    }

    fn submit_one(&mut self, i: usize) {
        let nonce = self.next_nonce[i];
        self.next_nonce[i] += 1;
        let envelope = envelope_with(
            &self.signers[i],
            nonce,
            u64::try_from(self.submitted).unwrap(),
            EnvelopeSpec {
                real_hash: true,
                ..Default::default()
            },
        );
        self.submitted += 1;
        self.position += 64;
        self.sent.insert(envelope.tx_hash, (envelope.sender, nonce));
        let lane = self.map.lane_for(envelope.sender);
        let position = BPosition {
            term_id: 0,
            term_offset: self.position,
        };
        self.replicas
            .iter_mut()
            .filter(|r| r.lanes.contains(&lane))
            .for_each(|r| {
                r.feed.0.push_back(Inbound {
                    lane,
                    loc: TxDataLoc::new(1, position),
                    stream_position: position.as_index(),
                    envelope: envelope.clone(),
                });
            });
    }

    /// One round: every replica runs, the sealer orders the new offers
    /// first seen first, and every ordered transaction goes back to every
    /// replica as a receipt floor.
    fn round(&mut self) {
        let offers: Vec<(String, TxRef)> = self
            .replicas
            .iter_mut()
            .flat_map(|r| {
                let name = r.name.clone();
                r.drive()
                    .into_iter()
                    .map(move |offer| (name.clone(), offer))
            })
            .collect();
        let new: Vec<TxRef> = offers
            .into_iter()
            .filter(|(_, r)| self.ordered.insert(r.tx_hash))
            .map(|(name, r)| {
                self.origin.insert(r.tx_hash, name);
                r
            })
            .collect();
        for r in &new {
            self.receipt(r);
        }
        self.canonical.extend(new);
        let count = u64::try_from(self.canonical.len()).unwrap();
        self.replicas.iter().for_each(|r| r.watermark.store(count));
    }

    fn receipt(&self, r: &TxRef) {
        let (sender, nonce) = self.sent[&r.tx_hash];
        let block = u64::try_from(self.canonical.len()).unwrap() + 1;
        self.replicas.iter().for_each(|replica| {
            let update =
                FloorUpdate::executed(sender, nonce).with_evidence(Evidence::Receipt { block });
            replica.floors.send(update.into()).unwrap();
        });
    }

    /// Run rounds until every submitted transaction is ordered.
    fn settle(&mut self) {
        let started = Instant::now();
        while self.canonical.len() < self.submitted {
            self.bounded_round(started);
        }
    }

    /// One round of [`Self::settle`], within its bound.
    fn bounded_round(&mut self, started: Instant) {
        assert!(
            started.elapsed() < SETTLE,
            "{} of {} transactions ordered",
            self.canonical.len(),
            self.submitted
        );
        self.paced_round();
    }

    /// One round, then a short pause, as the loops of a live lane turn.
    fn paced_round(&mut self) {
        self.round();
        std::thread::sleep(Duration::from_millis(5));
    }

    /// Run rounds for `wait`: the warm-up or the drain of a resize step.
    fn hold(&mut self, wait: Duration) {
        let started = Instant::now();
        while started.elapsed() < wait {
            self.paced_round();
        }
    }

    /// Every sender's nonces reach the canonical log exactly once, in
    /// order, with no gap.
    fn assert_exact(&self) {
        let mut by_sender: BTreeMap<Address, Vec<u64>> = BTreeMap::new();
        self.canonical.iter().for_each(|r| {
            let (sender, nonce) = self.sent[&r.tx_hash];
            by_sender.entry(sender).or_default().push(nonce);
        });
        assert_eq!(by_sender.len(), self.signers.len(), "every sender ordered");
        for (sender, nonces) in &by_sender {
            self.assert_sender(*sender, nonces);
        }
    }

    /// `sender` ordered `0..n` for its `n` submitted transactions.
    fn assert_sender(&self, sender: Address, nonces: &[u64]) {
        let index = self
            .signers
            .iter()
            .position(|s| s.address() == sender)
            .unwrap();
        let total = self.next_nonce[index];
        assert_eq!(nonces, (0..total).collect::<Vec<_>>(), "sender {sender}");
    }

    /// The replicas that started in shadow mode offered nothing while
    /// in it: the old lanes publish those slots until the warm-up passes.
    fn assert_shadow_silent(&self) {
        self.replicas.iter().for_each(|r| {
            assert_eq!(
                r.shadow_offers,
                [] as [TxRef; 0],
                "{} offered in shadow mode",
                r.name
            );
        });
    }

    /// How many of the transactions ordered since `from` were first
    /// offered by a replica named `prefix`.
    fn ordered_by(&self, prefix: &str, from: usize) -> usize {
        self.canonical[from..]
            .iter()
            .filter(|r| self.origin[&r.tx_hash].starts_with(prefix))
            .count()
    }

    /// The senders whose lane differs between `from` and `to`.
    fn moved(&self, from: &ShardMap, to: &ShardMap) -> usize {
        self.signers
            .iter()
            .filter(|s| from.lane_for(s.address()) != to.lane_for(s.address()))
            .count()
    }
}

/// The incoming slots of `lane` from `from` to `to`.
fn incoming(from: &ShardMap, to: &ShardMap, lane: u8) -> VslotSet {
    to.vslot_set(lane).difference(&from.vslot_set(lane))
}

#[test]
fn a_scale_out_and_in_with_a_killed_new_replica_orders_every_nonce_once() {
    let two = ShardMap::identity(2).unwrap();
    let three = two.rebalance(3).unwrap();
    let back = three.rebalance(2).unwrap();
    let mut chain = Chain::new(two.clone());
    assert!(
        chain.moved(&two, &three) >= 2 && chain.moved(&three, &back) >= 2,
        "a load whose senders move"
    );
    chain.start("lane0", 0, &two, &[], VslotSet::EMPTY);
    chain.start("lane1", 1, &two, &[], VslotSet::EMPTY);
    chain.submit();
    chain.settle();

    // Scale out, overlap: lane 2 warms its incoming slots in shadow mode,
    // reading the old lanes; the ingress still routes by the old map.
    let gaining = incoming(&two, &three, 2);
    chain.start("lane2-a", 2, &three, &[0, 1], gaining);
    chain.start("lane2-b", 2, &three, &[0, 1], gaining);
    chain.submit();
    chain.settle();
    // One replica of the new lane dies during the overlap and returns.
    chain.stop("lane2-b");
    chain.start("lane2-b'", 2, &three, &[0, 1], gaining);
    chain.hold(Duration::from_millis(WARM_MS + 100));
    chain.assert_shadow_silent();
    // The ingress rolls onto the new map; the old lanes drain one lifetime.
    chain.map = three.clone();
    let rolled = chain.canonical.len();
    chain.submit();
    chain.settle();
    assert!(
        chain.ordered_by("lane2", rolled) > 0,
        "the new lane orders the moved senders"
    );
    chain.hold(Duration::from_millis(TX_TTL_MS));
    // Steady: every lane restarts with its final slots.
    chain.stop("lane");
    chain.start("lane0", 0, &three, &[], VslotSet::EMPTY);
    chain.start("lane1", 1, &three, &[], VslotSet::EMPTY);
    chain.start("lane2", 2, &three, &[], VslotSet::EMPTY);
    chain.submit();
    chain.settle();

    // Scale in: lanes 0 and 1 warm lane 2's slots, then the map rolls.
    chain.stop("lane0");
    chain.stop("lane1");
    chain.start("lane0", 0, &back, &[2], incoming(&three, &back, 0));
    chain.start("lane1", 1, &back, &[2], incoming(&three, &back, 1));
    chain.hold(Duration::from_millis(WARM_MS + 100));
    chain.assert_shadow_silent();
    chain.map = back.clone();
    let rolled_back = chain.canonical.len();
    chain.submit();
    chain.settle();
    assert_eq!(
        chain.ordered_by("lane2", rolled_back),
        0,
        "the old lane 2 orders nothing after the roll"
    );
    chain.hold(Duration::from_millis(TX_TTL_MS));
    chain.stop("lane");
    chain.start("lane0", 0, &back, &[], VslotSet::EMPTY);
    chain.start("lane1", 1, &back, &[], VslotSet::EMPTY);
    chain.submit();
    chain.settle();

    chain.assert_exact();
}
