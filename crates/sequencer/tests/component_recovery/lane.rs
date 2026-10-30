//! Racing replicas of one lane: production admission loops share one
//! `tx_data` stream and offer to one controlled sealer.
//!
//! The sealer does the cluster's first-seen dedup by canonical id and its
//! per-sender contiguity guard. It relays every ordered reference to each
//! replica session as a canonical frame, and the controlled executor sends
//! the rows of each ordered transaction as one block. A replica has no
//! durable state: a restart starts empty and learns its senders from the
//! rows and from lookups.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use alloy_primitives::{Address, B256, U256};
use kardamom_cluster_adapter::wire::{self, GuardHeader, RelayedRef, TxRefOffer};
use kardamom_sequencer::admission::AccountState;
use kardamom_sequencer::inbound::Inbound;
use kardamom_sequencer::testkit::{EnvelopeSpec, envelope_with, pos, signer};
use kardamom_sequencer::v2::AccountEvent;
use kardamom_types::{TxDataLoc, TxEnvelope, TxRef};

pub(super) use super::replica::{BOUND, Lookups, Replica};

/// The `tx_data` session of the one ingress publisher.
const SESSION: i32 = 7;

/// One ordered transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Ordered {
    pub(super) sender: Address,
    pub(super) nonce: u64,
    pub(super) hash: B256,
    pub(super) replica: usize,
}

/// The controlled sealer and executor of the lane.
#[derive(Default)]
struct Sealer {
    order: Vec<Ordered>,
    seen: HashMap<B256, usize>,
    /// The next nonce of each sender: the contiguity guard and the
    /// executor's committed state.
    next: HashMap<Address, u64>,
    /// The exact frame that each transaction must arrive as.
    frames: HashMap<B256, Vec<u8>>,
}

/// The fault plan of one lane test: replicas, client load, sealer.
pub(super) struct Lane {
    id: u8,
    pub(super) replicas: Vec<Option<Replica>>,
    sealer: Sealer,
    position: u64,
    correlation: u64,
    /// The next nonce that the client signs, per signer seed.
    client: BTreeMap<u64, u64>,
    /// Replicas whose receipt stream is blocked.
    blind: Vec<bool>,
}

impl Lane {
    pub(super) fn start(lane: u8, replicas: usize) -> Self {
        Self {
            id: lane,
            replicas: (0..replicas)
                .map(|_| Some(Replica::start(lane, Lookups::Answered)))
                .collect(),
            sealer: Sealer::default(),
            position: 0,
            correlation: 0,
            client: BTreeMap::new(),
            blind: vec![false; replicas],
        }
    }

    pub(super) fn replica(&self, i: usize) -> &Replica {
        self.replicas[i].as_ref().expect("replica runs")
    }

    pub(super) fn replica_mut(&mut self, i: usize) -> &mut Replica {
        self.replicas[i].as_mut().expect("replica runs")
    }

    pub(super) fn ordered(&self) -> &[Ordered] {
        &self.sealer.order
    }

    pub(super) fn address(seed: u64) -> Address {
        signer(seed).address()
    }

    /// Start replica `i` from an empty memory, as a new process starts.
    pub(super) fn restart(&mut self, i: usize, mode: Lookups) {
        self.replicas[i] = Some(Replica::start(self.id, mode));
        self.blind[i] = false;
    }

    pub(super) fn stop(&mut self, i: usize) {
        self.replicas[i].take().unwrap().stop();
    }

    pub(super) fn kill(&mut self, i: usize) {
        self.replicas[i].take().unwrap().kill();
    }

    pub(super) fn set_blind(&mut self, i: usize, blind: bool) {
        self.blind[i] = blind;
    }

    /// The client submits the next nonce of `seed` through the ingress:
    /// every running replica's image receives the same envelope.
    pub(super) fn submit(&mut self, seed: u64) -> B256 {
        let nonce = *self.client.entry(seed).or_default();
        self.client.insert(seed, nonce + 1);
        self.correlation += 1;
        self.position += 64;
        let envelope = envelope_with(
            &signer(seed),
            nonce,
            self.correlation,
            EnvelopeSpec {
                real_hash: true,
                ..EnvelopeSpec::default()
            },
        );
        let position = pos(i32::try_from(self.position).unwrap());
        let loc = TxDataLoc::new(SESSION, position);
        self.sealer
            .frames
            .insert(envelope.tx_hash, self.frame(&envelope, nonce, loc));
        let hash = envelope.tx_hash;
        let lane = self.id;
        self.replicas
            .iter()
            .flatten()
            .filter(|r| !r.cut())
            .for_each(|r| {
                let inbound = Inbound {
                    lane,
                    loc,
                    stream_position: position.as_index(),
                    envelope: envelope.clone(),
                };
                r.input.send_timeout(inbound, BOUND).unwrap();
            });
        hash
    }

    fn frame(&self, envelope: &TxEnvelope, nonce: u64, loc: TxDataLoc) -> Vec<u8> {
        let bytes = kardamom_log::codec::encode(envelope).unwrap();
        TxRefOffer {
            guard: GuardHeader {
                sender: envelope.sender,
                nonce,
                max_inclusion_block: wire::POSITION_TAG | loc.position.as_index(),
                tip: 0,
            },
            reference: TxRef::new(envelope.tx_hash, self.id, loc.position, SESSION),
            envelope: &bytes,
        }
        .encode()
    }

    /// One pass of the sealer and the executor over the new offers of
    /// every session.
    pub(super) fn pump(&mut self) {
        let fresh: Vec<(usize, Vec<u8>)> = self
            .replicas
            .iter_mut()
            .enumerate()
            .filter_map(|(i, r)| r.as_mut().map(|r| (i, r)))
            .flat_map(|(i, r)| {
                let frames = r.peer.accepted();
                let new = frames[r.read..].to_vec();
                r.read = frames.len();
                new.into_iter().map(move |frame| (i, frame))
            })
            .collect();
        for (i, frame) in &fresh {
            self.on_frame(*i, frame);
        }
        self.answer_lookups();
    }

    /// The single-record frames of one session message: a batch frame
    /// carries `u16` entries, each a `u32` length and its record.
    fn entries(frame: &[u8]) -> Vec<&[u8]> {
        if frame.first() != Some(&wire::KIND_BATCH) {
            return vec![frame];
        }
        let count = u16::from_le_bytes([frame[1], frame[2]]);
        let mut at = 3;
        (0..count)
            .map(|_| {
                let len = u32::from_le_bytes(frame[at..at + 4].try_into().unwrap());
                let start = at + 4;
                at = start + usize::try_from(len).unwrap();
                &frame[start..at]
            })
            .collect()
    }

    fn on_frame(&mut self, replica: usize, frame: &[u8]) {
        for entry in Self::entries(frame) {
            self.on_offer(replica, entry);
        }
    }

    fn on_offer(&mut self, replica: usize, frame: &[u8]) {
        let offer = TxRefOffer::parse(frame).expect("an offer frame");
        let hash = offer.reference.tx_hash;
        assert_eq!(
            self.sealer.frames.get(&hash).map(Vec::as_slice),
            Some(frame),
            "replica {replica} offers bytes that differ from its envelope"
        );
        let (sender, nonce) = (offer.guard.sender, offer.guard.nonce);
        // The first-seen dedup absorbs the twin's copy.
        if self.sealer.seen.contains_key(&hash) {
            return;
        }
        let expected = self.sealer.next.get(&sender).copied().unwrap_or(0);
        assert_eq!(
            nonce, expected,
            "replica {replica} offers nonce {nonce} of {sender} before nonce {expected}"
        );
        self.order(Ordered {
            sender,
            nonce,
            hash,
            replica,
        });
    }

    fn order(&mut self, ordered: Ordered) {
        let index = u64::try_from(self.sealer.order.len()).unwrap();
        self.sealer
            .seen
            .insert(ordered.hash, self.sealer.order.len());
        self.sealer.order.push(ordered);
        self.sealer.next.insert(ordered.sender, ordered.nonce + 1);
        let canonical = RelayedRef {
            index,
            canonical_id: ordered.hash,
            record_type: wire::RT_TX,
        }
        .encode();
        let rows = AccountEvent::Rows {
            block: index + 1,
            rows: vec![(ordered.sender, self.state_of(ordered.sender))],
        };
        let blind = self.blind.clone();
        self.replicas
            .iter_mut()
            .zip(blind)
            .filter_map(|(r, blind)| r.as_mut().map(|r| (r, blind)))
            .filter(|(r, _)| !r.cut())
            .for_each(|(r, blind)| {
                assert!(r.rules.on_canonical_frame(&canonical));
                r.relayed += 1;
                if !blind {
                    r.accounts.send(rows.clone()).unwrap();
                }
            });
    }

    /// The executor's committed state of `sender`.
    pub(super) fn state_of(&self, sender: Address) -> AccountState {
        AccountState {
            nonce: self.sealer.next.get(&sender).copied().unwrap_or(0),
            balance: U256::MAX,
            delegated: false,
            tx_idx: u64::try_from(self.sealer.order.len()).unwrap(),
        }
    }

    fn answer_lookups(&mut self) {
        let requests: Vec<(usize, Address)> = self
            .replicas
            .iter_mut()
            .enumerate()
            .filter_map(|(i, r)| r.as_mut().map(|r| (i, r)))
            .filter(|(_, r)| matches!(r.mode, Lookups::Answered))
            .filter_map(|(i, r)| r.lookups.as_mut().map(|rx| (i, rx)))
            .flat_map(|(i, rx)| std::iter::from_fn(|| rx.try_recv().ok()).map(move |a| (i, a)))
            .collect();
        for (i, address) in requests {
            self.answer(i, address);
        }
    }

    /// The controlled executor answers one lookup with its committed state.
    fn answer(&self, replica: usize, address: Address) {
        let state = self.state_of(address);
        self.replica(replica)
            .accounts
            .send(AccountEvent::Answer { address, state })
            .unwrap();
    }

    /// Pump until `done` holds, within [`BOUND`].
    pub(super) fn until(&mut self, what: &str, mut done: impl FnMut(&Self) -> bool) {
        let end = Instant::now() + BOUND;
        let reached = std::iter::repeat_with(|| {
            self.pump();
            let ready = done(self);
            if !ready {
                std::thread::sleep(Duration::from_millis(1));
            }
            ready
        })
        .take_while(|_| Instant::now() < end)
        .any(|ready| ready);
        assert!(reached, "lane recovery bound exceeded: {what}");
    }

    /// Pump for `window` and require that nothing new is ordered.
    pub(super) fn quiet(&mut self, window: Duration) {
        let before = self.sealer.order.len();
        let end = Instant::now() + window;
        std::iter::repeat_with(|| self.pump())
            .take_while(|()| Instant::now() < end)
            .for_each(|()| std::thread::sleep(Duration::from_millis(1)));
        assert_eq!(self.sealer.order.len(), before, "a transaction was ordered");
    }

    /// Wait until every submitted transaction is ordered.
    pub(super) fn settle(&mut self) {
        let submitted: u64 = self.client.values().sum();
        let want = usize::try_from(submitted).unwrap();
        self.until("every submitted transaction ordered", |lane| {
            lane.sealer.order.len() == want
        });
    }

    /// The load verdict: each submitted nonce is ordered exactly once, in
    /// nonce order per sender, and nothing else is ordered.
    pub(super) fn assert_ledger(&self) {
        let mut got: BTreeMap<Address, Vec<u64>> = BTreeMap::new();
        self.sealer
            .order
            .iter()
            .for_each(|o| got.entry(o.sender).or_default().push(o.nonce));
        let want: BTreeMap<Address, Vec<u64>> = self
            .client
            .iter()
            .map(|(seed, next)| (Self::address(*seed), (0..*next).collect()))
            .collect();
        assert_eq!(got, want, "the canonical order is not the client's load");
    }

    /// Stop every replica gracefully, within the bound.
    pub(super) fn finish(mut self) {
        self.replicas.iter_mut().flatten().for_each(Replica::stop);
    }
}
