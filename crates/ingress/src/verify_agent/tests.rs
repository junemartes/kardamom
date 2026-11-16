//! The verify agent with no thread: each test drives `poll` over a fake
//! lane, as the sequencer tests drive their loops. The cases are those of
//! section 8 of the spec that the agent owns. The duplicate cache and the
//! full ring are cases of the handler.

use std::cell::Cell;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use alloy_primitives::U256;
use alloy_signer_local::PrivateKeySigner;
use kardamom_cache::{LiveAccounts, LiveAccountsConfig};
use kardamom_log::parked::{OFFER_DEADLINE, Offered};
use kardamom_types::{AccountRow, BPosition, TxEnvelope};
use tokio::sync::oneshot;

use super::agent::{VerifyAgent, VerifyItem};
use super::lane::{AgentLanes, Answer, LaneOffer};
use crate::config::IngressConfig;
use crate::error::IngressError;
use crate::test_support::{SignedTx, sign_legacy_tx};

/// A lane that refuses its first `refusals` offers, then takes each offer
/// and keeps the envelope.
#[derive(Clone, Default)]
struct FakeLane {
    refusals: Arc<Mutex<usize>>,
    taken: Arc<Mutex<Vec<TxEnvelope>>>,
}

impl FakeLane {
    fn refusing(refusals: usize) -> Self {
        let lane = Self::default();
        *lane.refusals.lock().unwrap() = refusals;
        lane
    }

    /// The nonces of the taken envelopes, in the order of the offers.
    fn nonces(&self) -> Vec<u64> {
        self.taken
            .lock()
            .unwrap()
            .iter()
            .map(|env| {
                let tx = <alloy_consensus::TxEnvelope as alloy_rlp::Decodable>::decode(
                    &mut env.raw_tx.as_ref(),
                )
                .unwrap();
                alloy_consensus::transaction::Transaction::nonce(&tx)
            })
            .collect()
    }
}

impl LaneOffer for FakeLane {
    fn offer(&self, bytes: &[u8]) -> Offered {
        let mut refusals = self.refusals.lock().unwrap();
        if *refusals > 0 {
            *refusals -= 1;
            return Offered::Refused(-2 /* BACK_PRESSURED */);
        }
        let env = kardamom_log::codec::materialize::<TxEnvelope>(bytes).unwrap();
        self.taken.lock().unwrap().push(env);
        Offered::Delivered(Ok(BPosition::default()))
    }
}

/// The item of the submit `signed`, pushed at `at`, and the receiver of its
/// answer.
fn item(signed: SignedTx, at: Instant) -> (VerifyItem, oneshot::Receiver<Answer>) {
    let (answer, rx) = oneshot::channel();
    let item = VerifyItem {
        raw_tx: signed.raw.into(),
        envelope: signed.env,
        arrived: at,
        correlation_id: 0,
        max_inclusion_block: u64::MAX,
        answer,
        queued: at,
    };
    (item, rx)
}

/// An agent with one lane for every sender, and the writer of its local
/// account layer.
struct Bench {
    agent: VerifyAgent<FakeLane>,
    lane: FakeLane,
    writer: kardamom_cache::LiveAccountsWriter,
    signer: PrivateKeySigner,
    clock: Cell<Instant>,
}

impl Bench {
    fn new(lane: FakeLane) -> Self {
        let cfg = IngressConfig {
            partition_count_m: std::num::NonZeroU32::MIN,
            ..IngressConfig::default()
        };
        let (live, writer) = LiveAccounts::new(&LiveAccountsConfig::default());
        let lanes = AgentLanes {
            lanes: vec![lane.clone()],
            stamps: None,
            idle_cap: Duration::from_micros(50),
        };
        Self {
            agent: VerifyAgent::new(lanes, cfg, live, 1024),
            lane,
            writer,
            signer: PrivateKeySigner::random(),
            clock: Cell::new(Instant::now()),
        }
    }

    /// Push the transaction of the signer at `nonce`, and return the
    /// receiver of its answer.
    fn push(&self, nonce: u64) -> oneshot::Receiver<Answer> {
        self.push_signed(sign_legacy_tx(&self.signer, nonce))
    }

    fn push_signed(&self, signed: SignedTx) -> oneshot::Receiver<Answer> {
        let (item, rx) = item(signed, self.clock.get());
        assert!(self.agent.input().submit(item).is_ok(), "the ring has room");
        rx
    }

    /// One pass, `after` past the last one.
    fn poll(&mut self, after: Duration) -> usize {
        self.clock.set(self.clock.get() + after);
        self.agent.poll(self.clock.get())
    }

    /// The local account layer knows the signer at `nonce`.
    fn known_at(&mut self, nonce: u64) {
        let row = AccountRow {
            address: self.signer.address(),
            nonce,
            balance: U256::MAX,
        };
        self.writer.apply(BPosition::from_index(1), &[row]);
    }
}

/// The answer that `rx` holds now.
fn answered(rx: &mut oneshot::Receiver<Answer>) -> Answer {
    rx.try_recv().expect("the submit has its answer")
}

#[test]
fn a_valid_submit_is_offered_on_its_lane_and_answers_its_hash_and_sender() {
    let mut b = Bench::new(FakeLane::default());
    let signed = sign_legacy_tx(&b.signer, 0);
    let expected = alloy_primitives::keccak256(&signed.raw);
    let mut rx = b.push_signed(signed);
    assert_eq!(b.poll(Duration::ZERO), 1);
    let admitted = answered(&mut rx).expect("admitted");
    assert_eq!(admitted.tx_hash, expected);
    assert_eq!(admitted.sender, b.signer.address());
    let taken = b.lane.taken.lock().unwrap();
    assert_eq!(taken.len(), 1);
    assert_eq!(taken[0].tx_hash, expected);
    assert_eq!(taken[0].sender, b.signer.address());
}

/// The lane refuses the first three offers. The frames wait in order, and
/// each later frame waits behind them, so the lane takes nonces 0, 1, 2 in
/// the order of arrival.
#[test]
fn a_refused_lane_keeps_the_order_of_arrival() {
    let mut b = Bench::new(FakeLane::refusing(3));
    let mut first = b.push(0);
    b.poll(Duration::ZERO);
    let mut second = b.push(1);
    let mut third = b.push(2);
    b.poll(Duration::from_micros(10));
    assert!(first.try_recv().is_err(), "the first frame still waits");
    (0..4).for_each(|_| {
        b.poll(Duration::from_micros(10));
    });
    assert_eq!(b.lane.nonces(), [0, 1, 2]);
    [&mut first, &mut second, &mut third]
        .into_iter()
        .for_each(|rx| assert!(answered(rx).is_ok()));
}

#[test]
fn a_lane_that_refuses_until_the_deadline_answers_txpool_is_full() {
    let mut b = Bench::new(FakeLane::refusing(usize::MAX));
    let mut rx = b.push(0);
    b.poll(Duration::ZERO);
    assert!(rx.try_recv().is_err(), "the frame waits for its lane");
    b.poll(OFFER_DEADLINE);
    assert!(matches!(answered(&mut rx), Err(IngressError::PoolFull)));
    assert!(b.lane.nonces().is_empty());
}

#[test]
fn a_nonce_below_the_local_row_is_nonce_too_low() {
    let mut b = Bench::new(FakeLane::default());
    b.known_at(5);
    let mut rx = b.push(4);
    b.poll(Duration::ZERO);
    assert!(matches!(
        answered(&mut rx),
        Err(IngressError::NonceTooLow {
            tx_nonce: 4,
            state_nonce: 5,
            ..
        })
    ));
    let mut rx = b.push(5);
    b.poll(Duration::ZERO);
    assert!(answered(&mut rx).is_ok(), "the known nonce itself admits");
}

/// A sender with no local row admits: the agent never waits for Redis.
#[test]
fn a_sender_with_no_local_row_admits() {
    let mut b = Bench::new(FakeLane::default());
    let mut rx = b.push(7);
    b.poll(Duration::ZERO);
    assert!(answered(&mut rx).is_ok());
    assert_eq!(b.lane.nonces(), [7]);
}

#[test]
fn a_bad_signature_answers_signature_invalid_and_offers_nothing() {
    let mut b = Bench::new(FakeLane::default());
    let mut signed = sign_legacy_tx(&b.signer, 0);
    let alloy_consensus::TxEnvelope::Legacy(tx) = signed.env else {
        panic!("a legacy transaction");
    };
    let (body, signature, _) = tx.into_parts();
    // r = 0 is no point on the curve, so the recovery fails.
    let broken = alloy_primitives::Signature::new(U256::ZERO, signature.s(), signature.v());
    signed.env =
        alloy_consensus::TxEnvelope::Legacy(alloy_consensus::Signed::new_unhashed(body, broken));
    let mut rx = b.push_signed(signed);
    b.poll(Duration::ZERO);
    assert!(matches!(
        answered(&mut rx),
        Err(IngressError::SignatureInvalid)
    ));
    assert!(b.lane.nonces().is_empty());
}

/// One pass takes at most one batch from the ring, so a burst does not
/// hold the parked frames back for longer than one batch.
#[test]
fn one_pass_takes_at_most_one_batch() {
    let mut b = Bench::new(FakeLane::default());
    let answers: Vec<_> = (0..100).map(|nonce| b.push(nonce)).collect();
    assert_eq!(b.poll(Duration::ZERO), 64);
    assert_eq!(b.poll(Duration::ZERO), 36);
    assert_eq!(b.lane.nonces(), (0..100).collect::<Vec<_>>());
    assert_eq!(answers.len(), 100);
}

/// A full ring refuses the item at once, so the handler answers with no
/// wait.
#[test]
fn a_full_ring_gives_the_item_back() {
    let (live, _writer) = LiveAccounts::new(&LiveAccountsConfig::default());
    let lanes = AgentLanes {
        lanes: vec![FakeLane::default()],
        stamps: None,
        idle_cap: Duration::from_micros(50),
    };
    let agent = VerifyAgent::new(lanes, IngressConfig::default(), live, 1);
    let signer = PrivateKeySigner::random();
    let push = |nonce| {
        agent
            .input()
            .submit(item(sign_legacy_tx(&signer, nonce), Instant::now()).0)
    };
    assert!(push(0).is_ok());
    assert!(push(1).is_err(), "the ring holds one");
}

/// The drop of the owner stops the agent thread. Each submit that the
/// agent holds gets its answer first, then the drop joins the thread.
#[test]
fn the_drop_answers_each_held_submit_then_joins() {
    let lane = FakeLane::default();
    let b = Bench::new(lane.clone());
    let handle = super::agent::VerifyAgentHandle::spawn(b.agent).expect("the thread starts");
    let signer = b.signer.clone();
    let answers: Vec<_> = (0..50)
        .map(|nonce| {
            let (item, rx) = item(sign_legacy_tx(&signer, nonce), Instant::now());
            assert!(handle.submit(item).is_ok());
            rx
        })
        .collect();
    drop(handle);
    for mut rx in answers {
        assert!(answered(&mut rx).is_ok());
    }
    assert_eq!(lane.nonces(), (0..50).collect::<Vec<_>>());
}
