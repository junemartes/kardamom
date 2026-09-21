//! The reader's side of the void rule: the vote, the read-ahead, the vacant
//! slots, and the three cases of a void record.
//!
//! No unit test can make an archive refuse a range, so these tests enter at
//! [`TxOrderingReader::on_unjoinable`], the point the join reaches when every
//! archive refused.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy_primitives::{Address, B256};
use alloy_signer_local::PrivateKeySigner;
use crossbeam_channel::{Receiver, unbounded};
use kardamom_cluster_adapter::OfferOutcome;
use kardamom_types::{BPosition, BlockBoundaryStart, TxOrderingMessage, TxRef, VoidRecord};

use super::join::TxDataKey;
use super::tests::pos;
use super::threads::Flow;
use super::void::{MAX_READ_AHEAD, ParkOutcome, ReadAhead, VoidPark};
use super::*;
use crate::error::ExecutorError;
use crate::exec_types::TxIndex;

type Votes = Arc<Mutex<Vec<(u8, VoidRecord)>>>;

/// An ordering subscription over a fixed queue. It records every vote.
struct VotingSub {
    queue: VecDeque<(BPosition, TxOrderingMessage)>,
    votes: Votes,
    outcome: OfferOutcome,
}

impl VotingSub {
    fn new(queue: Vec<TxOrderingMessage>) -> Self {
        Self {
            queue: queue.into_iter().map(|msg| (pos(0), msg)).collect(),
            votes: Votes::default(),
            outcome: OfferOutcome::Accepted,
        }
    }
}

impl TxOrderingSubscription for VotingSub {
    fn next(&mut self) -> Result<(BPosition, TxOrderingMessage), ExecutorError> {
        self.queue
            .pop_front()
            .ok_or(ExecutorError::TxOrderingClosed)
    }

    fn vote(&mut self, voter_id: u8, void: &VoidRecord) -> OfferOutcome {
        self.votes.lock().unwrap().push((voter_id, *void));
        self.outcome
    }
}

const LOST: B256 = B256::repeat_byte(0xA1);
const NEXT: B256 = B256::repeat_byte(0xA2);

fn tx_ref(hash: B256, data_offset: i32) -> TxRef {
    TxRef::new(hash, 0, pos(data_offset), 0)
}

fn boundary(block_number: u64) -> TxOrderingMessage {
    TxOrderingMessage::BoundaryStart(BlockBoundaryStart {
        block_number,
        end_tx_idx: pos(0),
        l2_timestamp: 1_700_000_000,
        l1_origin: 0,
    })
}

fn void(index: u64, tx_hash: B256) -> TxOrderingMessage {
    TxOrderingMessage::Void(VoidRecord { index, tx_hash })
}

fn voter_cfg() -> ReaderConfig {
    ReaderConfig {
        voter_id: Some(3),
        ..ReaderConfig::default()
    }
}

type TestReader = TxOrderingReader<VotingSub, crossbeam_channel::Sender<ReaderToExec>>;

/// A reader over `sub` that starts at `start`, with its exec sink's far end.
fn reader(
    sub: VotingSub,
    buffer: JoinBuffer,
    cfg: ReaderConfig,
    start: u64,
) -> (TestReader, Receiver<ReaderToExec>) {
    let (exec_out, rx) = unbounded();
    let reader = TxOrderingReader::new(TxOrderingInputs {
        sub,
        buffer,
        cfg,
        exec_out,
        start_tx_idx: TxIndex(start),
        recovery_factory: None,
    });
    (reader, rx)
}

/// The slot kinds the executor received, in order: `V` for a vacant slot,
/// `T` for a transaction, `B` for a boundary, each with its record index.
fn slots(rx: &Receiver<ReaderToExec>) -> Vec<(char, u64)> {
    rx.try_iter()
        .map(|msg| match msg {
            ReaderToExec::Vacant { tx_idx, .. } => ('V', tx_idx.0),
            ReaderToExec::Tx { tx_idx, .. } => ('T', tx_idx.0),
            ReaderToExec::Boundary(b) => ('B', b.block_number),
            other => panic!("unexpected message {other:?}"),
        })
        .collect()
}

#[test]
fn a_voter_drops_the_entry_when_the_void_record_arrives() {
    let signer = PrivateKeySigner::random();
    let envelope =
        |nonce| crate::actor::test_support::legacy(&signer, Address::from([0x22u8; 20]), nonce, 1);
    let buffer = JoinBuffer::new();
    buffer.insert(TxDataKey::new(0, 0, pos(50)), envelope(1));
    buffer.insert(TxDataKey::new(0, 0, pos(90)), envelope(0));
    // Behind the lost entry at index 0: one entry, one boundary, the void
    // record, then the lost transaction again with data that exists.
    let sub = VotingSub::new(vec![
        TxOrderingMessage::TxRef(tx_ref(NEXT, 50)),
        boundary(1),
        void(0, LOST),
        TxOrderingMessage::TxRef(tx_ref(LOST, 90)),
    ]);
    let votes = sub.votes.clone();
    let (mut reader, rx) = reader(sub, buffer, voter_cfg(), 0);

    let flow = reader
        .on_unjoinable(&tx_ref(LOST, 10), pos(0))
        .expect("voided");
    assert!(matches!(flow, Flow::Continue));
    // Nothing behind the entry reached the executor during the wait.
    assert_eq!(slots(&rx), vec![('V', 0)]);
    assert_eq!(
        *votes.lock().unwrap(),
        vec![(
            3,
            VoidRecord {
                index: 0,
                tx_hash: LOST
            }
        )]
    );

    reader.run().expect("clean close");
    // The void record has a slot of its own, and the same hash passes again.
    assert_eq!(slots(&rx), vec![('T', 1), ('B', 1), ('V', 2), ('T', 3)]);
}

#[test]
fn a_consumer_that_is_no_voter_stops_as_before() {
    let sub = VotingSub::new(vec![void(0, LOST)]);
    let votes = sub.votes.clone();
    let (mut reader, _rx) = reader(sub, JoinBuffer::new(), ReaderConfig::default(), 0);
    let err = reader.on_unjoinable(&tx_ref(LOST, 10), pos(0)).err();
    assert!(matches!(err, Some(ExecutorError::JoinTimeout { .. })));
    assert!(votes.lock().unwrap().is_empty());
}

#[test]
fn a_wait_that_ends_with_no_void_record_stops_the_reader() {
    let cfg = ReaderConfig {
        void_wait: Duration::ZERO,
        ..voter_cfg()
    };
    let sub = VotingSub::new(vec![void(0, LOST)]);
    let votes = sub.votes.clone();
    let (mut reader, rx) = reader(sub, JoinBuffer::new(), cfg, 0);
    let err = reader.on_unjoinable(&tx_ref(LOST, 10), pos(0)).err();
    assert!(matches!(err, Some(ExecutorError::JoinTimeout { .. })));
    // The vote went out before the wait ended: the sealer keeps it.
    assert_eq!(votes.lock().unwrap().len(), 1);
    assert!(slots(&rx).is_empty());
}

#[test]
fn a_void_record_for_another_entry_does_not_end_the_wait() {
    let sub = VotingSub::new(vec![void(0, NEXT), void(7, LOST)]);
    let (mut reader, rx) = reader(sub, JoinBuffer::new(), voter_cfg(), 0);
    // The order closes before the right record comes: a clean stop.
    let flow = reader
        .on_unjoinable(&tx_ref(LOST, 10), pos(0))
        .expect("closed");
    assert!(matches!(flow, Flow::Stop));
    assert!(slots(&rx).is_empty());
}

#[test]
fn a_void_record_for_an_entry_the_reader_sent_is_fatal() {
    let sub = VotingSub::new(vec![void(4, LOST)]);
    let (reader, _rx) = reader(sub, JoinBuffer::new(), voter_cfg(), 2);
    let err = reader.run().err();
    assert!(matches!(
        err,
        Some(ExecutorError::VoidOfExecutedEntry { index: 4, .. })
    ));
}

#[test]
fn a_void_record_below_the_start_index_counts_its_own_slot_only() {
    let sub = VotingSub::new(vec![void(4, LOST), boundary(9)]);
    let (reader, rx) = reader(sub, JoinBuffer::new(), voter_cfg(), 6);
    reader.run().expect("clean close");
    assert_eq!(slots(&rx), vec![('V', 6), ('B', 9)]);
}

fn park<'a>(
    sub: &'a mut VotingSub,
    backlog: &'a mut ReadAhead,
    index: u64,
) -> VoidPark<'a, VotingSub> {
    let record = VoidRecord {
        index,
        tx_hash: LOST,
    };
    VoidPark::new(sub, backlog, 3, record, Duration::from_secs(60))
}

#[test]
fn the_voter_sends_its_vote_again_after_the_interval() {
    let mut sub = VotingSub::new(vec![boundary(1), boundary(2), void(0, LOST)]);
    let mut backlog = ReadAhead::new();
    let mut wait = park(&mut sub, &mut backlog, 0);
    // An interval of zero has elapsed at every message.
    wait.revote_after = Duration::ZERO;
    let outcome = wait.run().expect("voided");
    assert!(matches!(outcome, ParkOutcome::Voided));
    // The first vote, then one before each of the three messages.
    assert_eq!(sub.votes.lock().unwrap().len(), 4);
    assert_eq!(backlog.len(), 3);
}

#[test]
fn the_voter_sends_one_vote_inside_the_interval() {
    let mut sub = VotingSub::new(vec![boundary(1), boundary(2), void(0, LOST)]);
    let mut backlog = ReadAhead::new();
    let outcome = park(&mut sub, &mut backlog, 0).run().expect("voided");
    assert!(matches!(outcome, ParkOutcome::Voided));
    assert_eq!(sub.votes.lock().unwrap().len(), 1);
}

#[test]
fn a_session_that_refuses_the_vote_does_not_stop_the_wait() {
    let mut sub = VotingSub::new(vec![void(0, LOST)]);
    sub.outcome = OfferOutcome::NotConnected;
    let mut backlog = ReadAhead::new();
    let outcome = park(&mut sub, &mut backlog, 0).run().expect("voided");
    assert!(matches!(outcome, ParkOutcome::Voided));
}

#[test]
fn the_wait_reads_the_backlog_first_and_keeps_the_canonical_order() {
    // An earlier wait read these ahead. The void record for index 5 is
    // among them, so this wait ends inside the backlog.
    let mut backlog: ReadAhead = [boundary(1), void(5, LOST), boundary(2)]
        .into_iter()
        .map(|msg| (pos(0), msg))
        .collect();
    let mut sub = VotingSub::new(vec![boundary(3)]);
    let outcome = park(&mut sub, &mut backlog, 5).run().expect("voided");
    assert!(matches!(outcome, ParkOutcome::Voided));
    let blocks: Vec<Option<u64>> = backlog
        .iter()
        .map(|(_, msg)| msg.as_boundary().map(|b| b.block_number))
        .collect();
    assert_eq!(blocks, vec![Some(1), None, Some(2)]);
    assert_eq!(sub.queue.len(), 1);
}

#[test]
fn the_read_ahead_has_a_bound() {
    let queue: Vec<TxOrderingMessage> = (0..=MAX_READ_AHEAD as u64).map(boundary).collect();
    let mut sub = VotingSub::new(queue);
    let mut backlog = ReadAhead::new();
    let outcome = park(&mut sub, &mut backlog, 0).run().expect("bound");
    assert!(matches!(outcome, ParkOutcome::GaveUp));
    assert_eq!(backlog.len(), MAX_READ_AHEAD);
}
